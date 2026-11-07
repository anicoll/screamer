package soak

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// RunnerConfig is one soak runner.
type RunnerConfig struct {
	// StreamDSN holds the events table and change stream, MetadataDSN Screamer's
	// partition metadata, and DeliveriesDSN the deliveries table. They may all
	// be the same database.
	StreamDSN, MetadataDSN, DeliveriesDSN string
	Tables                                Tables
	// RunnerID names the runner; a random one is used when it is empty.
	RunnerID string
	// Start is where a new stream starts reading: when the metadata table is
	// empty, the first runner starts here. Zero means now.
	Start time.Time
	// Options are passed to the subscriber.
	Options []screamer.Option
	// MaxConcurrentPartitions also sizes the change stream client's channel pool.
	MaxConcurrentPartitions int
	// StatsEvery is how often the runner logs its stats.
	StatsEvery time.Duration
}

// RunRunner subscribes to the soak stream and records every delivery until ctx
// ends. It returns nil when ctx ends, so a stopped runner is not an error.
func RunRunner(ctx context.Context, cfg RunnerConfig) error {
	if cfg.RunnerID == "" {
		cfg.RunnerID = uuid.NewString()
	}
	if cfg.StatsEvery <= 0 {
		cfg.StatsEvery = 30 * time.Second
	}

	streamClient, err := screamer.NewChangeStreamClient(ctx, cfg.StreamDSN, cfg.MaxConcurrentPartitions)
	if err != nil {
		return err
	}
	defer streamClient.Close()
	metaClient, err := spanner.NewClient(ctx, cfg.MetadataDSN)
	if err != nil {
		return err
	}
	defer metaClient.Close()
	deliveriesClient := metaClient
	if cfg.DeliveriesDSN != cfg.MetadataDSN {
		if deliveriesClient, err = spanner.NewClient(ctx, cfg.DeliveriesDSN); err != nil {
			return err
		}
		defer deliveriesClient.Close()
	}

	storage := partitionstorage.NewSpanner(metaClient, cfg.Tables.Metadata)
	if err := storage.RunMigrations(ctx); err != nil {
		return err
	}
	if err := storage.RegisterRunner(ctx, cfg.RunnerID); err != nil {
		return err
	}

	rec := NewRecorder(deliveriesClient, cfg.Tables.Deliveries, cfg.RunnerID)
	opts := slices.Clone(cfg.Options)
	if !cfg.Start.IsZero() {
		opts = append(opts, screamer.WithStartTimestamp(cfg.Start))
	}
	if cfg.MaxConcurrentPartitions > 0 {
		opts = append(opts, screamer.WithMaxConcurrentPartitions(cfg.MaxConcurrentPartitions))
	}
	sub := screamer.NewSubscriber(streamClient, cfg.Tables.Stream, cfg.RunnerID, storage, opts...)
	log.Info().Str("runner_id", cfg.RunnerID).Msg("soak runner starting")

	// The subscriber and the runner heartbeat stop with ctx. The recorder
	// keeps flushing until the subscriber has returned, so the deliveries it
	// acknowledged on the way out are written.
	subDone := make(chan struct{})
	eg, gctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(subDone)
		err := sub.SubscribeWithAck(gctx, rec)
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("subscriber stopped: %w", err)
	})
	eg.Go(func() error { return rec.flushLoop(context.WithoutCancel(gctx), subDone) })
	eg.Go(func() error {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				if err := storage.RefreshRunner(gctx, cfg.RunnerID); err != nil && gctx.Err() == nil {
					log.Warn().Err(err).Msg("failed to refresh runner")
				}
			}
		}
	})
	eg.Go(func() error {
		ticker := time.NewTicker(cfg.StatsEvery)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				logRunnerStats(cfg.RunnerID, sub, rec)
			}
		}
	})
	err = eg.Wait()
	logRunnerStats(cfg.RunnerID, sub, rec)
	return err
}

func logRunnerStats(runnerID string, sub *screamer.Subscriber, rec *Recorder) {
	s := rec.Stats()
	m := sub.GetMetrics()
	c := sub.GetClusterMetrics()
	byState := map[string]int{}
	for state, n := range c.PartitionsByState {
		byState[string(state)] = n
	}
	log.Info().Str("runner_id", runnerID).
		Int64("delivered", s.Delivered).
		Int64("parse_errors", s.ParseErrors).
		Int64("flush_failures", s.FlushFailures).
		Dur("lag_p50", s.LagP50).Dur("lag_p99", s.LagP99).Dur("lag_max", s.LagMax).
		Int("active_partitions", m.ActivePartitions).
		Interface("partition_metrics", m).
		Int("unfinished_partitions", c.UnfinishedPartitions).
		Interface("partitions_by_state", byState).
		Int("live_runners", c.LiveRunners).
		Int("desired_runners", c.DesiredRunners).
		Int("demand", c.Demand).
		Dur("watermark_lag", c.WatermarkLag).
		Msg("soak runner stats")
}

// delivery is one delivered write.
type delivery struct {
	id, seq   int64
	partition string
	commit    time.Time
	ack       screamer.AckFunc
}

// Recorder is a screamer.ConsumerWithAck that writes every delivered write to
// the deliveries table, and acknowledges it only once that write has
// committed. A runner killed mid-flush therefore leaves nothing acknowledged
// that was not recorded, and the checker never reports a write as missing
// that was in fact delivered.
// It also measures delivery lag. Order and gaps are left to Check, which sees
// every runner's deliveries.
type Recorder struct {
	client   *spanner.Client
	table    string
	runnerID string

	mu      sync.Mutex
	pending []delivery
	wake    chan struct{}   // signalled when pending gains deliveries
	lags    []time.Duration // since the last Stats call

	delivered     atomic.Int64
	parseErrors   atomic.Int64
	flushFailures atomic.Int64
	lagP50        atomic.Int64
	lagP99        atomic.Int64
	lagMax        atomic.Int64
}

// NewRecorder returns a recorder writing to table.
func NewRecorder(client *spanner.Client, table, runnerID string) *Recorder {
	return &Recorder{client: client, table: table, runnerID: runnerID, wake: make(chan struct{}, 1)}
}

type soakRecord struct {
	CommitTimestamp time.Time `json:"commit_timestamp"`
	PartitionToken  string    `json:"partition_token"`
	TableName       string    `json:"table_name"`
	ModType         string    `json:"mod_type"`
	Mods            []struct {
		Keys      map[string]any `json:"keys"`
		NewValues map[string]any `json:"new_values"`
	} `json:"mods"`
}

// Consume implements screamer.ConsumerWithAck.
func (r *Recorder) Consume(change []byte, ack screamer.AckFunc) error {
	var rec soakRecord
	if err := json.Unmarshal(change, &rec); err != nil {
		r.parseErrors.Add(1)
		ack(nil)
		return nil
	}
	now := time.Now()
	var ds []delivery
	for _, m := range rec.Mods {
		id, ok1 := asInt64(m.Keys["Id"])
		seq, ok2 := asInt64(m.NewValues["Seq"])
		if !ok1 || !ok2 {
			r.parseErrors.Add(1)
			continue
		}
		ds = append(ds, delivery{id: id, seq: seq, partition: rec.PartitionToken, commit: rec.CommitTimestamp})
	}
	if len(ds) == 0 {
		ack(nil)
		return nil
	}
	// One ack for the record, called when the last of its mods is written.
	// All of a record's mods are queued together, so they flush together.
	ds[len(ds)-1].ack = ack

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range ds {
		r.lags = append(r.lags, now.Sub(d.commit))
	}
	r.pending = append(r.pending, ds...)
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return nil
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	case float64:
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	}
	return 0, false
}

// flushLoop writes pending deliveries until done is closed, then once more.
// It writes as soon as there is something to write, and whatever arrives
// during a write goes in the next one, so a delivery waits for at most two
// commits and every partition's deliveries share them.
func (r *Recorder) flushLoop(ctx context.Context, done <-chan struct{}) error {
	for {
		select {
		case <-r.wake:
			r.flush(ctx)
		case <-done:
			r.flush(ctx)
			return nil
		}
	}
}

// flush writes every pending delivery and acknowledges the records. A write
// that fails nacks them, which makes the subscriber read the partition again
// from its last checkpoint.
func (r *Recorder) flush(ctx context.Context) {
	r.mu.Lock()
	ds := r.pending
	r.pending = nil
	r.mu.Unlock()
	if len(ds) == 0 {
		return
	}
	const chunk = 2000 // mutations per commit, well under Spanner's limit
	var err error
	for start := 0; start < len(ds) && err == nil; start += chunk {
		part := ds[start:min(start+chunk, len(ds))]
		ms := make([]*spanner.Mutation, 0, len(part))
		for _, d := range part {
			ms = append(ms, spanner.Insert(r.table,
				[]string{"Id", "Seq", "DeliveryId", "RunnerId", "PartitionToken", "CommitTimestamp", "DeliveredAt"},
				[]any{d.id, d.seq, uuid.NewString(), r.runnerID, d.partition, d.commit, spanner.CommitTimestamp}))
		}
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err = r.client.Apply(wctx, ms)
		cancel()
	}
	if err != nil {
		r.flushFailures.Add(1)
		log.Warn().Err(err).Int("deliveries", len(ds)).Msg("soak: failed to record deliveries; nacking them")
	} else {
		r.delivered.Add(int64(len(ds)))
	}
	for _, d := range ds {
		if d.ack != nil {
			d.ack(err)
		}
	}
}

// RecorderStats are a runner's figures.
type RecorderStats struct {
	Delivered, ParseErrors, FlushFailures int64
	// Lag is the time from commit to delivery of the writes delivered since
	// the previous call.
	LagP50, LagP99, LagMax time.Duration
}

// Stats returns the recorder's figures, and starts a new lag window.
func (r *Recorder) Stats() RecorderStats {
	r.mu.Lock()
	lags := r.lags
	r.lags = nil
	r.mu.Unlock()
	if len(lags) > 0 {
		slices.Sort(lags)
		r.lagP50.Store(int64(lags[len(lags)/2]))
		r.lagP99.Store(int64(lags[len(lags)*99/100]))
		r.lagMax.Store(int64(lags[len(lags)-1]))
	}
	return RecorderStats{
		Delivered:     r.delivered.Load(),
		ParseErrors:   r.parseErrors.Load(),
		FlushFailures: r.flushFailures.Load(),
		LagP50:        time.Duration(r.lagP50.Load()),
		LagP99:        time.Duration(r.lagP99.Load()),
		LagMax:        time.Duration(r.lagMax.Load()),
	}
}
