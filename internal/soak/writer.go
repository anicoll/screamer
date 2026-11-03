package soak

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// WriterConfig shapes the write load.
//
// Keys are split into hot keys, a contiguous key range that is written all the
// time at HotRate, and cold keys, which are written only during bursts at
// BurstRate. Between bursts the partitions that hold only cold keys see no
// writes, so they park; in a burst they all wake up at once, and the load can
// make Spanner split them.
type WriterConfig struct {
	// Keys is how many rows the writer updates.
	Keys int
	// HotFraction is the share of keys that are written all the time.
	HotFraction float64
	// HotRate is how many writes a second go to the hot keys.
	HotRate float64
	// BurstEvery is how often a burst starts, BurstFor how long it lasts and
	// BurstRate how many writes a second go to the cold keys during it.
	BurstEvery time.Duration
	BurstFor   time.Duration
	BurstRate  float64
	// PayloadBytes pads each row, so the table grows and Spanner splits it on
	// size as well as load.
	PayloadBytes int
	// Workers is how many writes run at once. Each key belongs to one worker,
	// so each key's writes commit one after another.
	Workers int
	// Batch is the most rows a worker writes in one transaction.
	Batch int
	// Shard and Shards split the keys between several writer processes, when
	// one cannot write fast enough. Each process owns the keys whose number
	// modulo Shards is Shard.
	Shard, Shards int
	// Duration stops the writer after this long; 0 runs until ctx ends.
	Duration time.Duration
}

// DefaultWriterConfig is a moderate load for a small instance.
var DefaultWriterConfig = WriterConfig{
	Keys:         20000,
	HotFraction:  0.05,
	HotRate:      200,
	BurstEvery:   10 * time.Minute,
	BurstFor:     2 * time.Minute,
	BurstRate:    1000,
	PayloadBytes: 512,
	Workers:      16,
	Batch:        50,
	Shards:       1,
}

func (c WriterConfig) validate() error {
	switch {
	case c.Keys <= 0:
		return fmt.Errorf("keys must be positive")
	case c.HotFraction < 0 || c.HotFraction > 1:
		return fmt.Errorf("hot fraction must be between 0 and 1")
	case c.Workers <= 0 || c.Batch <= 0:
		return fmt.Errorf("workers and batch must be positive")
	case c.Shards <= 0 || c.Shard < 0 || c.Shard >= c.Shards:
		return fmt.Errorf("shard must be in [0, shards)")
	case c.BurstRate > 0 && (c.BurstEvery <= 0 || c.BurstFor <= 0):
		return fmt.Errorf("burst-every and burst-for must be positive when there are bursts")
	}
	return nil
}

func (c WriterConfig) hotKeys() int { return int(float64(c.Keys) * c.HotFraction) }

// inBurst says whether cold keys are written at time t of a run started at start.
func (c WriterConfig) inBurst(start, t time.Time) bool {
	if c.BurstRate <= 0 {
		return false
	}
	return t.Sub(start)%c.BurstEvery < c.BurstFor
}

// Writer updates the events table. Each write sets a key's Seq to one more
// than its last value, so the checker knows exactly which writes to expect.
type Writer struct {
	client *spanner.Client
	table  string
	cfg    WriterConfig

	writes  atomic.Int64
	failed  atomic.Int64
	applies atomic.Int64
}

// NewWriter returns a writer for table.
func NewWriter(client *spanner.Client, table string, cfg WriterConfig) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Writer{client: client, table: table, cfg: cfg}, nil
}

// Writes returns how many writes have committed.
func (w *Writer) Writes() int64 { return w.writes.Load() }

// writerKey is one key a worker owns.
type writerKey struct {
	id  int64
	seq int64 // last committed sequence number
}

// Run writes until ctx ends or the configured duration has passed. It carries
// on from the sequence numbers already in the table, so a writer can be
// stopped and started again.
func (w *Writer) Run(ctx context.Context) error {
	seqs, err := w.loadSeqs(ctx)
	if err != nil {
		return err
	}
	if w.cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.cfg.Duration)
		defer cancel()
	}

	// Deal this shard's keys out to the workers.
	hot := make([][]*writerKey, w.cfg.Workers)
	cold := make([][]*writerKey, w.cfg.Workers)
	n, owned := w.cfg.hotKeys(), 0
	for i := w.cfg.Shard; i < w.cfg.Keys; i += w.cfg.Shards {
		id := keyID(i, w.cfg.Keys)
		k := &writerKey{id: id, seq: seqs[id]}
		worker := owned % w.cfg.Workers
		if i < n {
			hot[worker] = append(hot[worker], k)
		} else {
			cold[worker] = append(cold[worker], k)
		}
		owned++
	}
	share := float64(w.cfg.Shards * w.cfg.Workers)
	log.Info().Int("keys", owned).Int("hot_keys", n/w.cfg.Shards).Int("workers", w.cfg.Workers).
		Float64("hot_rate", w.cfg.HotRate/float64(w.cfg.Shards)).Float64("burst_rate", w.cfg.BurstRate/float64(w.cfg.Shards)).
		Msg("soak writer starting")

	start := time.Now()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { w.logLoop(ctx, start); return nil })
	for i := range w.cfg.Workers {
		eg.Go(func() error {
			return w.work(ctx, start, hot[i], cold[i], w.cfg.HotRate/share, w.cfg.BurstRate/share)
		})
	}
	err = eg.Wait()
	if ctx.Err() != nil {
		err = nil // stopped or ran for its duration
	}
	log.Info().Int64("writes", w.writes.Load()).Dur("ran_for", time.Since(start)).Msg("soak writer stopped")
	return err
}

func (w *Writer) loadSeqs(ctx context.Context) (map[int64]int64, error) {
	seqs := make(map[int64]int64)
	iter := w.client.Single().Query(ctx, spanner.Statement{SQL: fmt.Sprintf("SELECT Id, Seq FROM %s", w.table)})
	err := iter.Do(func(r *spanner.Row) error {
		var id, seq int64
		if err := r.Columns(&id, &seq); err != nil {
			return err
		}
		seqs[id] = seq
		return nil
	})
	return seqs, err
}

// work writes to one worker's keys. Every tick it works out how many writes
// are due at the configured rates and commits them in batches of distinct keys.
func (w *Writer) work(ctx context.Context, start time.Time, hot, cold []*writerKey, hotRate, burstRate float64) error {
	const tick = 100 * time.Millisecond
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	payload := strings.Repeat("x", w.cfg.PayloadBytes)
	var hotDue, coldDue float64
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			elapsed := now.Sub(last).Seconds()
			last = now
			if len(hot) > 0 {
				hotDue += hotRate * elapsed
			}
			if len(cold) > 0 && w.cfg.inBurst(start, now) {
				coldDue += burstRate * elapsed
			}
			// Never let a slow spell build a backlog bigger than a second's writes.
			hotDue = min(hotDue, max(hotRate, 1))
			coldDue = min(coldDue, max(burstRate, 1))
			for hotDue >= 1 || coldDue >= 1 {
				batch := make(map[*writerKey]struct{}, w.cfg.Batch)
				for len(batch) < w.cfg.Batch && hotDue >= 1 && len(batch) < len(hot) {
					batch[hot[rand.IntN(len(hot))]] = struct{}{}
					hotDue--
				}
				for len(batch) < w.cfg.Batch && coldDue >= 1 {
					batch[cold[rand.IntN(len(cold))]] = struct{}{}
					coldDue--
				}
				if err := w.apply(ctx, batch, payload); err != nil {
					return nil // ctx ended
				}
			}
		}
	}
}

// apply commits one write to each key in batch, retrying until it succeeds or
// ctx ends. A retry writes the same sequence numbers, so a commit that failed
// with an ambiguous error and in fact went through is at worst written twice,
// which the checker counts as a duplicate, never as a gap.
func (w *Writer) apply(ctx context.Context, batch map[*writerKey]struct{}, payload string) error {
	ms := make([]*spanner.Mutation, 0, len(batch))
	for k := range batch {
		ms = append(ms, spanner.InsertOrUpdate(w.table,
			[]string{"Id", "Seq", "Payload", "UpdatedAt"},
			[]any{k.id, k.seq + 1, payload, spanner.CommitTimestamp}))
	}
	backoff := 100 * time.Millisecond
	for {
		_, err := w.client.Apply(ctx, ms)
		w.applies.Add(1)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.failed.Add(1)
		log.Warn().Err(err).Int("rows", len(ms)).Msg("soak write failed, retrying")
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(2*backoff, 5*time.Second)
	}
	for k := range batch {
		k.seq++
	}
	w.writes.Add(int64(len(batch)))
	return nil
}

func (w *Writer) logLoop(ctx context.Context, start time.Time) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	prev, prevAt := int64(0), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n := w.writes.Load()
			rate := float64(n-prev) / now.Sub(prevAt).Seconds()
			prev, prevAt = n, now
			log.Info().Int64("writes", n).Float64("writes_per_sec", rate).Int64("failed_applies", w.failed.Load()).
				Bool("burst", w.cfg.inBurst(start, now)).Msg("soak writer progress")
		}
	}
}
