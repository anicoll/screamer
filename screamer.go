package screamer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// PartitionStorage defines the interface for managing change stream partition metadata.
// Implementations must be concurrency-safe.
type PartitionStorage interface {
	// GetUnfinishedMinWatermarkPartition returns the unfinished partition with the minimum watermark, or nil if none exist.
	GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error)
	// GetInterruptedPartitions returns partitions that are scheduled or running but have lost their runner.
	// The limit parameter restricts the maximum number of partitions to recover.
	GetInterruptedPartitions(ctx context.Context, runnerID string, limit int) ([]*PartitionMetadata, error)
	// InitializeRootPartition creates or updates the root partition metadata.
	InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error
	// GetAndSchedulePartitions finds partitions ready to be scheduled and assigns them to the given runnerID.
	// The limit parameter restricts the maximum number of partitions to schedule.
	GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, limit int) ([]*PartitionMetadata, error)
	// AddChildPartitions adds new child partitions for a parent partition based on a ChildPartitionsRecord.
	AddChildPartitions(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error
	// UpdateToRunning marks the given partition as running.
	UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error
	// RefreshRunner updates the liveness timestamp for the given runnerID.
	RefreshRunner(ctx context.Context, runnerID string) error
	// UpdateToFinished marks the given partition as finished.
	UpdateToFinished(ctx context.Context, partition *PartitionMetadata, runnerID string) error
	// UpdateWatermark updates the watermark for the given partition.
	UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error
	// GetActiveRunnerCount returns the number of active runners that have refreshed recently.
	// Used for metrics and observability.
	GetActiveRunnerCount(ctx context.Context) (int, error)
}

// Subscriber subscribes to a change stream and manages partition processing.
type Subscriber struct {
	spannerClient          *spanner.Client
	streamName             string
	runnerID               string
	startTimestamp         time.Time
	endTimestamp           time.Time
	heartbeatInterval      time.Duration
	spannerRequestPriority spannerpb.RequestOptions_Priority

	partitionStorage        PartitionStorage
	reader                  ChangeStreamReader
	consumer                Consumer
	consumerWithAck         ConsumerWithAck
	eg                      *errgroup.Group
	mu                      sync.Mutex
	serializedConsumer      bool
	consumerMu              sync.Mutex
	maxConcurrentPartitions int
	ackTimeout              time.Duration
	activePartitions        map[string]struct{}
	partitionCountMu        sync.RWMutex
	partitionMaxRetries     int
	partitionRetries        atomic.Int64
	meterProvider           metric.MeterProvider
	cluster                 clusterStats
	parentGating            bool
	gatedScheduler          ParentGatedScheduler
	checkpointInterval      time.Duration
	checkpoints             *checkpointer
	leaseTTL                time.Duration
	useLeases               bool
	leaser                  PartitionLeaser
	leases                  *leaseSet
	useParking              bool
	parker                  PartitionParker
	parkInterval            time.Duration
	parkIdleAfter           time.Duration
	parked                  atomic.Int64
}

var (
	defaultEndTimestamp      = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC) // Maximum value of Spanner TIMESTAMP type.
	defaultHeartbeatInterval = 3 * time.Second
)

// NewSubscriber creates a new subscriber of change streams.
// The returned Subscriber is ready to start processing with Subscribe.
func NewSubscriber(
	client *spanner.Client,
	streamName, runnerID string,
	partitionStorage PartitionStorage,
	options ...Option,
) *Subscriber {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	c := &config{
		startTimestamp:      time.Now(),
		endTimestamp:        defaultEndTimestamp,
		heartbeatInterval:   defaultHeartbeatInterval,
		logLevel:            zerolog.InfoLevel, // Default log level
		partitionMaxRetries: defaultPartitionMaxRetries,
		parentGating:        true,
		checkpointInterval:  defaultCheckpointInterval,
		leases:              true,
		leaseTTL:            defaultLeaseTTL,
		parking:             true,
		parkInterval:        defaultParkInterval,
		parkIdleAfter:       defaultParkIdleAfter,
	}
	for _, o := range options {
		o.Apply(c)
	}
	zerolog.SetGlobalLevel(c.logLevel)

	if min := 2 * c.heartbeatInterval; c.parkIdleAfter < min {
		c.parkIdleAfter = min
	}

	reader := c.changeStreamReader
	if reader == nil {
		reader = &spannerChangeStreamReader{
			client:     client,
			streamName: streamName,
			priority:   c.spannerRequestPriority,
		}
	}

	return &Subscriber{
		spannerClient:           client,
		reader:                  reader,
		streamName:              streamName,
		startTimestamp:          c.startTimestamp,
		endTimestamp:            c.endTimestamp,
		heartbeatInterval:       c.heartbeatInterval,
		spannerRequestPriority:  c.spannerRequestPriority,
		partitionStorage:        partitionStorage,
		runnerID:                runnerID,
		serializedConsumer:      c.serializedConsumer,
		maxConcurrentPartitions: c.maxConcurrentPartitions,
		ackTimeout:              c.ackTimeout,
		activePartitions:        make(map[string]struct{}),
		partitionMaxRetries:     c.partitionMaxRetries,
		meterProvider:           c.meterProvider,
		parentGating:            c.parentGating,
		checkpointInterval:      c.checkpointInterval,
		checkpoints:             newCheckpointer(),
		useLeases:               c.leases,
		leaseTTL:                c.leaseTTL,
		leases:                  newLeaseSet(),
		useParking:              c.parking,
		parkInterval:            c.parkInterval,
		parkIdleAfter:           c.parkIdleAfter,
	}
}

// Subscribe starts subscribing to the change stream and processing records using the provided Consumer.
// This method blocks until all partitions are processed or the context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	s.consumer = consumer
	return s.subscribe(ctx)
}

// SubscribeWithAck starts subscribing to the change stream using a ConsumerWithAck.
// Each record is delivered with an AckFunc that the consumer must call exactly once.
// The subscriber will not advance to the next batch until all acks for the current batch are received.
// This method blocks until all partitions are processed or the context is canceled.
func (s *Subscriber) SubscribeWithAck(ctx context.Context, consumer ConsumerWithAck) error {
	s.consumerWithAck = consumer
	return s.subscribe(ctx)
}

// SubscribeFuncWithAck is an adapter to allow the use of ordinary functions as ConsumerWithAck.
func (s *Subscriber) SubscribeFuncWithAck(ctx context.Context, f ConsumerFuncWithAck) error {
	return s.SubscribeWithAck(ctx, f)
}

func (s *Subscriber) subscribe(ctx context.Context) error {
	if s.consumer != nil && s.consumerWithAck != nil {
		return fmt.Errorf("screamer: Consumer and ConsumerWithAck are mutually exclusive; use Subscribe or SubscribeWithAck, not both")
	}
	if s.parentGating {
		g, ok := s.partitionStorage.(ParentGatedScheduler)
		if !ok {
			return fmt.Errorf("screamer: parent gating is on but %T does not implement screamer.ParentGatedScheduler; implement it, or turn gating off with WithParentGating(false) (see MIGRATION.md)", s.partitionStorage)
		}
		s.gatedScheduler = g
	}
	if s.useLeases {
		l, ok := s.partitionStorage.(PartitionLeaser)
		if !ok {
			return fmt.Errorf("screamer: leases are on but %T does not implement screamer.PartitionLeaser; implement it, or turn leases off with WithLeases(false) (see MIGRATION.md)", s.partitionStorage)
		}
		s.leaser = l
	}
	if err := s.parkingCheck(); err != nil {
		return err
	}

	log.Debug().Msg("Starting subscription to change stream")
	ctx = s.initErrGroup(ctx)

	// done is closed when every partition has finished (the end timestamp was
	// reached), so the background loops stop and Subscribe returns.
	done := make(chan struct{})

	if reg, err := s.registerMetrics(s.meterProvider); err != nil {
		log.Warn().Err(err).Msg("failed to register metrics; continuing without them")
	} else {
		defer func() { _ = reg.Unregister() }()
	}
	s.eg.Go(func() error { return s.clusterStatsLoop(ctx, done) })
	s.eg.Go(func() error { return s.checkpointLoop(ctx, done) })

	s.eg.Go(func() error {
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()
		metricsCounter := 0
		for {
			select {
			case <-ticker.C:
				err := s.partitionStorage.RefreshRunner(ctx, s.runnerID)
				switch err {
				case nil:
					// continue
				default:
					return err
				}

				// Log metrics every 30 seconds (15 ticks)
				metricsCounter++
				if metricsCounter >= 15 {
					metricsCounter = 0
					metrics := s.GetMetrics()
					cluster := s.GetClusterMetrics()
					log.Info().
						Str("runner_id", s.runnerID).
						Int("active_partitions", metrics.ActivePartitions).
						Int("max_concurrent", metrics.MaxConcurrentPartitions).
						Float64("capacity_used_percent", metrics.CapacityUsedPercent).
						Int("available_slots", metrics.AvailableSlots).
						Int64("partition_retries", metrics.PartitionRetries).
						Int("unfinished_partitions", cluster.UnfinishedPartitions).
						Int("live_runners", cluster.LiveRunners).
						Int("desired_runners", cluster.DesiredRunners).
						Dur("watermark_lag", cluster.WatermarkLag).
						Msg("partition processing metrics")
					if cluster.DesiredRunners > cluster.LiveRunners {
						log.Warn().
							Int("desired_runners", cluster.DesiredRunners).
							Int("live_runners", cluster.LiveRunners).
							Int("unfinished_partitions", cluster.UnfinishedPartitions).
							Msgf("cluster needs about %d runners to read every partition, %d are live", cluster.DesiredRunners, cluster.LiveRunners)
					}
				}
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Initialize root partition if this is the first run or if the previous run has already been completed.
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition on start subscribe: %w", err)
	}
	if minWatermarkPartition == nil {
		if err := s.partitionStorage.InitializeRootPartition(ctx, s.startTimestamp, s.endTimestamp, s.heartbeatInterval); err != nil {
			return fmt.Errorf("failed to initialize root partition: %w", err)
		}
	}

	// With leases, claiming covers partitions whose runner went away: their
	// leases expire and they are claimed like any other.
	if s.leaser == nil {
		if err := s.processInterruptedPartitions(ctx); err != nil {
			return err
		}
	}

	s.eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.detectNewPartitions(ctx)
				switch err {
				case errDone:
					close(done)
					return nil
				case nil:
					// continue
				default:
					return err
				}
			case <-ctx.Done():
				err := ctx.Err()
				return err
			}
		}
	})

	// periodically check for new stale partitions.
	s.eg.Go(func() error {
		if s.leaser != nil {
			return nil
		}
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.processInterruptedPartitions(ctx)
				switch err {
				case errDone:
					return nil
				case nil:
					// continue
				default:
					return err
				}
			case <-done:
				return nil
			case <-ctx.Done():
				err := ctx.Err()
				return err
			}
		}
	})

	return s.eg.Wait()
}

func (s *Subscriber) processInterruptedPartitions(ctx context.Context) error {
	// Calculate available capacity for recovering partitions
	availableSlots := s.getAvailablePartitionSlots()
	if availableSlots <= 0 {
		log.Trace().
			Str("runner_id", s.runnerID).
			Msg("at capacity, skipping interrupted partition recovery")
		return nil
	}

	interruptedPartitions, err := s.partitionStorage.GetInterruptedPartitions(ctx, s.runnerID, availableSlots)
	if err != nil {
		return fmt.Errorf("failed to get interrupted partitions: %w", err)
	}

	if len(interruptedPartitions) > 0 {
		log.Info().
			Str("runner_id", s.runnerID).
			Int("recovered_partitions", len(interruptedPartitions)).
			Msg("recovering interrupted partitions")
	}

	for _, p := range interruptedPartitions {
		p := p
		s.eg.Go(func() error {
			return s.queryChangeStream(ctx, p)
		})
	}
	return nil
}

// SubscribeFunc is an adapter to allow the use of ordinary functions as Consumer.
// The function might be called from multiple goroutines and must be re-entrant safe.
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error {
	return s.Subscribe(ctx, f)
}

func (s *Subscriber) initErrGroup(ctx context.Context) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.eg != nil {
		panic("Subscriber has already started subscribe.")
	}

	eg, ctx := errgroup.WithContext(ctx)
	s.eg = eg
	return ctx
}

var errDone = errors.New("all partitions have been processed")

func (s *Subscriber) detectNewPartitions(ctx context.Context) error {
	log.Debug().Str("stream_name", s.streamName).Str("runner_id", s.runnerID).Msg("detecting new partitions")

	// Calculate available capacity
	availableSlots := s.getAvailablePartitionSlots()
	if availableSlots <= 0 {
		log.Trace().
			Str("runner_id", s.runnerID).
			Int("active_partitions", s.getRunningPartitionCount()).
			Int("max_concurrent", s.maxConcurrentPartitions).
			Msg("at capacity, skipping partition detection")
		return nil
	}

	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition: %w", err)
	}

	if minWatermarkPartition == nil {
		return errDone
	}

	for batches := 0; ; batches++ {
		partitions, err := s.schedulePartitions(ctx, minWatermarkPartition.Watermark, availableSlots)
		if err != nil {
			return fmt.Errorf("failed to get schedulable partitions: %w", err)
		}
		if len(partitions) == 0 {
			return nil
		}

		log.Info().
			Str("runner_id", s.runnerID).
			Int("scheduled_partitions", len(partitions)).
			Int("available_slots", availableSlots).
			Msg("scheduled new partitions")

		for _, p := range partitions {
			p := p
			s.eg.Go(func() error {
				return s.queryChangeStream(ctx, p)
			})
		}

		// With no partition limit, keep scheduling in batches until nothing is
		// ready, rather than one batch a second: otherwise starting up with
		// 5,000 partitions, or a burst of splits, takes 50 seconds to schedule.
		// With a limit, the one call above already asked for every free slot.
		if s.maxConcurrentPartitions > 0 || len(partitions) < availableSlots || batches+1 >= maxScheduleBatchesPerTick {
			return nil
		}
	}
}

// maxScheduleBatchesPerTick bounds how many scheduling transactions one tick of
// partition detection runs when there is no partition limit.
const maxScheduleBatchesPerTick = 100

// consumerError marks an error from the consumer (a failed Consume, a nack or an
// ack timeout). It is never retried: it stops the subscriber, as before.
type consumerError struct{ err error }

func (e *consumerError) Error() string { return e.err.Error() }
func (e *consumerError) Unwrap() error { return e.err }

// queryChangeStream reads one partition to its end. A failure that does not come
// from the consumer (a dropped stream, a failed metadata write) is retried from the
// partition's last watermark with exponential backoff, so one bad partition does
// not stop every other partition on this runner. The retry budget resets whenever
// the partition makes progress. Once it is spent, the error stops the subscriber.
func (s *Subscriber) queryChangeStream(parent context.Context, p *PartitionMetadata) error {
	s.markPartitionActive(p.PartitionToken)
	defer s.markPartitionInactive(p.PartitionToken)

	ctx, release := s.leasedContext(parent, p)
	defer release()

	// A parked partition is read up to now, then parked again or streamed.
	slice := s.parker != nil && p.State == StateParked

	attempt := 0
	for {
		before := p.Watermark
		outcome, err := s.readPartition(ctx, p, slice)
		if err == nil {
			switch outcome {
			case readSliceData:
				slice, attempt = false, 0
				continue
			case readIdle, readSliceEmpty:
				s.park(ctx, p)
			}
			return nil
		}
		if parent.Err() != nil {
			// Stopping. Report why, not how the read noticed (an idle timer
			// or a lost lease can cancel it at the same moment).
			return parent.Err()
		}
		if errors.Is(context.Cause(ctx), errLeaseLost) {
			// Another runner owns the partition now and reads it from the last
			// watermark this runner wrote. Not an error for this runner.
			log.Info().Str("partition_token", p.PartitionToken).Msg("stopped reading partition: lease lost")
			return nil
		}
		var ce *consumerError
		if errors.As(err, &ce) {
			return ce.err
		}
		if p.Watermark.After(before) {
			attempt = 0 // made progress since the last failure
		}
		if attempt >= s.partitionMaxRetries {
			return fmt.Errorf("partition %s failed after %d retries: %w", p.PartitionToken, attempt, err)
		}
		delay := retryDelay(attempt)
		attempt++
		s.partitionRetries.Add(1)
		log.Warn().Err(err).
			Str("partition_token", p.PartitionToken).
			Int("attempt", attempt).
			Dur("backoff", delay).
			Msg("partition failed, retrying from last watermark")

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

const (
	retryBaseDelay = time.Second
	retryMaxDelay  = 30 * time.Second
)

// retryDelay is 1s, 2s, 4s ... capped at 30s, with up to 20% jitter so partitions
// that failed together do not retry together.
func retryDelay(attempt int) time.Duration {
	d := retryBaseDelay << attempt
	if d <= 0 || d > retryMaxDelay {
		d = retryMaxDelay
	}
	return d + time.Duration(rand.Int64N(int64(d)/5+1))
}

// readPartition reads p once. A streaming read (slice false) runs until the
// partition ends or, with parking, until it has been idle for parkIdleAfter.
// A catch-up read of a parked partition (slice true) runs until now.
func (s *Subscriber) readPartition(ctx context.Context, p *PartitionMetadata, slice bool) (readOutcome, error) {
	if err := s.partitionStorage.UpdateToRunning(ctx, p); err != nil {
		return 0, fmt.Errorf("failed to update to running: %w", err)
	}

	query := p
	var end time.Time
	if slice {
		end, slice = sliceEnd(p, time.Now())
		if slice {
			q := *p
			q.EndTimestamp = end
			query = &q
		}
	}
	// With parking, a streaming read is stopped once the partition has
	// delivered no data for parkIdleAfter. A timer does it rather than the
	// heartbeats, which may not arrive while nothing changes.
	readCtx := ctx
	var idle *time.Timer
	if s.parker != nil && !slice {
		var stop context.CancelCauseFunc
		readCtx, stop = context.WithCancelCause(ctx)
		defer stop(nil)
		idle = time.AfterFunc(s.parkIdleAfter, func() { stop(errIdle) })
		defer idle.Stop()
	}

	var data, child bool
	err := s.reader.Read(readCtx, query, func(records []*ChangeRecord) error {
		log.Trace().
			Str("partition_token", p.PartitionToken).
			Int("num_records", len(records)).
			Msg("processing partition")
		batchData := false
		for _, r := range records {
			batchData = batchData || len(r.DataChangeRecords) > 0
			child = child || len(r.ChildPartitionsRecords) > 0
		}
		if err := s.handle(readCtx, p, records); err != nil {
			return err
		}
		if batchData {
			data = true
			if idle != nil {
				idle.Reset(s.parkIdleAfter)
			}
		}
		return nil
	})
	if err != nil && errors.Is(context.Cause(readCtx), errIdle) && ctx.Err() == nil {
		return readIdle, nil
	}
	if err != nil {
		return 0, err
	}

	if slice && !child {
		// The query ended at end, so every change up to end has been delivered.
		if err := s.checkpoint(ctx, p, end); err != nil {
			return 0, fmt.Errorf("failed to update watermark: %w", err)
		}
		if end.After(p.Watermark) {
			p.Watermark = end
		}
		if data {
			return readSliceData, nil
		}
		return readSliceEmpty, nil
	}

	log.Info().Str("partition_token", p.PartitionToken).
		Msg("partition processing complete")
	// A finished partition's watermark no longer matters to anyone.
	if s.checkpoints != nil {
		s.checkpoints.forget(p.PartitionToken)
	}
	if err := s.partitionStorage.UpdateToFinished(ctx, p, s.runnerID); err != nil {
		return 0, fmt.Errorf("failed to update to finished: %w", err)
	}

	return readFinished, nil
}

type watermarker struct {
	watermark time.Time
}

func (w *watermarker) set(t time.Time) {
	if t.After(w.watermark) {
		w.watermark = t
	}
}

func (w *watermarker) get() time.Time {
	return w.watermark
}

func (s *Subscriber) handle(ctx context.Context, p *PartitionMetadata, records []*ChangeRecord) error {
	var wm watermarker

	// In ack mode, pre-count records so we know exactly how many acks to wait for.
	var ackCh chan error
	if s.consumerWithAck != nil {
		n := 0
		for _, cr := range records {
			n += len(cr.DataChangeRecords)
		}
		ackCh = make(chan error, n)
	}

	for _, cr := range records {
		log.Trace().
			Str("partition_token", p.PartitionToken).
			Int("data_change_records", len(cr.DataChangeRecords)).
			Msg("processing change record")
		for _, record := range cr.DataChangeRecords {
			wm.set(record.CommitTimestamp)
			out, err := encodeDataChangeRecord(record, p, wm.get())
			if err != nil {
				return &consumerError{err}
			}
			if err := s.dispatchRecord(out, ackCh); err != nil {
				return &consumerError{err}
			}
		}
		for _, record := range cr.HeartbeatRecords {
			wm.set(record.Timestamp)
		}
		for _, record := range cr.ChildPartitionsRecords {
			if err := s.partitionStorage.AddChildPartitions(ctx, p, record); err != nil {
				return fmt.Errorf("failed to add child partitions: %w", err)
			}
			wm.set(record.StartTimestamp)
		}
	}

	// In ack mode: block until every record in this batch has been acked or nacked
	// before advancing the watermark. This is the batch gate.
	if ackCh != nil {
		var timeoutCh <-chan time.Time
		if s.ackTimeout > 0 {
			timer := time.NewTimer(s.ackTimeout)
			defer timer.Stop()
			timeoutCh = timer.C
		}
		for i := 0; i < cap(ackCh); i++ {
			select {
			case err := <-ackCh:
				if err != nil {
					return &consumerError{fmt.Errorf("consumer nacked record: %w", err)}
				}
			case <-timeoutCh:
				return &consumerError{fmt.Errorf("ack timeout: consumer did not acknowledge all records within %s", s.ackTimeout)}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if err := s.checkpoint(ctx, p, wm.get()); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}
	// Keep the in-process copy current, so a retry resumes from here
	// whatever the PartitionStorage implementation does with p.
	if wm.get().After(p.Watermark) {
		p.Watermark = wm.get()
	}

	return nil
}

// dispatchRecord delivers a serialized DataChangeRecord to whichever consumer is configured.
// In ack mode, ackCh receives the result of the consumer's AckFunc call.
func (s *Subscriber) dispatchRecord(out []byte, ackCh chan error) error {
	if s.consumerWithAck != nil {
		ack := AckFunc(func(err error) { ackCh <- err })
		if s.serializedConsumer {
			s.consumerMu.Lock()
			err := s.consumerWithAck.Consume(out, ack)
			s.consumerMu.Unlock()
			return err
		}
		return s.consumerWithAck.Consume(out, ack)
	}

	if s.serializedConsumer {
		s.consumerMu.Lock()
		err := s.consumer.Consume(out)
		s.consumerMu.Unlock()
		return err
	}
	return s.consumer.Consume(out)
}

// markPartitionActive adds a partition to the active tracking set.
func (s *Subscriber) markPartitionActive(token string) {
	s.partitionCountMu.Lock()
	defer s.partitionCountMu.Unlock()
	s.activePartitions[token] = struct{}{}
}

// markPartitionInactive removes a partition from the active tracking set.
func (s *Subscriber) markPartitionInactive(token string) {
	s.partitionCountMu.Lock()
	defer s.partitionCountMu.Unlock()
	delete(s.activePartitions, token)
}

// getRunningPartitionCount returns the number of currently active partitions.
func (s *Subscriber) getRunningPartitionCount() int {
	s.partitionCountMu.RLock()
	defer s.partitionCountMu.RUnlock()
	return len(s.activePartitions)
}

// getAvailablePartitionSlots calculates how many partitions can be scheduled based on the current load.
// Returns 0 if at capacity, or the number of available slots if under the limit.
func (s *Subscriber) getAvailablePartitionSlots() int {
	if s.maxConcurrentPartitions == 0 {
		// No limit set, return a reasonable batch size
		return 100
	}

	currentlyRunning := s.getRunningPartitionCount()
	available := s.maxConcurrentPartitions - currentlyRunning

	if available <= 0 {
		return 0
	}
	return available
}

// PartitionMetrics holds observability metrics for partition processing.
type PartitionMetrics struct {
	// ActivePartitions is the number of partitions currently being processed by this runner.
	ActivePartitions int
	// MaxConcurrentPartitions is the configured limit (0 = unlimited).
	MaxConcurrentPartitions int
	// CapacityUsedPercent is the percentage of capacity in use (0-100, or -1 if unlimited).
	CapacityUsedPercent float64
	// AvailableSlots is the number of additional partitions this runner can accept.
	AvailableSlots int
	// PartitionRetries is the number of times a failed partition has been retried
	// since the subscriber started (see WithPartitionMaxRetries).
	PartitionRetries int64
	// LeasesHeld is the number of partition leases this runner holds. 0 when
	// leases are off.
	LeasesHeld int
	// LeasesLost is the number of leases this runner found taken by another
	// runner, typically after a pause longer than the lease TTL. Each one
	// stopped this runner reading that partition. It should stay near zero.
	LeasesLost int64
	// LeaseTakeovers is the number of partitions this runner claimed after
	// another runner's lease on them expired or was released.
	LeaseTakeovers int64
	// PartitionsParked is the number of times this runner parked an idle
	// partition (see WithParking).
	PartitionsParked int64
}

// GetMetrics returns current partition processing metrics for observability.
// This can be used for monitoring dashboards, health checks, and capacity planning.
func (s *Subscriber) GetMetrics() PartitionMetrics {
	active := s.getRunningPartitionCount()
	available := s.getAvailablePartitionSlots()

	metrics := PartitionMetrics{
		ActivePartitions:        active,
		MaxConcurrentPartitions: s.maxConcurrentPartitions,
		AvailableSlots:          available,
		PartitionRetries:        s.partitionRetries.Load(),
		LeasesHeld:              s.leases.count(),
		LeasesLost:              s.leases.lostCount(),
		LeaseTakeovers:          s.leases.takeoverCount(),
		PartitionsParked:        s.parked.Load(),
	}

	// Calculate capacity percentage
	if s.maxConcurrentPartitions == 0 {
		metrics.CapacityUsedPercent = -1 // Unlimited
	} else {
		metrics.CapacityUsedPercent = (float64(active) / float64(s.maxConcurrentPartitions)) * 100
	}

	return metrics
}
