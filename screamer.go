package screamer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
		startTimestamp:    time.Now(),
		endTimestamp:      defaultEndTimestamp,
		heartbeatInterval: defaultHeartbeatInterval,
		logLevel:          zerolog.InfoLevel, // Default log level
	}
	for _, o := range options {
		o.Apply(c)
	}
	zerolog.SetGlobalLevel(c.logLevel)

	return &Subscriber{
		spannerClient:           client,
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

	log.Debug().Msg("Starting subscription to change stream")
	ctx = s.initErrGroup(ctx)

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
					log.Info().
						Str("runner_id", s.runnerID).
						Int("active_partitions", metrics.ActivePartitions).
						Int("max_concurrent", metrics.MaxConcurrentPartitions).
						Float64("capacity_used_percent", metrics.CapacityUsedPercent).
						Int("available_slots", metrics.AvailableSlots).
						Msg("partition processing metrics")
				}
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

	if err := s.processInterruptedPartitions(ctx); err != nil {
		return err
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

	partitions, err := s.partitionStorage.GetAndSchedulePartitions(ctx, minWatermarkPartition.Watermark, s.runnerID, availableSlots)
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

	return nil
}

func (s *Subscriber) queryChangeStream(ctx context.Context, p *PartitionMetadata) error {
	s.markPartitionActive(p.PartitionToken)
	defer s.markPartitionInactive(p.PartitionToken)

	if err := s.partitionStorage.UpdateToRunning(ctx, p); err != nil {
		return fmt.Errorf("failed to update to running: %w", err)
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)", s.streamName),
		Params: map[string]interface{}{
			"startTimestamp":        p.Watermark,
			"endTimestamp":          p.EndTimestamp,
			"partitionToken":        p.PartitionToken,
			"heartbeatMilliseconds": p.HeartbeatMillis,
		},
	}

	if p.IsRootPartition() {
		// Must be converted to NULL (root partition).
		stmt.Params["partitionToken"] = nil
	}

	iter := s.spannerClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.spannerRequestPriority})

	if err := iter.Do(func(r *spanner.Row) error {
		records := []*ChangeRecord{}
		if err := r.Columns(&records); err != nil {
			return err
		}
		log.Trace().
			Str("partition_token", p.PartitionToken).
			Int("num_records", len(records)).
			Msg("processing partition")
		if err := s.handle(ctx, p, records); err != nil {
			return err
		}
		log.Trace().
			Str("partition_token", p.PartitionToken).
			Int("num_records", len(records)).
			Msg("processing partition complete")
		return nil
	}); err != nil {
		return err
	}

	log.Info().Str("partition_token", p.PartitionToken).
		Msg("partition processing complete")
	if err := s.partitionStorage.UpdateToFinished(ctx, p, s.runnerID); err != nil {
		return fmt.Errorf("failed to update to finished: %w", err)
	}

	return nil
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
			out, err := json.Marshal(record.DecodeToNonSpannerType(p, wm.get()))
			if err != nil {
				return err
			}
			if err := s.dispatchRecord(out, ackCh); err != nil {
				return err
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
					return fmt.Errorf("consumer nacked record: %w", err)
				}
			case <-timeoutCh:
				return fmt.Errorf("ack timeout: consumer did not acknowledge all records within %s", s.ackTimeout)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if err := s.partitionStorage.UpdateWatermark(ctx, p, wm.get()); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
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
	}

	// Calculate capacity percentage
	if s.maxConcurrentPartitions == 0 {
		metrics.CapacityUsedPercent = -1 // Unlimited
	} else {
		metrics.CapacityUsedPercent = (float64(active) / float64(s.maxConcurrentPartitions)) * 100
	}

	return metrics
}
