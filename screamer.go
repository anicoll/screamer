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
	GetInterruptedPartitions(ctx context.Context, runnerID string) ([]*PartitionMetadata, error)
	// InitializeRootPartition creates or updates the root partition metadata.
	InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error
	// GetAndSchedulePartitions finds partitions ready to be scheduled and assigns them to the given runnerID.
	GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string) ([]*PartitionMetadata, error)
	// AddChildPartitions adds new child partitions for a parent partition based on a ChildPartitionsRecord.
	AddChildPartitions(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error
	// UpdateToRunning marks the given partition as running.
	UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error
	// RefreshRunner updates the liveness timestamp for the given runnerID.
	RefreshRunner(ctx context.Context, runnerID string) error
	// UpdateToFinished marks the given partition as finished.
	UpdateToFinished(ctx context.Context, partition *PartitionMetadata) error
	// UpdateWatermark updates the watermark for the given partition.
	UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error
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

	partitionStorage   PartitionStorage
	consumer           Consumer
	eg                 *errgroup.Group
	mu                 sync.Mutex
	serializedConsumer bool
	consumerMu         sync.Mutex
}

// Option configures a Subscriber via functional options.
type Option interface {
	Apply(*config)
}

type config struct {
	startTimestamp         time.Time
	endTimestamp           time.Time
	heartbeatInterval      time.Duration
	spannerRequestPriority spannerpb.RequestOptions_Priority
	serializedConsumer     bool
	logLevel               zerolog.Level
}

type withStartTimestamp time.Time

func (o withStartTimestamp) Apply(c *config) {
	c.startTimestamp = time.Time(o)
}

// WithStartTimestamp sets the start timestamp option for reading change streams.
// The value must be within the retention period of the change stream and before the current time.
// Default value is current timestamp.
func WithStartTimestamp(startTimestamp time.Time) Option {
	return withStartTimestamp(startTimestamp)
}

type withEndTimestamp time.Time

func (o withEndTimestamp) Apply(c *config) {
	c.endTimestamp = time.Time(o)
}

// WithLogLevel sets the log level for the subscriber.
func WithLogLevel(logLevel string) Option {
	ll, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		log.Warn().Err(err).Msgf("Invalid log level %s, using default level info", logLevel)
		ll = zerolog.InfoLevel // Default log level
	}
	return withLogLevel(ll)
}

type withLogLevel zerolog.Level

func (o withLogLevel) Apply(c *config) {
	c.logLevel = zerolog.Level(o)
}

// WithEndTimestamp sets the end timestamp option for reading change streams.
// The value must be within the retention period of the change stream and must be after the start timestamp.
// If not set, reads latest changes until canceled.
func WithEndTimestamp(endTimestamp time.Time) Option {
	return withEndTimestamp(endTimestamp)
}

type withHeartbeatInterval time.Duration

func (o withHeartbeatInterval) Apply(c *config) {
	c.heartbeatInterval = time.Duration(o)
}

// WithHeartbeatInterval sets the heartbeat interval for reading change streams.
// Default value is 10 seconds.
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option {
	return withHeartbeatInterval(heartbeatInterval)
}

type withSpannerRequestPriotiry spannerpb.RequestOptions_Priority

func (o withSpannerRequestPriotiry) Apply(c *config) {
	c.spannerRequestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithSpannerRequestPriotiry sets the request priority option for reading change streams.
// Default value is unspecified, equivalent to high.
func WithSpannerRequestPriotiry(priority spannerpb.RequestOptions_Priority) Option {
	return withSpannerRequestPriotiry(priority)
}

type withSerializedConsumer bool

func (o withSerializedConsumer) Apply(c *config) {
	c.serializedConsumer = bool(o)
}

// WithSerializedConsumer enables or disables serialized processing of records by the Consumer.
// When true, a mutex ensures that s.consumer.Consume() is called serially, simplifying
// Consumer implementations that are not re-entrant safe. This may impact performance.
// Default is false (concurrent consumption is allowed if the Consumer is re-entrant safe).
func WithSerializedConsumer(serialized bool) Option {
	return withSerializedConsumer(serialized)
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
		spannerClient:          client,
		streamName:             streamName,
		startTimestamp:         c.startTimestamp,
		endTimestamp:           c.endTimestamp,
		heartbeatInterval:      c.heartbeatInterval,
		spannerRequestPriority: c.spannerRequestPriority,
		partitionStorage:       partitionStorage,
		runnerID:               runnerID,
		serializedConsumer:     c.serializedConsumer,
	}
}

// Subscribe starts subscribing to the change stream and processing records using the provided Consumer.
// This method blocks until all partitions are processed or the context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	log.Debug().Msg("Starting subscription to change stream")
	ctx = s.initErrGroup(ctx)
	s.consumer = consumer

	s.eg.Go(func() error {
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()
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
	interruptedPartitions, err := s.partitionStorage.GetInterruptedPartitions(ctx, s.runnerID)
	if err != nil {
		return fmt.Errorf("failed to get interrupted partitions: %w", err)
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
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition: %w", err)
	}

	if minWatermarkPartition == nil {
		return errDone
	}

	partitions, err := s.partitionStorage.GetAndSchedulePartitions(ctx, minWatermarkPartition.Watermark, s.runnerID)
	if err != nil {
		return fmt.Errorf("failed to get schedulable partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil
	}

	for _, p := range partitions {
		p := p
		s.eg.Go(func() error {
			return s.queryChangeStream(ctx, p)
		})
	}

	return nil
}

func (s *Subscriber) queryChangeStream(ctx context.Context, p *PartitionMetadata) error {
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
	defer iter.Stop()
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
	if err := s.partitionStorage.UpdateToFinished(ctx, p); err != nil {
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
	var watermarker watermarker
	for _, cr := range records {
		log.Trace().
			Str("partition_token", p.PartitionToken).
			Int("data_change_records", len(cr.DataChangeRecords)).
			Msg("processing change record")
		for _, record := range cr.DataChangeRecords {
			watermarker.set(record.CommitTimestamp)
			out, err := json.Marshal(record.DecodeToNonSpannerType(p, watermarker.get()))
			if err != nil {
				return err
			}
			if s.serializedConsumer {
				s.consumerMu.Lock()
				defer s.consumerMu.Unlock()
				if err = s.consumer.Consume(out); err != nil {
					return err
				}
			} else {
				if err := s.consumer.Consume(out); err != nil {
					return err
				}
			}
		}
		for _, record := range cr.HeartbeatRecords {
			watermarker.set(record.Timestamp)
		}
		for _, record := range cr.ChildPartitionsRecords {
			if err := s.partitionStorage.AddChildPartitions(ctx, p, record); err != nil {
				return fmt.Errorf("failed to add child partitions: %w", err)
			}
			watermarker.set(record.StartTimestamp)
		}
	}

	if err := s.partitionStorage.UpdateWatermark(ctx, p, watermarker.get()); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}

	return nil
}
