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
	"github.com/prometheus/client_golang/prometheus"
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
	partitionStorage       PartitionStorage
	consumer               Consumer
	eg                     *errgroup.Group
	mu                     sync.Mutex
	serializedConsumer     bool
	consumerMu             sync.Mutex
	metrics                *screamerMetrics
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
	c := &config{
		startTimestamp:    time.Now(),
		endTimestamp:      defaultEndTimestamp,
		heartbeatInterval: defaultHeartbeatInterval,
	}
	for _, o := range options {
		o.Apply(c)
	}

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
		metrics:                newScreamerMetrics(prometheus.DefaultRegisterer, streamName, runnerID),
	}
}

type screamerMetrics struct {
	recordsProcessedTotal          *prometheus.CounterVec
	recordsProcessingErrorsTotal   *prometheus.CounterVec
	consumerDurationSeconds        *prometheus.HistogramVec
	activePartitions               prometheus.Gauge
	partitionWatermarkSeconds      *prometheus.GaugeVec
	runnerHeartbeatsTotal          prometheus.Counter
	spannerQueryDurationSeconds    *prometheus.HistogramVec
	childPartitionsCreatedTotal    *prometheus.CounterVec
}

func newScreamerMetrics(reg prometheus.Registerer, streamName, runnerID string) *screamerMetrics {
	metrics := &screamerMetrics{
		recordsProcessedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "screamer_records_processed_total",
				Help:        "Total number of records processed successfully by the consumer.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{},
		),
		recordsProcessingErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "screamer_records_processing_errors_total",
				Help:        "Total number of errors encountered during record processing.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{},
		),
		consumerDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "screamer_consumer_duration_seconds",
				Help:        "Duration of each consumer.Consume() call.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{},
		),
		activePartitions: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        "screamer_active_partitions",
				Help:        "Number of partitions a runner is currently processing.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
		),
		partitionWatermarkSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "screamer_partition_watermark_seconds",
				Help:        "Latest watermark for a partition, in Unix timestamp seconds.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{"partition_token"},
		),
		runnerHeartbeatsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:        "screamer_runner_heartbeats_total",
				Help:        "Total number of successful runner heartbeats.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
		),
		spannerQueryDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "screamer_spanner_query_duration_seconds",
				Help:        "Duration of spannerClient.Single().QueryWithOptions() calls.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{},
		),
		childPartitionsCreatedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "screamer_child_partitions_created_total",
				Help:        "Total number of child partitions created successfully.",
				ConstLabels: prometheus.Labels{"stream_name": streamName, "runner_id": runnerID},
			},
			[]string{"parent_partition_token"},
		),
	}
	reg.MustRegister(metrics.recordsProcessedTotal)
	reg.MustRegister(metrics.recordsProcessingErrorsTotal)
	reg.MustRegister(metrics.consumerDurationSeconds)
	reg.MustRegister(metrics.activePartitions)
	reg.MustRegister(metrics.partitionWatermarkSeconds)
	reg.MustRegister(metrics.runnerHeartbeatsTotal)
	reg.MustRegister(metrics.spannerQueryDurationSeconds)
	reg.MustRegister(metrics.childPartitionsCreatedTotal)
	return metrics
}

// Subscribe starts subscribing to the change stream and processing records using the provided Consumer.
// This method blocks until all partitions are processed or the context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
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
					s.metrics.runnerHeartbeatsTotal.Inc()
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

	return s.eg.Wait()
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
		s.metrics.activePartitions.Inc()
		s.eg.Go(func() error {
			defer s.metrics.activePartitions.Dec()
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

	start := time.Now()
	iter := s.spannerClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.spannerRequestPriority})
	defer func() {
		s.metrics.spannerQueryDurationSeconds.WithLabelValues().Observe(time.Since(start).Seconds())
	}()
	if err := iter.Do(func(r *spanner.Row) error {
		records := []*ChangeRecord{}
		if err := r.Columns(&records); err != nil {
			return err
		}
		if err := s.handle(ctx, p, records); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

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
		for _, record := range cr.DataChangeRecords {
			out, err := json.Marshal(record.DecodeToNonSpannerType())
			if err != nil {
				s.metrics.recordsProcessingErrorsTotal.WithLabelValues().Inc()
				return err
			}
			consumerStart := time.Now()
			if s.serializedConsumer {
				s.consumerMu.Lock()
				err = s.consumer.Consume(out)
				s.consumerMu.Unlock()
			} else {
				err = s.consumer.Consume(out)
			}
			s.metrics.consumerDurationSeconds.WithLabelValues().Observe(time.Since(consumerStart).Seconds())
			if err != nil {
				s.metrics.recordsProcessingErrorsTotal.WithLabelValues().Inc()
				return err
			}
			s.metrics.recordsProcessedTotal.WithLabelValues().Inc()
			watermarker.set(record.CommitTimestamp)
		}
		for _, record := range cr.HeartbeatRecords {
			watermarker.set(record.Timestamp)
		}
		for _, record := range cr.ChildPartitionsRecords {
			if err := s.partitionStorage.AddChildPartitions(ctx, p, record); err != nil {
				return fmt.Errorf("failed to add child partitions: %w", err)
			}
			s.metrics.childPartitionsCreatedTotal.WithLabelValues(p.PartitionToken).Inc()
			watermarker.set(record.StartTimestamp)
		}
	}

	watermark := watermarker.get()
	if err := s.partitionStorage.UpdateWatermark(ctx, p, watermark); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}
	s.metrics.partitionWatermarkSeconds.WithLabelValues(p.PartitionToken).Set(float64(watermark.Unix()))

	return nil
}
