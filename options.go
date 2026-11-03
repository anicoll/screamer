package screamer

import (
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric"
)

// Option configures a Subscriber via functional options.
type Option interface {
	Apply(*config)
}

type config struct {
	startTimestamp          time.Time
	endTimestamp            time.Time
	heartbeatInterval       time.Duration
	spannerRequestPriority  spannerpb.RequestOptions_Priority
	serializedConsumer      bool
	logLevel                zerolog.Level
	maxConcurrentPartitions int
	ackTimeout              time.Duration
	changeStreamReader      ChangeStreamReader
	partitionMaxRetries     int
	meterProvider           metric.MeterProvider
	parentGating            bool
	checkpointInterval      time.Duration
	leases                  bool
	leaseTTL                time.Duration
	parking                 bool
	parkInterval            time.Duration
	parkIdleAfter           time.Duration
}

type (
	withStartTimestamp          time.Time
	withEndTimestamp            time.Time
	withLogLevel                zerolog.Level
	withHeartbeatInterval       time.Duration
	withSpannerRequestPriotiry  spannerpb.RequestOptions_Priority
	withSerializedConsumer      bool
	withMaxConcurrentPartitions int
	withAckTimeout              time.Duration
)

func (o withStartTimestamp) Apply(c *config) {
	c.startTimestamp = time.Time(o)
}

// WithStartTimestamp sets the start timestamp option for reading change streams.
// The value must be within the retention period of the change stream and before the current time.
// Default value is current timestamp.
func WithStartTimestamp(startTimestamp time.Time) Option {
	return withStartTimestamp(startTimestamp)
}

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

func (o withLogLevel) Apply(c *config) {
	c.logLevel = zerolog.Level(o)
}

// WithEndTimestamp sets the end timestamp option for reading change streams.
// The value must be within the retention period of the change stream and must be after the start timestamp.
// If not set, reads latest changes until canceled.
func WithEndTimestamp(endTimestamp time.Time) Option {
	return withEndTimestamp(endTimestamp)
}

func (o withHeartbeatInterval) Apply(c *config) {
	c.heartbeatInterval = time.Duration(o)
}

// WithHeartbeatInterval sets the heartbeat interval for reading change streams.
// Default value is 10 seconds.
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option {
	return withHeartbeatInterval(heartbeatInterval)
}

func (o withSpannerRequestPriotiry) Apply(c *config) {
	c.spannerRequestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithSpannerRequestPriotiry sets the request priority option for reading change streams.
// Default value is unspecified, equivalent to high.
func WithSpannerRequestPriotiry(priority spannerpb.RequestOptions_Priority) Option {
	return withSpannerRequestPriotiry(priority)
}

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

func (o withMaxConcurrentPartitions) Apply(c *config) {
	c.maxConcurrentPartitions = int(o)
}

// WithMaxConcurrentPartitions sets the maximum number of partitions that can be processed concurrently by a single runner.
// This helps prevent resource exhaustion when dealing with large numbers of partitions.
// A value of 0 (default) means no limit, maintaining backward compatibility.
// Recommended value: 100 for production workloads.
func WithMaxConcurrentPartitions(max int) Option {
	return withMaxConcurrentPartitions(max)
}

func (o withAckTimeout) Apply(c *config) {
	c.ackTimeout = time.Duration(o)
}

// WithAckTimeout sets the maximum duration the subscriber will wait for a consumer to
// acknowledge each batch of records when using ConsumerWithAck.
// If the timeout elapses before all acks in a batch are received, the subscriber returns
// an error and stops processing that partition.
// A value of 0 (default) disables the timeout — the subscriber waits indefinitely.
func WithAckTimeout(d time.Duration) Option {
	return withAckTimeout(d)
}

type withChangeStreamReader struct{ r ChangeStreamReader }

func (o withChangeStreamReader) Apply(c *config) {
	c.changeStreamReader = o.r
}

// WithChangeStreamReader replaces the reader used to query change stream partitions.
// By default the subscriber queries Spanner's READ_<stream> function with the client
// passed to NewSubscriber. Use this option to run the subscriber against a simulated
// change stream (see pkg/simulator) or to wrap the default behaviour.
func WithChangeStreamReader(r ChangeStreamReader) Option {
	return withChangeStreamReader{r: r}
}

const defaultPartitionMaxRetries = 5

type withPartitionMaxRetries int

func (o withPartitionMaxRetries) Apply(c *config) {
	c.partitionMaxRetries = int(o)
}

// WithPartitionMaxRetries sets how many times in a row a failing partition is retried
// before its error stops the subscriber. Retries resume from the partition's last
// watermark with exponential backoff (1s doubling to 30s), and the count resets when
// the partition makes progress. Errors from the consumer (a failed Consume, a nack or
// an ack timeout) are never retried.
// Default is 5. 0 disables retries: any partition error stops the subscriber.
func WithPartitionMaxRetries(n int) Option {
	if n < 0 {
		n = 0
	}
	return withPartitionMaxRetries(n)
}

type withMeterProvider struct{ mp metric.MeterProvider }

func (o withMeterProvider) Apply(c *config) {
	c.meterProvider = o.mp
}

// WithMeterProvider sets the OpenTelemetry MeterProvider used for the subscriber's
// metrics. Default: the global provider (otel.GetMeterProvider()), which records
// nothing unless the application installs one. Screamer ships no exporter: use the
// OpenTelemetry Prometheus exporter or an OTLP exporter in your application.
//
// Per-runner metrics: screamer.partitions.active, screamer.partitions.capacity,
// screamer.partition.retries. Cluster-wide metrics, the same on every runner (take
// the max, not the sum): screamer.partitions.unfinished, screamer.runners.live,
// screamer.runners.desired, screamer.watermark.lag.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return withMeterProvider{mp: mp}
}

type withParentGating bool

func (o withParentGating) Apply(c *config) {
	c.parentGating = bool(o)
}

// WithParentGating sets whether a child partition waits for all of its parents
// to finish before it is read. It is on by default, which keeps changes to each
// key in commit order across partition splits and merges, and needs a
// PartitionStorage that implements ParentGatedScheduler (both built-in storages
// do).
//
// Turning it off schedules a child as soon as it is reported, as releases
// before parent gating did. That can deliver changes to the same key out of
// order when a merged child's other parent is behind.
func WithParentGating(enabled bool) Option {
	return withParentGating(enabled)
}

type withCheckpointInterval time.Duration

func (o withCheckpointInterval) Apply(c *config) {
	c.checkpointInterval = time.Duration(o)
}

// WithCheckpointInterval sets how often partition watermarks are written to the
// PartitionStorage. Watermarks are buffered in memory and written together, so
// each partition is written at most once per interval however many batches it
// reads. Default is 1 second.
//
// After a crash, a partition resumes from its last written watermark, so up to
// one interval of changes can be delivered again. 0 writes the watermark after
// every batch, as releases before batched checkpoints did.
func WithCheckpointInterval(d time.Duration) Option {
	if d < 0 {
		d = 0
	}
	return withCheckpointInterval(d)
}

type withLeases bool

func (o withLeases) Apply(c *config) {
	c.leases = bool(o)
}

// WithLeases sets whether partitions are owned through leases with fencing. It
// is on by default, and needs a PartitionStorage that implements
// PartitionLeaser (both built-in storages do; the Spanner one needs the columns
// that RunMigrations adds, see MIGRATION.md).
//
// With leases, a runner keeps a partition until its lease expires (see
// WithLeaseTTL), and a runner that loses a lease while paused stops reading
// that partition and writes nothing more for it. Turning leases off goes back
// to taking over a partition whose runner has not refreshed for 3 seconds, as
// releases before leases did.
func WithLeases(enabled bool) Option {
	return withLeases(enabled)
}

type withLeaseTTL time.Duration

func (o withLeaseTTL) Apply(c *config) {
	c.leaseTTL = time.Duration(o)
}

// WithLeaseTTL sets how long a partition lease lasts without being renewed.
// Default is 30 seconds; values under 3 seconds are raised to 3 seconds.
//
// Leases are renewed every third of the TTL, together with the checkpoint
// writes, so a pause or a metadata outage shorter than about two thirds of the
// TTL changes nothing. When a runner dies without releasing its leases, other
// runners take its partitions over after one TTL. When it stops cleanly, it
// releases them and they are taken over within about a second.
//
// The checkpoint interval (see WithCheckpointInterval) is capped at a third of
// the TTL, so renewals are never late.
func WithLeaseTTL(d time.Duration) Option {
	if d < minLeaseTTL {
		d = minLeaseTTL
	}
	return withLeaseTTL(d)
}

type withParking bool

func (o withParking) Apply(c *config) {
	c.parking = bool(o)
}

// WithParking sets whether idle partitions are parked. It is on by default,
// and needs leases (see WithLeases) and a PartitionStorage that implements
// PartitionParker (both built-in storages do).
//
// A partition that delivers nothing but heartbeats for WithParkIdleAfter (one
// minute by default) is parked: its query is closed and its lease released.
// Every WithParkInterval (10 seconds by default) any runner reads it up to the
// current time. If that finds changes, the partition is streamed again;
// otherwise it is parked again. Parked partitions hold no query and no slot of
// WithMaxConcurrentPartitions, so a runner can look after far more partitions
// than it can stream at once.
//
// The cost is latency: a change to a parked partition is delivered up to one
// park interval later than it would be if the partition were streaming.
// Turning parking off streams every partition, as releases before parking did.
func WithParking(enabled bool) Option {
	return withParking(enabled)
}

type withParkInterval time.Duration

func (o withParkInterval) Apply(c *config) {
	c.parkInterval = time.Duration(o)
}

// WithParkInterval sets how long a parked partition waits between catch-up
// reads, and so the most extra delay a change to an idle partition sees.
// Default is 10 seconds; values under 1 second are raised to 1 second.
func WithParkInterval(d time.Duration) Option {
	if d < time.Second {
		d = time.Second
	}
	return withParkInterval(d)
}

type withParkIdleAfter time.Duration

func (o withParkIdleAfter) Apply(c *config) {
	c.parkIdleAfter = time.Duration(o)
}

// WithParkIdleAfter sets how long a streaming partition delivers nothing but
// heartbeats before it is parked. Default is one minute; values under twice
// the heartbeat interval are raised to that when the subscriber starts.
func WithParkIdleAfter(d time.Duration) Option {
	return withParkIdleAfter(d)
}
