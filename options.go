package screamer

import (
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

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

type (
	withStartTimestamp         time.Time
	withEndTimestamp           time.Time
	withLogLevel               zerolog.Level
	withHeartbeatInterval      time.Duration
	withSpannerRequestPriotiry spannerpb.RequestOptions_Priority
	withSerializedConsumer     bool
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
