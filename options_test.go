package screamer

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestWithStartTimestamp(t *testing.T) {
	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	option := WithStartTimestamp(timestamp)

	c := &config{}
	option.Apply(c)

	assert.Equal(t, timestamp, c.startTimestamp)
}

func TestWithEndTimestamp(t *testing.T) {
	timestamp := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	option := WithEndTimestamp(timestamp)

	c := &config{}
	option.Apply(c)

	assert.Equal(t, timestamp, c.endTimestamp)
}

func TestWithHeartbeatInterval(t *testing.T) {
	interval := 5 * time.Second
	option := WithHeartbeatInterval(interval)

	c := &config{}
	option.Apply(c)

	assert.Equal(t, interval, c.heartbeatInterval)
}

func TestWithSpannerRequestPriority(t *testing.T) {
	tests := []struct {
		name     string
		priority spannerpb.RequestOptions_Priority
	}{
		{
			name:     "high priority",
			priority: spannerpb.RequestOptions_PRIORITY_HIGH,
		},
		{
			name:     "medium priority",
			priority: spannerpb.RequestOptions_PRIORITY_MEDIUM,
		},
		{
			name:     "low priority",
			priority: spannerpb.RequestOptions_PRIORITY_LOW,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := WithSpannerRequestPriority(tt.priority)
			c := &config{}
			option.Apply(c)
			assert.Equal(t, tt.priority, c.spannerRequestPriority)
		})
	}
}

func TestWithSpannerRequestPriotiry_Deprecated(t *testing.T) {
	// Test the deprecated function still works for backward compatibility
	priority := spannerpb.RequestOptions_PRIORITY_HIGH
	option := WithSpannerRequestPriotiry(priority)
	c := &config{}
	option.Apply(c)
	assert.Equal(t, priority, c.spannerRequestPriority)
}

func TestWithSerializedConsumer(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		option := WithSerializedConsumer(true)
		c := &config{}
		option.Apply(c)
		assert.True(t, c.serializedConsumer)
	})

	t.Run("disabled", func(t *testing.T) {
		option := WithSerializedConsumer(false)
		c := &config{}
		option.Apply(c)
		assert.False(t, c.serializedConsumer)
	})
}

func TestWithMaxConnections(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"small value", 10},
		{"default value", 400},
		{"large value", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := WithMaxConnections(tt.value)
			c := &config{}
			option.Apply(c)
			assert.Equal(t, tt.value, c.maxConnections)
		})
	}
}

func TestWithRebalancingInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{"short interval", 10 * time.Second},
		{"medium interval", 1 * time.Minute},
		{"long interval", 5 * time.Minute},
		{"zero disables rebalancing", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := WithRebalancingInterval(tt.interval)
			c := &config{}
			option.Apply(c)
			assert.Equal(t, tt.interval, c.rebalancingInterval)
		})
	}
}

func TestWithLeaseDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
	}{
		{"short duration", 15 * time.Second},
		{"default duration", 30 * time.Second},
		{"long duration", 2 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := WithLeaseDuration(tt.duration)
			c := &config{}
			option.Apply(c)
			assert.Equal(t, tt.duration, c.leaseDuration)
		})
	}
}

func TestWithLogLevel_Valid(t *testing.T) {
	tests := []struct {
		name          string
		level         string
		expectedLevel zerolog.Level
	}{
		{"debug", "debug", zerolog.DebugLevel},
		{"info", "info", zerolog.InfoLevel},
		{"warn", "warn", zerolog.WarnLevel},
		{"error", "error", zerolog.ErrorLevel},
		{"trace", "trace", zerolog.TraceLevel},
		{"fatal", "fatal", zerolog.FatalLevel},
		{"panic", "panic", zerolog.PanicLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			option := WithLogLevel(tt.level)
			c := &config{}
			option.Apply(c)
			assert.Equal(t, tt.expectedLevel, c.logLevel)
		})
	}
}

func TestWithLogLevel_Invalid(t *testing.T) {
	// Invalid log levels should default to info
	option := WithLogLevel("invalid-level")
	c := &config{}
	option.Apply(c)
	assert.Equal(t, zerolog.InfoLevel, c.logLevel)
}

func TestMultipleOptions(t *testing.T) {
	// Test applying multiple options together
	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	heartbeat := 7 * time.Second
	maxConn := 250
	leaseDur := 45 * time.Second

	c := &config{}

	options := []Option{
		WithStartTimestamp(startTime),
		WithEndTimestamp(endTime),
		WithHeartbeatInterval(heartbeat),
		WithMaxConnections(maxConn),
		WithLeaseDuration(leaseDur),
		WithSerializedConsumer(true),
		WithLogLevel("debug"),
	}

	for _, opt := range options {
		opt.Apply(c)
	}

	assert.Equal(t, startTime, c.startTimestamp)
	assert.Equal(t, endTime, c.endTimestamp)
	assert.Equal(t, heartbeat, c.heartbeatInterval)
	assert.Equal(t, maxConn, c.maxConnections)
	assert.Equal(t, leaseDur, c.leaseDuration)
	assert.True(t, c.serializedConsumer)
	assert.Equal(t, zerolog.DebugLevel, c.logLevel)
}

func TestConfigDefaults(t *testing.T) {
	// Test that config has proper zero values
	c := &config{}

	assert.True(t, c.startTimestamp.IsZero())
	assert.True(t, c.endTimestamp.IsZero())
	assert.Equal(t, time.Duration(0), c.heartbeatInterval)
	assert.Equal(t, 0, c.maxConnections)
	assert.Equal(t, time.Duration(0), c.leaseDuration)
	assert.False(t, c.serializedConsumer)
	assert.Equal(t, zerolog.Level(0), c.logLevel)
}
