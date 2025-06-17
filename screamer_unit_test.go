package screamer

import (
	"testing"

	"github.com/rs/zerolog"
)

// Test for the WithLogLevel function
func TestWithLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		expected zerolog.Level
	}{
		{"debug level", "debug", zerolog.DebugLevel},
		{"info level", "info", zerolog.InfoLevel},
		{"warning level", "warn", zerolog.WarnLevel},
		{"error level", "error", zerolog.ErrorLevel},
		{"invalid level", "invalid", zerolog.InfoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &config{}
			WithLogLevel(tt.level).Apply(c)
			if c.logLevel != tt.expected {
				t.Errorf("expected log level %v, got %v", tt.expected, c.logLevel)
			}
		})
	}
}
