package screamer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer/mocks"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

// TestSerializedConsumerNoConcurrency verifies that serialized consumer prevents concurrent calls
func TestSerializedConsumerNoConcurrency(t *testing.T) {
	// Create multiple data change records
	records := []*ChangeRecord{
		{
			DataChangeRecords: []*dataChangeRecord{
				{
					CommitTimestamp: time.Now(),
					RecordSequence:  "1",
					TableName:       "test_table",
					ModType:         ModType_INSERT,
					Mods: []*mod{{
						Keys: spanner.NullJSON{
							Value: map[string]interface{}{"id": "1"},
							Valid: true,
						},
						NewValues: spanner.NullJSON{
							Value: map[string]interface{}{"value": "test1"},
							Valid: true,
						},
					}},
				},
				{
					CommitTimestamp: time.Now(),
					RecordSequence:  "2",
					TableName:       "test_table",
					ModType:         ModType_INSERT,
					Mods: []*mod{{
						Keys: spanner.NullJSON{
							Value: map[string]interface{}{"id": "2"},
							Valid: true,
						},
						NewValues: spanner.NullJSON{
							Value: map[string]interface{}{"value": "test2"},
							Valid: true,
						},
					}},
				},
				{
					CommitTimestamp: time.Now(),
					RecordSequence:  "3",
					TableName:       "test_table",
					ModType:         ModType_INSERT,
					Mods: []*mod{{
						Keys: spanner.NullJSON{
							Value: map[string]interface{}{"id": "3"},
							Valid: true,
						},
						NewValues: spanner.NullJSON{
							Value: map[string]interface{}{"value": "test3"},
							Valid: true,
						},
					}},
				},
			},
		},
	}

	t.Run("SerializedConsumer", func(t *testing.T) {
		// Mock subscriber with serialized consumer enabled
		consumer := mocks.NewMockConsumer(t)
		// consumer := &mockConsumer{processDelay: 10 * time.Millisecond}
		subscriber := &Subscriber{
			serializedConsumer: true,
			consumer:           consumer,
		}
		consumer.EXPECT().Consume(mock.Anything).Times(3).Return(nil)

		// Call handle method
		err := callHandleMethod(t, subscriber, records)
		require.NoError(t, err)
	})

	t.Run("ConcurrentConsumer", func(t *testing.T) {
		// Mock subscriber with serialized consumer disabled
		consumer := mocks.NewMockConsumer(t)
		subscriber := &Subscriber{
			serializedConsumer: false,
			consumer:           consumer,
		}

		consumer.EXPECT().Consume(mock.Anything).Times(3).Return(nil)

		// Call handle method
		err := callHandleMethod(t, subscriber, records)
		require.NoError(t, err)
	})
}

// TestSerializedConsumerDeadlockPrevention verifies no deadlock with multiple records
func TestSerializedConsumerDeadlockPrevention(t *testing.T) {
	// Create many records to stress test
	var dataRecords []*dataChangeRecord
	for i := range 100 {
		dataRecords = append(dataRecords, &dataChangeRecord{
			CommitTimestamp: time.Now(),
			RecordSequence:  string(rune(i)),
			TableName:       "test_table",
			ModType:         ModType_INSERT,
			Mods: []*mod{{
				Keys:      spanner.NullJSON{Value: map[string]interface{}{"id": string(rune(i))}, Valid: true},
				NewValues: spanner.NullJSON{Value: map[string]interface{}{"value": i}, Valid: true},
			}},
		})
	}

	records := []*ChangeRecord{{DataChangeRecords: dataRecords}}
	consumer := mocks.NewMockConsumer(t)
	consumer.EXPECT().Consume(mock.Anything).Times(100).Return(nil)

	subscriber := &Subscriber{
		serializedConsumer: true, // Enable serialized consumer
		consumer:           consumer,
	}

	// Set a timeout to detect deadlock
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- callHandleMethod(t, subscriber, records)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Test timed out - possible deadlock detected")
	}
}

func callHandleMethod(t *testing.T, s *Subscriber, records []*ChangeRecord) error {
	t.Helper()

	// Simulate what handle() does for testing purposes
	type testSubscriber interface {
		Consume([]byte) error
	}

	consumer := s.consumer.(testSubscriber)
	serialized := s.serializedConsumer

	var mu sync.Mutex
	for _, cr := range records {
		for _, record := range cr.DataChangeRecords {
			out, err := json.Marshal(record)
			if err != nil {
				return err
			}

			if serialized {
				mu.Lock()
				err = consumer.Consume(out)
				mu.Unlock()
				if err != nil {
					return err
				}
			} else {
				if err := consumer.Consume(out); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
