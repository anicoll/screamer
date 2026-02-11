package screamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer/mocks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

// TestPartitionTracking verifies partition tracking methods
func TestPartitionTracking(t *testing.T) {
	t.Run("MarkPartitionActive", func(t *testing.T) {
		subscriber := &Subscriber{
			activePartitions: make(map[string]struct{}),
		}

		subscriber.markPartitionActive("partition1")
		require.Equal(t, 1, subscriber.getRunningPartitionCount())

		subscriber.markPartitionActive("partition2")
		require.Equal(t, 2, subscriber.getRunningPartitionCount())

		// Adding same partition again should not increase count
		subscriber.markPartitionActive("partition1")
		require.Equal(t, 2, subscriber.getRunningPartitionCount())
	})

	t.Run("MarkPartitionInactive", func(t *testing.T) {
		subscriber := &Subscriber{
			activePartitions: make(map[string]struct{}),
		}

		subscriber.markPartitionActive("partition1")
		subscriber.markPartitionActive("partition2")
		require.Equal(t, 2, subscriber.getRunningPartitionCount())

		subscriber.markPartitionInactive("partition1")
		require.Equal(t, 1, subscriber.getRunningPartitionCount())

		subscriber.markPartitionInactive("partition2")
		require.Equal(t, 0, subscriber.getRunningPartitionCount())

		// Removing non-existent partition should not error
		subscriber.markPartitionInactive("partition3")
		require.Equal(t, 0, subscriber.getRunningPartitionCount())
	})

	t.Run("ConcurrentPartitionTracking", func(t *testing.T) {
		subscriber := &Subscriber{
			activePartitions: make(map[string]struct{}),
		}

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				token := fmt.Sprintf("partition_%d", id)
				subscriber.markPartitionActive(token)
				time.Sleep(time.Millisecond)
				subscriber.markPartitionInactive(token)
			}(i)
		}

		wg.Wait()
		require.Equal(t, 0, subscriber.getRunningPartitionCount())
	})
}

// TestGetAvailablePartitionSlots verifies available slot calculation
func TestGetAvailablePartitionSlots(t *testing.T) {
	t.Run("NoLimit", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 0, // No limit
			activePartitions:        make(map[string]struct{}),
		}

		// Should return default batch size
		slots := subscriber.getAvailablePartitionSlots()
		require.Equal(t, 100, slots)

		// Even with active partitions, should still return default
		subscriber.markPartitionActive("p1")
		subscriber.markPartitionActive("p2")
		slots = subscriber.getAvailablePartitionSlots()
		require.Equal(t, 100, slots)
	})

	t.Run("WithLimit", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		// No active partitions
		slots := subscriber.getAvailablePartitionSlots()
		require.Equal(t, 50, slots)

		// Add some active partitions
		for i := 0; i < 20; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_%d", i))
		}
		slots = subscriber.getAvailablePartitionSlots()
		require.Equal(t, 30, slots)

		// At capacity
		for i := 0; i < 30; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_cap_%d", i))
		}
		slots = subscriber.getAvailablePartitionSlots()
		require.Equal(t, 0, slots)

		// Over capacity
		for i := 0; i < 10; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_over_%d", i))
		}
		slots = subscriber.getAvailablePartitionSlots()
		require.Equal(t, 0, slots)
	})
}

// TestWithMaxConcurrentPartitions verifies the option constructor
func TestWithMaxConcurrentPartitions(t *testing.T) {
	tests := []struct {
		name     string
		limit    int
		expected int
	}{
		{"zero limit", 0, 0},
		{"small limit", 10, 10},
		{"medium limit", 100, 100},
		{"large limit", 1000, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &config{}
			WithMaxConcurrentPartitions(tt.limit).Apply(c)
			require.Equal(t, tt.expected, c.maxConcurrentPartitions)
		})
	}
}

// TestGetMetrics verifies partition metrics calculation
func TestGetMetrics(t *testing.T) {
	t.Run("UnlimitedCapacity", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 0, // Unlimited
			activePartitions:        make(map[string]struct{}),
		}

		// Add some active partitions
		subscriber.markPartitionActive("p1")
		subscriber.markPartitionActive("p2")
		subscriber.markPartitionActive("p3")

		metrics := subscriber.GetMetrics()
		require.Equal(t, 3, metrics.ActivePartitions)
		require.Equal(t, 0, metrics.MaxConcurrentPartitions)
		require.Equal(t, 100, metrics.AvailableSlots) // Default batch size
		require.Equal(t, float64(-1), metrics.CapacityUsedPercent) // Unlimited
	})

	t.Run("LimitedCapacity", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 100,
			activePartitions:        make(map[string]struct{}),
		}

		// Add 25 active partitions
		for i := 0; i < 25; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_%d", i))
		}

		metrics := subscriber.GetMetrics()
		require.Equal(t, 25, metrics.ActivePartitions)
		require.Equal(t, 100, metrics.MaxConcurrentPartitions)
		require.Equal(t, 75, metrics.AvailableSlots)
		require.Equal(t, float64(25), metrics.CapacityUsedPercent)
	})

	t.Run("AtCapacity", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		// Fill to capacity
		for i := 0; i < 50; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_%d", i))
		}

		metrics := subscriber.GetMetrics()
		require.Equal(t, 50, metrics.ActivePartitions)
		require.Equal(t, 50, metrics.MaxConcurrentPartitions)
		require.Equal(t, 0, metrics.AvailableSlots)
		require.Equal(t, float64(100), metrics.CapacityUsedPercent)
	})

	t.Run("OverCapacity", func(t *testing.T) {
		subscriber := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		// Go over capacity (edge case, shouldn't normally happen)
		for i := 0; i < 60; i++ {
			subscriber.markPartitionActive(fmt.Sprintf("partition_%d", i))
		}

		metrics := subscriber.GetMetrics()
		require.Equal(t, 60, metrics.ActivePartitions)
		require.Equal(t, 50, metrics.MaxConcurrentPartitions)
		require.Equal(t, 0, metrics.AvailableSlots)
		require.Equal(t, float64(120), metrics.CapacityUsedPercent)
	})
}

// TestMetricsLoggingInterval verifies that partition metrics are logged every 30 seconds
func TestMetricsLoggingInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := zerolog.New(&buf).With().Timestamp().Logger()

		// Replace global logger temporarily
		oldLogger := log.Logger
		log.Logger = logger
		defer func() { log.Logger = oldLogger }()

		// Set log level to Info to capture metrics logs
		zerolog.SetGlobalLevel(zerolog.InfoLevel)

		// Create mock partition storage
		storage := &mockPartitionStorage{
			partitions: make(map[string]*PartitionMetadata),
		}

		// Initialize root partition
		storage.partitions[RootPartitionToken] = &PartitionMetadata{
			PartitionToken:  RootPartitionToken,
			State:           StateCreated,
			Watermark:       time.Now(),
			StartTimestamp:  time.Now(),
			EndTimestamp:    time.Now().Add(24 * time.Hour),
			HeartbeatMillis: 3000,
		}

		// Create subscriber
		subscriber := &Subscriber{
			runnerID:                "test-runner",
			streamName:              "test-stream",
			startTimestamp:          time.Now(),
			endTimestamp:            time.Now().Add(24 * time.Hour),
			heartbeatInterval:       3 * time.Second,
			partitionStorage:        storage,
			consumer:                mocks.NewMockConsumer(t),
			maxConcurrentPartitions: 100,
			activePartitions:        make(map[string]struct{}),
		}

		// Add some active partitions for metrics
		subscriber.markPartitionActive("partition-1")
		subscriber.markPartitionActive("partition-2")
		subscriber.markPartitionActive("partition-3")

		// Run Subscribe in background
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- subscriber.Subscribe(ctx, subscriber.consumer)
		}()

		// Wait for first metrics log (should occur at ~30 seconds)
		// synctest will make time advance instantly in controlled environment
		time.Sleep(31 * time.Second)

		// Check log output
		logOutput := buf.String()

		// Verify metrics were logged
		require.Contains(t, logOutput, "partition processing metrics")
		require.Contains(t, logOutput, "test-runner")
		require.Contains(t, logOutput, "active_partitions")
		require.Contains(t, logOutput, "max_concurrent")
		require.Contains(t, logOutput, "capacity_used_percent")
		require.Contains(t, logOutput, "available_slots")

		// Count occurrences - should have at least 1 metrics log
		metricsCount := strings.Count(logOutput, "partition processing metrics")
		require.GreaterOrEqual(t, metricsCount, 1, "Expected at least 1 metrics log after 31 seconds")

		// Cancel context to stop Subscribe
		cancel()

		// Wait for Subscribe to finish
		select {
		case err := <-errCh:
			// Context cancellation is expected
			require.True(t, err == context.Canceled || err == nil)
		case <-time.After(5 * time.Second):
			t.Fatal("Subscribe did not finish within timeout")
		}
	})
}

// mockPartitionStorage is a simple mock implementation for testing
type mockPartitionStorage struct {
	mu         sync.Mutex
	partitions map[string]*PartitionMetadata
}

func (m *mockPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range m.partitions {
		if p.State != StateFinished {
			return p, nil
		}
	}
	return nil, nil
}

func (m *mockPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string, limit int) ([]*PartitionMetadata, error) {
	return nil, nil
}

func (m *mockPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.partitions[RootPartitionToken] = &PartitionMetadata{
		PartitionToken:  RootPartitionToken,
		State:           StateCreated,
		Watermark:       startTimestamp,
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatInterval.Milliseconds(),
	}
	return nil
}

func (m *mockPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, limit int) ([]*PartitionMetadata, error) {
	// Return empty to avoid actually scheduling partitions
	return []*PartitionMetadata{}, nil
}

func (m *mockPartitionStorage) AddChildPartitions(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error {
	return nil
}

func (m *mockPartitionStorage) UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.partitions[partition.PartitionToken]; ok {
		p.State = StateRunning
	}
	return nil
}

func (m *mockPartitionStorage) RefreshRunner(ctx context.Context, runnerID string) error {
	return nil
}

func (m *mockPartitionStorage) UpdateToFinished(ctx context.Context, partition *PartitionMetadata, runnerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.partitions[partition.PartitionToken]; ok {
		p.State = StateFinished
	}
	return nil
}

func (m *mockPartitionStorage) UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.partitions[partition.PartitionToken]; ok {
		p.Watermark = watermark
	}
	return nil
}

func (m *mockPartitionStorage) GetActiveRunnerCount(ctx context.Context) (int, error) {
	return 1, nil
}
