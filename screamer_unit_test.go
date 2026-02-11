package screamer

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer/mocks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestWithLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		expected zerolog.Level
	}{
		{"debug", "debug", zerolog.DebugLevel},
		{"info", "info", zerolog.InfoLevel},
		{"warn", "warn", zerolog.WarnLevel},
		{"error", "error", zerolog.ErrorLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &config{}
			WithLogLevel(tt.level).Apply(c)
			require.Equal(t, tt.expected, c.logLevel)
		})
	}
}

func TestWithMaxConcurrentPartitions(t *testing.T) {
	tests := []struct {
		limit    int
		expected int
	}{
		{0, 0},
		{10, 10},
		{100, 100},
		{1000, 1000},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("limit_%d", tt.limit), func(t *testing.T) {
			c := &config{}
			WithMaxConcurrentPartitions(tt.limit).Apply(c)
			require.Equal(t, tt.expected, c.maxConcurrentPartitions)
		})
	}
}

func TestPartitionTracking(t *testing.T) {
	t.Run("basic_operations", func(t *testing.T) {
		s := &Subscriber{activePartitions: make(map[string]struct{})}

		// Add partitions
		s.markPartitionActive("p1")
		s.markPartitionActive("p2")
		require.Equal(t, 2, s.getRunningPartitionCount())

		// Idempotent add
		s.markPartitionActive("p1")
		require.Equal(t, 2, s.getRunningPartitionCount())

		// Remove partitions
		s.markPartitionInactive("p1")
		require.Equal(t, 1, s.getRunningPartitionCount())

		s.markPartitionInactive("p2")
		require.Equal(t, 0, s.getRunningPartitionCount())

		// Idempotent remove
		s.markPartitionInactive("p3")
		require.Equal(t, 0, s.getRunningPartitionCount())
	})

	t.Run("concurrent_operations", func(t *testing.T) {
		s := &Subscriber{activePartitions: make(map[string]struct{})}

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				token := fmt.Sprintf("p%d", id)
				s.markPartitionActive(token)
				time.Sleep(time.Millisecond)
				s.markPartitionInactive(token)
			}(i)
		}

		wg.Wait()
		require.Equal(t, 0, s.getRunningPartitionCount())
	})
}

func TestGetAvailablePartitionSlots(t *testing.T) {
	t.Run("no_limit", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 0,
			activePartitions:        make(map[string]struct{}),
		}

		require.Equal(t, 100, s.getAvailablePartitionSlots(), "Should return default batch size")

		s.markPartitionActive("p1")
		s.markPartitionActive("p2")
		require.Equal(t, 100, s.getAvailablePartitionSlots(), "Should still return default")
	})

	t.Run("with_limit", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		require.Equal(t, 50, s.getAvailablePartitionSlots())

		// Add 20 active partitions
		for i := 0; i < 20; i++ {
			s.markPartitionActive(fmt.Sprintf("p%d", i))
		}
		require.Equal(t, 30, s.getAvailablePartitionSlots())

		// At capacity
		for i := 20; i < 50; i++ {
			s.markPartitionActive(fmt.Sprintf("p%d", i))
		}
		require.Equal(t, 0, s.getAvailablePartitionSlots())

		// Over capacity
		s.markPartitionActive("p100")
		require.Equal(t, 0, s.getAvailablePartitionSlots())
	})
}

func TestGetMetrics(t *testing.T) {
	t.Run("unlimited", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 0,
			activePartitions:        make(map[string]struct{}),
		}

		s.markPartitionActive("p1")
		s.markPartitionActive("p2")
		s.markPartitionActive("p3")

		m := s.GetMetrics()
		require.Equal(t, 3, m.ActivePartitions)
		require.Equal(t, 0, m.MaxConcurrentPartitions)
		require.Equal(t, 100, m.AvailableSlots)
		require.Equal(t, float64(-1), m.CapacityUsedPercent)
	})

	t.Run("limited", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 100,
			activePartitions:        make(map[string]struct{}),
		}

		for i := 0; i < 25; i++ {
			s.markPartitionActive(fmt.Sprintf("p%d", i))
		}

		m := s.GetMetrics()
		require.Equal(t, 25, m.ActivePartitions)
		require.Equal(t, 100, m.MaxConcurrentPartitions)
		require.Equal(t, 75, m.AvailableSlots)
		require.Equal(t, float64(25), m.CapacityUsedPercent)
	})

	t.Run("at_capacity", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		for i := 0; i < 50; i++ {
			s.markPartitionActive(fmt.Sprintf("p%d", i))
		}

		m := s.GetMetrics()
		require.Equal(t, 50, m.ActivePartitions)
		require.Equal(t, 50, m.MaxConcurrentPartitions)
		require.Equal(t, 0, m.AvailableSlots)
		require.Equal(t, float64(100), m.CapacityUsedPercent)
	})

	t.Run("over_capacity", func(t *testing.T) {
		s := &Subscriber{
			maxConcurrentPartitions: 50,
			activePartitions:        make(map[string]struct{}),
		}

		for i := 0; i < 60; i++ {
			s.markPartitionActive(fmt.Sprintf("p%d", i))
		}

		m := s.GetMetrics()
		require.Equal(t, 60, m.ActivePartitions)
		require.Equal(t, 50, m.MaxConcurrentPartitions)
		require.Equal(t, 0, m.AvailableSlots)
		require.Equal(t, float64(120), m.CapacityUsedPercent)
	})
}

func TestMetricsLoggingInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf).With().Timestamp().Logger()
		oldLogger := log.Logger
		log.Logger = logger
		defer func() { log.Logger = oldLogger }()

		zerolog.SetGlobalLevel(zerolog.InfoLevel)

		storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
		storage.partitions[RootPartitionToken] = &PartitionMetadata{
			PartitionToken:  RootPartitionToken,
			State:           StateCreated,
			Watermark:       time.Now(),
			StartTimestamp:  time.Now(),
			EndTimestamp:    time.Now().Add(24 * time.Hour),
			HeartbeatMillis: 3000,
		}

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

		subscriber.markPartitionActive("p1")
		subscriber.markPartitionActive("p2")
		subscriber.markPartitionActive("p3")

		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		go subscriber.Subscribe(ctx, subscriber.consumer)
		time.Sleep(31 * time.Second)

		logOutput := buf.String()
		require.Contains(t, logOutput, "partition processing metrics")
		require.Contains(t, logOutput, "test-runner")
		require.Contains(t, logOutput, "active_partitions")
		require.Contains(t, logOutput, "max_concurrent")

		metricsCount := strings.Count(logOutput, "partition processing metrics")
		require.GreaterOrEqual(t, metricsCount, 1)

		cancel()
	})
}

// Mock partition storage for testing
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
