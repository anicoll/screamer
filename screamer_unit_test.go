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

// safeBuffer is a thread-safe bytes.Buffer wrapper
type safeBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (sb *safeBuffer) Write(p []byte) (n int, err error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *safeBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.String()
}

func TestMetricsLoggingInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buf := &safeBuffer{}
		logger := zerolog.New(buf).With().Timestamp().Logger()
		oldLogger := log.Logger
		log.Logger = logger
		defer func() {
			// Restore logger only after all goroutines are done
			log.Logger = oldLogger
		}()

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
			consumer:                NewMockConsumer(t),
			maxConcurrentPartitions: 100,
			activePartitions:        make(map[string]struct{}),
		}

		subscriber.markPartitionActive("p1")
		subscriber.markPartitionActive("p2")
		subscriber.markPartitionActive("p3")

		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)

		done := make(chan struct{})
		go func() {
			defer close(done)
			subscriber.Subscribe(ctx, subscriber.consumer)
		}()

		time.Sleep(31 * time.Second)

		logOutput := buf.String()
		require.Contains(t, logOutput, "partition processing metrics")
		require.Contains(t, logOutput, "test-runner")
		require.Contains(t, logOutput, "active_partitions")
		require.Contains(t, logOutput, "max_concurrent")

		metricsCount := strings.Count(logOutput, "partition processing metrics")
		require.GreaterOrEqual(t, metricsCount, 1)

		// Cancel and wait for goroutine to finish before defer runs
		cancel()
		<-done
	})
}

func makePartitionAndRecords(commitTime time.Time) (*PartitionMetadata, []*ChangeRecord) {
	p := &PartitionMetadata{
		PartitionToken: "test-token",
		StartTimestamp: commitTime.Add(-time.Second),
		Watermark:      commitTime.Add(-time.Second),
		CreatedAt:      commitTime.Add(-time.Second),
	}
	records := []*ChangeRecord{
		{
			DataChangeRecords: []*dataChangeRecord{
				{
					CommitTimestamp: commitTime,
					TableName:       "TestTable",
					ModType:         "INSERT",
				},
			},
		},
	}
	return p, records
}

func TestSubscribeMutualExclusion(t *testing.T) {
	s := &Subscriber{
		partitionStorage: &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)},
		activePartitions: make(map[string]struct{}),
		consumer:         ConsumerFunc(func([]byte) error { return nil }),
		consumerWithAck:  ConsumerFuncWithAck(func([]byte, AckFunc) error { return nil }),
	}
	err := s.subscribe(context.Background())
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestHandleWithAck(t *testing.T) {
	t.Run("ack_advances_watermark", func(t *testing.T) {
		storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
		commitTime := time.Now().Truncate(time.Millisecond)
		p, records := makePartitionAndRecords(commitTime)
		storage.partitions[p.PartitionToken] = p

		s := &Subscriber{
			partitionStorage: storage,
			activePartitions: make(map[string]struct{}),
		}
		s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
			go ack(nil) // ack asynchronously
			return nil
		})

		err := s.handle(context.Background(), p, records)
		require.NoError(t, err)
		require.Equal(t, commitTime, storage.partitions[p.PartitionToken].Watermark)
	})

	t.Run("nack_returns_error", func(t *testing.T) {
		storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
		commitTime := time.Now().Truncate(time.Millisecond)
		p, records := makePartitionAndRecords(commitTime)
		storage.partitions[p.PartitionToken] = p

		s := &Subscriber{
			partitionStorage: storage,
			activePartitions: make(map[string]struct{}),
		}
		nackErr := fmt.Errorf("processing failed")
		s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
			go ack(nackErr)
			return nil
		})

		err := s.handle(context.Background(), p, records)
		require.ErrorContains(t, err, "consumer nacked record")
		require.ErrorContains(t, err, "processing failed")
	})

	t.Run("consume_error_stops_immediately", func(t *testing.T) {
		storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
		commitTime := time.Now().Truncate(time.Millisecond)
		p, records := makePartitionAndRecords(commitTime)
		storage.partitions[p.PartitionToken] = p

		s := &Subscriber{
			partitionStorage: storage,
			activePartitions: make(map[string]struct{}),
		}
		consumeErr := fmt.Errorf("consume failed immediately")
		s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
			return consumeErr
		})

		err := s.handle(context.Background(), p, records)
		require.ErrorIs(t, err, consumeErr)
	})

	t.Run("batch_gate_waits_for_all_acks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
			commitTime := time.Now().Truncate(time.Millisecond)
			p := &PartitionMetadata{
				PartitionToken: "test-token",
				StartTimestamp: commitTime.Add(-time.Second),
				Watermark:      commitTime.Add(-time.Second),
				CreatedAt:      commitTime.Add(-time.Second),
			}
			// Three records in one batch
			records := []*ChangeRecord{
				{
					DataChangeRecords: []*dataChangeRecord{
						{CommitTimestamp: commitTime, TableName: "T", ModType: "INSERT"},
						{CommitTimestamp: commitTime.Add(time.Millisecond), TableName: "T", ModType: "UPDATE"},
						{CommitTimestamp: commitTime.Add(2 * time.Millisecond), TableName: "T", ModType: "DELETE"},
					},
				},
			}
			storage.partitions[p.PartitionToken] = p

			var acks []AckFunc
			var mu sync.Mutex
			s := &Subscriber{
				partitionStorage: storage,
				activePartitions: make(map[string]struct{}),
			}
			s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
				mu.Lock()
				acks = append(acks, ack)
				mu.Unlock()
				return nil
			})

			done := make(chan error, 1)
			go func() { done <- s.handle(context.Background(), p, records) }()

			// Wait until handle is blocked on the ack select.
			synctest.Wait()
			select {
			case <-done:
				t.Fatal("handle returned before all acks were called")
			default:
			}

			require.Len(t, acks, 3)

			acks[0](nil)
			acks[1](nil)

			// Wait until handle processes those two acks and blocks again on the third.
			synctest.Wait()
			select {
			case <-done:
				t.Fatal("handle returned before last ack was called")
			default:
			}

			acks[2](nil)
			require.NoError(t, <-done)
		})
	})

	t.Run("context_cancellation_unblocks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
			commitTime := time.Now().Truncate(time.Millisecond)
			p, records := makePartitionAndRecords(commitTime)
			storage.partitions[p.PartitionToken] = p

			s := &Subscriber{
				partitionStorage: storage,
				activePartitions: make(map[string]struct{}),
			}
			// Consume never calls ack.
			s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
				return nil
			})

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- s.handle(ctx, p, records) }()

			// Wait until handle is blocked in the ack select, then cancel.
			synctest.Wait()
			cancel()

			err := <-done
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("watermark_not_updated_before_all_acks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
			commitTime := time.Now().Truncate(time.Millisecond)
			p, records := makePartitionAndRecords(commitTime)
			initialWatermark := p.Watermark
			storage.partitions[p.PartitionToken] = p

			ackReady := make(chan AckFunc, 1)
			s := &Subscriber{
				partitionStorage: storage,
				activePartitions: make(map[string]struct{}),
			}
			s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
				ackReady <- ack
				return nil
			})

			done := make(chan error, 1)
			go func() { done <- s.handle(context.Background(), p, records) }()

			// Receive the ack callback, then wait until handle is blocked in the ack select.
			ack := <-ackReady
			synctest.Wait()

			// Watermark must not have advanced yet.
			storage.mu.Lock()
			watermarkBeforeAck := storage.partitions[p.PartitionToken].Watermark
			storage.mu.Unlock()
			require.Equal(t, initialWatermark, watermarkBeforeAck, "watermark must not be updated before ack is called")

			// Now ack — handle should complete and update the watermark.
			ack(nil)
			require.NoError(t, <-done)

			storage.mu.Lock()
			watermarkAfterAck := storage.partitions[p.PartitionToken].Watermark
			storage.mu.Unlock()
			require.Equal(t, commitTime, watermarkAfterAck, "watermark must equal commit time after ack")
		})
	})

	t.Run("ack_timeout_returns_error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
			commitTime := time.Now().Truncate(time.Millisecond)
			p, records := makePartitionAndRecords(commitTime)
			storage.partitions[p.PartitionToken] = p

			const ackTimeout = 5 * time.Second
			s := &Subscriber{
				partitionStorage: storage,
				activePartitions: make(map[string]struct{}),
				ackTimeout:       ackTimeout,
			}
			// Consume never calls ack — timeout should fire.
			s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
				return nil
			})

			done := make(chan error, 1)
			go func() { done <- s.handle(context.Background(), p, records) }()

			// Advance fake time past the ack timeout. Once the handle goroutine is blocked
			// in its select (timer + ackCh + ctx.Done), all goroutines are idle and the
			// fake clock advances, firing the timer.
			time.Sleep(ackTimeout + time.Millisecond)

			err := <-done
			require.ErrorContains(t, err, "ack timeout")
		})
	})

	t.Run("serialized_consumer_serializes_consume_calls", func(t *testing.T) {
		storage := &mockPartitionStorage{partitions: make(map[string]*PartitionMetadata)}
		commitTime := time.Now().Truncate(time.Millisecond)
		p := &PartitionMetadata{
			PartitionToken: "test-token",
			StartTimestamp: commitTime.Add(-time.Second),
			Watermark:      commitTime.Add(-time.Second),
			CreatedAt:      commitTime.Add(-time.Second),
		}
		records := []*ChangeRecord{
			{
				DataChangeRecords: []*dataChangeRecord{
					{CommitTimestamp: commitTime, TableName: "T", ModType: "INSERT"},
					{CommitTimestamp: commitTime.Add(time.Millisecond), TableName: "T", ModType: "UPDATE"},
				},
			},
		}
		storage.partitions[p.PartitionToken] = p

		callOrder := make([]int, 0, 2)
		var orderMu sync.Mutex
		s := &Subscriber{
			partitionStorage:   storage,
			activePartitions:   make(map[string]struct{}),
			serializedConsumer: true,
		}
		i := 0
		s.consumerWithAck = ConsumerFuncWithAck(func(change []byte, ack AckFunc) error {
			orderMu.Lock()
			callOrder = append(callOrder, i)
			i++
			orderMu.Unlock()
			go ack(nil)
			return nil
		})

		err := s.handle(context.Background(), p, records)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, callOrder)
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
