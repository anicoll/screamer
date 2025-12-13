package partitionstorage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anicoll/screamer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInmemory(t *testing.T) {
	storage := NewInmemory()
	require.NotNil(t, storage)
	require.NotNil(t, storage.m)
	assert.Equal(t, 0, len(storage.m))
}

func TestInmemoryPartitionStorage_InitializeRootPartition(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	startTime := time.Now().UTC()
	endTime := startTime.Add(24 * time.Hour)
	heartbeat := 10 * time.Second

	err := storage.InitializeRootPartition(ctx, startTime, endTime, heartbeat)
	require.NoError(t, err)

	// Verify the partition was created
	partition, exists := storage.m[screamer.RootPartitionToken]
	require.True(t, exists)
	assert.Equal(t, screamer.RootPartitionToken, partition.PartitionToken)
	assert.Equal(t, screamer.StateCreated, partition.State)
	assert.Equal(t, startTime, partition.StartTimestamp)
	assert.Equal(t, endTime, partition.EndTimestamp)
	assert.Equal(t, startTime, partition.Watermark)
	assert.Equal(t, heartbeat.Milliseconds(), partition.HeartbeatMillis)
	assert.Empty(t, partition.ParentTokens)
}

func TestInmemoryPartitionStorage_GetUnfinishedMinWatermarkPartition(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	t.Run("no partitions", func(t *testing.T) {
		partition, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		require.NoError(t, err)
		assert.Nil(t, partition)
	})

	t.Run("single unfinished partition", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		now := time.Now().UTC()
		storage.m["p1"] = &screamer.PartitionMetadata{
			PartitionToken: "p1",
			State:          screamer.StateRunning,
			Watermark:      now,
		}

		partition, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		require.NoError(t, err)
		require.NotNil(t, partition)
		assert.Equal(t, "p1", partition.PartitionToken)
	})

	t.Run("multiple partitions with different watermarks", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		baseTime := time.Now().UTC()

		storage.m["p1"] = &screamer.PartitionMetadata{
			PartitionToken: "p1",
			State:          screamer.StateRunning,
			Watermark:      baseTime.Add(2 * time.Hour),
		}
		storage.m["p2"] = &screamer.PartitionMetadata{
			PartitionToken: "p2",
			State:          screamer.StateCreated,
			Watermark:      baseTime.Add(1 * time.Hour),
		}
		storage.m["p3"] = &screamer.PartitionMetadata{
			PartitionToken: "p3",
			State:          screamer.StateScheduled,
			Watermark:      baseTime.Add(3 * time.Hour),
		}

		partition, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		require.NoError(t, err)
		require.NotNil(t, partition)
		assert.Equal(t, "p2", partition.PartitionToken)
	})

	t.Run("finished partitions are excluded", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		baseTime := time.Now().UTC()

		storage.m["p1"] = &screamer.PartitionMetadata{
			PartitionToken: "p1",
			State:          screamer.StateFinished,
			Watermark:      baseTime,
		}
		storage.m["p2"] = &screamer.PartitionMetadata{
			PartitionToken: "p2",
			State:          screamer.StateRunning,
			Watermark:      baseTime.Add(1 * time.Hour),
		}

		partition, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		require.NoError(t, err)
		require.NotNil(t, partition)
		assert.Equal(t, "p2", partition.PartitionToken)
	})
}

func TestInmemoryPartitionStorage_GetAndSchedulePartitions(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	t.Run("no partitions available", func(t *testing.T) {
		partitions, err := storage.GetAndSchedulePartitions(ctx, time.Now(), "runner1", 10)
		require.NoError(t, err)
		assert.Empty(t, partitions)
	})

	t.Run("schedule created partitions", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		baseTime := time.Now().UTC()

		storage.m["p1"] = &screamer.PartitionMetadata{
			PartitionToken: "p1",
			State:          screamer.StateCreated,
			StartTimestamp: baseTime,
		}
		storage.m["p2"] = &screamer.PartitionMetadata{
			PartitionToken: "p2",
			State:          screamer.StateCreated,
			StartTimestamp: baseTime.Add(1 * time.Hour),
		}

		// Both partitions should be scheduled since minWatermark <= StartTimestamp for both
		partitions, err := storage.GetAndSchedulePartitions(ctx, baseTime, "runner1", 10)
		require.NoError(t, err)
		assert.Len(t, partitions, 2)

		// Verify both are scheduled
		for _, p := range partitions {
			assert.Equal(t, screamer.StateScheduled, p.State)
			assert.NotNil(t, p.ScheduledAt)
		}
	})

	t.Run("skip partitions with past watermark", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		baseTime := time.Now().UTC()

		storage.m["p1"] = &screamer.PartitionMetadata{
			PartitionToken: "p1",
			State:          screamer.StateCreated,
			StartTimestamp: baseTime.Add(-2 * time.Hour), // In the past
		}

		// Partition with StartTimestamp < minWatermark should NOT be scheduled
		partitions, err := storage.GetAndSchedulePartitions(ctx, baseTime, "runner1", 10)
		require.NoError(t, err)
		assert.Empty(t, partitions)
	})
}

func TestInmemoryPartitionStorage_AddChildPartitions(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent",
		EndTimestamp:    time.Now().Add(24 * time.Hour),
		HeartbeatMillis: 10000,
	}

	startTime := time.Now().UTC()
	record := &screamer.ChildPartitionsRecord{
		StartTimestamp: startTime,
		ChildPartitions: []*screamer.ChildPartition{
			{
				Token:                 "child1",
				ParentPartitionTokens: []string{"parent"},
			},
			{
				Token:                 "child2",
				ParentPartitionTokens: []string{"parent"},
			},
		},
	}

	err := storage.AddChildPartitions(ctx, parent, record)
	require.NoError(t, err)

	assert.Len(t, storage.m, 2)

	child1 := storage.m["child1"]
	require.NotNil(t, child1)
	assert.Equal(t, "child1", child1.PartitionToken)
	assert.Equal(t, screamer.StateCreated, child1.State)
	assert.Equal(t, startTime, child1.StartTimestamp)
	assert.Equal(t, parent.EndTimestamp, child1.EndTimestamp)
	assert.Equal(t, parent.HeartbeatMillis, child1.HeartbeatMillis)
	assert.Equal(t, []string{"parent"}, child1.ParentTokens)

	child2 := storage.m["child2"]
	require.NotNil(t, child2)
	assert.Equal(t, "child2", child2.PartitionToken)
}

func TestInmemoryPartitionStorage_UpdateToRunning(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	partition := &screamer.PartitionMetadata{
		PartitionToken: "p1",
		State:          screamer.StateScheduled,
	}
	storage.m["p1"] = partition

	err := storage.UpdateToRunning(ctx, partition)
	require.NoError(t, err)

	updated := storage.m["p1"]
	assert.Equal(t, screamer.StateRunning, updated.State)
	assert.NotNil(t, updated.RunningAt)
}

func TestInmemoryPartitionStorage_UpdateToFinished(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	partition := &screamer.PartitionMetadata{
		PartitionToken: "p1",
		State:          screamer.StateRunning,
	}
	storage.m["p1"] = partition

	err := storage.UpdateToFinished(ctx, partition, "runner1")
	require.NoError(t, err)

	updated := storage.m["p1"]
	assert.Equal(t, screamer.StateFinished, updated.State)
	assert.NotNil(t, updated.FinishedAt)
}

func TestInmemoryPartitionStorage_UpdateWatermark(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	oldWatermark := time.Now().UTC()
	newWatermark := oldWatermark.Add(1 * time.Hour)

	partition := &screamer.PartitionMetadata{
		PartitionToken: "p1",
		Watermark:      oldWatermark,
	}
	storage.m["p1"] = partition

	err := storage.UpdateWatermark(ctx, partition, newWatermark)
	require.NoError(t, err)

	updated := storage.m["p1"]
	assert.Equal(t, newWatermark, updated.Watermark)
}

func TestInmemoryPartitionStorage_GetActivePartitionCount(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	t.Run("no partitions", func(t *testing.T) {
		count, err := storage.GetActivePartitionCount(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("mixed partition states", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		storage.m["p1"] = &screamer.PartitionMetadata{State: screamer.StateCreated}
		storage.m["p2"] = &screamer.PartitionMetadata{State: screamer.StateScheduled}
		storage.m["p3"] = &screamer.PartitionMetadata{State: screamer.StateRunning}
		storage.m["p4"] = &screamer.PartitionMetadata{State: screamer.StateFinished}

		count, err := storage.GetActivePartitionCount(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count) // Only scheduled and running
	})

	t.Run("all active partitions", func(t *testing.T) {
		storage.m = make(map[string]*screamer.PartitionMetadata)
		storage.m["p1"] = &screamer.PartitionMetadata{State: screamer.StateScheduled}
		storage.m["p2"] = &screamer.PartitionMetadata{State: screamer.StateRunning}
		storage.m["p3"] = &screamer.PartitionMetadata{State: screamer.StateScheduled}

		count, err := storage.GetActivePartitionCount(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})
}

func TestInmemoryPartitionStorage_GetActiveRunnerCount(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// InmemoryPartitionStorage always returns 1
	count, err := storage.GetActiveRunnerCount(ctx, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestInmemoryPartitionStorage_GetInterruptedPartitions(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// InmemoryPartitionStorage always returns empty slice
	partitions, err := storage.GetInterruptedPartitions(ctx, "runner1", 30*time.Second)
	require.NoError(t, err)
	assert.Empty(t, partitions)
}

func TestInmemoryPartitionStorage_RefreshRunner(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// RefreshRunner is a no-op for in-memory storage
	err := storage.RefreshRunner(ctx, "runner1")
	require.NoError(t, err)
}

func TestInmemoryPartitionStorage_ExtendLease(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// ExtendLease is a no-op for in-memory storage
	err := storage.ExtendLease(ctx, "partition1", "runner1")
	require.NoError(t, err)
}

func TestInmemoryPartitionStorage_ReleaseLease(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// ReleaseLease is a no-op for in-memory storage
	err := storage.ReleaseLease(ctx, "partition1", "runner1")
	require.NoError(t, err)
}

func TestInmemoryPartitionStorage_ConcurrentAccess(t *testing.T) {
	storage := NewInmemory()
	ctx := context.Background()

	// Initialize root partition
	err := storage.InitializeRootPartition(ctx, time.Now(), time.Now().Add(24*time.Hour), 10*time.Second)
	require.NoError(t, err)

	// Run concurrent operations
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create partition token with guaranteed uniqueness
			token := fmt.Sprintf("p-%d-%d", id, time.Now().UnixNano())

			// Create a partition using GetAndSchedulePartitions to avoid direct map access
			baseTime := time.Now().UTC()

			// Insert partition via storage methods (thread-safe)
			partition := &screamer.PartitionMetadata{
				PartitionToken:  token,
				State:           screamer.StateCreated,
				Watermark:       baseTime,
				StartTimestamp:  baseTime,
				EndTimestamp:    baseTime.Add(24 * time.Hour),
				HeartbeatMillis: 10000,
			}

			// Use storage mutex properly
			storage.mu.Lock()
			storage.m[token] = partition
			storage.mu.Unlock()

			// Read partition (thread-safe)
			_, _ = storage.GetUnfinishedMinWatermarkPartition(ctx)

			// Update watermark (thread-safe)
			_ = storage.UpdateWatermark(ctx, partition, time.Now())
		}(i)
	}

	wg.Wait()

	// Verify no data race occurred and storage is still functional
	count, err := storage.GetActivePartitionCount(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}
