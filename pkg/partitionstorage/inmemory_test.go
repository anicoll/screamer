package partitionstorage

import (
	"context"
	"testing"
	"time"

	"github.com/anicoll/screamer"
	"github.com/stretchr/testify/require"
)

func TestInMemoryAddChildPartitions_Split(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	splitTime := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent1",
		StartTimestamp:  startTime,
		EndTimestamp:    endTime,
		HeartbeatMillis: 3000,
		State:           screamer.StateRunning,
		Watermark:       splitTime,
	}

	// Simulate partition split: 1 parent -> 2 children
	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: splitTime,
		RecordSequence: "seq-001",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child1", ParentPartitionTokens: []string{"parent1"}},
			{Token: "child2", ParentPartitionTokens: []string{"parent1"}},
		},
	}

	err := storage.AddChildPartitions(ctx, parent, childRecord)
	require.NoError(t, err)

	// Verify both children were created with correct metadata
	for _, child := range childRecord.ChildPartitions {
		p, ok := storage.m[child.Token]
		require.True(t, ok, "Child partition %s should exist", child.Token)

		require.Equal(t, child.Token, p.PartitionToken)
		require.Equal(t, child.ParentPartitionTokens, p.ParentTokens)
		require.Equal(t, splitTime, p.StartTimestamp)
		require.Equal(t, endTime, p.EndTimestamp)
		require.Equal(t, int64(3000), p.HeartbeatMillis)
		require.Equal(t, screamer.StateCreated, p.State)
		require.Equal(t, splitTime, p.Watermark)
	}
}

func TestInMemoryAddChildPartitions_Merge(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	mergeTime := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent1",
		EndTimestamp:    endTime,
		HeartbeatMillis: 5000,
	}

	// Simulate partition merge: 2 parents -> 1 child
	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: mergeTime,
		RecordSequence: "seq-002",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "merged-child", ParentPartitionTokens: []string{"parent1", "parent2"}},
		},
	}

	err := storage.AddChildPartitions(ctx, parent, childRecord)
	require.NoError(t, err)

	// Verify merged child was created with both parent tokens
	p, ok := storage.m["merged-child"]
	require.True(t, ok)
	require.Equal(t, "merged-child", p.PartitionToken)
	require.Equal(t, []string{"parent1", "parent2"}, p.ParentTokens)
}

func TestInMemoryAddChildPartitions_MultipleChildren(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent",
		EndTimestamp:    time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
		HeartbeatMillis: 3000,
	}

	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
		RecordSequence: "seq-003",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child1", ParentPartitionTokens: []string{"parent"}},
			{Token: "child2", ParentPartitionTokens: []string{"parent"}},
			{Token: "child3", ParentPartitionTokens: []string{"parent"}},
		},
	}

	err := storage.AddChildPartitions(ctx, parent, childRecord)
	require.NoError(t, err)

	// Verify all children were created
	require.Len(t, storage.m, 3)
	for _, child := range childRecord.ChildPartitions {
		_, ok := storage.m[child.Token]
		require.True(t, ok, "Child partition %s should exist", child.Token)
	}
}

func TestInMemoryGetActiveRunnerCount(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	count, err := storage.GetActiveRunnerCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count, "In-memory storage always returns 1 for single runner")
}
