package partitionstorage

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/stretchr/testify/require"
)

func claim(t *testing.T, s *InmemoryPartitionStorage, runner string, limit int) []*screamer.PartitionMetadata {
	t.Helper()
	ps, err := s.ClaimPartitions(context.Background(), screamer.ClaimRequest{RunnerID: runner, TTL: 30 * time.Second, Limit: limit, ParentGating: true})
	require.NoError(t, err)
	return ps
}

func TestInMemoryLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := NewInmemory()
		start := time.Now()
		require.NoError(t, s.InitializeRootPartition(ctx, start, start.Add(time.Hour), time.Second))

		// A first claim takes the lease at epoch 1 and schedules the partition.
		ps := claim(t, s, "a", 10)
		require.Len(t, ps, 1)
		root := ps[0]
		require.Equal(t, "a", *root.Owner)
		require.EqualValues(t, 1, *root.Epoch)
		require.Equal(t, screamer.StateScheduled, root.State)
		require.Equal(t, start.Add(30*time.Second), root.LeaseExpiresAt.Local())

		// A held lease cannot be claimed.
		require.Empty(t, claim(t, s, "b", 10))

		// Renewing moves the watermark forward and pushes the expiry out.
		time.Sleep(20 * time.Second)
		lost, err := s.RenewLeases(ctx, "a", 30*time.Second, []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 1, Watermark: start.Add(5 * time.Second)}})
		require.NoError(t, err)
		require.Empty(t, lost)
		require.Equal(t, start.Add(5*time.Second), s.m[root.PartitionToken].Watermark)
		time.Sleep(20 * time.Second)
		require.Empty(t, claim(t, s, "b", 10), "the renewed lease has not expired yet")

		// Once it expires, another runner claims it at the next epoch.
		time.Sleep(11 * time.Second)
		ps = claim(t, s, "b", 10)
		require.Len(t, ps, 1)
		require.Equal(t, "b", *ps[0].Owner)
		require.EqualValues(t, 2, *ps[0].Epoch)
		require.Equal(t, start.Add(5*time.Second), ps[0].Watermark, "the new owner resumes from the last checkpoint")

		// The old owner is fenced: its renewal is rejected and writes nothing.
		lost, err = s.RenewLeases(ctx, "a", 30*time.Second, []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 1, Watermark: start.Add(9 * time.Second)}})
		require.NoError(t, err)
		require.Equal(t, []string{root.PartitionToken}, lost)
		require.Equal(t, start.Add(5*time.Second), s.m[root.PartitionToken].Watermark)
		require.NoError(t, s.ReleaseLeases(ctx, "a", []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 1, Watermark: start.Add(9 * time.Second)}}))
		require.Equal(t, "b", *s.m[root.PartitionToken].Owner, "a stale release must not end the new owner's lease")

		// A watermark never moves backwards.
		_, err = s.RenewLeases(ctx, "b", 30*time.Second, []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 2, Watermark: start.Add(time.Second)}})
		require.NoError(t, err)
		require.Equal(t, start.Add(5*time.Second), s.m[root.PartitionToken].Watermark)

		// Releasing writes the watermark and frees the partition at once.
		require.NoError(t, s.ReleaseLeases(ctx, "b", []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 2, Watermark: start.Add(7 * time.Second)}}))
		ps = claim(t, s, "c", 10)
		require.Len(t, ps, 1)
		require.EqualValues(t, 3, *ps[0].Epoch)
		require.Equal(t, start.Add(7*time.Second), ps[0].Watermark)

		// A finished partition is never claimed again, and its lease cannot be renewed.
		require.NoError(t, s.UpdateToFinished(ctx, ps[0], "c"))
		time.Sleep(time.Minute)
		require.Empty(t, claim(t, s, "d", 10))
		lost, err = s.RenewLeases(ctx, "c", 30*time.Second, []screamer.Lease{{PartitionToken: root.PartitionToken, Epoch: 3}})
		require.NoError(t, err)
		require.Equal(t, []string{root.PartitionToken}, lost)
	})
}

// A partition with no lease that is SCHEDULED or RUNNING was taken by a runner
// that did not use leases (an older release). It is claimed straight away.
func TestInMemoryClaimTakesPartitionsWithoutALease(t *testing.T) {
	ctx := context.Background()
	s := NewInmemory()
	start := time.Now()
	require.NoError(t, s.InitializeRootPartition(ctx, start, start.Add(time.Hour), time.Second))
	_, err := s.GetAndScheduleReadyPartitions(ctx, "old-runner", 10)
	require.NoError(t, err)

	ps := claim(t, s, "new-runner", 10)
	require.Len(t, ps, 1)
	require.EqualValues(t, 1, *ps[0].Epoch)
}

func TestInMemoryClaimRespectsLimitAndGating(t *testing.T) {
	ctx := context.Background()
	s := NewInmemory()
	start := time.Now()
	parent := &screamer.PartitionMetadata{PartitionToken: "p", EndTimestamp: start.Add(time.Hour), State: screamer.StateRunning, Watermark: start}
	s.m["p"] = parent
	require.NoError(t, s.AddChildPartitions(ctx, parent, &screamer.ChildPartitionsRecord{
		StartTimestamp: start.Add(time.Second),
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "c1", ParentPartitionTokens: []string{"p"}},
			{Token: "c2", ParentPartitionTokens: []string{"p"}},
		},
	}))

	// "p" has no lease, so it is claimable; its children wait for it to finish.
	ps := claim(t, s, "a", 10)
	require.Len(t, ps, 1)
	require.Equal(t, "p", ps[0].PartitionToken)

	require.NoError(t, s.UpdateToFinished(ctx, ps[0], "a"))
	require.Len(t, claim(t, s, "a", 1), 1, "the limit applies")
	require.Len(t, claim(t, s, "a", 10), 1)
}

func TestInMemoryUpdateToRunningKeepsFinishedPartitionsFinished(t *testing.T) {
	ctx := context.Background()
	s := NewInmemory()
	require.NoError(t, s.InitializeRootPartition(ctx, time.Now(), time.Now().Add(time.Hour), time.Second))
	root := s.m[screamer.RootPartitionToken]
	require.NoError(t, s.UpdateToFinished(ctx, root, "a"))
	require.NoError(t, s.UpdateToRunning(ctx, root))
	require.Equal(t, screamer.StateFinished, s.m[screamer.RootPartitionToken].State)
}

func TestInMemoryPrunesFinishedPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := NewInmemory(WithFinishedRetention(time.Minute))
		start := time.Now()
		for _, token := range []string{"a", "b", "c"} {
			s.m[token] = &screamer.PartitionMetadata{PartitionToken: token, State: screamer.StateRunning, Watermark: start}
		}
		require.NoError(t, s.UpdateToFinished(ctx, s.m["a"], "r"))
		time.Sleep(30 * time.Second)
		require.NoError(t, s.UpdateToFinished(ctx, s.m["b"], "r"))
		time.Sleep(31 * time.Second)
		require.NoError(t, s.UpdateToFinished(ctx, s.m["c"], "r"))

		require.NotContains(t, s.m, "a", "finished over a minute ago")
		require.Contains(t, s.m, "b")
		require.Contains(t, s.m, "c")
	})
}

func TestInMemoryKeepsFinishedPartitionsWithoutRetention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := NewInmemory(WithFinishedRetention(0))
		s.m["a"] = &screamer.PartitionMetadata{PartitionToken: "a", State: screamer.StateRunning}
		s.m["b"] = &screamer.PartitionMetadata{PartitionToken: "b", State: screamer.StateRunning}
		require.NoError(t, s.UpdateToFinished(ctx, s.m["a"], "r"))
		time.Sleep(48 * time.Hour)
		require.NoError(t, s.UpdateToFinished(ctx, s.m["b"], "r"))
		require.Contains(t, s.m, "a")
	})
}
