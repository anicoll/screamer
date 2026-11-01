package simulator_test

// Baseline tests run today's Subscriber against the simulator. Some of them
// assert known problems from docs/PARTITION_SCALING_DESIGN.md. When a later
// change fixes one of those problems, the matching test should be flipped to
// assert the fixed behaviour.

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

func newSubscriber(sim *simulator.Simulator, opts ...screamer.Option) *screamer.Subscriber {
	base := []screamer.Option{
		screamer.WithChangeStreamReader(sim),
		screamer.WithHeartbeatInterval(time.Second),
		screamer.WithLogLevel("error"),
	}
	return screamer.NewSubscriber(nil, "SimStream", "runner-1", partitionstorage.NewInmemory(), append(base, opts...)...)
}

// With enough capacity and a bounded end timestamp, every write is delivered
// through splits and merges.
func TestBaseline_DeliversEverythingThroughSplitsAndMerges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:                  1,
			Start:                 start,
			Horizon:               10 * time.Minute,
			InitialPartitions:     20,
			KeySpace:              5_000,
			MeanPartitionLifetime: 2 * time.Minute,
			MergeFraction:         0.4,
			MinPartitions:         5,
			MaxPartitions:         100,
			WritesPerSecond:       50,
			HotKeyFraction:        0.05,
			HotWriteShare:         0.8,
		})
		checker := simulator.NewChecker(sim)
		end := start.Add(10 * time.Minute)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		t.Logf("partitions=%d splits=%d merges=%d %s stats=%+v", len(sim.Partitions()), sim.Splits(), sim.Merges(), r, sim.Stats())
		require.Positive(t, r.Expected)
		require.Zero(t, r.Missing)
		require.Zero(t, r.ParseErrors)
	})
}

// Known problem 1: WithMaxConcurrentPartitions does not queue partitions, it
// starves them. A partition only ends when it splits, merges or reaches the end
// timestamp, so partitions over the cap are never read.
func TestBaseline_ConcurrencyCapStarvesPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:              2,
			Start:             start,
			Horizon:           5 * time.Minute,
			InitialPartitions: 40,
			KeySpace:          4_000,
			WritesPerSecond:   20,
		})
		checker := simulator.NewChecker(sim)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithMaxConcurrentPartitions(10))
		err := sub.Subscribe(ctx, checker)
		require.True(t, errors.Is(err, context.DeadlineExceeded), "unexpected error: %v", err)

		// Allow for the last few seconds of writes still being in flight.
		r := checker.Report(start.Add(2*time.Minute - 10*time.Second))
		st := sim.Stats()
		t.Logf("%s stats=%+v", r, st)

		// Root + 10 partitions. The cap can overshoot by a few, because a partition only
		// counts against it once its goroutine starts, but most partitions are never opened.
		neverOpened := 40 - (st.PartitionsQueried - 1)
		require.GreaterOrEqual(t, neverOpened, 20)
		require.GreaterOrEqual(t, r.PartitionsWithMissing, neverOpened)
	})
}

// Known problem 6: a child partition is scheduled as soon as any parent reports
// it, without waiting for every parent to finish. When a parent is behind (here
// because queries queue for gRPC streams while catching up), records for the
// same key are delivered out of commit order.
func TestBaseline_NoParentGatingBreaksPerKeyOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		start := now.Add(-10 * time.Minute) // catch up on ten minutes of history
		sim := simulator.New(simulator.Config{
			Seed:                  3,
			Start:                 start,
			Horizon:               10 * time.Minute,
			InitialPartitions:     30,
			KeySpace:              3_000,
			MeanPartitionLifetime: time.Minute,
			MergeFraction:         0.5,
			MinPartitions:         10,
			MaxPartitions:         60,
			WritesPerSecond:       100,
			QueryLatency:          200 * time.Millisecond,
			MaxConcurrentStreams:  8,
		})
		checker := simulator.NewChecker(sim)
		end := start.Add(10 * time.Minute)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		t.Logf("partitions=%d merges=%d %s stats=%+v", len(sim.Partitions()), sim.Merges(), r, sim.Stats())
		require.Zero(t, r.Missing)
		require.Positive(t, r.OrderViolations, "expected out-of-order delivery without parent gating")
	})
}

// Scale check: 5,000 partitions on one runner with no cap. This is where real
// deployments fall over; in the simulator it shows how many streams one runner
// asks for at once.
func TestBaseline_FiveThousandPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("slow under -short")
	}
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:              4,
			Start:             start,
			Horizon:           2 * time.Minute,
			InitialPartitions: 5_000,
			KeySpace:          50_000,
			WritesPerSecond:   200,
			HotKeyFraction:    0.01,
			HotWriteShare:     0.9,
		})
		checker := simulator.NewChecker(sim)
		end := start.Add(2 * time.Minute)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		st := sim.Stats()
		t.Logf("%s stats=%+v", r, st)
		require.Zero(t, r.Missing)
		require.EqualValues(t, 5_000, st.MaxOpenStreams, "one runner opens a stream per partition")
	})
}
