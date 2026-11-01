package simulator_test

// Fair share (on by default): runners even out the streaming partitions
// between them, so a runner that joins takes on work straight away instead of
// waiting for partitions to split.

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// busyStream has 40 partitions that never split, each with steady writes, so
// none of them parks.
func busyStream(start time.Time, horizon time.Duration) simulator.Config {
	return simulator.Config{
		Seed:              51,
		Start:             start,
		Horizon:           horizon,
		InitialPartitions: 40,
		KeySpace:          4_000,
		WritesPerSecond:   40,
	}
}

// joinLate runs runner-a alone for a minute, then starts runner-b, and returns
// how many partitions each held three minutes after that.
func joinLate(t *testing.T, opts ...screamer.Option) (a, b screamer.PartitionMetrics, r simulator.Report) {
	synctest.Test(t, func(t *testing.T) {
		const horizon = 6 * time.Minute
		start := time.Now()
		end := start.Add(horizon)
		sim := simulator.New(busyStream(start, horizon))
		checker := simulator.NewChecker(sim)
		store := newRecordingStorage()

		subs := make([]*screamer.Subscriber, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, id := range []string{"runner-a", "runner-b"} {
			subs[i] = screamer.NewSubscriber(nil, "SimStream", id, store, simOptions(sim, start, end, opts...)...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				if i == 1 {
					time.Sleep(time.Minute)
				}
				errs[i] = subs[i].Subscribe(context.Background(), checker)
			}()
		}
		time.Sleep(4 * time.Minute)
		a, b = subs[0].GetMetrics(), subs[1].GetMetrics()
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		r = checker.Report(end)
		require.Empty(t, store.backwards)
	})
	return a, b, r
}

// A runner that joins a busy cluster gets about half of the partitions within
// a couple of minutes. The first runner hands over its quietest ones a few at
// a time, and does not take them back.
func TestFairShare_JoiningRunnerTakesHalf(t *testing.T) {
	a, b, r := joinLate(t)
	t.Logf("fair share on:  runner-a holds %d (handed over %d, share %d), runner-b holds %d; %s",
		a.LeasesHeld, a.PartitionsHandedOver, a.FairShare, b.LeasesHeld, r)
	require.Zero(t, r.Missing)
	require.Zero(t, r.OrderViolations)
	require.Positive(t, a.PartitionsHandedOver)
	// Each runner aims for at most ceil(40 / 2 × 1.1) = 22.
	require.Equal(t, 22, a.FairShare)
	require.LessOrEqual(t, a.LeasesHeld, a.FairShare)
	require.GreaterOrEqual(t, b.LeasesHeld, 40-a.FairShare, "runner-b should hold the rest, about half")

	a, b, _ = joinLate(t, screamer.WithFairShare(false))
	t.Logf("fair share off: runner-a holds %d, runner-b holds %d", a.LeasesHeld, b.LeasesHeld)
	require.Zero(t, b.LeasesHeld, "without fair share the first runner keeps everything")
}

// The runners' demand counts a parked partition as the share of a query slot
// its catch-up reads use, not a whole slot, so DesiredRunners does not ask for
// a runner per idle partition.
func TestFairShare_DemandDiscountsParkedPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const horizon = 5 * time.Minute
		start := time.Now()
		sim := simulator.New(sparseStream(start, horizon))
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithMaxConcurrentPartitions(50),
			screamer.WithParkIdleAfter(10*time.Second))
		_ = sub.Subscribe(ctx, simulator.NewChecker(sim))

		c := sub.GetClusterMetrics()
		t.Logf("by state %v, unfinished %d, demand %d, desired runners %d", c.PartitionsByState, c.UnfinishedPartitions, c.Demand, c.DesiredRunners)
		require.Positive(t, c.PartitionsByState[screamer.StateParked])
		require.Less(t, c.Demand, c.UnfinishedPartitions/2, "parked partitions should count for a fraction of a slot")
		require.Less(t, c.DesiredRunners, screamer.DesiredRunners(c.UnfinishedPartitions, 50))
	})
}
