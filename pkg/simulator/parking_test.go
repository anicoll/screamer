package simulator_test

// Parking idle partitions (on by default). These tests use a stream where most
// partitions are idle most of the time, which is the common case on a large
// Spanner instance.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// sparseStream has 200 partitions sharing one write a second, so each one sees
// a write every few minutes and is idle in between.
func sparseStream(start time.Time, horizon time.Duration) simulator.Config {
	return simulator.Config{
		Seed:                  41,
		Start:                 start,
		Horizon:               horizon,
		InitialPartitions:     200,
		KeySpace:              20_000,
		MeanPartitionLifetime: 3 * time.Minute,
		MergeFraction:         0.3,
		MinPartitions:         150,
		MaxPartitions:         250,
		WritesPerSecond:       1,
	}
}

// delayMeter records how long after its commit each record was delivered.
type delayMeter struct {
	next screamer.Consumer
	mu   sync.Mutex
	max  time.Duration
}

func (d *delayMeter) Consume(b []byte) error {
	var rec struct {
		CommitTimestamp time.Time `json:"commit_timestamp"`
	}
	if err := json.Unmarshal(b, &rec); err == nil {
		delay := time.Since(rec.CommitTimestamp)
		d.mu.Lock()
		d.max = max(d.max, delay)
		d.mu.Unlock()
	}
	return d.next.Consume(b)
}

// Known problem 1 from the baseline tests, fixed: with a partition limit far
// below the partition count, parking lets one runner read every partition.
// Idle partitions are parked and read in short catch-up queries, so the limit
// becomes a queue that drains instead of a wall. With parking off, most
// partitions are never opened.
func TestParking_LimitNoLongerStarvesIdlePartitions(t *testing.T) {
	const horizon = 10 * time.Minute
	run := func(t *testing.T, opts ...screamer.Option) (simulator.Report, simulator.Stats, screamer.PartitionMetrics) {
		var (
			r  simulator.Report
			st simulator.Stats
			m  screamer.PartitionMetrics
		)
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			sim := simulator.New(sparseStream(start, horizon))
			checker := simulator.NewChecker(sim)
			ctx, cancel := context.WithTimeout(context.Background(), horizon)
			defer cancel()
			sub := newSubscriber(sim, append([]screamer.Option{
				screamer.WithStartTimestamp(start),
				screamer.WithMaxConcurrentPartitions(20),
				screamer.WithParkIdleAfter(10 * time.Second),
			}, opts...)...)
			err := sub.Subscribe(ctx, checker)
			require.True(t, errors.Is(err, context.DeadlineExceeded), "unexpected error: %v", err)
			// Leave a minute for the last park interval and the queue.
			r, st, m = checker.Report(start.Add(horizon-time.Minute)), sim.Stats(), sub.GetMetrics()
		})
		return r, st, m
	}

	parked, pst, pm := run(t)
	t.Logf("parking on:  %s max_open_streams=%d parks=%d", parked, pst.MaxOpenStreams, pm.PartitionsParked)
	streaming, sst, _ := run(t, screamer.WithParking(false))
	t.Logf("parking off: %s max_open_streams=%d", streaming, sst.MaxOpenStreams)

	require.Zero(t, parked.Missing)
	require.Zero(t, parked.OrderViolations)
	require.Positive(t, pm.PartitionsParked)
	require.LessOrEqual(t, pst.MaxOpenStreams, int64(25), "the limit still bounds open queries")

	require.Greater(t, streaming.PartitionsWithMissing, 100, "without parking most partitions are never read")
}

// Without a partition limit, parking holds far fewer queries open than there
// are partitions, and a change to a parked partition is delivered at most
// about one park interval late.
func TestParking_BoundsOpenStreamsAndDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const horizon = 6 * time.Minute
		start := time.Now()
		end := start.Add(horizon)
		sim := simulator.New(sparseStream(start, horizon))
		checker := simulator.NewChecker(sim)
		meter := &delayMeter{next: checker}

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end),
			screamer.WithParkInterval(10*time.Second), screamer.WithParkIdleAfter(10*time.Second))

		// Every partition streams when it is first opened, so sample the open
		// queries once things have settled.
		var openMax int64
		go func() {
			time.Sleep(time.Minute)
			for time.Now().Before(end.Add(-time.Minute)) {
				openMax = max(openMax, sim.Stats().OpenStreams)
				time.Sleep(time.Second)
			}
		}()
		require.NoError(t, sub.Subscribe(context.Background(), meter))

		r, st := checker.Report(end), sim.Stats()
		t.Logf("%s open_streams_after_first_minute=%d queries=%d parks=%d max_delay=%s", r, openMax, st.Queries, sub.GetMetrics().PartitionsParked, meter.max)
		require.Zero(t, r.Missing)
		require.Zero(t, r.OrderViolations)
		require.Less(t, openMax, int64(50), "most of the 200 partitions should be parked at any time")
		// One park interval, plus up to a second to claim it and a second of
		// checkpoint or heartbeat rounding.
		require.LessOrEqual(t, meter.max, 13*time.Second)
	})
}

// Parked partitions still split and merge correctly, and parent gating still
// holds: a merged child waits for a parked parent to be read to its end.
func TestParking_KeepsOrderThroughSplitsAndMerges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now().Add(-2 * time.Minute)
		end := start.Add(5 * time.Minute)
		cfg := orderScenario(start)
		cfg.WritesPerSecond = 2 // mostly idle, so partitions park between writes
		sim := simulator.New(cfg)
		checker := simulator.NewChecker(sim)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end),
			screamer.WithParkIdleAfter(5*time.Second))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		t.Logf("%s parks=%d", r, sub.GetMetrics().PartitionsParked)
		require.Zero(t, r.Missing)
		require.Zero(t, r.OrderViolations)
		require.Positive(t, sub.GetMetrics().PartitionsParked)
	})
}

// Two runners share parked partitions: whichever claims a parked partition
// when it is due reads it, and nothing is delivered twice.
func TestParking_TwoRunnersShareParkedPartitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const horizon = 5 * time.Minute
		start := time.Now()
		end := start.Add(horizon)
		sim := simulator.New(sparseStream(start, horizon))
		checker := simulator.NewChecker(sim)
		store := newRecordingStorage()

		var wg sync.WaitGroup
		subs := make([]*screamer.Subscriber, 2)
		errs := make([]error, 2)
		for i, id := range []string{"runner-a", "runner-b"} {
			subs[i] = screamer.NewSubscriber(nil, "SimStream", id, store,
				simOptions(sim, start, end, screamer.WithParkIdleAfter(10*time.Second))...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = subs[i].Subscribe(context.Background(), checker)
			}()
		}
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		r := checker.Report(end)
		a, b := subs[0].GetMetrics(), subs[1].GetMetrics()
		t.Logf("%s parks a=%d b=%d", r, a.PartitionsParked, b.PartitionsParked)
		require.Zero(t, r.Missing)
		require.Zero(t, r.Duplicates)
		require.Zero(t, r.OrderViolations)
		require.Positive(t, a.PartitionsParked)
		require.Positive(t, b.PartitionsParked)
		require.Empty(t, store.backwards)
	})
}
