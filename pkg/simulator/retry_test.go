package simulator_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func flakyConfig(start time.Time) simulator.Config {
	return simulator.Config{
		Seed:                  5,
		Start:                 start,
		Horizon:               5 * time.Minute,
		InitialPartitions:     20,
		KeySpace:              2_000,
		MeanPartitionLifetime: 2 * time.Minute,
		MergeFraction:         0.3,
		MinPartitions:         5,
		MaxPartitions:         40,
		WritesPerSecond:       40,
		ReadFailureRate:       0.3,
		ReadFailureWithin:     time.Minute,
	}
}

// A dropped stream is retried from the partition's watermark; the runner keeps going.
func TestRetry_DroppedStreamsAreRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(flakyConfig(start))
		checker := simulator.NewChecker(sim)
		end := start.Add(5 * time.Minute)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		st := sim.Stats()
		t.Logf("%s stats=%+v retries=%d", r, st, sub.GetMetrics().PartitionRetries)
		require.Positive(t, st.InjectedFailures)
		require.EqualValues(t, st.InjectedFailures, sub.GetMetrics().PartitionRetries)
		require.Zero(t, r.Missing)
	})
}

// With retries disabled, the first dropped stream stops the subscriber, as before.
func TestRetry_DisabledStopsOnFirstFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(flakyConfig(start))

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(start.Add(5*time.Minute)),
			screamer.WithPartitionMaxRetries(0))
		err := sub.Subscribe(context.Background(), simulator.NewChecker(sim))
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err), "got %v", err)
	})
}

// Consumer errors are never retried.
func TestRetry_ConsumerErrorIsNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		cfg := flakyConfig(start)
		cfg.ReadFailureRate = 0
		sim := simulator.New(cfg)

		boom := errors.New("downstream is down")
		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(start.Add(5*time.Minute)))
		err := sub.Subscribe(context.Background(), screamer.ConsumerFunc(func([]byte) error { return boom }))
		require.ErrorIs(t, err, boom)
		require.Zero(t, sub.GetMetrics().PartitionRetries)
	})
}
