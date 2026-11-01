package simulator_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// countingStorage counts watermark writes, whichever method makes them.
type countingStorage struct {
	*partitionstorage.InmemoryPartitionStorage
	single atomic.Int64 // UpdateWatermark calls
	writes atomic.Int64 // calls that wrote at least one watermark
	rows   atomic.Int64 // watermarks written
}

func (c *countingStorage) UpdateWatermark(ctx context.Context, p *screamer.PartitionMetadata, wm time.Time) error {
	c.single.Add(1)
	c.writes.Add(1)
	c.rows.Add(1)
	return c.InmemoryPartitionStorage.UpdateWatermark(ctx, p, wm)
}

func (c *countingStorage) UpdateWatermarks(ctx context.Context, ws []screamer.PartitionWatermark) error {
	c.writes.Add(1)
	c.rows.Add(int64(len(ws)))
	return c.InmemoryPartitionStorage.UpdateWatermarks(ctx, ws)
}

// RenewLeases writes watermarks when leases are on. Renewals with no watermark
// to write are not counted.
func (c *countingStorage) RenewLeases(ctx context.Context, runnerID string, ttl time.Duration, leases []screamer.Lease) ([]string, error) {
	n := 0
	for _, l := range leases {
		if !l.Watermark.IsZero() {
			n++
		}
	}
	if n > 0 {
		c.writes.Add(1)
		c.rows.Add(int64(n))
	}
	return c.InmemoryPartitionStorage.RenewLeases(ctx, runnerID, ttl, leases)
}

func runCounting(t *testing.T, opts ...screamer.Option) (*countingStorage, simulator.Report, time.Duration) {
	var (
		store   *countingStorage
		r       simulator.Report
		horizon = 3 * time.Minute
	)
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:                  11,
			Start:                 start,
			Horizon:               horizon,
			InitialPartitions:     50,
			KeySpace:              5_000,
			MeanPartitionLifetime: 2 * time.Minute,
			MergeFraction:         0.3,
			MinPartitions:         20,
			MaxPartitions:         100,
			WritesPerSecond:       50,
		})
		checker := simulator.NewChecker(sim)
		end := start.Add(horizon)
		store = &countingStorage{InmemoryPartitionStorage: partitionstorage.NewInmemory()}

		base := []screamer.Option{
			screamer.WithChangeStreamReader(sim),
			screamer.WithHeartbeatInterval(time.Second),
			screamer.WithLogLevel("error"),
			screamer.WithStartTimestamp(start),
			screamer.WithEndTimestamp(end),
		}
		sub := screamer.NewSubscriber(nil, "SimStream", "runner-1", store, append(base, opts...)...)
		require.NoError(t, sub.Subscribe(context.Background(), checker))
		r = checker.Report(end)
	})
	return store, r, horizon
}

// By default watermarks are buffered and written together once a second, rather
// than once per batch per partition.
func TestCheckpoints_AreBatched(t *testing.T) {
	perBatch, r0, _ := runCounting(t, screamer.WithCheckpointInterval(0))
	batched, r1, horizon := runCounting(t)

	t.Logf("per batch: %d writes of %d watermarks; batched: %d writes of %d watermarks",
		perBatch.writes.Load(), perBatch.rows.Load(), batched.writes.Load(), batched.rows.Load())
	require.Zero(t, r0.Missing)
	require.Zero(t, r1.Missing)
	require.Zero(t, r1.OrderViolations)

	require.Zero(t, batched.single.Load(), "batched checkpoints should not write watermarks one at a time")
	// About one write per second of the run.
	require.LessOrEqual(t, batched.writes.Load(), int64(horizon/time.Second)+5)
	// Far fewer storage transactions, and never more rows: each partition is
	// written at most once per flush however many batches it read.
	require.Less(t, batched.writes.Load()*20, perBatch.writes.Load())
	require.LessOrEqual(t, batched.rows.Load(), perBatch.rows.Load())
}

// When the subscriber stops, buffered watermarks are written one last time, so a
// restart resumes from where delivery stopped rather than from the last flush.
func TestCheckpoints_FlushedOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:              12,
			Start:             start,
			Horizon:           5 * time.Minute,
			InitialPartitions: 10,
			KeySpace:          1_000,
			WritesPerSecond:   50,
		})
		store := partitionstorage.NewInmemory()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute+15*time.Second)
		defer cancel()

		// Checkpoints every 30s: without the final flush the stored watermarks would
		// be up to 30s behind when the subscriber stops.
		sub := newSubscriberWithStore(sim, store, screamer.WithStartTimestamp(start), screamer.WithCheckpointInterval(30*time.Second))
		err := sub.Subscribe(ctx, simulator.NewChecker(sim))
		require.True(t, errors.Is(err, context.DeadlineExceeded), "unexpected error: %v", err)

		p, err := store.GetUnfinishedMinWatermarkPartition(context.Background())
		require.NoError(t, err)
		require.NotNil(t, p)
		lag := start.Add(2*time.Minute + 15*time.Second).Sub(p.Watermark)
		t.Logf("oldest stored watermark is %s behind the stop time", lag)
		require.Less(t, lag, 5*time.Second)
	})
}
