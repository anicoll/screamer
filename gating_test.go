package screamer_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// basicStorage hides the optional interfaces of the storage it wraps, like a
// PartitionStorage written before they existed.
type basicStorage struct {
	screamer.PartitionStorage
}

func TestParentGatingNeedsAGatedScheduler(t *testing.T) {
	sub := screamer.NewSubscriber(nil, "Stream", "runner-1", basicStorage{partitionstorage.NewInmemory()})
	err := sub.Subscribe(context.Background(), screamer.ConsumerFunc(func([]byte) error { return nil }))
	require.ErrorContains(t, err, "ParentGatedScheduler")
	require.ErrorContains(t, err, "WithParentGating(false)")
}

// A storage without the optional interfaces still works with parent gating off,
// and batched checkpoints fall back to one UpdateWatermark per partition.
func TestBasicStorageWithoutParentGating(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:                  7,
			Start:                 start,
			Horizon:               3 * time.Minute,
			InitialPartitions:     5,
			KeySpace:              500,
			MeanPartitionLifetime: time.Minute,
			MinPartitions:         2,
			MaxPartitions:         20,
			WritesPerSecond:       20,
		})
		checker := simulator.NewChecker(sim)
		end := start.Add(3 * time.Minute)

		sub := screamer.NewSubscriber(nil, "SimStream", "runner-1", basicStorage{partitionstorage.NewInmemory()},
			screamer.WithChangeStreamReader(sim),
			screamer.WithHeartbeatInterval(time.Second),
			screamer.WithLogLevel("error"),
			screamer.WithStartTimestamp(start),
			screamer.WithEndTimestamp(end),
			screamer.WithParentGating(false),
		)
		require.NoError(t, sub.Subscribe(context.Background(), checker))
		r := checker.Report(end)
		require.Positive(t, r.Expected)
		require.Zero(t, r.Missing)
	})
}
