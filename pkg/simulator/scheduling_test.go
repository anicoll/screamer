package simulator_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// With no partition limit, every ready partition is scheduled straight away.
// Before, detection scheduled at most 100 partitions a second, so a runner
// starting with 2,000 partitions took 20 seconds to open them all, and the last
// ones fell 20 seconds behind before they were read at all.
func TestScheduling_UnlimitedOpensEveryPartitionAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:              21,
			Start:             start,
			Horizon:           time.Minute,
			InitialPartitions: 2_000,
			KeySpace:          20_000,
			WritesPerSecond:   10,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start))
		_ = sub.Subscribe(ctx, screamer.ConsumerFunc(func([]byte) error { return nil }))

		require.EqualValues(t, 2_000, sim.Stats().MaxOpenStreams, "every partition should be open within 3s")
	})
}
