package simulator_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func gauges(t *testing.T, reader *sdkmetric.ManualReader) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				out[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Gauge[float64]:
				out[m.Name] = d.DataPoints[0].Value
			case metricdata.Sum[int64]:
				out[m.Name] = float64(d.DataPoints[0].Value)
			}
		}
	}
	return out
}

// Under-provisioned: 40 partitions, one runner capped at 10. The metrics say so.
func TestMetrics_ReportDesiredRunnersAndLag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(simulator.Config{
			Seed:              6,
			Start:             start,
			Horizon:           5 * time.Minute,
			InitialPartitions: 40,
			KeySpace:          4_000,
			WritesPerSecond:   20,
		})
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

		ctx, cancel := context.WithCancel(context.Background())
		sub := newSubscriber(sim,
			screamer.WithStartTimestamp(start),
			screamer.WithMaxConcurrentPartitions(10),
			screamer.WithMeterProvider(mp))
		errc := make(chan error, 1)
		go func() { errc <- sub.Subscribe(ctx, simulator.NewChecker(sim)) }()

		time.Sleep(2 * time.Minute)
		synctest.Wait()

		c := sub.GetClusterMetrics()
		t.Logf("cluster=%+v", c)
		require.Equal(t, 40, c.UnfinishedPartitions)
		require.Equal(t, 6, c.DesiredRunners, "ceil(40 / (10 × 0.75))")
		require.Equal(t, 1, c.LiveRunners)
		require.Greater(t, c.WatermarkLag, time.Minute, "partitions that are never read fall behind")

		g := gauges(t, reader)
		t.Logf("gauges=%v", g)
		require.EqualValues(t, 6, g["screamer.runners.desired"])
		require.EqualValues(t, 40, g["screamer.partitions.unfinished"])
		require.EqualValues(t, 10, g["screamer.partitions.capacity"])
		require.LessOrEqual(t, g["screamer.partitions.active"], 15.0)
		require.Greater(t, g["screamer.watermark.lag"], 60.0)

		cancel()
		<-errc
	})
}

func TestDesiredRunners(t *testing.T) {
	for _, tc := range []struct{ unfinished, limit, want int }{
		{0, 100, 1},
		{75, 100, 1},
		{76, 100, 2},
		{360, 100, 5},
		{5000, 100, 67},
		{100, 0, 0},  // no limit: unknown capacity
		{-1, 100, 0}, // storage cannot count
	} {
		require.Equal(t, tc.want, screamer.DesiredRunners(tc.unfinished, tc.limit), "%+v", tc)
	}
}
