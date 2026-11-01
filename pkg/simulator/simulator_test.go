package simulator

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/stretchr/testify/require"
)

func churnConfig(start time.Time) Config {
	return Config{
		Seed:                  42,
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
	}
}

func TestTopologyCoversKeySpace(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := churnConfig(start)
	sim := New(cfg)

	require.Positive(t, sim.Splits())
	require.Positive(t, sim.Merges())

	// At any instant the live partitions tile the key space exactly.
	for d := time.Duration(0); d < cfg.Horizon; d += 7 * time.Second {
		live := sim.LiveAt(start.Add(d))
		require.NotEmpty(t, live)
		require.Equal(t, int64(0), live[0].KeyLow)
		for i := 1; i < len(live); i++ {
			require.Equal(t, live[i-1].KeyHigh, live[i].KeyLow, "gap or overlap at %s", d)
		}
		require.Equal(t, int64(cfg.KeySpace), live[len(live)-1].KeyHigh)
		require.LessOrEqual(t, len(live), cfg.MaxPartitions)
	}

	// Every write belongs to exactly one partition, which covered its key at its commit time.
	for _, w := range sim.Writes() {
		p := sim.Partitions()[w.Partition]
		require.NotNil(t, p)
		require.True(t, w.Key >= p.KeyLow && w.Key < p.KeyHigh)
		require.False(t, w.CommitTimestamp.Before(p.Start))
		if !p.End.IsZero() {
			require.True(t, w.CommitTimestamp.Before(p.End))
		}
	}

	// Parent/child links are consistent.
	for _, p := range sim.Partitions() {
		for _, c := range p.Children {
			require.Contains(t, sim.Partitions()[c].Parents, p.Token)
			require.Equal(t, p.End, sim.Partitions()[c].Start)
		}
	}
}

func TestSameSeedSameTopology(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a, b := New(churnConfig(start)), New(churnConfig(start))
	require.Equal(t, len(a.Partitions()), len(b.Partitions()))
	require.Equal(t, len(a.Writes()), len(b.Writes()))
	require.Equal(t, a.Splits(), b.Splits())
}

func TestReadStreamsInRealTimeAndEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := New(Config{
			Start:                 start,
			Horizon:               time.Minute,
			InitialPartitions:     1,
			KeySpace:              100,
			MeanPartitionLifetime: 30 * time.Second,
			WritesPerSecond:       10,
			MaxPartitions:         2,
		})

		root := &screamer.PartitionMetadata{PartitionToken: screamer.RootPartitionToken, Watermark: start, EndTimestamp: start.Add(time.Hour)}
		var initial []*screamer.ChildPartition
		require.NoError(t, sim.Read(context.Background(), root, func(rs []*screamer.ChangeRecord) error {
			initial = rs[0].ChildPartitionsRecords[0].ChildPartitions
			return nil
		}))
		require.Len(t, initial, 1)

		part := sim.Partitions()[initial[0].Token]
		p := &screamer.PartitionMetadata{PartitionToken: part.Token, Watermark: start, EndTimestamp: start.Add(time.Hour), HeartbeatMillis: 1000}

		var data, heartbeats int
		var children []*screamer.ChildPartition
		require.NoError(t, sim.Read(context.Background(), p, func(rs []*screamer.ChangeRecord) error {
			for _, r := range rs {
				for _, d := range r.DataChangeRecords {
					require.False(t, d.CommitTimestamp.After(time.Now()), "record delivered before it was committed")
				}
				data += len(r.DataChangeRecords)
				heartbeats += len(r.HeartbeatRecords)
				for _, c := range r.ChildPartitionsRecords {
					children = append(children, c.ChildPartitions...)
				}
			}
			return nil
		}))
		require.Equal(t, len(part.writes), data)
		if part.End.IsZero() {
			require.Empty(t, children)
		} else {
			require.Len(t, children, len(part.Children))
			require.Equal(t, part.End, time.Now(), "query ends when the partition ends")
		}
		_ = heartbeats
	})
}

func TestReadHeartbeatsWhenIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := New(Config{Start: start, Horizon: time.Minute, InitialPartitions: 1})
		p := &screamer.PartitionMetadata{PartitionToken: sim.LiveAt(start)[0].Token, Watermark: start, EndTimestamp: start.Add(10 * time.Second), HeartbeatMillis: 1000}
		var heartbeats []time.Time
		require.NoError(t, sim.Read(context.Background(), p, func(rs []*screamer.ChangeRecord) error {
			for _, h := range rs[0].HeartbeatRecords {
				heartbeats = append(heartbeats, h.Timestamp)
			}
			return nil
		}))
		require.Len(t, heartbeats, 9, "one per second, none at the end timestamp")
		require.Equal(t, start.Add(10*time.Second), time.Now())
	})
}

func TestMaxConcurrentStreamsBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := New(Config{Start: start, Horizon: time.Minute, InitialPartitions: 4, MaxConcurrentStreams: 2})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done := make(chan error, 4)
		for _, p := range sim.LiveAt(start) {
			pm := &screamer.PartitionMetadata{PartitionToken: p.Token, Watermark: start, EndTimestamp: start.Add(time.Hour), HeartbeatMillis: 1000}
			go func() { done <- sim.Read(ctx, pm, func([]*screamer.ChangeRecord) error { return nil }) }()
		}
		synctest.Wait()
		st := sim.Stats()
		require.EqualValues(t, 2, st.OpenStreams)
		require.EqualValues(t, 2, st.BlockedOnStreams)
		for range 4 {
			require.ErrorIs(t, <-done, context.DeadlineExceeded)
		}
	})
}
