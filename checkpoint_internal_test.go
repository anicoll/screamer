package screamer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckpointer(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := newCheckpointer()

	c.set("a", t0.Add(2*time.Second))
	c.set("a", t0.Add(time.Second)) // older: ignored
	c.set("b", t0)
	require.Equal(t, 2, c.pendingCount())

	ws := c.take()
	require.ElementsMatch(t, []PartitionWatermark{
		{PartitionToken: "a", Watermark: t0.Add(2 * time.Second)},
		{PartitionToken: "b", Watermark: t0},
	}, ws)
	require.Zero(t, c.pendingCount())
	require.Nil(t, c.take())

	// A failed flush is restored, but never over a later watermark set meanwhile.
	c.set("a", t0.Add(5*time.Second))
	c.restore(ws)
	require.ElementsMatch(t, []PartitionWatermark{
		{PartitionToken: "a", Watermark: t0.Add(5 * time.Second)},
		{PartitionToken: "b", Watermark: t0},
	}, c.take())

	c.set("a", t0)
	c.forget("a")
	require.Nil(t, c.take())
}
