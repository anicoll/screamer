package screamer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelsForPartitions(t *testing.T) {
	for _, tc := range []struct{ partitions, want int }{
		{0, 4},    // unlimited: the client default
		{100, 4},  // 2 needed, never below the default
		{200, 4},  // exactly 4
		{201, 5},  // rounds up
		{500, 10}, // the size where deployments start failing today
		{5000, 100},
	} {
		require.Equal(t, tc.want, ChannelsForPartitions(tc.partitions), "partitions=%d", tc.partitions)
	}
}

func TestRetryDelay(t *testing.T) {
	for attempt, base := range []float64{1, 2, 4, 8, 16, 30, 30} {
		d := retryDelay(attempt).Seconds()
		require.GreaterOrEqual(t, d, base, "attempt %d", attempt)
		require.LessOrEqual(t, d, base*1.2, "attempt %d", attempt)
	}
	require.LessOrEqual(t, retryDelay(100).Seconds(), 36.0, "no overflow")
}
