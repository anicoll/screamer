package screamer

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

const (
	// StreamsPerChannel is how many concurrent change stream queries Screamer plans
	// for on each gRPC channel. Google front ends typically allow about 100
	// concurrent streams per HTTP/2 connection; half that leaves headroom for
	// other requests and for partitions that are starting or finishing.
	StreamsPerChannel = 50

	defaultSpannerChannels = 4 // the Spanner Go client's own default
)

// ChannelsForPartitions returns the number of gRPC channels a Spanner client needs
// to hold maxConcurrentPartitions change stream queries open at once without
// queueing. It never returns less than the Spanner client's default of 4.
// For maxConcurrentPartitions <= 0 (unlimited) it returns the default.
func ChannelsForPartitions(maxConcurrentPartitions int) int {
	if maxConcurrentPartitions <= 0 {
		return defaultSpannerChannels
	}
	n := (maxConcurrentPartitions + StreamsPerChannel - 1) / StreamsPerChannel
	if n < defaultSpannerChannels {
		return defaultSpannerChannels
	}
	return n
}

// NewChangeStreamClient creates a Spanner client sized for reading change streams:
// its gRPC channel pool can hold maxConcurrentPartitions open queries
// (see ChannelsForPartitions).
//
// Use this client only for the subscriber's change stream reads, and give the
// PartitionStorage its own client. Change stream queries stay open for as long as
// a partition lives. When they share channels with metadata writes and runner
// heartbeats, a full channel pool delays those writes, the runner looks dead to
// the other runners, and they take over partitions it is still reading.
func NewChangeStreamClient(ctx context.Context, database string, maxConcurrentPartitions int, opts ...option.ClientOption) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{
		NumChannels: ChannelsForPartitions(maxConcurrentPartitions),
	}, opts...)
}
