package screamer

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

// ChangeStreamReader reads the change records of a single change stream partition.
//
// Read starts at partition.Watermark (inclusive) and streams until the partition
// ends (Spanner returns its ChildPartitionsRecords), partition.EndTimestamp is
// reached, ctx is canceled, or an error occurs. Each call to fn carries the
// records of one result row, in commit timestamp order. If fn returns an error,
// Read stops and returns that error.
//
// For the root partition (partition.IsRootPartition()) the query is made without
// a partition token, and Spanner returns the initial child partitions.
//
// The default implementation queries Spanner. A custom implementation can be
// set with WithChangeStreamReader, for example to run the subscriber against a
// simulated change stream in tests.
//
// Implementations must be safe for concurrent use: Read is called once per
// partition, from many goroutines at the same time.
type ChangeStreamReader interface {
	Read(ctx context.Context, partition *PartitionMetadata, fn func(records []*ChangeRecord) error) error
}

// spannerChangeStreamReader reads a change stream with the READ_<stream> table-valued function.
type spannerChangeStreamReader struct {
	client     *spanner.Client
	streamName string
	priority   spannerpb.RequestOptions_Priority
}

// Read implements ChangeStreamReader.
func (r *spannerChangeStreamReader) Read(ctx context.Context, p *PartitionMetadata, fn func([]*ChangeRecord) error) error {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)", r.streamName),
		Params: map[string]interface{}{
			"startTimestamp":        p.Watermark,
			"endTimestamp":          p.EndTimestamp,
			"partitionToken":        p.PartitionToken,
			"heartbeatMilliseconds": p.HeartbeatMillis,
		},
	}

	if p.IsRootPartition() {
		// Must be converted to NULL (root partition).
		stmt.Params["partitionToken"] = nil
	}

	iter := r.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: r.priority})

	return iter.Do(func(row *spanner.Row) error {
		records := []*ChangeRecord{}
		if err := row.Columns(&records); err != nil {
			return err
		}
		return fn(records)
	})
}
