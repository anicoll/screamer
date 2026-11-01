package screamer

import (
	"context"
	"time"
)

// ParentGatedScheduler is an optional extension of PartitionStorage that schedules
// a child partition only once every one of its parents has finished. Both
// built-in storages implement it.
//
// A merged child partition has several parents. Its first record can be later
// than the last records of every parent, but the parents finish at different
// times. Reading the child while a parent is still behind delivers changes to
// the same key out of commit order. Parent gating prevents that.
//
// Parent gating is on by default (see WithParentGating). Subscribe returns an
// error if it is on and the PartitionStorage does not implement this interface.
type ParentGatedScheduler interface {
	// GetAndScheduleReadyPartitions finds CREATED partitions whose parents have all
	// FINISHED, marks up to limit of them SCHEDULED for runnerID and returns them.
	// A parent that is no longer in the storage (for example deleted by a row
	// deletion policy after it finished) counts as finished.
	GetAndScheduleReadyPartitions(ctx context.Context, runnerID string, limit int) ([]*PartitionMetadata, error)
}

// schedulePartitions picks the next partitions to read, honouring parent gating
// when it is on.
func (s *Subscriber) schedulePartitions(ctx context.Context, minWatermark time.Time, limit int) ([]*PartitionMetadata, error) {
	if s.gatedScheduler != nil {
		return s.gatedScheduler.GetAndScheduleReadyPartitions(ctx, s.runnerID, limit)
	}
	return s.partitionStorage.GetAndSchedulePartitions(ctx, minWatermark, s.runnerID, limit)
}
