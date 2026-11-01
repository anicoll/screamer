package partitionstorage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/anicoll/screamer"
)

// InmemoryPartitionStorage implements PartitionStorage that stores PartitionMetadata in memory.
type InmemoryPartitionStorage struct {
	mu sync.Mutex
	m  map[string]*screamer.PartitionMetadata
}

// NewInmemory creates a new instance of InmemoryPartitionStorage.
func NewInmemory() *InmemoryPartitionStorage {
	return &InmemoryPartitionStorage{
		m: make(map[string]*screamer.PartitionMetadata),
	}
}

// GetUnfinishedMinWatermarkPartition returns the unfinished partition with the minimum watermark.
// Returns nil if there are no unfinished partitions.
func (s *InmemoryPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*screamer.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A single pass: this runs every second, and sorting every partition to find
	// the minimum cost O(n log n) time and an O(n) allocation each time.
	var min *screamer.PartitionMetadata
	for _, p := range s.m {
		if p.State == screamer.StateFinished {
			continue
		}
		if min == nil || p.Watermark.Before(min.Watermark) || (p.Watermark.Equal(min.Watermark) && p.PartitionToken < min.PartitionToken) {
			min = p
		}
	}
	if min == nil {
		return nil, nil
	}
	// Return a copy: the stored partition keeps changing under s.mu while the caller reads it.
	c := *min
	return &c, nil
}

// GetInterruptedPartitions is a no-op for in-memory storage and always returns nil.
// The limit parameter is ignored as this implementation doesn't track runner assignments.
func (s *InmemoryPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	// InmemoryPartitionStorage can't return any partitions
	return nil, nil
}

// InitializeRootPartition creates or updates the root partition metadata in memory.
func (s *InmemoryPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := &screamer.PartitionMetadata{
		PartitionToken:  screamer.RootPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatInterval.Milliseconds(),
		State:           screamer.StateCreated,
		Watermark:       startTimestamp,
		CreatedAt:       time.Now().UTC(),
	}
	s.m[p.PartitionToken] = p

	return nil
}

// GetSchedulablePartitions returns partitions that are ready to be scheduled based on the minimum watermark.
func (s *InmemoryPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*screamer.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*screamer.PartitionMetadata{}
	for _, p := range s.m {
		if p.State == screamer.StateCreated && !minWatermark.After(p.StartTimestamp) {
			partitions = append(partitions, p)
		}
	}

	return partitions, nil
}

// GetAndSchedulePartitions finds partitions ready to be scheduled and marks them as scheduled.
// The limit parameter restricts the maximum number of partitions to schedule.
func (s *InmemoryPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*screamer.PartitionMetadata{}
	now := time.Now().UTC()

	for _, p := range s.m {
		if len(partitions) >= limit {
			break
		}
		if p.State == screamer.StateCreated && !minWatermark.After(p.StartTimestamp) {
			p.ScheduledAt = &now
			p.State = screamer.StateScheduled
			// Return a copy: the stored partition keeps changing under s.mu while the caller reads it.
			c := *p
			partitions = append(partitions, &c)
		}
	}

	return partitions, nil
}

// GetAndScheduleReadyPartitions finds CREATED partitions whose parents have all
// finished, marks up to limit of them as scheduled and returns them, oldest first.
func (s *InmemoryPartitionStorage) GetAndScheduleReadyPartitions(ctx context.Context, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ready []*screamer.PartitionMetadata
	for _, p := range s.m {
		if p.State == screamer.StateCreated && s.parentsFinished(p) {
			ready = append(ready, p)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].StartTimestamp.Equal(ready[j].StartTimestamp) {
			return ready[i].StartTimestamp.Before(ready[j].StartTimestamp)
		}
		return ready[i].PartitionToken < ready[j].PartitionToken
	})
	if len(ready) > limit {
		ready = ready[:limit]
	}

	now := time.Now().UTC()
	out := make([]*screamer.PartitionMetadata, 0, len(ready))
	for _, p := range ready {
		p.ScheduledAt = &now
		p.State = screamer.StateScheduled
		c := *p
		out = append(out, &c)
	}
	return out, nil
}

// parentsFinished reports whether every parent of p has finished. s.mu must be held.
//
// A parent that is not stored yet may still be on its way: the other parent of a
// merge can report the child before this parent's own ancestors have even
// reported it. So a missing parent only counts as finished once no unfinished
// partition is behind the child's start, which also covers a parent that
// finished and was removed.
func (s *InmemoryPartitionStorage) parentsFinished(p *screamer.PartitionMetadata) bool {
	missing := false
	for _, token := range p.ParentTokens {
		parent, ok := s.m[token]
		if !ok {
			missing = true
			continue
		}
		if parent.State != screamer.StateFinished {
			return false
		}
	}
	if !missing {
		return true
	}
	for _, o := range s.m {
		if o.State != screamer.StateFinished && o.Watermark.Before(p.StartTimestamp) {
			return false
		}
	}
	return true
}

// UpdateWatermarks updates the watermarks of many partitions at once. A watermark
// never moves backwards, and unknown partitions are ignored.
func (s *InmemoryPartitionStorage) UpdateWatermarks(ctx context.Context, watermarks []screamer.PartitionWatermark) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, w := range watermarks {
		if p, ok := s.m[w.PartitionToken]; ok && w.Watermark.After(p.Watermark) {
			p.Watermark = w.Watermark
		}
	}
	return nil
}

// RefreshRunner is a no-op for in-memory storage.
func (s *InmemoryPartitionStorage) RefreshRunner(ctx context.Context, runnerID string) error {
	return nil
}

// AddChildPartitions adds new child partitions for a parent partition based on a ChildPartitionsRecord.
func (s *InmemoryPartitionStorage) AddChildPartitions(ctx context.Context, parent *screamer.PartitionMetadata, r *screamer.ChildPartitionsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range r.ChildPartitions {
		// A merged child is reported by every parent; only the first report creates it.
		// Overwriting it would reset a running or finished child to CREATED and read it again.
		if _, ok := s.m[v.Token]; ok {
			continue
		}
		p := &screamer.PartitionMetadata{
			PartitionToken:  v.Token,
			ParentTokens:    v.ParentPartitionTokens,
			StartTimestamp:  r.StartTimestamp,
			EndTimestamp:    parent.EndTimestamp,
			HeartbeatMillis: parent.HeartbeatMillis,
			State:           screamer.StateCreated,
			Watermark:       r.StartTimestamp,
		}
		s.m[p.PartitionToken] = p
	}

	return nil
}

// UpdateToScheduled marks the given partitions as scheduled and sets the ScheduledAt timestamp.
func (s *InmemoryPartitionStorage) UpdateToScheduled(ctx context.Context, partitions []*screamer.PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	for _, p := range partitions {
		p = s.m[p.PartitionToken]
		p.ScheduledAt = &now
		p.State = screamer.StateScheduled
	}

	return nil
}

// UpdateToRunning marks the given partition as running and sets the RunningAt timestamp.
func (s *InmemoryPartitionStorage) UpdateToRunning(ctx context.Context, partition *screamer.PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	p := s.m[partition.PartitionToken]
	p.RunningAt = &now
	p.State = screamer.StateRunning

	return nil
}

// UpdateToFinished marks the given partition as finished and sets the FinishedAt timestamp.
func (s *InmemoryPartitionStorage) UpdateToFinished(ctx context.Context, partition *screamer.PartitionMetadata, runnerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()

	p := s.m[partition.PartitionToken]
	p.FinishedAt = &now
	p.State = screamer.StateFinished

	return nil
}

// UpdateWatermark updates the watermark for the given partition.
func (s *InmemoryPartitionStorage) UpdateWatermark(ctx context.Context, partition *screamer.PartitionMetadata, watermark time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[partition.PartitionToken].Watermark = watermark

	return nil
}

// GetActiveRunnerCount returns 1 for in-memory storage since it only supports single runner.
// This is a no-op implementation as in-memory storage doesn't track multiple runners.
func (s *InmemoryPartitionStorage) GetActiveRunnerCount(ctx context.Context) (int, error) {
	// In-memory storage is always single-runner
	return 1, nil
}

// CountUnfinishedPartitions returns the number of partitions that are not FINISHED.
func (s *InmemoryPartitionStorage) CountUnfinishedPartitions(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, p := range s.m {
		if p.State != screamer.StateFinished {
			n++
		}
	}
	return n, nil
}

var (
	_ screamer.PartitionCounter      = (*InmemoryPartitionStorage)(nil)
	_ screamer.ParentGatedScheduler  = (*InmemoryPartitionStorage)(nil)
	_ screamer.WatermarkBatchUpdater = (*InmemoryPartitionStorage)(nil)
)

// Assert that InmemoryPartitionStorage implements PartitionStorage.
var _ screamer.PartitionStorage = (*InmemoryPartitionStorage)(nil)
