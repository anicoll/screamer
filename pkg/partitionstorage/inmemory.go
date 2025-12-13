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

	partitions := []*screamer.PartitionMetadata{}
	for _, p := range s.m {
		if p.State != screamer.StateFinished {
			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		return nil, nil
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Watermark.Before(partitions[j].Watermark) })
	return partitions[0], nil
}

// GetInterruptedPartitions returns partitions that are scheduled or running but have lost their runner.
func (s *InmemoryPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string, leaseDuration time.Duration) ([]*screamer.PartitionMetadata, error) {
	// InmemoryPartitionStorage can't return any partitions
	return nil, nil
}

// GetActiveRunnerCount returns the number of active runners.
func (s *InmemoryPartitionStorage) GetActiveRunnerCount(ctx context.Context, leaseDuration time.Duration) (int64, error) {
	return 1, nil
}

// GetActivePartitionCount returns the number of active (scheduled or running) partitions.
func (s *InmemoryPartitionStorage) GetActivePartitionCount(ctx context.Context) (int64, error) {
	return 0, nil
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
func (s *InmemoryPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, maxConnections int) ([]*screamer.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*screamer.PartitionMetadata{}
	now := time.Now().UTC()

	for _, p := range s.m {
		if p.State == screamer.StateCreated && !minWatermark.After(p.StartTimestamp) {
			p = s.m[p.PartitionToken]
			p.ScheduledAt = &now
			p.State = screamer.StateScheduled
			partitions = append(partitions, p)
		}
	}

	return partitions, nil
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

// ExtendLease is a no-op for in-memory storage.
func (s *InmemoryPartitionStorage) ExtendLease(ctx context.Context, partitionToken string, runnerID string) error {
	return nil
}

// ReleaseLease is a no-op for in-memory storage.
func (s *InmemoryPartitionStorage) ReleaseLease(ctx context.Context, partitionToken string, runnerID string) error {
	return nil
}

// Assert that InmemoryPartitionStorage implements PartitionStorage.
var _ screamer.PartitionStorage = (*InmemoryPartitionStorage)(nil)
