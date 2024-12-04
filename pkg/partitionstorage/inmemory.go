package partitionstorage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/anicoll/screamer/pkg/model"
	"github.com/anicoll/screamer/pkg/scream"
)

// InmemoryPartitionStorage implements PartitionStorage that stores PartitionMetadata in memory.
type InmemoryPartitionStorage struct {
	mu sync.Mutex
	m  map[string]*model.PartitionMetadata
}

// NewInmemory creates new instance of InmemoryPartitionStorage
func NewInmemory() *InmemoryPartitionStorage {
	return &InmemoryPartitionStorage{
		m: make(map[string]*model.PartitionMetadata),
	}
}

func (s *InmemoryPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*model.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*model.PartitionMetadata{}
	for _, p := range s.m {
		if p.State != model.StateFinished {
			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		return nil, nil
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Watermark.Before(partitions[j].Watermark) })
	return partitions[0], nil
}

func (s *InmemoryPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*model.PartitionMetadata, error) {
	// InmemoryPartitionStorage can't return any partitions
	return nil, nil
}

func (s *InmemoryPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := &model.PartitionMetadata{
		PartitionToken:  model.RootPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatInterval.Milliseconds(),
		State:           model.StateCreated,
		Watermark:       startTimestamp,
		CreatedAt:       time.Now(),
	}
	s.m[p.PartitionToken] = p

	return nil
}

func (s *InmemoryPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*model.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*model.PartitionMetadata{}
	for _, p := range s.m {
		if p.State == model.StateCreated && !minWatermark.After(p.StartTimestamp) {
			partitions = append(partitions, p)
		}
	}

	return partitions, nil
}

func (s *InmemoryPartitionStorage) AddChildPartitions(ctx context.Context, parent *model.PartitionMetadata, r *model.ChildPartitionsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range r.ChildPartitions {
		p := &model.PartitionMetadata{
			PartitionToken:  v.Token,
			ParentTokens:    v.ParentPartitionTokens,
			StartTimestamp:  r.StartTimestamp,
			EndTimestamp:    parent.EndTimestamp,
			HeartbeatMillis: parent.HeartbeatMillis,
			State:           model.StateCreated,
			Watermark:       r.StartTimestamp,
		}
		s.m[p.PartitionToken] = p
	}

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToScheduled(ctx context.Context, partitions []*model.PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, p := range partitions {
		p = s.m[p.PartitionToken]
		p.ScheduledAt = &now
		p.State = model.StateScheduled
	}

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToRunning(ctx context.Context, partition *model.PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partition.PartitionToken]
	p.RunningAt = &now
	p.State = model.StateRunning

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToFinished(ctx context.Context, partition *model.PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partition.PartitionToken]
	p.FinishedAt = &now
	p.State = model.StateFinished

	return nil
}

func (s *InmemoryPartitionStorage) UpdateWatermark(ctx context.Context, partition *model.PartitionMetadata, watermark time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[partition.PartitionToken].Watermark = watermark

	return nil
}

// Assert that InmemoryPartitionStorage implements PartitionStorage.
var _ scream.PartitionStorage = (*InmemoryPartitionStorage)(nil)
