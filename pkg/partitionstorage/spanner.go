package partitionstorage

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/anicoll/scream/pkg/model"
	"github.com/anicoll/scream/pkg/scream"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// InmemoryPartitionStorage implements PartitionStorage that stores PartitionMetadata in Cloud Spanner.
type SpannerPartitionStorage struct {
	client          *spanner.Client
	tableName       string
	requestPriority spannerpb.RequestOptions_Priority
}

type spannerConfig struct {
	requestPriority spannerpb.RequestOptions_Priority
}

type spannerOption interface {
	Apply(*spannerConfig)
}

type withRequestPriotiry spannerpb.RequestOptions_Priority

func (o withRequestPriotiry) Apply(c *spannerConfig) {
	c.requestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithRequestPriotiry set the priority option for spanner requests.
// Default value is unspecified, equivalent to high.
func WithRequestPriotiry(priority spannerpb.RequestOptions_Priority) spannerOption {
	return withRequestPriotiry(priority)
}

// NewSpanner creates new instance of SpannerPartitionStorage
func NewSpanner(client *spanner.Client, tableName string, options ...spannerOption) *SpannerPartitionStorage {
	c := &spannerConfig{}
	for _, o := range options {
		o.Apply(c)
	}

	return &SpannerPartitionStorage{
		client:          client,
		tableName:       tableName,
		requestPriority: c.requestPriority,
	}
}

const (
	columnPartitionToken  = "PartitionToken"
	columnParentTokens    = "ParentTokens"
	columnStartTimestamp  = "StartTimestamp"
	columnEndTimestamp    = "EndTimestamp"
	columnHeartbeatMillis = "HeartbeatMillis"
	columnState           = "State"
	columnWatermark       = "Watermark"
	columnCreatedAt       = "CreatedAt"
	columnScheduledAt     = "ScheduledAt"
	columnRunningAt       = "RunningAt"
	columnFinishedAt      = "FinishedAt"
)

func (s *SpannerPartitionStorage) CreateTableIfNotExists(ctx context.Context) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
  %[2]s STRING(MAX) NOT NULL,
  %[3]s ARRAY<STRING(MAX)> NOT NULL,
  %[4]s TIMESTAMP NOT NULL,
  %[5]s TIMESTAMP NOT NULL,
  %[6]s INT64 NOT NULL,
  %[7]s STRING(MAX) NOT NULL,
  %[8]s TIMESTAMP NOT NULL,
  %[9]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  %[10]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  %[11]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  %[12]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (%[2]s), ROW DELETION POLICY (OLDER_THAN(%[12]s, INTERVAL 1 DAY))`,
		s.tableName,
		columnPartitionToken,
		columnParentTokens,
		columnStartTimestamp,
		columnEndTimestamp,
		columnHeartbeatMillis,
		columnState,
		columnWatermark,
		columnCreatedAt,
		columnScheduledAt,
		columnRunningAt,
		columnFinishedAt,
	)

	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.client.DatabaseName(),
		Statements: []string{stmt},
	}
	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return err
	}

	if err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (s *SpannerPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*model.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State != @state ORDER BY Watermark ASC LIMIT 1", s.tableName),
		Params: map[string]interface{}{
			"state": model.StateFinished,
		},
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})
	defer iter.Stop()

	r, err := iter.Next()
	switch err {
	case iterator.Done:
		return nil, nil
	case nil:
		// break
	default:
		return nil, err
	}

	partition := new(model.PartitionMetadata)
	if err := r.ToStruct(partition); err != nil {
		return nil, err
	}

	return partition, nil
}

func (s *SpannerPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*model.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State IN UNNEST(@states) ORDER BY Watermark ASC", s.tableName),
		Params: map[string]interface{}{
			"states": []model.State{model.StateScheduled, model.StateRunning},
		},
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})

	partitions := []*model.PartitionMetadata{}
	if err := iter.Do(func(r *spanner.Row) error {
		p := new(model.PartitionMetadata)
		if err := r.ToStruct(p); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

func (s *SpannerPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	m := spanner.InsertOrUpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken:  model.RootPartitionToken,
		columnParentTokens:    []string{},
		columnStartTimestamp:  startTimestamp,
		columnEndTimestamp:    endTimestamp,
		columnHeartbeatMillis: heartbeatInterval.Milliseconds(),
		columnState:           model.StateCreated,
		columnWatermark:       startTimestamp,
		columnCreatedAt:       spanner.CommitTimestamp,
		columnScheduledAt:     nil,
		columnRunningAt:       nil,
		columnFinishedAt:      nil,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*model.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State = @state AND StartTimestamp >= @minWatermark ORDER BY StartTimestamp ASC", s.tableName),
		Params: map[string]interface{}{
			"state":        model.StateCreated,
			"minWatermark": minWatermark,
		},
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})

	partitions := []*model.PartitionMetadata{}
	if err := iter.Do(func(r *spanner.Row) error {
		p := new(model.PartitionMetadata)
		if err := r.ToStruct(p); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

func (s *SpannerPartitionStorage) AddChildPartitions(ctx context.Context, parent *model.PartitionMetadata, r *model.ChildPartitionsRecord) error {
	for _, p := range r.ChildPartitions {
		m := spanner.InsertMap(s.tableName, map[string]interface{}{
			columnPartitionToken:  p.Token,
			columnParentTokens:    p.ParentPartitionTokens,
			columnStartTimestamp:  r.StartTimestamp,
			columnEndTimestamp:    parent.EndTimestamp,
			columnHeartbeatMillis: parent.HeartbeatMillis,
			columnState:           model.StateCreated,
			columnWatermark:       r.StartTimestamp,
			columnCreatedAt:       spanner.CommitTimestamp,
		})

		if _, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority)); err != nil {
			// Ignore the AlreadyExists error because a child partition can be found multiple times if partitions are merged.
			if spanner.ErrCode(err) == codes.AlreadyExists {
				continue
			}
			return err
		}
	}

	return nil
}

func (s *SpannerPartitionStorage) UpdateToScheduled(ctx context.Context, partitions []*model.PartitionMetadata) error {
	mutations := make([]*spanner.Mutation, 0, len(partitions))
	for _, p := range partitions {
		m := spanner.UpdateMap(s.tableName, map[string]interface{}{
			columnPartitionToken: p.PartitionToken,
			columnState:          model.StateScheduled,
			columnScheduledAt:    spanner.CommitTimestamp,
		})
		mutations = append(mutations, m)
	}

	_, err := s.client.Apply(ctx, mutations, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateToRunning(ctx context.Context, partition *model.PartitionMetadata) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          model.StateRunning,
		columnRunningAt:      spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateToFinished(ctx context.Context, partition *model.PartitionMetadata) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          model.StateFinished,
		columnFinishedAt:     spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateWatermark(ctx context.Context, partition *model.PartitionMetadata, watermark time.Time) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnWatermark:      watermark,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

// Assert that SpannerPartitionStorage implements PartitionStorage.
var _ scream.PartitionStorage = (*SpannerPartitionStorage)(nil)
