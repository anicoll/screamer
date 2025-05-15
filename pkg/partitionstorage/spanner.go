package partitionstorage

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/anicoll/screamer"
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
	columnUpdatedAt       = "UpdatedAt"
	columnEndTimestamp    = "EndTimestamp"
	columnHeartbeatMillis = "HeartbeatMillis"
	columnState           = "State"
	columnWatermark       = "Watermark"
	columnCreatedAt       = "CreatedAt"
	columnScheduledAt     = "ScheduledAt"
	columnRunningAt       = "RunningAt"
	columnFinishedAt      = "FinishedAt"
	columnRunnerID        = "RunnerID"
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
  %[6]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  %[7]s INT64 NOT NULL,
  %[8]s STRING(MAX) NOT NULL,
  %[9]s TIMESTAMP NOT NULL,
  %[10]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  %[11]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  %[12]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  %[13]s TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  %[14]s STRING(MAX),
) PRIMARY KEY (%[2]s), ROW DELETION POLICY (OLDER_THAN(%[13]s, INTERVAL 1 DAY))`,
		s.tableName,
		columnPartitionToken,
		columnParentTokens,
		columnStartTimestamp,
		columnEndTimestamp,
		columnUpdatedAt,
		columnHeartbeatMillis,
		columnState,
		columnWatermark,
		columnCreatedAt,
		columnScheduledAt,
		columnRunningAt,
		columnFinishedAt,
		columnRunnerID,
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

func (s *SpannerPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*screamer.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State != @state ORDER BY Watermark ASC LIMIT 1", s.tableName),
		Params: map[string]interface{}{
			"state": screamer.StateFinished,
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

	partition := new(screamer.PartitionMetadata)
	if err := r.ToStruct(partition); err != nil {
		return nil, err
	}

	return partition, nil
}

func (s *SpannerPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string) ([]*screamer.PartitionMetadata, error) {
	var partitions []*screamer.PartitionMetadata

	if _, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		partitions = make([]*screamer.PartitionMetadata, 0)
		stmt := spanner.Statement{
			SQL: fmt.Sprintf(
				"SELECT * FROM %s WHERE State IN ('Scheduled', 'Running') AND UpdatedAt < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 SECOND) ORDER BY Watermark ASC FOR UPDATE",
				s.tableName,
			),
		}

		iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})

		if err := iter.Do(func(r *spanner.Row) error {
			p := new(screamer.PartitionMetadata)
			if err := r.ToStruct(p); err != nil {
				return err
			}
			// dont process if taken by another runner and still running.
			if p.RunnerID != nil && *p.RunnerID != runnerID {
				return nil
			}
			partitions = append(partitions, p)

			m := spanner.UpdateMap(s.tableName, map[string]interface{}{
				columnPartitionToken: p.PartitionToken,
				columnRunnerID:       runnerID,
				columnUpdatedAt:      spanner.CommitTimestamp,
			})

			return tx.BufferWrite([]*spanner.Mutation{m})
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

func (s *SpannerPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	m := spanner.InsertOrUpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken:  screamer.RootPartitionToken,
		columnParentTokens:    []string{},
		columnStartTimestamp:  startTimestamp,
		columnEndTimestamp:    endTimestamp,
		columnHeartbeatMillis: heartbeatInterval.Milliseconds(),
		columnState:           screamer.StateCreated,
		columnWatermark:       startTimestamp,
		columnCreatedAt:       spanner.CommitTimestamp,
		columnUpdatedAt:       spanner.CommitTimestamp,
		columnScheduledAt:     nil,
		columnRunningAt:       nil,
		columnFinishedAt:      nil,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string) ([]*screamer.PartitionMetadata, error) {
	var partitions []*screamer.PartitionMetadata

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		partitions = make([]*screamer.PartitionMetadata, 0)
		mutations := make([]*spanner.Mutation, 0, len(partitions))

		stmt := spanner.Statement{
			SQL: fmt.Sprintf("SELECT * FROM %s WHERE State = @state AND StartTimestamp >= @minWatermark ORDER BY StartTimestamp ASC FOR UPDATE", s.tableName),
			Params: map[string]interface{}{
				"state":        screamer.StateCreated,
				"minWatermark": minWatermark,
			},
		}

		iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})

		if err := iter.Do(func(r *spanner.Row) error {
			p := new(screamer.PartitionMetadata)
			if err := r.ToStruct(p); err != nil {
				return err
			}

			m := spanner.UpdateMap(s.tableName, map[string]interface{}{
				columnPartitionToken: p.PartitionToken,
				columnState:          screamer.StateScheduled,
				columnScheduledAt:    spanner.CommitTimestamp,
				columnRunnerID:       runnerID,
				columnUpdatedAt:      spanner.CommitTimestamp,
			})
			mutations = append(mutations, m)

			partitions = append(partitions, p)
			return nil
		}); err != nil {
			return err
		}

		// writes the updates to spanner.
		return tx.BufferWrite(mutations)
	})
	if err != nil {
		return nil, err
	}
	return partitions, nil
}

func (s *SpannerPartitionStorage) AddChildPartitions(ctx context.Context, parent *screamer.PartitionMetadata, r *screamer.ChildPartitionsRecord) error {
	for _, p := range r.ChildPartitions {
		m := spanner.InsertMap(s.tableName, map[string]interface{}{
			columnPartitionToken:  p.Token,
			columnParentTokens:    p.ParentPartitionTokens,
			columnStartTimestamp:  r.StartTimestamp,
			columnEndTimestamp:    parent.EndTimestamp,
			columnHeartbeatMillis: parent.HeartbeatMillis,
			columnState:           screamer.StateCreated,
			columnWatermark:       r.StartTimestamp,
			columnCreatedAt:       spanner.CommitTimestamp,
			columnUpdatedAt:       spanner.CommitTimestamp,
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

func (s *SpannerPartitionStorage) UpdateToScheduled(ctx context.Context, partitions []*screamer.PartitionMetadata) error {
	mutations := make([]*spanner.Mutation, 0, len(partitions))
	for _, p := range partitions {
		m := spanner.UpdateMap(s.tableName, map[string]interface{}{
			columnPartitionToken: p.PartitionToken,
			columnState:          screamer.StateScheduled,
			columnScheduledAt:    spanner.CommitTimestamp,
			columnUpdatedAt:      spanner.CommitTimestamp,
		})
		mutations = append(mutations, m)
	}

	_, err := s.client.Apply(ctx, mutations, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateToRunning(ctx context.Context, partition *screamer.PartitionMetadata) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          screamer.StateRunning,
		columnRunningAt:      spanner.CommitTimestamp,
		columnUpdatedAt:      spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateToFinished(ctx context.Context, partition *screamer.PartitionMetadata) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          screamer.StateFinished,
		columnFinishedAt:     spanner.CommitTimestamp,
		columnUpdatedAt:      spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

func (s *SpannerPartitionStorage) UpdateWatermark(ctx context.Context, partition *screamer.PartitionMetadata, watermark time.Time) error {
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnWatermark:      watermark,
		columnUpdatedAt:      spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority))
	return err
}

// Assert that SpannerPartitionStorage implements PartitionStorage.
var _ screamer.PartitionStorage = (*SpannerPartitionStorage)(nil)
