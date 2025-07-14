package partitionstorage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/utils"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// SpannerPartitionStorage implements PartitionStorage that stores PartitionMetadata in Cloud Spanner.
type (
	partitionCounter struct {
		m     sync.Mutex
		count int64
	}

	SpannerPartitionStorage struct {
		client          *spanner.Client
		tableName       string
		requestPriority spannerpb.RequestOptions_Priority
		counter         *partitionCounter
	}
	spannerConfig struct {
		requestPriority spannerpb.RequestOptions_Priority
	}

	spannerOption interface {
		Apply(*spannerConfig)
	}

	withRequestPriotiry spannerpb.RequestOptions_Priority

	reader interface {
		QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
		ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	}
)

func (pc *partitionCounter) Increment() {
	pc.m.Lock()
	defer pc.m.Unlock()
	pc.count++
}

func (pc *partitionCounter) Add(delta int64) {
	pc.m.Lock()
	defer pc.m.Unlock()
	pc.count += delta
}

func (pc *partitionCounter) Decrement() {
	pc.m.Lock()
	defer pc.m.Unlock()
	pc.count--
}

func (pc *partitionCounter) Get() int64 {
	pc.m.Lock()
	defer pc.m.Unlock()
	return pc.count
}

func (o withRequestPriotiry) Apply(c *spannerConfig) {
	c.requestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithRequestPriotiry sets the priority option for Spanner requests.
// Default value is unspecified, equivalent to high.
func WithRequestPriotiry(priority spannerpb.RequestOptions_Priority) spannerOption {
	return withRequestPriotiry(priority)
}

// NewSpanner creates a new instance of SpannerPartitionStorage for the given Spanner client and table name.
// Optional spannerOption(s) can be provided to configure request priority.
func NewSpanner(client *spanner.Client, tableName string, options ...spannerOption) *SpannerPartitionStorage {
	c := &spannerConfig{}
	for _, o := range options {
		o.Apply(c)
	}

	return &SpannerPartitionStorage{
		client:          client,
		tableName:       tableName,
		requestPriority: c.requestPriority,
		counter:         &partitionCounter{},
	}
}

const (
	tablePartitionToRunner = "PartitionToRunner"
	tableRunner            = "Runner"

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

	columnUpdatedAt      = "UpdatedAt"
	columnRunnerID       = "RunnerID"
	columnPartitionCount = "PartitionCount"
)

// GetUnfinishedMinWatermarkPartition returns the unfinished partition with the minimum watermark.
// Returns nil if there are no unfinished partitions.
func (s *SpannerPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*screamer.PartitionMetadata, error) {
	log.Trace().Msg("GetUnfinishedMinWatermarkPartition called")
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State != @state ORDER BY Watermark ASC LIMIT 1", s.tableName),
		Params: map[string]interface{}{
			"state": screamer.StateFinished,
		},
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: "GetUnfinishedMinWatermarkPartition"})
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

	log.Trace().Str("partition_token", partition.PartitionToken).
		Str("watermark", partition.Watermark.Format(time.RFC3339Nano)).
		Msg("GetUnfinishedMinWatermarkPartition result")

	return partition, nil
}

// RegisterRunner registers a runner in the Runner table with the given runnerID.
// Used for distributed lock and partition assignment.
func (s *SpannerPartitionStorage) RegisterRunner(ctx context.Context, runnerID string) error {
	log.Debug().Str("runner_id", runnerID).Msg("RegisterRunner called")
	_, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
		columnRunnerID:       runnerID,
		columnUpdatedAt:      spanner.CommitTimestamp,
		columnCreatedAt:      spanner.CommitTimestamp,
		columnPartitionCount: int64(0),
	})}, spanner.TransactionTag("RegisterRunner"))
	return err
}

// RefreshRunner updates the UpdatedAt timestamp for the given runnerID in the Runner table.
// Used to indicate liveness of a runner.
func (s *SpannerPartitionStorage) RefreshRunner(ctx context.Context, runnerID string) error {
	log.Trace().Str("runner_id", runnerID).Msg("RefreshRunner called")
	_, err := s.client.Apply(ctx, []*spanner.Mutation{spanner.UpdateMap(tableRunner, map[string]interface{}{
		columnRunnerID:  runnerID,
		columnUpdatedAt: spanner.CommitTimestamp,
	})}, spanner.TransactionTag("RefreshRunner"))
	return err
}

// GetInterruptedPartitions returns partitions that are scheduled or running but have lost their runner.
// Assigns the current runnerID to these partitions for recovery.
func (s *SpannerPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string) ([]*screamer.PartitionMetadata, error) {
	log.Trace().Str("runner_id", runnerID).Msg("GetInterruptedPartitions called")
	var partitions []*screamer.PartitionMetadata

	_, errOuter := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		mutations := []*spanner.Mutation{}
		partitions = make([]*screamer.PartitionMetadata, 0) // Reset for each transaction attempt
		stmt := spanner.Statement{
			SQL: fmt.Sprintf(`
				WITH StaleRunners AS (
					SELECT RunnerID
						FROM %[1]s
						WHERE UpdatedAt <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 SECOND)
					),
				StalePartitions AS (
					SELECT 
						ptr.PartitionToken,
						MAX(r.UpdatedAt) as UpdatedAt
					FROM %[1]s r
					INNER JOIN %[2]s ptr ON ptr.RunnerID = r.RunnerID
					GROUP BY ptr.PartitionToken
					HAVING MAX(r.UpdatedAt) <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 SECOND)
				)
				SELECT DISTINCT m.* EXCEPT (ParentTokens)
				FROM %[3]s m
				LEFT JOIN %[2]s ptr ON m.PartitionToken = ptr.PartitionToken
				WHERE m.State IN UNNEST(@states)
					AND (ptr.PartitionToken IS NULL OR ptr.PartitionToken IN (SELECT PartitionToken FROM StalePartitions))
				ORDER BY m.Watermark ASC
				LIMIT 100
				FOR UPDATE`, tableRunner, tablePartitionToRunner, s.tableName),
			Params: map[string]interface{}{
				"states": []screamer.State{screamer.StateScheduled, screamer.StateRunning},
			},
		}

		iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})

		if err := iter.Do(func(r *spanner.Row) error {
			p := new(screamer.PartitionMetadata)
			if err := r.ToStruct(p); err != nil {
				return err
			}

			ptrMut := spanner.InsertOrUpdateMap(tablePartitionToRunner, map[string]interface{}{
				columnPartitionToken: p.PartitionToken,
				columnRunnerID:       runnerID,
				columnUpdatedAt:      spanner.CommitTimestamp,
				columnCreatedAt:      spanner.CommitTimestamp,
			})
			log.Info().Str("runner_id", runnerID).
				Str("partition_token", p.PartitionToken).
				Msg("assigned stale partition to runner")

			mutations = append(mutations, ptrMut)
			partitions = append(partitions, p)

			return nil
		}); err != nil {
			return fmt.Errorf("error processing partitions to reassign for runner %s: %w", runnerID, err)
		}

		return tx.BufferWrite(mutations)
	}, spanner.TransactionOptions{TransactionTag: "GetInterruptedPartitions"})

	if errOuter != nil {
		return nil, fmt.Errorf("transaction failed for GetInterruptedPartitions with runner %s: %w", runnerID, errOuter)
	}

	if len(partitions) > 0 {
		s.counter.Add(int64(len(partitions)))
		updateRunnerMutation := s.updateRunnerMutation(runnerID)
		if _, err := s.client.Apply(ctx, []*spanner.Mutation{updateRunnerMutation}, spanner.Priority(s.requestPriority)); err != nil {
			return nil, fmt.Errorf("failed to update runner partition count for runner %s: %w", runnerID, err)
		}
	}
	log.Trace().Str("runner_id", runnerID).
		Int("interrupted_partitions", len(partitions)).
		Msg("GetInterruptedPartitions completed")
	return partitions, nil
}

// InitializeRootPartition creates or updates the root partition metadata in the table.
// Used to start a new change stream subscription.
func (s *SpannerPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	log.Debug().
		Time("start_timestamp", startTimestamp).
		Time("end_timestamp", endTimestamp).
		Dur("heartbeat_interval", heartbeatInterval).
		Msg("InitializeRootPartition called")
	m := spanner.InsertOrUpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken:  screamer.RootPartitionToken,
		columnParentTokens:    []string{},
		columnStartTimestamp:  startTimestamp,
		columnEndTimestamp:    endTimestamp,
		columnHeartbeatMillis: heartbeatInterval.Milliseconds(),
		columnState:           screamer.StateCreated,
		columnWatermark:       startTimestamp,
		columnCreatedAt:       spanner.CommitTimestamp,
		columnScheduledAt:     nil,
		columnRunningAt:       nil,
		columnFinishedAt:      nil,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority), spanner.TransactionTag("InitializeRootPartition"))
	return err
}

// GetAndSchedulePartitions finds partitions ready to be scheduled and assigns them to the given runnerID.
// Returns the scheduled partitions.
func (s *SpannerPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string) ([]*screamer.PartitionMetadata, error) {
	log.Trace().
		Time("min_watermark", minWatermark).
		Str("runner_id", runnerID).
		Msg("GetAndSchedulePartitions called")
	var partitions []*screamer.PartitionMetadata

	ts, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		partitions = make([]*screamer.PartitionMetadata, 0)
		mutations := make([]*spanner.Mutation, 0, len(partitions))
		canAssignPartitions, err := s.shouldAssignPartitionsToRunner(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to check if runner %s should be assigned partitions: %w", runnerID, err)
		}
		// early exit if runner should not be assigned partitions (its already busy, leave some for others!)
		if !canAssignPartitions {
			return nil
		}

		stmt := spanner.Statement{
			SQL: fmt.Sprintf("SELECT * FROM %s WHERE State = @state AND StartTimestamp >= @minWatermark ORDER BY StartTimestamp ASC FOR UPDATE", s.tableName),
			Params: map[string]interface{}{
				"state":        screamer.StateCreated,
				"minWatermark": minWatermark,
			},
		}

		iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: "GetAndSchedulePartitions"})

		if err := iter.Do(func(r *spanner.Row) error {
			p := new(screamer.PartitionMetadata)
			if err := r.ToStruct(p); err != nil {
				return err
			}

			m := spanner.UpdateMap(s.tableName, map[string]interface{}{
				columnPartitionToken: p.PartitionToken,
				columnState:          screamer.StateScheduled,
				columnScheduledAt:    spanner.CommitTimestamp,
			})
			p.State = screamer.StateScheduled // mark partition as scheduled
			mutations = append(mutations, m)
			m = spanner.InsertMap(tablePartitionToRunner, map[string]interface{}{
				columnPartitionToken: p.PartitionToken,
				columnRunnerID:       runnerID,
				columnUpdatedAt:      spanner.CommitTimestamp,
				columnCreatedAt:      spanner.CommitTimestamp,
			})
			log.Debug().
				Str("runner_id", runnerID).
				Str("partition_token", p.PartitionToken).
				Msg("scheduled partition for runner")
			mutations = append(mutations, m)

			partitions = append(partitions, p)
			return nil
		}); err != nil {
			return err
		}

		// writes the updates to spanner.
		return tx.BufferWrite(mutations)
	}, spanner.TransactionOptions{TransactionTag: "GetAndSchedulePartitions"})
	if err != nil {
		return nil, err
	}
	for _, p := range partitions {
		p.ScheduledAt = utils.ToPtr(ts.CommitTs.UTC())
	}

	// Update runner's partition count if we scheduled any partitions
	if len(partitions) > 0 {
		s.counter.Add(int64(len(partitions)))
		updateRunnerMutation := s.updateRunnerMutation(runnerID)
		if _, err := s.client.Apply(ctx, []*spanner.Mutation{updateRunnerMutation}, spanner.Priority(s.requestPriority)); err != nil {
			return nil, fmt.Errorf("failed to update runner partition count for runner %s: %w", runnerID, err)
		}
	}
	log.Trace().
		Int("partitions", len(partitions)).
		Str("runner_id", runnerID).
		Msg("GetAndSchedulePartitions completed")
	return partitions, nil
}

func (s *SpannerPartitionStorage) updateRunnerMutation(runnerID string) *spanner.Mutation {
	updateRunnerMutation := spanner.UpdateMap(tableRunner, map[string]interface{}{
		columnRunnerID:       runnerID,
		columnPartitionCount: max(s.counter.Get(), 0),
		columnUpdatedAt:      spanner.CommitTimestamp,
	})

	return updateRunnerMutation
}

func (s *SpannerPartitionStorage) shouldAssignPartitionsToRunner(ctx context.Context, tx reader) (bool, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			WITH ActiveRunners AS (
				SELECT %[2]s, %[3]s
				FROM %[1]s
				WHERE %[4]s >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 SECOND)
			)
			SELECT COUNT(*) as ActiveRunnerCount, COALESCE(MIN(%[3]s), 0) as MinPartitionCount
			FROM ActiveRunners
		`, tableRunner, columnRunnerID, columnPartitionCount, columnUpdatedAt),
	}

	iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return false, err
	}

	var activeRunnerCount int64
	var minPartitionCount int64
	if err := row.Columns(&activeRunnerCount, &minPartitionCount); err != nil {
		return false, err
	}

	// If this is the only active runner, assign partitions
	if activeRunnerCount == 1 {
		return true, nil
	}

	return s.counter.Get() <= minPartitionCount, nil
}

// AddChildPartitions adds new child partitions for a parent partition based on a ChildPartitionsRecord.
// Used when a partition splits or merges.
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
		})

		if _, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority), spanner.TransactionTag("AddChildPartitions")); err != nil {
			// Ignore the AlreadyExists error because a child partition can be found multiple times if partitions are merged.
			if spanner.ErrCode(err) == codes.AlreadyExists {
				continue
			}
			return err
		}
	}

	return nil
}

// UpdateToRunning marks the given partition as running and sets the RunningAt timestamp.
func (s *SpannerPartitionStorage) UpdateToRunning(ctx context.Context, partition *screamer.PartitionMetadata) error {
	log.Debug().
		Str("partition_token", partition.PartitionToken).
		Msg("UpdateToRunning called")
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          screamer.StateRunning,
		columnRunningAt:      spanner.CommitTimestamp,
	})

	ts, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority), spanner.TransactionTag("UpdateToRunning"))
	partition.RunningAt = utils.ToPtr(ts.UTC())
	return err
}

// UpdateToFinished marks the given partition as finished and sets the FinishedAt timestamp.
func (s *SpannerPartitionStorage) UpdateToFinished(ctx context.Context, partition *screamer.PartitionMetadata, runnerID string) error {
	log.Debug().
		Str("partition_token", partition.PartitionToken).
		Str("runner_id", runnerID).
		Msg("UpdateToFinished called")

	s.counter.Decrement()
	ts, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		updateRunnerMutation := s.updateRunnerMutation(runnerID)

		// Update partition state to finished
		partitionMutation := spanner.UpdateMap(s.tableName, map[string]interface{}{
			columnPartitionToken: partition.PartitionToken,
			columnState:          screamer.StateFinished,
			columnFinishedAt:     spanner.CommitTimestamp,
		})

		return tx.BufferWrite([]*spanner.Mutation{partitionMutation, updateRunnerMutation})
	}, spanner.TransactionOptions{TransactionTag: "UpdateToFinished"})
	if err != nil {
		return err
	}

	partition.FinishedAt = utils.ToPtr(ts.CommitTs.UTC())
	return nil
}

// UpdateWatermark updates the watermark for the given partition.
func (s *SpannerPartitionStorage) UpdateWatermark(ctx context.Context, partition *screamer.PartitionMetadata, watermark time.Time) error {
	log.Trace().
		Str("partition_token", partition.PartitionToken).
		Time("watermark", watermark).
		Msg("UpdateWatermark called")
	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnWatermark:      watermark,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority), spanner.TransactionTag("UpdateWatermark"))
	partition.Watermark = watermark.UTC()

	return err
}

// Assert that SpannerPartitionStorage implements PartitionStorage.
var _ screamer.PartitionStorage = (*SpannerPartitionStorage)(nil)
