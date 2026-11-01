package partitionstorage

import (
	"context"
	"fmt"
	"strings"
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
	SpannerPartitionStorage struct {
		client          *spanner.Client
		tableName       string
		requestPriority spannerpb.RequestOptions_Priority
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

	columnUpdatedAt = "UpdatedAt"
	columnRunnerID  = "RunnerID"

	columnOwner          = "Owner"
	columnEpoch          = "Epoch"
	columnLeaseExpiresAt = "LeaseExpiresAt"
)

// unfinishedStates lists every state but FINISHED. Queries filter on
// State IN UNNEST(@unfinished) rather than State != FINISHED so that they can
// use the State index and skip the finished rows kept by the row deletion
// policy.
var unfinishedStates = []screamer.State{screamer.StateCreated, screamer.StateScheduled, screamer.StateRunning}

// GetUnfinishedMinWatermarkPartition returns the unfinished partition with the minimum watermark.
// Returns nil if there are no unfinished partitions.
func (s *SpannerPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*screamer.PartitionMetadata, error) {
	log.Trace().Msg("GetUnfinishedMinWatermarkPartition called")
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State IN UNNEST(@unfinished) ORDER BY Watermark ASC LIMIT 1", s.tableName),
		Params: map[string]interface{}{
			"unfinished": unfinishedStates,
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
		columnRunnerID:  runnerID,
		columnUpdatedAt: spanner.CommitTimestamp,
		columnCreatedAt: spanner.CommitTimestamp,
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
// The limit parameter restricts the maximum number of partitions to recover.
func (s *SpannerPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	log.Trace().
		Str("runner_id", runnerID).
		Int("limit", limit).
		Msg("GetInterruptedPartitions called")
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
				LIMIT @limit`, tableRunner, tablePartitionToRunner, s.tableName),
			Params: map[string]interface{}{
				"states": []screamer.State{screamer.StateScheduled, screamer.StateRunning},
				"limit":  limit,
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
// Returns the scheduled partitions. The limit parameter restricts the maximum number of partitions to schedule.
// Uses optimistic concurrency with randomized ordering to reduce conflicts between multiple runners.
func (s *SpannerPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	log.Trace().
		Time("min_watermark", minWatermark).
		Str("runner_id", runnerID).
		Int("limit", limit).
		Msg("GetAndSchedulePartitions called")
	// Use randomized ordering to reduce conflicts between runners
	// No FOR UPDATE - optimistic concurrency handles conflicts via transaction retry
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT * FROM %s
			WHERE State = @state
			  AND StartTimestamp >= @minWatermark
			ORDER BY FARM_FINGERPRINT(CAST(CURRENT_TIMESTAMP() AS STRING) || PartitionToken),
			         StartTimestamp ASC
			LIMIT @limit`, s.tableName),
		Params: map[string]interface{}{
			"state":        screamer.StateCreated,
			"minWatermark": minWatermark,
			"limit":        limit,
		},
	}
	return s.schedule(ctx, stmt, runnerID, "GetAndSchedulePartitions")
}

// GetAndScheduleReadyPartitions finds CREATED partitions whose parents have all
// FINISHED and assigns up to limit of them to runnerID. The parent check runs in
// the same read-write transaction as the assignment, so a child is never
// scheduled while a parent is still being read.
//
// A parent with no row may still be on its way: the other parent of a merge can
// report the child before this parent's own ancestors have reported it. So a
// missing parent only counts as finished once no unfinished partition has a
// watermark before the child's start. That also covers a parent that finished
// and was removed by the row deletion policy.
func (s *SpannerPartitionStorage) GetAndScheduleReadyPartitions(ctx context.Context, runnerID string, limit int) ([]*screamer.PartitionMetadata, error) {
	log.Trace().
		Str("runner_id", runnerID).
		Int("limit", limit).
		Msg("GetAndScheduleReadyPartitions called")

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT c.* FROM %[1]s AS c
			WHERE c.State = @created
			  AND NOT EXISTS (
			    SELECT 1 FROM %[1]s AS p
			    WHERE p.PartitionToken IN UNNEST(c.ParentTokens)
			      AND p.State != @finished
			  )
			  AND (
			    (SELECT COUNT(*) FROM %[1]s AS p WHERE p.PartitionToken IN UNNEST(c.ParentTokens)) = ARRAY_LENGTH(c.ParentTokens)
			    OR NOT EXISTS (
			      SELECT 1 FROM %[1]s AS o
			      WHERE o.State IN UNNEST(@unfinished)
			        AND o.Watermark < c.StartTimestamp
			    )
			  )
			ORDER BY FARM_FINGERPRINT(CAST(CURRENT_TIMESTAMP() AS STRING) || c.PartitionToken),
			         c.StartTimestamp ASC
			LIMIT @limit`, s.tableName),
		Params: map[string]interface{}{
			"created":    screamer.StateCreated,
			"finished":   screamer.StateFinished,
			"unfinished": unfinishedStates,
			"limit":      limit,
		},
	}
	return s.schedule(ctx, stmt, runnerID, "GetAndScheduleReadyPartitions")
}

// schedule runs stmt, which selects CREATED partitions, and assigns every
// partition it returns to runnerID in the same transaction.
func (s *SpannerPartitionStorage) schedule(ctx context.Context, stmt spanner.Statement, runnerID, tag string) ([]*screamer.PartitionMetadata, error) {
	var partitions []*screamer.PartitionMetadata

	ts, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		partitions = make([]*screamer.PartitionMetadata, 0)
		mutations := make([]*spanner.Mutation, 0)

		iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: tag})

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
	}, spanner.TransactionOptions{TransactionTag: tag})
	if err != nil {
		return nil, err
	}
	for _, p := range partitions {
		p.ScheduledAt = utils.ToPtr(ts.CommitTs.UTC())
	}

	log.Trace().
		Int("partitions", len(partitions)).
		Str("runner_id", runnerID).
		Msg(tag + " completed")
	return partitions, nil
}

func (s *SpannerPartitionStorage) updateRunnerMutation(runnerID string) *spanner.Mutation {
	updateRunnerMutation := spanner.UpdateMap(tableRunner, map[string]interface{}{
		columnRunnerID:  runnerID,
		columnUpdatedAt: spanner.CommitTimestamp,
	})

	return updateRunnerMutation
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

// UpdateToRunning marks the given partition as running and sets the RunningAt
// timestamp. A FINISHED partition is left as it is: a runner that lost its
// lease while the new owner finished the partition may still retry it.
func (s *SpannerPartitionStorage) UpdateToRunning(ctx context.Context, partition *screamer.PartitionMetadata) error {
	log.Debug().
		Str("partition_token", partition.PartitionToken).
		Msg("UpdateToRunning called")
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`UPDATE %s SET State = @running, RunningAt = PENDING_COMMIT_TIMESTAMP()
			WHERE PartitionToken = @token AND State != @finished`, s.tableName),
		Params: map[string]interface{}{
			"token":    partition.PartitionToken,
			"running":  screamer.StateRunning,
			"finished": screamer.StateFinished,
		},
	}
	resp, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.UpdateWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: "UpdateToRunning"})
		return err
	}, spanner.TransactionOptions{TransactionTag: "UpdateToRunning"})
	if err != nil {
		return err
	}
	partition.RunningAt = utils.ToPtr(resp.CommitTs.UTC())
	return nil
}

// UpdateToFinished marks the given partition as finished and sets the FinishedAt timestamp.
func (s *SpannerPartitionStorage) UpdateToFinished(ctx context.Context, partition *screamer.PartitionMetadata, runnerID string) error {
	log.Debug().
		Str("partition_token", partition.PartitionToken).
		Str("runner_id", runnerID).
		Msg("UpdateToFinished called")

	m := spanner.UpdateMap(s.tableName, map[string]interface{}{
		columnPartitionToken: partition.PartitionToken,
		columnState:          screamer.StateFinished,
		columnFinishedAt:     spanner.CommitTimestamp,
	})

	ts, err := s.client.Apply(ctx, []*spanner.Mutation{m}, spanner.Priority(s.requestPriority), spanner.TransactionTag("UpdateToFinished"))
	partition.FinishedAt = utils.ToPtr(ts.UTC())
	return err
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

// watermarkBatchSize is the most watermarks written in one transaction.
const watermarkBatchSize = 500

// UpdateWatermarks writes many partitions' watermarks, in transactions of up to
// watermarkBatchSize statements. A watermark only moves forward, and a partition
// that is not in the table is skipped.
func (s *SpannerPartitionStorage) UpdateWatermarks(ctx context.Context, watermarks []screamer.PartitionWatermark) error {
	log.Trace().Int("partitions", len(watermarks)).Msg("UpdateWatermarks called")
	sql := fmt.Sprintf("UPDATE %s SET Watermark = @watermark WHERE PartitionToken = @token AND Watermark < @watermark", s.tableName)

	for start := 0; start < len(watermarks); start += watermarkBatchSize {
		chunk := watermarks[start:min(start+watermarkBatchSize, len(watermarks))]
		stmts := make([]spanner.Statement, 0, len(chunk))
		for _, w := range chunk {
			stmts = append(stmts, spanner.Statement{
				SQL:    sql,
				Params: map[string]interface{}{"token": w.PartitionToken, "watermark": w.Watermark},
			})
		}
		_, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.BatchUpdateWithOptions(ctx, stmts, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: "UpdateWatermarks"})
			return err
		}, spanner.TransactionOptions{TransactionTag: "UpdateWatermarks"})
		if err != nil {
			return err
		}
	}
	return nil
}

// leaseBatchSize is the most leases renewed or released in one transaction.
const leaseBatchSize = 500

// ClaimPartitions leases up to req.Limit partitions to req.RunnerID in one
// read-write transaction: first SCHEDULED or RUNNING partitions whose lease has
// expired or that have none, lowest watermark first, then CREATED partitions
// that are ready to read. Each claimed partition's Epoch is incremented. Lease
// expiry is set from Spanner's clock, so runners' clocks do not need to agree.
func (s *SpannerPartitionStorage) ClaimPartitions(ctx context.Context, req screamer.ClaimRequest) ([]*screamer.PartitionMetadata, error) {
	log.Trace().Str("runner_id", req.RunnerID).Int("limit", req.Limit).Msg("ClaimPartitions called")
	if req.Limit <= 0 {
		return nil, nil
	}

	expired := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT * FROM %s
			WHERE State IN UNNEST(@claimable)
			  AND (LeaseExpiresAt IS NULL OR LeaseExpiresAt < CURRENT_TIMESTAMP())
			ORDER BY Watermark ASC
			LIMIT @limit`, s.tableName),
		Params: map[string]interface{}{
			"claimable": []screamer.State{screamer.StateScheduled, screamer.StateRunning},
		},
	}
	ready := s.readyStatement(req)

	var partitions []*screamer.PartitionMetadata
	ts, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		partitions = make([]*screamer.PartitionMetadata, 0)
		var reclaimed, created []string

		expired.Params["limit"] = req.Limit
		if err := s.queryPartitions(ctx, tx, expired, "ClaimPartitions", func(p *screamer.PartitionMetadata) {
			reclaimed = append(reclaimed, p.PartitionToken)
			partitions = append(partitions, p)
		}); err != nil {
			return err
		}
		if left := req.Limit - len(partitions); left > 0 {
			ready.Params["limit"] = left
			if err := s.queryPartitions(ctx, tx, ready, "ClaimPartitions", func(p *screamer.PartitionMetadata) {
				p.State = screamer.StateScheduled
				created = append(created, p.PartitionToken)
				partitions = append(partitions, p)
			}); err != nil {
				return err
			}
		}
		if len(partitions) == 0 {
			return nil
		}

		const lease = `Owner = @owner, Epoch = COALESCE(Epoch, 0) + 1,
			LeaseExpiresAt = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MILLISECOND)`
		params := func(tokens []string) map[string]interface{} {
			return map[string]interface{}{
				"tokens":    tokens,
				"owner":     req.RunnerID,
				"ttl":       req.TTL.Milliseconds(),
				"scheduled": screamer.StateScheduled,
			}
		}
		var stmts []spanner.Statement
		if len(reclaimed) > 0 {
			stmts = append(stmts, spanner.Statement{
				SQL:    fmt.Sprintf("UPDATE %s SET %s WHERE PartitionToken IN UNNEST(@tokens)", s.tableName, lease),
				Params: params(reclaimed),
			})
		}
		if len(created) > 0 {
			stmts = append(stmts, spanner.Statement{
				SQL:    fmt.Sprintf("UPDATE %s SET State = @scheduled, ScheduledAt = PENDING_COMMIT_TIMESTAMP(), %s WHERE PartitionToken IN UNNEST(@tokens)", s.tableName, lease),
				Params: params(created),
			})
		}
		_, err := tx.BatchUpdateWithOptions(ctx, stmts, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: "ClaimPartitions"})
		return err
	}, spanner.TransactionOptions{TransactionTag: "ClaimPartitions"})
	if err != nil {
		return nil, leaseColumnsHint(err)
	}

	commit := ts.CommitTs.UTC()
	expires := commit.Add(req.TTL)
	for _, p := range partitions {
		epoch := int64(1)
		if p.Epoch != nil {
			epoch = *p.Epoch + 1
		}
		owner := req.RunnerID
		p.Owner, p.Epoch, p.LeaseExpiresAt = &owner, &epoch, utils.ToPtr(expires)
		if p.State == screamer.StateScheduled && p.ScheduledAt == nil {
			p.ScheduledAt = utils.ToPtr(commit)
		}
		log.Debug().Str("runner_id", req.RunnerID).Str("partition_token", p.PartitionToken).Int64("epoch", epoch).Msg("claimed partition")
	}
	return partitions, nil
}

// readyStatement selects CREATED partitions that are ready to read: with parent
// gating, those whose parents have all finished (see
// GetAndScheduleReadyPartitions); otherwise those starting at or after
// req.MinWatermark.
func (s *SpannerPartitionStorage) readyStatement(req screamer.ClaimRequest) spanner.Statement {
	if !req.ParentGating {
		return spanner.Statement{
			SQL: fmt.Sprintf(`
				SELECT * FROM %s
				WHERE State = @created AND StartTimestamp >= @minWatermark
				ORDER BY FARM_FINGERPRINT(CAST(CURRENT_TIMESTAMP() AS STRING) || PartitionToken), StartTimestamp ASC
				LIMIT @limit`, s.tableName),
			Params: map[string]interface{}{
				"created":      screamer.StateCreated,
				"minWatermark": req.MinWatermark,
			},
		}
	}
	return spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT c.* FROM %[1]s AS c
			WHERE c.State = @created
			  AND NOT EXISTS (
			    SELECT 1 FROM %[1]s AS p
			    WHERE p.PartitionToken IN UNNEST(c.ParentTokens)
			      AND p.State != @finished
			  )
			  AND (
			    (SELECT COUNT(*) FROM %[1]s AS p WHERE p.PartitionToken IN UNNEST(c.ParentTokens)) = ARRAY_LENGTH(c.ParentTokens)
			    OR NOT EXISTS (
			      SELECT 1 FROM %[1]s AS o
			      WHERE o.State IN UNNEST(@unfinished)
			        AND o.Watermark < c.StartTimestamp
			    )
			  )
			ORDER BY FARM_FINGERPRINT(CAST(CURRENT_TIMESTAMP() AS STRING) || c.PartitionToken),
			         c.StartTimestamp ASC
			LIMIT @limit`, s.tableName),
		Params: map[string]interface{}{
			"created":    screamer.StateCreated,
			"finished":   screamer.StateFinished,
			"unfinished": unfinishedStates,
		},
	}
}

func (s *SpannerPartitionStorage) queryPartitions(ctx context.Context, tx *spanner.ReadWriteTransaction, stmt spanner.Statement, tag string, fn func(*screamer.PartitionMetadata)) error {
	iter := tx.QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: tag})
	return iter.Do(func(r *spanner.Row) error {
		p := new(screamer.PartitionMetadata)
		if err := r.ToStruct(p); err != nil {
			return err
		}
		fn(p)
		return nil
	})
}

// RenewLeases extends the leases runnerID still holds at the given epochs and
// moves their watermarks forward, in transactions of up to leaseBatchSize
// statements. It returns the tokens of the leases runnerID no longer holds.
func (s *SpannerPartitionStorage) RenewLeases(ctx context.Context, runnerID string, ttl time.Duration, leases []screamer.Lease) ([]string, error) {
	log.Trace().Str("runner_id", runnerID).Int("leases", len(leases)).Msg("RenewLeases called")
	sql := fmt.Sprintf(`UPDATE %s SET
			LeaseExpiresAt = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MILLISECOND),
			Watermark = IF(@watermark > Watermark, @watermark, Watermark)
		WHERE PartitionToken = @token AND Owner = @owner AND Epoch = @epoch AND State != @finished`, s.tableName)
	return s.updateLeases(ctx, runnerID, leases, sql, map[string]interface{}{"ttl": ttl.Milliseconds()}, "RenewLeases")
}

// ReleaseLeases moves the watermarks of the leases runnerID still holds forward
// and ends them, so other runners can claim the partitions straight away.
func (s *SpannerPartitionStorage) ReleaseLeases(ctx context.Context, runnerID string, leases []screamer.Lease) error {
	log.Trace().Str("runner_id", runnerID).Int("leases", len(leases)).Msg("ReleaseLeases called")
	sql := fmt.Sprintf(`UPDATE %s SET
			Owner = NULL, LeaseExpiresAt = NULL,
			Watermark = IF(@watermark > Watermark, @watermark, Watermark)
		WHERE PartitionToken = @token AND Owner = @owner AND Epoch = @epoch AND State != @finished`, s.tableName)
	_, err := s.updateLeases(ctx, runnerID, leases, sql, nil, "ReleaseLeases")
	return err
}

// updateLeases runs sql once per lease and returns the tokens it matched no row for.
func (s *SpannerPartitionStorage) updateLeases(ctx context.Context, runnerID string, leases []screamer.Lease, sql string, extra map[string]interface{}, tag string) ([]string, error) {
	var lost []string
	for start := 0; start < len(leases); start += leaseBatchSize {
		chunk := leases[start:min(start+leaseBatchSize, len(leases))]
		stmts := make([]spanner.Statement, 0, len(chunk))
		for _, l := range chunk {
			params := map[string]interface{}{
				"token":     l.PartitionToken,
				"owner":     runnerID,
				"epoch":     l.Epoch,
				"watermark": l.Watermark,
				"finished":  screamer.StateFinished,
			}
			for k, v := range extra {
				params[k] = v
			}
			stmts = append(stmts, spanner.Statement{SQL: sql, Params: params})
		}
		var counts []int64
		_, err := s.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			var err error
			counts, err = tx.BatchUpdateWithOptions(ctx, stmts, spanner.QueryOptions{Priority: s.requestPriority, RequestTag: tag})
			return err
		}, spanner.TransactionOptions{TransactionTag: tag})
		if err != nil {
			return nil, leaseColumnsHint(err)
		}
		for i, n := range counts {
			if n == 0 {
				lost = append(lost, chunk[i].PartitionToken)
			}
		}
	}
	return lost, nil
}

// leaseColumnsHint explains an error caused by a metadata table that does not
// have the lease columns yet.
func leaseColumnsHint(err error) error {
	msg := err.Error()
	for _, c := range []string{columnOwner, columnEpoch, columnLeaseExpiresAt} {
		if strings.Contains(msg, c) && (spanner.ErrCode(err) == codes.InvalidArgument || spanner.ErrCode(err) == codes.NotFound) {
			return fmt.Errorf("%w (the partition metadata table has no %s column: run RunMigrations, or add the lease columns as MIGRATION.md shows)", err, c)
		}
	}
	return err
}

// GetActiveRunnerCount returns the number of active runners that have refreshed within the last 5 seconds.
// Used for metrics and observability to understand cluster size.
func (s *SpannerPartitionStorage) GetActiveRunnerCount(ctx context.Context) (int, error) {
	log.Trace().Msg("GetActiveRunnerCount called")

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT COUNT(*) as ActiveRunners
			FROM %s
			WHERE UpdatedAt > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND)`, tableRunner),
	}

	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{
		Priority:   s.requestPriority,
		RequestTag: "GetActiveRunnerCount",
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, fmt.Errorf("failed to query active runner count: %w", err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, fmt.Errorf("failed to scan active runner count: %w", err)
	}

	log.Trace().Int("active_runners", int(count)).Msg("GetActiveRunnerCount completed")
	return int(count), nil
}

// CountUnfinishedPartitions returns the number of partitions that are not FINISHED.
// Used for cluster-wide scaling metrics.
func (s *SpannerPartitionStorage) CountUnfinishedPartitions(ctx context.Context) (int, error) {
	stmt := spanner.Statement{
		SQL:    fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE State IN UNNEST(@unfinished)", s.tableName),
		Params: map[string]interface{}{"unfinished": unfinishedStates},
	}
	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{
		Priority:   s.requestPriority,
		RequestTag: "CountUnfinishedPartitions",
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, fmt.Errorf("failed to count unfinished partitions: %w", err)
	}
	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, fmt.Errorf("failed to scan unfinished partition count: %w", err)
	}
	return int(count), nil
}

var (
	_ screamer.PartitionCounter      = (*SpannerPartitionStorage)(nil)
	_ screamer.ParentGatedScheduler  = (*SpannerPartitionStorage)(nil)
	_ screamer.WatermarkBatchUpdater = (*SpannerPartitionStorage)(nil)
	_ screamer.PartitionLeaser       = (*SpannerPartitionStorage)(nil)
)

// Assert that SpannerPartitionStorage implements PartitionStorage.
var _ screamer.PartitionStorage = (*SpannerPartitionStorage)(nil)
