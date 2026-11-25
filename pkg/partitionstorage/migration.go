package partitionstorage

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// migrationAttempts is how many times RunMigrations tries before giving up
// when another runner is migrating the same tables at the same time.
const migrationAttempts = 5

// RunMigrations creates or updates the necessary Spanner tables and indexes for partition and runner metadata management.
// It is idempotent and can be safely called multiple times, including by
// several runners starting at once.
func (s *SpannerPartitionStorage) RunMigrations(ctx context.Context) error {
	var err error
	for attempt := 1; ; attempt++ {
		if err = s.runMigrations(ctx); err == nil || attempt == migrationAttempts || !concurrentMigration(err) {
			return err
		}
		// Another runner changed the schema between our read of it and our
		// change: most often both added the same column, since Spanner has
		// no ADD COLUMN IF NOT EXISTS. Read the schema again and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
}

// concurrentMigration says whether a schema change failed because another
// schema change ran at the same time.
func concurrentMigration(err error) bool {
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.Aborted, codes.AlreadyExists:
		return true
	}
	return false
}

func (s *SpannerPartitionStorage) runMigrations(ctx context.Context) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	partitionStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
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

	partitionToRunnerStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
		%[2]s STRING(MAX) NOT NULL,
		%[3]s STRING(MAX) NOT NULL,
		%[4]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		%[5]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		) PRIMARY KEY (%[2]s, %[3]s), ROW DELETION POLICY (OLDER_THAN(%[5]s, INTERVAL 1 DAY))`,
		tablePartitionToRunner,
		columnPartitionToken,
		columnRunnerID,
		columnCreatedAt,
		columnUpdatedAt,
	)

	runnerStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
		%[2]s STRING(MAX) NOT NULL,
		%[3]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		%[4]s TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		) PRIMARY KEY (%[2]s), ROW DELETION POLICY (OLDER_THAN(%[4]s, INTERVAL 1 DAY))`,
		tableRunner,
		columnRunnerID,
		columnCreatedAt,
		columnUpdatedAt,
	)

	runnerIndexStmt := fmt.Sprintf("CREATE INDEX IF NOT EXISTS Runner_%[1]s_idx ON Runner(%[1]s)", columnUpdatedAt)
	partitionMetaIndexStmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_idx ON %[1]s(%[2]s) STORING (%[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]s, %[9]s, %[10]s, %[11]s)`,
		s.tableName, columnWatermark, columnCreatedAt, columnEndTimestamp, columnFinishedAt, columnHeartbeatMillis, columnParentTokens, columnRunningAt, columnScheduledAt, columnStartTimestamp, columnState)

	stmts := []string{partitionStmt, partitionToRunnerStmt, runnerStmt, runnerIndexStmt, partitionMetaIndexStmt}

	// Lease and parking columns, added to tables created before them. Spanner has no
	// ADD COLUMN IF NOT EXISTS, so add only the ones that are missing.
	existing, err := s.columns(ctx, s.tableName)
	if err != nil {
		return err
	}
	for _, c := range []struct{ name, ddl string }{
		{columnOwner, "STRING(MAX)"},
		{columnEpoch, "INT64"},
		{columnLeaseExpiresAt, "TIMESTAMP"},
		{columnNextReadAt, "TIMESTAMP"},
	} {
		if !existing[c.name] {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", s.tableName, c.name, c.ddl))
		}
	}

	// Runners record which metadata table they work on, so runners of
	// different streams sharing the Runner table are counted apart.
	runnerColumns, err := s.columns(ctx, tableRunner)
	if err != nil {
		return err
	}
	if !runnerColumns[columnMetadataTable] {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s STRING(MAX)", tableRunner, columnMetadataTable))
	}

	// Unfinished partitions by state. Scheduling, claiming and counting read
	// only these rows instead of scanning up to a day of finished ones.
	stmts = append(stmts, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_%[2]s_idx ON %[1]s(%[2]s, %[3]s) STORING (%[4]s, %[5]s, %[6]s, %[7]s, %[8]s)`,
		s.tableName, columnState, columnWatermark, columnParentTokens, columnStartTimestamp, columnOwner, columnEpoch, columnLeaseExpiresAt))

	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.client.DatabaseName(),
		Statements: stmts,
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

// columns returns the names of a table's columns, or none if the table does
// not exist yet.
func (s *SpannerPartitionStorage) columns(ctx context.Context, table string) (map[string]bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table",
		Params: map[string]interface{}{"table": table},
	}
	cols := map[string]bool{}
	err := s.client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var name string
		if err := r.Columns(&name); err != nil {
			return err
		}
		cols[name] = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read the columns of %s: %w", table, err)
	}
	return cols, nil
}
