package partitionstorage

import (
	"context"
	"fmt"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// RunMigrations creates or updates the necessary Spanner tables and indexes for partition and runner metadata management.
// It is idempotent and can be safely called multiple times.
func (s *SpannerPartitionStorage) RunMigrations(ctx context.Context) error {
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
		s.tableName,
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

	runnerIndexStmt := "CREATE INDEX IF NOT EXISTS Runner_UpdatedAt_idx ON Runner(UpdatedAt)"

	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   s.client.DatabaseName(),
		Statements: []string{partitionStmt, partitionToRunnerStmt, runnerStmt, runnerIndexStmt},
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
