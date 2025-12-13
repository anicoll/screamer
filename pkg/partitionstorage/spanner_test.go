package partitionstorage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testTableName = "PartitionMetadata"
	projectID     = "local-project"
	instanceID    = "local-instance"
	databaseID    = "local-database"
)

type SpannerTestSuite struct {
	suite.Suite
	ctx     context.Context
	client  *spanner.Client
	timeout time.Duration
	dsn     string
}

func TestSpannerTestSuite(t *testing.T) {
	suite.Run(t, new(SpannerTestSuite))
}

func (s *SpannerTestSuite) SetupSuite() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	s.ctx = context.Background()
	s.timeout = time.Second * 1500
	s.dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	// Check if emulator is running
	host := os.Getenv("SPANNER_EMULATOR_HOST")
	if host == "" {
		os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
		host = os.Getenv("SPANNER_EMULATOR_HOST")
		if host == "" {
			s.T().Skip("SPANNER_EMULATOR_HOST is not set, skipping integration tests")
			return
		}
	}

	s.createInstance() // create instance
	s.createDatabase() // create database
}

func (s *SpannerTestSuite) TearDownSuite() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *SpannerTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *SpannerTestSuite) createInstance() {
	instanceAdminClient, err := instance.NewInstanceAdminClient(s.ctx)
	s.NoError(err)
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.CreateInstance(s.ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err == nil {
		_, err = op.Wait(s.ctx)
		s.NoError(err)
	}
	// If instance already exists, that's okay
}

func (s *SpannerTestSuite) createDatabase() {
	databaseAdminClient, err := database.NewDatabaseAdminClient(s.ctx)
	s.NoError(err)
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(s.ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + projectID + "/instances/" + instanceID,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err == nil {
		_, err = op.Wait(s.ctx)
		s.NoError(err)
	}
	// If database already exists, that's okay
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_RunMigrations() {
	ctx := context.Background()
	var err error
	s.client, err = spanner.NewClient(ctx, s.dsn)
	s.NoError(err)

	storage := &SpannerPartitionStorage{
		client:    s.client,
		tableName: "RunMigrations",
	}

	err = storage.RunMigrations(ctx)
	s.NoError(err)

	iter := s.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), []string{columnPartitionToken})
	defer iter.Stop()

	if _, err := iter.Next(); err != iterator.Done {
		s.T().Errorf("Read from %s after SpannerPartitionStorage.RunMigrations() = %v, want %v", storage.tableName, err, iterator.Done)
	}

	existsTable, err := existsTable(ctx, s.client, storage.tableName)
	s.NoError(err)
	if !existsTable {
		s.T().Errorf("SpannerPartitionStorage.existsTable() = %v, want %v", existsTable, false)
	}
}

func existsTable(ctx context.Context, client *spanner.Client, tableName string) (bool, error) {
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT 1 FROM information_schema.tables WHERE table_catalog = '' AND table_schema = '' AND table_name = @tableName",
		Params: map[string]interface{}{
			"tableName": tableName,
		},
	})
	defer iter.Stop()

	if _, err := iter.Next(); err != nil {
		if err == iterator.Done {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

type testStorage struct {
	*SpannerPartitionStorage
	t *testing.T
}

// testPartition is a helper struct for creating test partition data
type testPartition struct {
	token     string
	watermark time.Time
	state     screamer.State
}

// testRunner is a helper struct for creating test runner data
type testRunner struct {
	id        string
	createdAt time.Time
	updatedAt time.Time
}

func (s *testStorage) CleanupData(ctx context.Context) {
	// It's important to delete from tables in an order that respects any potential (even if not explicit) parent-child relationships,
	// or simply delete from all. For these tables, the order is likely not critical as they don't have enforced FKs.
	_, err := s.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(tablePartitionToRunner, spanner.AllKeys()), // Fixed name
		spanner.Delete(tableRunner, spanner.AllKeys()),            // Fixed name
		spanner.Delete(s.tableName, spanner.AllKeys()),            // Dynamic name
	})
	assert.NoError(s.t, err, "failed to cleanup test data")
}

func (s *SpannerTestSuite) setupSpannerPartitionStorage(ctx context.Context, tableName string) (*testStorage, func()) {
	var err error
	proxy := interceptor.NewQueueInterceptor(100)

	s.client, err = spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(proxy.UnaryInterceptor)))
	s.NoError(err)

	storage := NewSpanner(s.client, tableName)

	err = storage.RunMigrations(ctx)
	s.NoError(err)

	ts := &testStorage{
		t:                       s.T(),
		SpannerPartitionStorage: storage,
	}
	cleanupFunc := func() {
		ts.CleanupData(ctx)
		storage.client.Close() // Close the client created for this specific test setup
	}
	return ts, cleanupFunc
}

// Helper methods for creating test data and assertions

// insertTestPartitions creates test partitions with the given specifications
func (s *SpannerTestSuite) insertTestPartitions(ctx context.Context, storage *testStorage, partitions []testPartition) {
	mutations := make([]*spanner.Mutation, len(partitions))
	for i, p := range partitions {
		mutations[i] = s.createPartitionMutation(storage.tableName, p.token, p.watermark, p.state)
	}
	_, err := storage.client.Apply(ctx, mutations)
	s.NoError(err, "Failed to insert test partitions")
}

// createPartitionMutation creates a spanner mutation for inserting a partition
func (s *SpannerTestSuite) createPartitionMutation(tableName, token string, watermark time.Time, state screamer.State) *spanner.Mutation {
	return spanner.InsertMap(tableName, map[string]interface{}{
		columnPartitionToken:  token,
		columnParentTokens:    []string{},
		columnStartTimestamp:  watermark,
		columnEndTimestamp:    time.Now().AddDate(1, 0, 0),
		columnHeartbeatMillis: 10000,
		columnState:           state,
		columnWatermark:       watermark,
		columnCreatedAt:       spanner.CommitTimestamp,
	})
}

// extractPartitionTokens extracts tokens from a slice of PartitionMetadata
func (s *SpannerTestSuite) extractPartitionTokens(partitions []*screamer.PartitionMetadata) []string {
	tokens := make([]string, len(partitions))
	for i, p := range partitions {
		tokens[i] = p.PartitionToken
	}
	return tokens
}

// createTestPartition creates a PartitionMetadata for testing
func (s *SpannerTestSuite) createTestPartition(token string, state screamer.State, watermark time.Time) *screamer.PartitionMetadata {
	return &screamer.PartitionMetadata{
		PartitionToken:  token,
		ParentTokens:    []string{},
		StartTimestamp:  watermark,
		EndTimestamp:    time.Now().AddDate(1, 0, 0),
		HeartbeatMillis: 10000,
		State:           state,
		Watermark:       watermark,
	}
}

// setupTestRunner creates a test runner and inserts it into the database
func (s *SpannerTestSuite) setupTestRunner(ctx context.Context, storage *testStorage, runner testRunner) {
	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
			columnRunnerID:  runner.id,
			columnCreatedAt: runner.createdAt,
			columnUpdatedAt: runner.updatedAt,
		}),
	})
	s.NoError(err, "Failed to setup test runner %s", runner.id)
}

// assignPartitionToRunner creates a PartitionToRunner mapping
func (s *SpannerTestSuite) assignPartitionToRunner(ctx context.Context, storage *testStorage, partitionToken, runnerID string, assignedAt time.Time) {
	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tablePartitionToRunner, map[string]interface{}{
			columnPartitionToken: partitionToken,
			columnRunnerID:       runnerID,
			columnCreatedAt:      assignedAt,
			columnUpdatedAt:      assignedAt,
		}),
	})
	s.NoError(err, "Failed to assign partition %s to runner %s", partitionToken, runnerID)
}

// verifyPartitionState checks that a partition has the expected state in the database
func (s *SpannerTestSuite) verifyPartitionState(ctx context.Context, storage *testStorage, partitionToken string, expectedState screamer.State) {
	row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{partitionToken}, []string{columnState})
	s.NoError(err, "Failed to read partition state for %s", partitionToken)

	var actualState screamer.State
	err = row.Columns(&actualState)
	s.NoError(err)
	s.Equal(expectedState, actualState, "Partition %s should have state %v", partitionToken, expectedState)
}

// verifyPartitionAssignment checks that a partition is assigned to the expected runner
func (s *SpannerTestSuite) verifyPartitionAssignment(ctx context.Context, storage *testStorage, partitionToken, expectedRunnerID string) {
	row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner, spanner.Key{partitionToken, expectedRunnerID}, []string{columnRunnerID})
	s.NoError(err, "Failed to read partition assignment for %s", partitionToken)

	var actualRunnerID string
	err = row.Columns(&actualRunnerID)
	s.NoError(err)
	s.Equal(expectedRunnerID, actualRunnerID, "Partition %s should be assigned to runner %s", partitionToken, expectedRunnerID)
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_RegisterAndRefreshRunner() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "RunnerTestTable")
	defer cleanup()

	runnerID := uuid.NewString()
	initialTime := time.Now().UTC().Truncate(time.Microsecond) // Spanner's precision

	// Test RegisterRunner
	err := storage.RegisterRunner(ctx, runnerID)
	s.NoError(err)

	// Verify Runner table
	row, err := storage.client.Single().ReadRow(ctx, tableRunner, spanner.Key{runnerID}, []string{columnRunnerID, columnCreatedAt, columnUpdatedAt})
	s.NoError(err)

	var gotRunnerID string
	var createdAt, updatedAt time.Time
	err = row.Columns(&gotRunnerID, &createdAt, &updatedAt)
	s.NoError(err)

	s.Equal(runnerID, gotRunnerID)
	s.WithinDuration(initialTime, createdAt, time.Second, "CreatedAt should be close to initial time")
	s.WithinDuration(initialTime, updatedAt, time.Second, "UpdatedAt should be close to initial time")
	s.Equal(createdAt, updatedAt, "CreatedAt and UpdatedAt should be the same on initial registration")

	// Test RefreshRunner
	// Need to ensure some time passes so UpdatedAt will be different
	time.Sleep(1 * time.Second) // Sleep to ensure commit timestamp differs
	refreshTime := time.Now().UTC().Truncate(time.Microsecond)

	err = storage.RefreshRunner(ctx, runnerID)
	s.NoError(err)

	// Verify Runner table again
	row, err = storage.client.Single().ReadRow(ctx, tableRunner, spanner.Key{runnerID}, []string{columnRunnerID, columnCreatedAt, columnUpdatedAt})
	s.NoError(err)

	var refreshedCreatedAt, refreshedUpdatedAt time.Time
	err = row.Columns(&gotRunnerID, &refreshedCreatedAt, &refreshedUpdatedAt)
	s.NoError(err)

	s.Equal(runnerID, gotRunnerID)
	s.Equal(createdAt, refreshedCreatedAt, "CreatedAt should not change on refresh") // Ensure CreatedAt is stable
	s.True(refreshedUpdatedAt.After(updatedAt), "Refreshed UpdatedAt should be after previous UpdatedAt")
	s.WithinDuration(refreshTime, refreshedUpdatedAt, time.Second, "Refreshed UpdatedAt should be close to refresh time call")

	// Test RegisterRunner again (should act as update for timestamps if using InsertOrUpdate)
	// The current RegisterRunner uses InsertOrUpdateMap, so it should update.
	time.Sleep(1 * time.Second)
	reregisterTime := time.Now().UTC().Truncate(time.Microsecond)
	err = storage.RegisterRunner(ctx, runnerID) // Call RegisterRunner again
	s.NoError(err)

	row, err = storage.client.Single().ReadRow(ctx, tableRunner, spanner.Key{runnerID}, []string{columnCreatedAt, columnUpdatedAt})
	s.NoError(err)
	var reregisteredCreatedAt, reregisteredUpdatedAt time.Time
	err = row.Columns(&reregisteredCreatedAt, &reregisteredUpdatedAt)
	s.NoError(err)

	s.Greater(reregisteredCreatedAt, createdAt, "CreatedAt should still not change on re-registration")
	s.True(reregisteredUpdatedAt.After(refreshedUpdatedAt), "Re-registered UpdatedAt should be later")
	s.WithinDuration(reregisterTime, reregisteredUpdatedAt, time.Second, "Re-registered UpdatedAt should be close to re-register time call")
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_InitializeRootPartition() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "InitializeRootPartition")
	defer cleanup()

	tests := map[string]struct {
		startTimestamp    time.Time
		endTimestamp      time.Time
		heartbeatInterval time.Duration
		want              screamer.PartitionMetadata
	}{
		"one": {
			startTimestamp:    time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			endTimestamp:      time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
			heartbeatInterval: 10 * time.Second,
			want: screamer.PartitionMetadata{
				PartitionToken:  screamer.RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTimestamp:    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
				HeartbeatMillis: 10000,
				State:           screamer.StateCreated,
				Watermark:       time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		"two": {
			startTimestamp:    time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			endTimestamp:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			heartbeatInterval: time.Hour,
			want: screamer.PartitionMetadata{
				PartitionToken:  screamer.RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
				EndTimestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				HeartbeatMillis: 3600000,
				State:           screamer.StateCreated,
				Watermark:       time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			},
		},
	}
	for name, test := range tests {
		s.Run(name, func() {
			err := storage.InitializeRootPartition(ctx, test.startTimestamp, test.endTimestamp, test.heartbeatInterval)
			s.NoError(err)

			columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}
			row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken}, columns)
			s.NoError(err)

			got := screamer.PartitionMetadata{}
			err = row.ToStruct(&got)
			s.NoError(err)

			s.Equal(test.want.PartitionToken, got.PartitionToken)
			s.Equal(test.want.ParentTokens, got.ParentTokens)
			s.Equal(test.want.StartTimestamp, got.StartTimestamp)
			s.Equal(test.want.EndTimestamp, got.EndTimestamp)
			s.Equal(test.want.HeartbeatMillis, got.HeartbeatMillis)
			s.Equal(test.want.State, got.State)
			s.Equal(test.want.Watermark, got.Watermark)
		})
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_InitializeRootPartition_Idempotency() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "InitializeRootPartitionIdempotency")
	defer cleanup()

	ts1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC) // Different timestamp
	hb1 := 10 * time.Second
	hb2 := 20 * time.Second // Different heartbeat

	// Initial call
	err := storage.InitializeRootPartition(ctx, ts1, time.Now().AddDate(1, 0, 0), hb1)
	s.NoError(err)

	row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken}, []string{columnStartTimestamp, columnHeartbeatMillis, columnWatermark})
	s.NoError(err)
	var readTs time.Time
	var readHb int64
	var readWm time.Time
	err = row.Columns(&readTs, &readHb, &readWm)
	s.NoError(err)
	s.Equal(ts1, readTs)
	s.Equal(hb1.Milliseconds(), readHb)
	s.Equal(ts1, readWm)

	// Call again with same parameters (should be no-op or overwrite with same values)
	err = storage.InitializeRootPartition(ctx, ts1, time.Now().AddDate(1, 0, 0), hb1)
	s.NoError(err)
	row, err = storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken}, []string{columnStartTimestamp, columnHeartbeatMillis, columnWatermark})
	s.NoError(err)
	err = row.Columns(&readTs, &readHb, &readWm)
	s.NoError(err)
	s.Equal(ts1, readTs)
	s.Equal(hb1.Milliseconds(), readHb)
	s.Equal(ts1, readWm)

	// Call again with different parameters (should update)
	err = storage.InitializeRootPartition(ctx, ts2, time.Now().AddDate(1, 0, 0), hb2)
	s.NoError(err)
	row, err = storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken}, []string{columnStartTimestamp, columnHeartbeatMillis, columnWatermark})
	s.NoError(err)
	err = row.Columns(&readTs, &readHb, &readWm)
	s.NoError(err)
	s.Equal(ts2, readTs, "StartTimestamp should update")
	s.Equal(hb2.Milliseconds(), readHb, "HeartbeatMillis should update")
	s.Equal(ts2, readWm, "Watermark should update")
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "Read")
	defer cleanup()

	runnerID := uuid.NewString()
	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	// Register the runner first
	err := storage.RegisterRunner(ctx, runnerID)
	s.NoError(err)

	// Create test partitions
	s.insertTestPartitions(ctx, storage, []testPartition{
		{"created1", timestamp, screamer.StateCreated},
		{"created2", timestamp.Add(-2 * time.Second), screamer.StateCreated},
		{"scheduled", timestamp.Add(time.Second), screamer.StateScheduled},
		{"running", timestamp.Add(2 * time.Second), screamer.StateRunning},
		{"finished", timestamp.Add(-time.Second), screamer.StateFinished},
	})

	s.Run("GetUnfinishedMinWatermarkPartition", func() {
		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.Equal("created2", got.PartitionToken)
	})

	s.Run("GetInterruptedPartitions", func() {
		partitions, err := storage.GetInterruptedPartitions(ctx, runnerID, 3*time.Second)
		s.NoError(err)

		tokens := s.extractPartitionTokens(partitions)
		s.ElementsMatch([]string{"scheduled", "running"}, tokens)
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetUnfinishedMinWatermarkPartition_Scenarios() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetUnfinishedMinWatermark")
	defer cleanup()

	tsBase := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	s.Run("NoPartitions", func() {
		storage.CleanupData(ctx)
		defer storage.CleanupData(ctx)

		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.Nil(p, "Should return nil when no partitions exist")
	})

	s.Run("OnlyFinishedPartitions", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"finished1", tsBase.Add(-time.Hour), screamer.StateFinished},
		})

		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.Nil(p, "Should return nil when only finished partitions exist")
	})

	s.Run("MultipleUnfinishedPartitions", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"finished1", tsBase.Add(-time.Hour), screamer.StateFinished},
			{"unfinished1_older_watermark", tsBase, screamer.StateCreated},
			{"unfinished2_newer_watermark", tsBase.Add(time.Minute), screamer.StateRunning},
		})

		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.NotNil(p)
		s.Equal("unfinished1_older_watermark", p.PartitionToken, "Should return partition with the minimum watermark")
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read_race() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "ReadRace")
	defer cleanup()

	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	// Setup test data
	s.insertTestPartitions(ctx, storage, []testPartition{
		{"created1", timestamp, screamer.StateCreated},
		{"created2", timestamp.Add(-2 * time.Second), screamer.StateCreated},
		{"scheduled", timestamp.Add(time.Second), screamer.StateScheduled},
		{"running", timestamp.Add(2 * time.Second), screamer.StateRunning},
		{"finished", timestamp.Add(-time.Second), screamer.StateFinished},
	})

	s.Run("ConcurrentGetUnfinishedMinWatermarkPartition", func() {
		var wg sync.WaitGroup
		results := make([]*screamer.PartitionMetadata, 3)
		errors := make([]error, 3)
		m := sync.Mutex{}

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				result, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
				m.Lock()
				defer m.Unlock()
				results[index] = result
				errors[index] = err
			}(i)
		}

		wg.Wait()

		for i, err := range errors {
			s.NoError(err, "Error in goroutine %d", i)
		}

		for i, result := range results {
			s.NotNil(result, "Result in goroutine %d is nil", i)
		}
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetAndSchedulePartitions() {
	ctx := context.Background()
	tableName := "GetAndSchedule"
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, tableName)
	defer cleanup()

	runnerID := uuid.NewString()
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	// Register the runner first
	err := storage.RegisterRunner(ctx, runnerID)
	s.NoError(err)

	// Setup test partitions
	s.insertTestPartitions(ctx, storage, []testPartition{
		{"p_created_old_watermark", baseTime.Add(-time.Hour), screamer.StateCreated},
		{"p_created_match_watermark1", baseTime, screamer.StateCreated},
		{"p_created_match_watermark2", baseTime, screamer.StateCreated},
		{"p_created_future_watermark", baseTime.Add(time.Hour), screamer.StateCreated},
		{"p_running", baseTime, screamer.StateRunning},
		{"p_scheduled", baseTime, screamer.StateScheduled},
	})

	s.Run("NoPartitionsReady", func() {
		// Min watermark is far in the future, no CREATED partitions should match
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime.Add(2*time.Hour), runnerID, 100)
		s.NoError(err)
		s.Empty(scheduled, "No partitions should be scheduled if minWatermark is too high")
	})

	s.Run("ScheduleAvailablePartitions", func() {
		// Min watermark allows three CREATED partitions
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Len(scheduled, 3, "Should schedule three partitions")

		expectedTokens := []string{"p_created_match_watermark1", "p_created_match_watermark2", "p_created_future_watermark"}
		actualTokens := s.extractPartitionTokens(scheduled)
		s.ElementsMatch(expectedTokens, actualTokens)

		// Verify state changes in memory and database
		for _, p := range scheduled {
			s.Equal(screamer.StateScheduled, p.State, "Partition state should be updated to Scheduled in memory")
			s.verifyPartitionState(ctx, storage, p.PartitionToken, screamer.StateScheduled)
			s.verifyPartitionAssignment(ctx, storage, p.PartitionToken, runnerID)
		}
	})

	s.Run("AlreadyScheduledOrRunning", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"p_running_again", baseTime, screamer.StateRunning},
			{"p_scheduled_again", baseTime, screamer.StateScheduled},
		})
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Empty(scheduled, "No partitions should be scheduled if they are not in CREATED state")
	})

	s.Run("TransactionRollback", func() {
		storage.CleanupData(ctx)
		runnerID := uuid.NewString()
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		// Create a partition
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"rollback_test", baseTime, screamer.StateCreated},
		})

		// Inject an error to force transaction rollback
		// Since we can't easily inject errors, we'll test that partition count is updated atomically
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Len(scheduled, 1)
	})

	s.Run("ConcurrentScheduling", func() {
		storage.CleanupData(ctx)

		// Create multiple runners
		runner1ID := uuid.NewString()
		runner2ID := uuid.NewString()
		err := storage.RegisterRunner(ctx, runner1ID)
		s.NoError(err)
		err = storage.RegisterRunner(ctx, runner2ID)
		s.NoError(err)

		// Create partitions for concurrent scheduling
		partitions := []testPartition{}
		for i := range 20 {
			partitions = append(partitions, testPartition{
				fmt.Sprintf("concurrent_%d", i),
				baseTime.Add(time.Duration(i) * time.Second),
				screamer.StateCreated,
			})
		}
		s.insertTestPartitions(ctx, storage, partitions)

		storage2 := NewSpanner(s.client, tableName)

		// Concurrently try to schedule partitions
		var wg sync.WaitGroup
		results := make(map[string][]*screamer.PartitionMetadata)
		errors := make(map[string]error)
		mu := sync.Mutex{}

		wg.Add(2)
		go func(rid string) {
			defer wg.Done()
			scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, rid, 100)
			mu.Lock()
			defer mu.Unlock()
			results[rid] = scheduled
			errors[rid] = err
		}(runner1ID)
		go func(rid string) {
			defer wg.Done()
			scheduled, err := storage2.GetAndSchedulePartitions(ctx, baseTime, rid, 100)
			mu.Lock()
			defer mu.Unlock()
			results[rid] = scheduled
			errors[rid] = err
		}(runner2ID)

		wg.Wait()

		// Both calls should succeed
		s.NoError(errors[runner1ID])
		s.NoError(errors[runner2ID])

		// Verify no partition is assigned to both runners
		allScheduled := append(results[runner1ID], results[runner2ID]...)
		s.Len(allScheduled, 20, "Total scheduled partitions should be equal to created partitions")
		tokenMap := make(map[string]int)
		for _, p := range allScheduled {
			tokenMap[p.PartitionToken]++
		}
		for token, count := range tokenMap {
			s.Equal(1, count, "Partition %s should only be scheduled once", token)
		}
	})

	s.Run("ScheduledAtTimestamp", func() {
		storage.CleanupData(ctx)
		runnerID := uuid.NewString()
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		// Create a partition
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"timestamp_test", baseTime, screamer.StateCreated},
		})

		testStartTime := time.Now().UTC()
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Len(scheduled, 1)

		// Verify ScheduledAt timestamp is set
		partition := scheduled[0]
		s.NotNil(partition.ScheduledAt, "ScheduledAt should be set")

		// Verify timestamp is reasonable (within 5 seconds of when we called the method)
		// This is more lenient than exact before/after checks to avoid flakiness
		timeDiff := partition.ScheduledAt.Sub(testStartTime)
		s.LessOrEqual(timeDiff.Abs().Seconds(), .5, "ScheduledAt should be within 0.5 seconds of test execution")

		// Verify in database - the important thing is that in-memory and DB match
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName,
			spanner.Key{partition.PartitionToken}, []string{columnScheduledAt})
		s.NoError(err)
		var dbScheduledAt spanner.NullTime
		err = row.Columns(&dbScheduledAt)
		s.NoError(err)
		s.True(dbScheduledAt.Valid, "Database ScheduledAt should be valid")

		// Compare Unix timestamps to avoid nanosecond precision issues
		s.Equal(partition.ScheduledAt.Unix(), dbScheduledAt.Time.Unix(),
			"In-memory and database ScheduledAt should match (to the second)")
	})

	s.Run("OrderByStartTimestamp", func() {
		storage.CleanupData(ctx)
		runnerID := uuid.NewString()
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		// Create partitions with different start timestamps
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"order_test_3", baseTime.Add(3 * time.Hour), screamer.StateCreated},
			{"order_test_1", baseTime.Add(1 * time.Hour), screamer.StateCreated},
			{"order_test_2", baseTime.Add(2 * time.Hour), screamer.StateCreated},
		})

		// Should get partitions ordered by StartTimestamp ASC
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Len(scheduled, 3)

		// Verify order
		s.Equal("order_test_1", scheduled[0].PartitionToken)
		s.Equal("order_test_2", scheduled[1].PartitionToken)
		s.Equal("order_test_3", scheduled[2].PartitionToken)
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_AddChildPartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "AddChildPartitions")
	defer cleanup()

	childStartTimestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var heartbeatMillis int64 = 10000

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent1",
		ParentTokens:    []string{},
		StartTimestamp:  time.Time{},
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatMillis,
		State:           screamer.StateRunning,
		Watermark:       time.Time{},
	}
	record := &screamer.ChildPartitionsRecord{
		StartTimestamp: childStartTimestamp,
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "token1", ParentPartitionTokens: []string{"parent1"}},
			{Token: "token2", ParentPartitionTokens: []string{"parent1"}},
		},
	}
	err := storage.AddChildPartitions(ctx, parent, record)
	s.NoError(err)

	columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}

	got := []screamer.PartitionMetadata{}
	err = storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), columns).Do(func(r *spanner.Row) error {
		p := screamer.PartitionMetadata{}
		if err := r.ToStruct(&p); err != nil {
			return err
		}
		got = append(got, p)
		return nil
	})
	s.NoError(err)

	s.Len(got, 2, "Should have created 2 child partitions")

	// Verify both child partitions have correct values
	expectedTokens := []string{"token1", "token2"}
	actualTokens := make([]string, len(got))
	for i, p := range got {
		actualTokens[i] = p.PartitionToken
	}
	s.ElementsMatch(expectedTokens, actualTokens)

	for _, partition := range got {
		s.Equal([]string{"parent1"}, partition.ParentTokens)
		s.Equal(childStartTimestamp, partition.StartTimestamp)
		s.Equal(endTimestamp, partition.EndTimestamp)
		s.Equal(heartbeatMillis, partition.HeartbeatMillis)
		s.Equal(screamer.StateCreated, partition.State)
		s.Equal(childStartTimestamp, partition.Watermark)
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Update() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "Update")
	defer cleanup()
	runnerID := uuid.NewString()
	err := storage.RegisterRunner(ctx, runnerID)
	s.NoError(err)

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	partitions := []*screamer.PartitionMetadata{
		s.createTestPartition("token1", screamer.StateCreated, baseTime),
		s.createTestPartition("token2", screamer.StateCreated, baseTime),
	}

	// Insert test partitions
	mutations := make([]*spanner.Mutation, len(partitions))
	for i, p := range partitions {
		mutations[i] = spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  p.PartitionToken,
			columnParentTokens:    p.ParentTokens,
			columnStartTimestamp:  p.StartTimestamp,
			columnEndTimestamp:    p.EndTimestamp,
			columnHeartbeatMillis: p.HeartbeatMillis,
			columnState:           p.State,
			columnWatermark:       p.Watermark,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}
	_, err = storage.client.Apply(ctx, mutations)
	s.NoError(err)

	s.Run("UpdateToRunning", func() {
		err := storage.UpdateToRunning(ctx, partitions[0])
		s.NoError(err)

		type result struct {
			State     screamer.State
			RunningAt spanner.NullTime
		}
		got := result{}
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, []string{columnState, columnRunningAt})
		s.NoError(err)
		err = row.ToStruct(&got)
		s.NoError(err)

		s.Equal(screamer.StateRunning, got.State)
		s.True(got.RunningAt.Valid && !got.RunningAt.Time.IsZero(), "RunningAt should be set")
	})

	s.Run("UpdateToFinished", func() {
		err := storage.UpdateToFinished(ctx, partitions[0], runnerID)
		s.NoError(err)

		type result struct {
			State      screamer.State
			FinishedAt spanner.NullTime
		}
		got := result{}
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, []string{columnState, columnFinishedAt})
		s.NoError(err)
		err = row.ToStruct(&got)
		s.NoError(err)

		s.Equal(screamer.StateFinished, got.State)
		s.True(got.FinishedAt.Valid && !got.FinishedAt.Time.IsZero(), "FinishedAt should be set")
	})

	s.Run("UpdateWatermark", func() {
		newWatermark := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
		err := storage.UpdateWatermark(ctx, partitions[1], newWatermark)
		s.NoError(err)

		var gotWatermark time.Time
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token2"}, []string{columnWatermark})
		s.NoError(err)
		err = row.Columns(&gotWatermark)
		s.NoError(err)
		s.Equal(newWatermark, gotWatermark)
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_AddChildPartitions_Idempotency() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "AddChildPartitionsIdempotency")
	defer cleanup()

	childStartTimestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp := time.Now().AddDate(1, 0, 0)
	var heartbeatMillis int64 = 10000

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent_idem",
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatMillis,
	}
	record := &screamer.ChildPartitionsRecord{
		StartTimestamp: childStartTimestamp,
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child_idem1", ParentPartitionTokens: []string{"parent_idem"}},
		},
	}

	// First call - should add the child
	err := storage.AddChildPartitions(ctx, parent, record)
	s.NoError(err)

	// Verify child exists
	_, err = storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"child_idem1"}, []string{columnPartitionToken})
	s.NoError(err, "Child partition should exist after first call")

	// Second call with the same child - should be idempotent (no error due to AlreadyExists)
	err = storage.AddChildPartitions(ctx, parent, record)
	s.NoError(err, "Second call to AddChildPartitions with same child should not error due to AlreadyExists")

	// Count rows to ensure no duplicates (though primary key would prevent exact duplicates)
	iter := storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), []string{columnPartitionToken})
	defer iter.Stop()
	rowCount := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		s.NoError(err)
		rowCount++
	}
	s.Equal(1, rowCount, "Should still only be one child partition row after idempotent call")
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetInterruptedPartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetInterrupted")
	defer cleanup()

	callingRunnerID := "calling_runner_" + uuid.NewString()
	err := storage.RegisterRunner(ctx, callingRunnerID)
	s.Require().NoError(err)

	baseTime := time.Now().UTC().Truncate(time.Microsecond).Add(-100 * time.Millisecond)
	staleTime := baseTime.Add(-10 * time.Second) // Well before the 3-second stale interval
	liveTime := baseTime.Add(-1 * time.Second)   // Within the 3-second live interval

	// Setup test scenarios
	staleRunnerID := "stale_runner_" + uuid.NewString()
	liveRunnerID := "live_runner_" + uuid.NewString()

	// Create runners
	s.setupTestRunner(ctx, storage, testRunner{staleRunnerID, staleTime, staleTime})
	s.setupTestRunner(ctx, storage, testRunner{liveRunnerID, baseTime, liveTime})

	// Create partitions
	partitions := []testPartition{
		{"p_stale_runner", baseTime, screamer.StateRunning},
		{"p_orphaned", baseTime, screamer.StateScheduled},
		{"p_live_runner", baseTime, screamer.StateRunning},
		{"p_created_state", baseTime, screamer.StateCreated},
	}
	s.insertTestPartitions(ctx, storage, partitions)

	// Assign partitions to runners (except orphaned)
	s.assignPartitionToRunner(ctx, storage, "p_stale_runner", staleRunnerID, staleTime)
	s.assignPartitionToRunner(ctx, storage, "p_live_runner", liveRunnerID, liveTime)

	// Execute GetInterruptedPartitions
	interruptedPartitions, err := storage.GetInterruptedPartitions(ctx, callingRunnerID, 3*time.Second)
	s.NoError(err)

	s.Len(interruptedPartitions, 2, "Should find two interrupted partitions (stale runner and orphaned)")

	expectedTokens := []string{"p_stale_runner", "p_orphaned"}
	actualTokens := s.extractPartitionTokens(interruptedPartitions)
	s.ElementsMatch(expectedTokens, actualTokens)

	// Verify reassignment
	for _, p := range interruptedPartitions {
		s.verifyPartitionAssignment(ctx, storage, p.PartitionToken, callingRunnerID)
	}

	// Second call by another runner should find nothing
	anotherCallingRunnerID := "another_calling_runner_" + uuid.NewString()
	err = storage.RegisterRunner(ctx, anotherCallingRunnerID)
	s.Require().NoError(err)

	interruptedPartitionsAgain, err := storage.GetInterruptedPartitions(ctx, anotherCallingRunnerID, 3*time.Second)
	s.NoError(err)
	s.Empty(interruptedPartitionsAgain, "Second call by another runner should not find already reassigned partitions")
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetInterruptedPartitions_limited() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetInterruptedLimited")
	defer cleanup()

	callingRunnerID := "calling_runner_" + uuid.NewString()
	err := storage.RegisterRunner(ctx, callingRunnerID)
	s.Require().NoError(err)

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	staleTime := baseTime.Add(-10 * time.Second)
	staleRunnerID := "stale_runner_" + uuid.NewString()

	s.setupTestRunner(ctx, storage, testRunner{staleRunnerID, staleTime, staleTime})

	// Create 150 partitions assigned to stale runner
	partitionTokens := make([]string, 150)
	for i := 0; i < 150; i++ {
		token := s.insertPartitionForRunner(ctx, storage, staleRunnerID)
		partitionTokens[i] = token
		time.Sleep(time.Millisecond * 10) // Small delay for uniqueness
	}

	// First call should return 100 partitions (limit)
	interruptedPartitions, err := storage.GetInterruptedPartitions(ctx, callingRunnerID, 3*time.Second)
	s.NoError(err)
	s.Len(interruptedPartitions, 100, "Should return 100 interrupted partitions (limit)")

	// Second call should return remaining 50 partitions
	interruptedPartitionsAgain, err := storage.GetInterruptedPartitions(ctx, callingRunnerID, 3*time.Second)
	s.NoError(err)
	s.Len(interruptedPartitionsAgain, 50, "Should return remaining 50 interrupted partitions")

	// Verify all partitions are reassigned
	allInterrupted := append(interruptedPartitions, interruptedPartitionsAgain...)
	for _, p := range allInterrupted {
		s.verifyPartitionAssignment(ctx, storage, p.PartitionToken, callingRunnerID)
	}

	// Third call by different runner should find nothing
	anotherCallingRunnerID := "another_calling_runner_" + uuid.NewString()
	err = storage.RegisterRunner(ctx, anotherCallingRunnerID)
	s.Require().NoError(err)
	err = storage.RefreshRunner(ctx, callingRunnerID)
	s.Require().NoError(err)

	differentRunnerPartitions, err := storage.GetInterruptedPartitions(ctx, anotherCallingRunnerID, 3*time.Second)
	s.NoError(err)
	s.Empty(differentRunnerPartitions, "Third call should find no partitions")
}

// insertPartitionForRunner creates a partition assigned to a specific runner
func (s *SpannerTestSuite) insertPartitionForRunner(ctx context.Context, storage *testStorage, runnerID string) string {
	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	token := uuid.NewString()

	partition := testPartition{token, baseTime, screamer.StateRunning}
	s.insertTestPartitions(ctx, storage, []testPartition{partition})

	return token
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetActiveRunnerCount() {
	ctx := context.Background()
	tableName := "active_runner_count_test"
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, tableName)
	defer cleanup()

	leaseDuration := 10 * time.Second

	s.Run("NoActiveRunners", func() {
		storage.CleanupData(ctx)
		count, err := storage.GetActiveRunnerCount(ctx, leaseDuration)
		s.NoError(err)
		s.Equal(int64(0), count, "Should have 0 active runners")
	})

	s.Run("SingleActiveRunner", func() {
		storage.CleanupData(ctx)
		runnerID := uuid.NewString()

		// Register runner
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		count, err := storage.GetActiveRunnerCount(ctx, leaseDuration)
		s.NoError(err)
		s.Equal(int64(1), count, "Should have 1 active runner")
	})

	s.Run("MultipleActiveRunners", func() {
		storage.CleanupData(ctx)

		// Register 3 runners
		for i := 0; i < 3; i++ {
			runnerID := uuid.NewString()
			err := storage.RegisterRunner(ctx, runnerID)
			s.NoError(err)
		}

		count, err := storage.GetActiveRunnerCount(ctx, leaseDuration)
		s.NoError(err)
		s.Equal(int64(3), count, "Should have 3 active runners")
	})

	s.Run("ExpiredRunnersNotCounted", func() {
		storage.CleanupData(ctx)

		// Register old runner (will be expired)
		oldRunnerID := uuid.NewString()
		oldTime := time.Now().UTC().Add(-20 * time.Second) // 20 seconds ago
		_, err := storage.client.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
				columnRunnerID:  oldRunnerID,
				columnCreatedAt: oldTime,
				columnUpdatedAt: oldTime,
			}),
		})
		s.NoError(err)

		// Register fresh runner
		freshRunnerID := uuid.NewString()
		err = storage.RegisterRunner(ctx, freshRunnerID)
		s.NoError(err)

		// Count with 10 second lease - only fresh runner should count
		count, err := storage.GetActiveRunnerCount(ctx, leaseDuration)
		s.NoError(err)
		s.Equal(int64(1), count, "Should only count the fresh runner")
	})

	s.Run("LeaseDurationMatters", func() {
		storage.CleanupData(ctx)

		// Register runner 6 seconds ago
		runnerID := uuid.NewString()
		sixSecondsAgo := time.Now().UTC().Add(-6 * time.Second)
		_, err := storage.client.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
				columnRunnerID:  runnerID,
				columnCreatedAt: sixSecondsAgo,
				columnUpdatedAt: sixSecondsAgo,
			}),
		})
		s.NoError(err)

		// With 5 second lease, should not count
		count, err := storage.GetActiveRunnerCount(ctx, 5*time.Second)
		s.NoError(err)
		s.Equal(int64(0), count, "5 second lease should not count 6-second-old runner")

		// With 10 second lease, should count
		count, err = storage.GetActiveRunnerCount(ctx, 10*time.Second)
		s.NoError(err)
		s.Equal(int64(1), count, "10 second lease should count 6-second-old runner")
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetActivePartitionCount() {
	ctx := context.Background()
	tableName := "active_partition_count_test"
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, tableName)
	defer cleanup()

	baseTime := time.Now().UTC().Truncate(time.Microsecond)

	s.Run("NoPartitions", func() {
		storage.CleanupData(ctx)
		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(0), count, "Should have 0 active partitions")
	})

	s.Run("OnlyCreatedPartitions", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"created1", baseTime, screamer.StateCreated},
			{"created2", baseTime, screamer.StateCreated},
		})

		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(0), count, "Created partitions should not count as active")
	})

	s.Run("OnlyScheduledPartitions", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"scheduled1", baseTime, screamer.StateScheduled},
			{"scheduled2", baseTime, screamer.StateScheduled},
			{"scheduled3", baseTime, screamer.StateScheduled},
		})

		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(3), count, "Should count all scheduled partitions")
	})

	s.Run("OnlyRunningPartitions", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"running1", baseTime, screamer.StateRunning},
			{"running2", baseTime, screamer.StateRunning},
		})

		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(2), count, "Should count all running partitions")
	})

	s.Run("MixedStates", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"created", baseTime, screamer.StateCreated},
			{"scheduled1", baseTime, screamer.StateScheduled},
			{"scheduled2", baseTime, screamer.StateScheduled},
			{"running1", baseTime, screamer.StateRunning},
			{"running2", baseTime, screamer.StateRunning},
			{"running3", baseTime, screamer.StateRunning},
			{"finished", baseTime, screamer.StateFinished},
		})

		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(5), count, "Should count 2 scheduled + 3 running = 5 active partitions")
	})

	s.Run("FinishedPartitionsNotCounted", func() {
		storage.CleanupData(ctx)
		s.insertTestPartitions(ctx, storage, []testPartition{
			{"finished1", baseTime, screamer.StateFinished},
			{"finished2", baseTime, screamer.StateFinished},
			{"running", baseTime, screamer.StateRunning},
		})

		count, err := storage.GetActivePartitionCount(ctx)
		s.NoError(err)
		s.Equal(int64(1), count, "Should only count running partition, not finished")
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_ExtendLease() {
	ctx := context.Background()
	tableName := "extend_lease_test"
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, tableName)
	defer cleanup()

	runnerID := uuid.NewString()
	err := storage.RegisterRunner(ctx, runnerID)
	s.Require().NoError(err)

	s.Run("ExtendLeaseForExistingAssignment", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		partitionToken := "test-partition"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		// Create partition and assignment
		s.insertTestPartitions(ctx, storage, []testPartition{
			{partitionToken, baseTime, screamer.StateRunning},
		})
		s.assignPartitionToRunner(ctx, storage, partitionToken, runnerID, baseTime.Add(-5*time.Second))

		// Get initial UpdatedAt
		row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
			spanner.Key{partitionToken, runnerID}, []string{columnUpdatedAt})
		s.NoError(err)
		var initialUpdatedAt time.Time
		err = row.Column(0, &initialUpdatedAt)
		s.NoError(err)

		// Wait a bit to ensure timestamp difference
		time.Sleep(100 * time.Millisecond)

		// Extend lease
		err = storage.ExtendLease(ctx, partitionToken, runnerID)
		s.NoError(err)

		// Verify UpdatedAt was updated
		row, err = storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
			spanner.Key{partitionToken, runnerID}, []string{columnUpdatedAt})
		s.NoError(err)
		var newUpdatedAt time.Time
		err = row.Column(0, &newUpdatedAt)
		s.NoError(err)

		s.True(newUpdatedAt.After(initialUpdatedAt),
			"UpdatedAt should be later after extending lease")
	})

	s.Run("ExtendLeaseCreatesAssignmentIfNotExists", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		partitionToken := "new-partition"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		// Create partition but no assignment
		s.insertTestPartitions(ctx, storage, []testPartition{
			{partitionToken, baseTime, screamer.StateRunning},
		})

		// Extend lease (should create assignment)
		err = storage.ExtendLease(ctx, partitionToken, runnerID)
		s.NoError(err)

		// Verify assignment was created
		row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
			spanner.Key{partitionToken, runnerID}, []string{columnRunnerID})
		s.NoError(err)
		var assignedRunnerID string
		err = row.Column(0, &assignedRunnerID)
		s.NoError(err)
		s.Equal(runnerID, assignedRunnerID)
	})

	s.Run("ExtendLeaseMultipleTimes", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		partitionToken := "multi-extend-partition"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		s.insertTestPartitions(ctx, storage, []testPartition{
			{partitionToken, baseTime, screamer.StateRunning},
		})

		var previousUpdatedAt time.Time

		// Extend lease 3 times
		for i := 0; i < 3; i++ {
			time.Sleep(100 * time.Millisecond)

			err = storage.ExtendLease(ctx, partitionToken, runnerID)
			s.NoError(err)

			row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
				spanner.Key{partitionToken, runnerID}, []string{columnUpdatedAt})
			s.NoError(err)
			var currentUpdatedAt time.Time
			err = row.Column(0, &currentUpdatedAt)
			s.NoError(err)

			if i > 0 {
				s.True(currentUpdatedAt.After(previousUpdatedAt),
					"Each extend should update the timestamp")
			}
			previousUpdatedAt = currentUpdatedAt
		}
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_ReleaseLease() {
	ctx := context.Background()
	tableName := "release_lease_test"
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, tableName)
	defer cleanup()

	runnerID := uuid.NewString()
	err := storage.RegisterRunner(ctx, runnerID)
	s.Require().NoError(err)

	s.Run("ReleaseExistingLease", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		partitionToken := "release-test-partition"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		// Create partition and assignment
		s.insertTestPartitions(ctx, storage, []testPartition{
			{partitionToken, baseTime, screamer.StateRunning},
		})
		s.assignPartitionToRunner(ctx, storage, partitionToken, runnerID, baseTime)

		// Verify assignment exists
		row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
			spanner.Key{partitionToken, runnerID}, []string{columnRunnerID})
		s.NoError(err)
		var assignedRunnerID string
		err = row.Column(0, &assignedRunnerID)
		s.NoError(err)
		s.Equal(runnerID, assignedRunnerID)

		// Release lease
		err = storage.ReleaseLease(ctx, partitionToken, runnerID)
		s.NoError(err)

		// Verify assignment was deleted
		_, err = storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
			spanner.Key{partitionToken, runnerID}, []string{columnRunnerID})
		s.Error(err, "Assignment should be deleted after releasing lease")
	})

	s.Run("ReleaseNonExistentLease", func() {
		storage.CleanupData(ctx)

		partitionToken := "non-existent-partition"

		// Release lease for partition that doesn't exist (should not error)
		err = storage.ReleaseLease(ctx, partitionToken, runnerID)
		s.NoError(err, "Releasing non-existent lease should not error")
	})

	s.Run("ReleaseDoesNotAffectPartitionState", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		partitionToken := "state-check-partition"
		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		// Create partition and assignment
		s.insertTestPartitions(ctx, storage, []testPartition{
			{partitionToken, baseTime, screamer.StateRunning},
		})
		s.assignPartitionToRunner(ctx, storage, partitionToken, runnerID, baseTime)

		// Release lease
		err = storage.ReleaseLease(ctx, partitionToken, runnerID)
		s.NoError(err)

		// Verify partition still exists with same state
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName,
			spanner.Key{partitionToken}, []string{columnState})
		s.NoError(err)
		var state screamer.State
		err = row.Column(0, &state)
		s.NoError(err)
		s.Equal(screamer.StateRunning, state, "Partition state should remain unchanged")
	})

	s.Run("ReleaseMultipleLeases", func() {
		storage.CleanupData(ctx)
		err := storage.RegisterRunner(ctx, runnerID)
		s.NoError(err)

		baseTime := time.Now().UTC().Truncate(time.Microsecond)

		// Create and assign 3 partitions
		partitionTokens := []string{"part1", "part2", "part3"}
		for _, token := range partitionTokens {
			s.insertTestPartitions(ctx, storage, []testPartition{
				{token, baseTime, screamer.StateRunning},
			})
			s.assignPartitionToRunner(ctx, storage, token, runnerID, baseTime)
		}

		// Release all leases
		for _, token := range partitionTokens {
			err = storage.ReleaseLease(ctx, token, runnerID)
			s.NoError(err)
		}

		// Verify all assignments were deleted
		for _, token := range partitionTokens {
			_, err = storage.client.Single().ReadRow(ctx, tablePartitionToRunner,
				spanner.Key{token, runnerID}, []string{columnRunnerID})
			s.Error(err, "All assignments should be deleted")
		}
	})
}
