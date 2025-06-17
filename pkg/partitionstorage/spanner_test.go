package partitionstorage

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/internal/helper"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
	ctx       context.Context
	container testcontainers.Container
	client    *spanner.Client
	timeout   time.Duration
	dsn       string
}

func TestSpannerTestSuite(t *testing.T) {
	suite.Run(t, new(SpannerTestSuite))
}

func (s *SpannerTestSuite) SetupSuite() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	image := "gcr.io/cloud-spanner-emulator/emulator"
	ports := []string{"9010/tcp"}
	s.ctx = context.Background()
	s.timeout = time.Second * 1500
	s.dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	envVars := make(map[string]string)
	var err error
	s.container, err = helper.NewTestContainer(s.ctx, image, envVars, ports, wait.ForLog("gRPC server listening at"))
	s.NoError(err)

	mappedPort, err := s.container.MappedPort(s.ctx, "9010")
	s.NoError(err)
	hostIP, err := s.container.Host(s.ctx)
	s.NoError(err)
	hostPort := fmt.Sprintf("%s:%s", hostIP, mappedPort.Port())

	os.Setenv("SPANNER_EMULATOR_HOST", hostPort)

	s.createInstance() // create instance
	s.createDatabase() // create database
}

func (s *SpannerTestSuite) TearDownSuite() {
	if s.container != nil {
		err := s.container.Terminate(s.ctx)
		s.NoError(err)
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
			Config:      "emulator-config",
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	s.NoError(err)

	_, err = op.Wait(s.ctx)
	s.NoError(err)
}

func (s *SpannerTestSuite) createDatabase() {
	databaseAdminClient, err := database.NewDatabaseAdminClient(s.ctx)
	s.NoError(err)
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(s.ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + projectID + "/instances/" + instanceID,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	s.NoError(err)
	_, err = op.Wait(s.ctx)
	s.NoError(err)
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

	storage := &SpannerPartitionStorage{
		client:    s.client,
		tableName: tableName,
	}

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
			if err := storage.InitializeRootPartition(ctx, test.startTimestamp, test.endTimestamp, test.heartbeatInterval); err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}

			columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}
			row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken}, columns)
			if err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}

			got := screamer.PartitionMetadata{}
			if err := row.ToStruct(&got); err != nil {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				s.T().Errorf("InitializeRootPartition(%q, %q, %q): got = %+v, want %+v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, got, test.want)
			}
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

	insert := func(token string, start time.Time, state screamer.State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  token,
			columnParentTokens:    []string{},
			columnStartTimestamp:  start,
			columnEndTimestamp:    time.Time{},
			columnHeartbeatMillis: 0,
			columnState:           state,
			columnWatermark:       start,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert("created1", timestamp, screamer.StateCreated),
		insert("created2", timestamp.Add(-2*time.Second), screamer.StateCreated),
		insert("scheduled", timestamp.Add(time.Second), screamer.StateScheduled),
		insert("running", timestamp.Add(2*time.Second), screamer.StateRunning),
		insert("finished", timestamp.Add(-time.Second), screamer.StateFinished),
	})
	s.NoError(err)

	s.Run("GetUnfinishedMinWatermarkPartition", func() {
		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)

		want := "created2"
		if got.PartitionToken != want {
			s.T().Errorf("GetUnfinishedMinWatermarkPartition(ctx) = %v, want = %v", got.PartitionToken, want)
		}
	})

	s.Run("GetInterruptedPartitions", func() {
		partitions, err := storage.GetInterruptedPartitions(ctx, runnerID)
		s.NoError(err)
		got := []string{}
		for _, p := range partitions {
			got = append(got, p.PartitionToken)
		}

		want := []string{"scheduled", "running"}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("GetInterruptedPartitions(ctx) = %+v, want = %+v", got, want)
		}
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetUnfinishedMinWatermarkPartition_Scenarios() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetUnfinishedMinWatermark")
	defer cleanup()

	tsBase := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	mutations := []*spanner.Mutation{
		// Finished partition
		spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken: "finished1", columnParentTokens: []string{}, columnStartTimestamp: tsBase.Add(-time.Hour), columnEndTimestamp: time.Now().AddDate(1, 0, 0),
			columnHeartbeatMillis: 10000, columnState: screamer.StateFinished, columnWatermark: tsBase.Add(-time.Hour), columnCreatedAt: spanner.CommitTimestamp,
		}),
		// Unfinished partitions
		spanner.InsertMap(storage.tableName, map[string]interface{}{ // This should be picked
			columnPartitionToken: "unfinished1_older_watermark", columnParentTokens: []string{}, columnStartTimestamp: tsBase, columnEndTimestamp: time.Now().AddDate(1, 0, 0),
			columnHeartbeatMillis: 10000, columnState: screamer.StateCreated, columnWatermark: tsBase, columnCreatedAt: spanner.CommitTimestamp,
		}),
		spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken: "unfinished2_newer_watermark", columnParentTokens: []string{}, columnStartTimestamp: tsBase.Add(time.Minute), columnEndTimestamp: time.Now().AddDate(1, 0, 0),
			columnHeartbeatMillis: 10000, columnState: screamer.StateRunning, columnWatermark: tsBase.Add(time.Minute), columnCreatedAt: spanner.CommitTimestamp,
		}),
	}

	s.Run("NoPartitions", func() {
		// Ensure table is empty for this sub-test by cleaning again (or run it first)
		storage.CleanupData(ctx)       // Clean specifically for this sub-test
		defer storage.CleanupData(ctx) // And clean after

		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.Nil(p, "Should return nil when no partitions exist")
	})

	storage.CleanupData(ctx) // Clean before next set of tests that expect data

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{mutations[0]}) // Only finished
	s.NoError(err)
	s.Run("OnlyFinishedPartitions", func() {
		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.Nil(p, "Should return nil when only finished partitions exist")
	})
	storage.CleanupData(ctx) // Clean again

	_, err = storage.client.Apply(ctx, mutations) // All partitions
	s.NoError(err)
	s.Run("MultipleUnfinishedPartitions", func() {
		p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		s.NoError(err)
		s.NotNil(p)
		s.Equal("unfinished1_older_watermark", p.PartitionToken, "Should return partition with the minimum watermark")
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read_race() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "ReadRace") // Changed table name for clarity
	defer cleanup()

	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	insert := func(token string, start time.Time, state screamer.State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  token,
			columnParentTokens:    []string{},
			columnStartTimestamp:  start,
			columnEndTimestamp:    time.Time{},
			columnHeartbeatMillis: 0,
			columnState:           state,
			columnWatermark:       start,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert("created1", timestamp, screamer.StateCreated),
		insert("created2", timestamp.Add(-2*time.Second), screamer.StateCreated),
		insert("scheduled", timestamp.Add(time.Second), screamer.StateScheduled),
		insert("running", timestamp.Add(2*time.Second), screamer.StateRunning),
		insert("finished", timestamp.Add(-time.Second), screamer.StateFinished),
	})
	s.NoError(err)

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
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetAndSchedule")
	defer cleanup()

	runnerID := uuid.NewString()
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	defaultEnd := time.Now().AddDate(1, 0, 0) // Use a consistent far-future end time

	createPartition := func(token string, start time.Time, state screamer.State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken: token, columnParentTokens: []string{}, columnStartTimestamp: start, columnEndTimestamp: defaultEnd,
			columnHeartbeatMillis: 10000, columnState: state, columnWatermark: start, columnCreatedAt: spanner.CommitTimestamp,
		})
	}

	mutations := []*spanner.Mutation{
		createPartition("p_created_old_watermark", baseTime.Add(-time.Hour), screamer.StateCreated),   // Should not be picked if minWatermark is baseTime
		createPartition("p_created_match_watermark1", baseTime, screamer.StateCreated),                // Should be picked
		createPartition("p_created_match_watermark2", baseTime, screamer.StateCreated),                // Should be picked
		createPartition("p_created_future_watermark", baseTime.Add(time.Hour), screamer.StateCreated), // Should be picked
		createPartition("p_running", baseTime, screamer.StateRunning),                                 // Not CREATED state
		createPartition("p_scheduled", baseTime, screamer.StateScheduled),                             // Not CREATED state
	}
	_, err := storage.client.Apply(ctx, mutations)
	s.NoError(err)

	s.Run("NoPartitionsReady", func() {
		// Min watermark is far in the future, no CREATED partitions should match
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime.Add(2*time.Hour), runnerID)
		s.NoError(err)
		s.Empty(scheduled, "No partitions should be scheduled if minWatermark is too high")
	})

	s.Run("ScheduleAvailablePartitions", func() {
		// Min watermark allows p_created_match_watermark1, p_created_match_watermark2, and p_created_future_watermark
		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID)
		s.NoError(err)
		s.Len(scheduled, 3, "Should schedule three partitions")

		foundTokens := make(map[string]bool)
		for _, p := range scheduled {
			s.Equal(screamer.StateScheduled, p.State, "Partition state should be updated to Scheduled in memory (original state was Created)")
			foundTokens[p.PartitionToken] = true

			// Verify in DB
			row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{p.PartitionToken}, []string{columnState, columnScheduledAt})
			s.NoError(err)
			var dbState screamer.State
			var dbScheduledAt time.Time
			err = row.Columns(&dbState, &dbScheduledAt)
			s.NoError(err)
			s.Equal(screamer.StateScheduled, dbState, "Partition state in DB should be Scheduled")
			s.False(dbScheduledAt.IsZero(), "ScheduledAt should be set")

			// Verify PartitionToRunner
			row, err = storage.client.Single().ReadRow(ctx, tablePartitionToRunner, spanner.Key{p.PartitionToken, runnerID}, []string{columnRunnerID})
			s.NoError(err)
			var dbRunnerID string
			err = row.Columns(&dbRunnerID)
			s.NoError(err)
			s.Equal(runnerID, dbRunnerID, "Partition should be assigned to the correct runner")
		}
		s.True(foundTokens["p_created_match_watermark1"])
		s.True(foundTokens["p_created_match_watermark2"])
		s.True(foundTokens["p_created_future_watermark"])
	})

	s.Run("AlreadyScheduledOrRunning", func() {
		// Call again, no new CREATED partitions should be found as they are now SCHEDULED
		storage.CleanupData(ctx) // Clean and set up only non-CREATED states or already handled.
		mutations := []*spanner.Mutation{
			createPartition("p_running_again", baseTime, screamer.StateRunning),
			createPartition("p_scheduled_again", baseTime, screamer.StateScheduled),
		}
		_, err := storage.client.Apply(ctx, mutations)
		s.NoError(err)

		scheduled, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID)
		s.NoError(err)
		s.Empty(scheduled, "No partitions should be scheduled if they are not in CREATED state")
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

	want := []screamer.PartitionMetadata{
		{
			PartitionToken:  "token1",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           screamer.StateCreated,
			Watermark:       childStartTimestamp,
		},
		{
			PartitionToken:  "token2",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           screamer.StateCreated,
			Watermark:       childStartTimestamp,
		},
	}
	if !reflect.DeepEqual(got, want) {
		s.T().Errorf("AddChildPartitions(ctx, %+v, %+v): got = %+v, want %+v", parent, record, got, want)
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Update() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "Update")
	defer cleanup()

	create := func(token string) *screamer.PartitionMetadata {
		return &screamer.PartitionMetadata{
			PartitionToken:  token,
			ParentTokens:    []string{},
			StartTimestamp:  time.Time{},
			EndTimestamp:    time.Time{},
			HeartbeatMillis: 0,
			State:           screamer.StateCreated,
			Watermark:       time.Time{},
		}
	}

	insert := func(p *screamer.PartitionMetadata) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
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

	partitions := []*screamer.PartitionMetadata{create("token1"), create("token2")}

	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert(partitions[0]),
		insert(partitions[1]),
	})
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
		err := storage.UpdateToFinished(ctx, partitions[0]) // Using partitions[0] which is now "Running"
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
		timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		err := storage.UpdateWatermark(ctx, partitions[1], timestamp) // Using partitions[1] for this
		s.NoError(err)

		gotWatermark := time.Time{}
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token2"}, []string{columnWatermark})
		s.NoError(err)
		err = row.Columns(&gotWatermark)
		s.NoError(err)
		s.Equal(timestamp, gotWatermark)
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

// --- Helper methods for setting up test data ---

// insertRawPartitionData inserts a generic partition metadata row.
func (s *SpannerTestSuite) insertRawPartitionData(ctx context.Context, client *spanner.Client, tableName string, part screamer.PartitionMetadata) {
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap(tableName, map[string]interface{}{
			columnPartitionToken:  part.PartitionToken,
			columnParentTokens:    part.ParentTokens,
			columnStartTimestamp:  part.StartTimestamp,
			columnEndTimestamp:    part.EndTimestamp,
			columnHeartbeatMillis: part.HeartbeatMillis,
			columnState:           part.State,
			columnWatermark:       part.Watermark,
			columnCreatedAt:       spanner.CommitTimestamp, // Default to now
			// ScheduledAt, RunningAt, FinishedAt can be spanner.NullTime or actual times
		}),
	})
	s.Require().NoError(err, "Failed to insert raw partition data for token %s", part.PartitionToken)
}

// insertRawRunnerData inserts a runner row.
func (s *SpannerTestSuite) insertRawRunnerData(ctx context.Context, client *spanner.Client, runnerID string, createdAt, updatedAt time.Time) {
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
			columnRunnerID:  runnerID,
			columnCreatedAt: createdAt,
			columnUpdatedAt: updatedAt,
		}),
	})
	s.Require().NoError(err, "Failed to insert raw runner data for runner %s", runnerID)
}

// insertRawPartitionToRunnerData inserts a partition to runner mapping.
func (s *SpannerTestSuite) insertRawPartitionToRunnerData(ctx context.Context, client *spanner.Client, partitionToken, runnerID string, createdAt, updatedAt time.Time) {
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tablePartitionToRunner, map[string]interface{}{
			columnPartitionToken: partitionToken,
			columnRunnerID:       runnerID,
			columnCreatedAt:      createdAt,
			columnUpdatedAt:      updatedAt,
		}),
	})
	s.Require().NoError(err, "Failed to insert raw partition to runner data for token %s, runner %s", partitionToken, runnerID)
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_GetInterruptedPartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupSpannerPartitionStorage(ctx, "GetInterrupted")
	defer cleanup()

	callingRunnerID := "calling_runner_" + uuid.NewString() // Runner calling GetInterruptedPartitions
	err := storage.RegisterRunner(ctx, callingRunnerID)     // Ensure calling runner is registered and live
	s.Require().NoError(err)

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	staleTime := baseTime.Add(-10 * time.Second) // Well before the 3-second stale interval
	liveTime := baseTime.Add(-1 * time.Second)   // Within the 3-second live interval

	defaultEnd := time.Now().AddDate(1, 0, 0)

	// Scenario 1: Partition assigned to a stale runner
	staleRunnerID := "stale_runner_" + uuid.NewString()
	s.insertRawRunnerData(ctx, storage.client, staleRunnerID, staleTime, staleTime) // Stale runner
	partitionForStaleRunner := screamer.PartitionMetadata{
		PartitionToken: "p_stale_runner", ParentTokens: []string{}, StartTimestamp: baseTime, EndTimestamp: defaultEnd, State: screamer.StateRunning, Watermark: baseTime, HeartbeatMillis: 10000,
	}
	s.insertRawPartitionData(ctx, storage.client, storage.tableName, partitionForStaleRunner)
	s.insertRawPartitionToRunnerData(ctx, storage.client, partitionForStaleRunner.PartitionToken, staleRunnerID, staleTime, staleTime)

	// Scenario 2: Orphaned partition (in Running state but no PartitionToRunner entry)
	orphanedPartition := screamer.PartitionMetadata{
		PartitionToken: "p_orphaned", ParentTokens: []string{}, StartTimestamp: baseTime, EndTimestamp: defaultEnd, State: screamer.StateScheduled, Watermark: baseTime, HeartbeatMillis: 10000,
	}
	s.insertRawPartitionData(ctx, storage.client, storage.tableName, orphanedPartition)
	// No entry in PartitionToRunner for this one

	// Scenario 3: Partition assigned to a live runner (should not be picked up)
	liveRunnerID := "live_runner_" + uuid.NewString()
	s.insertRawRunnerData(ctx, storage.client, liveRunnerID, baseTime, liveTime) // Live runner
	partitionForLiveRunner := screamer.PartitionMetadata{
		PartitionToken: "p_live_runner", ParentTokens: []string{}, StartTimestamp: baseTime, EndTimestamp: defaultEnd, State: screamer.StateRunning, Watermark: baseTime, HeartbeatMillis: 10000,
	}
	s.insertRawPartitionData(ctx, storage.client, storage.tableName, partitionForLiveRunner)
	s.insertRawPartitionToRunnerData(ctx, storage.client, partitionForLiveRunner.PartitionToken, liveRunnerID, baseTime, liveTime)

	// Scenario 4: Partition in CREATED state (should not be picked by GetInterruptedPartitions)
	createdPartition := screamer.PartitionMetadata{
		PartitionToken: "p_created_state", ParentTokens: []string{}, StartTimestamp: baseTime, EndTimestamp: defaultEnd, State: screamer.StateCreated, Watermark: baseTime, HeartbeatMillis: 10000,
	}
	s.insertRawPartitionData(ctx, storage.client, storage.tableName, createdPartition)

	// Execute GetInterruptedPartitions
	s.T().Log("Attempting to run GetInterruptedPartitions. If this hangs or fails with 'FOR UPDATE not supported', the emulator version may have limitations.")
	interruptedPartitions, err := storage.GetInterruptedPartitions(ctx, callingRunnerID)
	s.NoError(err, "GetInterruptedPartitions failed")

	s.Len(interruptedPartitions, 2, "Should find two interrupted partitions (stale runner and orphaned)")

	foundTokens := make(map[string]bool)
	for _, p := range interruptedPartitions {
		foundTokens[p.PartitionToken] = true
		// Verify reassignment in PartitionToRunner table
		row, err := storage.client.Single().ReadRow(ctx, tablePartitionToRunner, spanner.Key{p.PartitionToken, callingRunnerID}, []string{columnRunnerID})
		s.NoError(err, "Failed to read reassigned partition %s for runner %s", p.PartitionToken, callingRunnerID)
		var dbRunnerID string
		err = row.Columns(&dbRunnerID)
		s.NoError(err)
		s.Equal(callingRunnerID, dbRunnerID, "Partition %s should be assigned to callingRunnerID", p.PartitionToken)
	}

	s.True(foundTokens[partitionForStaleRunner.PartitionToken], "Partition from stale runner should be interrupted")
	s.True(foundTokens[orphanedPartition.PartitionToken], "Orphaned partition should be interrupted")
	s.False(foundTokens[partitionForLiveRunner.PartitionToken], "Partition from live runner should NOT be interrupted")
	s.False(foundTokens[createdPartition.PartitionToken], "Partition in CREATED state should NOT be interrupted by this method")

	// Scenario 4: Multiple runners call it (simplified sequential test)
	// First call reassigns. Second call by another runner should find nothing for those specific partitions.
	anotherCallingRunnerID := "another_calling_runner_" + uuid.NewString()
	err = storage.RegisterRunner(ctx, anotherCallingRunnerID)
	s.Require().NoError(err)

	interruptedPartitionsAgain, err := storage.GetInterruptedPartitions(ctx, anotherCallingRunnerID)
	s.NoError(err)
	s.Empty(interruptedPartitionsAgain, "Second call by another runner should not find the already reassigned partitions")
}
