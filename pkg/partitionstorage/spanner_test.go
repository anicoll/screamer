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
	"github.com/anicoll/screamer/internal/helper"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	projectID  = "local-project"
	instanceID = "local-instance"
	databaseID = "local-database"
)

type SpannerTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	client    *spanner.Client
	dsn       string
}

func TestSpannerTestSuite(t *testing.T) {
	suite.Run(t, new(SpannerTestSuite))
}

func (s *SpannerTestSuite) SetupSuite() {
	image := "gcr.io/cloud-spanner-emulator/emulator"
	ports := []string{"9010/tcp"}
	s.ctx = context.Background()
	s.dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	var err error
	s.container, err = helper.NewTestContainer(s.ctx, image, nil, ports, wait.ForLog("gRPC server listening at"))
	s.NoError(err)

	mappedPort, err := s.container.MappedPort(s.ctx, "9010")
	s.NoError(err)
	hostIP, err := s.container.Host(s.ctx)
	s.NoError(err)

	os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%s", hostIP, mappedPort.Port()))

	s.createInstance()
	s.createDatabase()
}

func (s *SpannerTestSuite) TearDownSuite() {
	if s.container != nil {
		s.container.Terminate(s.ctx)
	}
}

func (s *SpannerTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *SpannerTestSuite) createInstance() {
	client, err := instance.NewInstanceAdminClient(s.ctx)
	s.NoError(err)
	defer client.Close()

	op, err := client.CreateInstance(s.ctx, &instancepb.CreateInstanceRequest{
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
	client, err := database.NewDatabaseAdminClient(s.ctx)
	s.NoError(err)
	defer client.Close()

	op, err := client.CreateDatabase(s.ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	s.NoError(err)
	_, err = op.Wait(s.ctx)
	s.NoError(err)
}

// --- Test Helpers ---

type testStorage struct {
	*SpannerPartitionStorage
	t *testing.T
}

func (ts *testStorage) cleanup(ctx context.Context) {
	_, err := ts.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(tablePartitionToRunner, spanner.AllKeys()),
		spanner.Delete(tableRunner, spanner.AllKeys()),
		spanner.Delete(ts.tableName, spanner.AllKeys()),
	})
	assert.NoError(ts.t, err)
}

func (s *SpannerTestSuite) setupStorage(ctx context.Context, tableName string) (*testStorage, func()) {
	proxy := interceptor.NewQueueInterceptor(100)
	client, err := spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(proxy.UnaryInterceptor)))
	s.NoError(err)

	storage := NewSpanner(client, tableName)
	s.NoError(storage.RunMigrations(ctx))

	ts := &testStorage{SpannerPartitionStorage: storage, t: s.T()}
	return ts, func() {
		ts.cleanup(ctx)
		client.Close()
	}
}

func (s *SpannerTestSuite) insertPartitions(ctx context.Context, storage *testStorage, partitions map[string]screamer.State, watermark time.Time) {
	mutations := make([]*spanner.Mutation, 0, len(partitions))
	for token, state := range partitions {
		mutations = append(mutations, spanner.InsertMap(storage.tableName, map[string]interface{}{
			columnPartitionToken:  token,
			columnParentTokens:    []string{},
			columnStartTimestamp:  watermark,
			columnEndTimestamp:    time.Now().AddDate(1, 0, 0),
			columnHeartbeatMillis: 10000,
			columnState:           state,
			columnWatermark:       watermark,
			columnCreatedAt:       spanner.CommitTimestamp,
		}))
	}
	_, err := storage.client.Apply(ctx, mutations)
	s.NoError(err)
}

func (s *SpannerTestSuite) setupRunner(ctx context.Context, storage *testStorage, runnerID string, updatedAt time.Time) {
	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tableRunner, map[string]interface{}{
			columnRunnerID:  runnerID,
			columnCreatedAt: updatedAt,
			columnUpdatedAt: updatedAt,
		}),
	})
	s.NoError(err)
}

func (s *SpannerTestSuite) assignPartition(ctx context.Context, storage *testStorage, partitionToken, runnerID string, assignedAt time.Time) {
	_, err := storage.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdateMap(tablePartitionToRunner, map[string]interface{}{
			columnPartitionToken: partitionToken,
			columnRunnerID:       runnerID,
			columnCreatedAt:      assignedAt,
			columnUpdatedAt:      assignedAt,
		}),
	})
	s.NoError(err)
}

func (s *SpannerTestSuite) getPartitionState(ctx context.Context, storage *testStorage, token string) screamer.State {
	row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{token}, []string{columnState})
	s.NoError(err)
	var state screamer.State
	s.NoError(row.Columns(&state))
	return state
}

// --- Tests ---

func (s *SpannerTestSuite) TestRunMigrations() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, s.dsn)
	s.NoError(err)
	defer client.Close()

	storage := &SpannerPartitionStorage{client: client, tableName: "RunMigrations"}
	s.NoError(storage.RunMigrations(ctx))

	// Verify table exists
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM information_schema.tables WHERE table_catalog = '' AND table_schema = '' AND table_name = @tableName",
		Params: map[string]interface{}{"tableName": storage.tableName},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err = iter.Next()
	s.NoError(err, "Table should exist after migrations")

	// Verify table is readable
	iter = client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), []string{columnPartitionToken})
	defer iter.Stop()
	_, err = iter.Next()
	s.Equal(iterator.Done, err, "Table should be empty after migrations")
}

func (s *SpannerTestSuite) TestRegisterAndRefreshRunner() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "RunnerTest")
	defer cleanup()

	runnerID := uuid.NewString()

	// Register runner
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	// Verify registration
	row, err := storage.client.Single().ReadRow(ctx, tableRunner, spanner.Key{runnerID}, []string{columnRunnerID, columnCreatedAt, columnUpdatedAt})
	s.NoError(err)
	var gotID string
	var createdAt, updatedAt time.Time
	s.NoError(row.Columns(&gotID, &createdAt, &updatedAt))
	s.Equal(runnerID, gotID)
	s.Equal(createdAt, updatedAt, "Initial CreatedAt and UpdatedAt should match")

	// Refresh runner
	time.Sleep(time.Second)
	s.NoError(storage.RefreshRunner(ctx, runnerID))

	// Verify refresh updated timestamp
	row, err = storage.client.Single().ReadRow(ctx, tableRunner, spanner.Key{runnerID}, []string{columnCreatedAt, columnUpdatedAt})
	s.NoError(err)
	var newCreatedAt, newUpdatedAt time.Time
	s.NoError(row.Columns(&newCreatedAt, &newUpdatedAt))
	s.Equal(createdAt, newCreatedAt, "CreatedAt should NOT update")
	s.True(newUpdatedAt.After(updatedAt), "UpdatedAt should be later after refresh")
}

func (s *SpannerTestSuite) TestInitializeRootPartition() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "RootPartition")
	defer cleanup()

	tests := []struct {
		name              string
		start             time.Time
		end               time.Time
		heartbeat         time.Duration
		expectedHeartbeat int64
	}{
		{
			name:              "epoch_to_max",
			start:             time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			end:               time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
			heartbeat:         10 * time.Second,
			expectedHeartbeat: 10000,
		},
		{
			name:              "recent_with_hour_heartbeat",
			start:             time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			end:               time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			heartbeat:         time.Hour,
			expectedHeartbeat: 3600000,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.NoError(storage.InitializeRootPartition(ctx, tt.start, tt.end, tt.heartbeat))

			// Verify partition
			row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{screamer.RootPartitionToken},
				[]string{columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark})
			s.NoError(err)

			var start, end, watermark time.Time
			var heartbeat int64
			var state screamer.State
			s.NoError(row.Columns(&start, &end, &heartbeat, &state, &watermark))

			s.Equal(tt.start, start)
			s.Equal(tt.end, end)
			s.Equal(tt.expectedHeartbeat, heartbeat)
			s.Equal(screamer.StateCreated, state)
			s.Equal(tt.start, watermark)

			storage.cleanup(ctx) // Clean for next test
		})
	}
}

func (s *SpannerTestSuite) TestGetUnfinishedMinWatermarkPartition() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "MinWatermark")
	defer cleanup()

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		partitions map[string]screamer.State
		watermarks map[string]time.Time
		expected   string
	}{
		{
			name:       "no_partitions",
			partitions: map[string]screamer.State{},
			expected:   "",
		},
		{
			name:       "only_finished",
			partitions: map[string]screamer.State{"p1": screamer.StateFinished},
			watermarks: map[string]time.Time{"p1": baseTime},
			expected:   "",
		},
		{
			name: "multiple_unfinished",
			partitions: map[string]screamer.State{
				"p1": screamer.StateCreated,
				"p2": screamer.StateRunning,
				"p3": screamer.StateFinished,
			},
			watermarks: map[string]time.Time{
				"p1": baseTime.Add(time.Hour),
				"p2": baseTime,
				"p3": baseTime.Add(-time.Hour),
			},
			expected: "p2",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			storage.cleanup(ctx)

			// Insert partitions with specific watermarks
			for token, state := range tt.partitions {
				watermark := baseTime
				if wm, ok := tt.watermarks[token]; ok {
					watermark = wm
				}
				s.insertPartitions(ctx, storage, map[string]screamer.State{token: state}, watermark)
			}

			p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
			s.NoError(err)

			if tt.expected == "" {
				s.Nil(p)
			} else {
				s.NotNil(p)
				s.Equal(tt.expected, p.PartitionToken)
			}
		})
	}
}

func (s *SpannerTestSuite) TestGetAndSchedulePartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "Schedule")
	defer cleanup()

	runnerID := uuid.NewString()
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	s.Run("schedule_available_partitions", func() {
		storage.cleanup(ctx)
		s.insertPartitions(ctx, storage, map[string]screamer.State{
			"p1": screamer.StateCreated,
			"p2": screamer.StateCreated,
			"p3": screamer.StateRunning,
		}, baseTime)

		partitions, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
		s.NoError(err)
		s.Len(partitions, 2, "Should schedule 2 CREATED partitions")

		// Verify state changes
		for _, p := range partitions {
			s.Equal(screamer.StateScheduled, s.getPartitionState(ctx, storage, p.PartitionToken))
		}
	})

	s.Run("respect_limit", func() {
		storage.cleanup(ctx)
		s.NoError(storage.RegisterRunner(ctx, runnerID))

		for i := 0; i < 10; i++ {
			s.insertPartitions(ctx, storage, map[string]screamer.State{fmt.Sprintf("p%d", i): screamer.StateCreated}, baseTime)
		}

		partitions, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 5)
		s.NoError(err)
		s.Len(partitions, 5, "Should respect limit of 5")
	})

	s.Run("concurrent_scheduling", func() {
		storage.cleanup(ctx)

		runner1 := uuid.NewString()
		runner2 := uuid.NewString()
		s.NoError(storage.RegisterRunner(ctx, runner1))
		s.NoError(storage.RegisterRunner(ctx, runner2))

		// Create 20 partitions
		for i := 0; i < 20; i++ {
			s.insertPartitions(ctx, storage, map[string]screamer.State{fmt.Sprintf("p%d", i): screamer.StateCreated}, baseTime)
		}

		// Schedule concurrently
		var wg sync.WaitGroup
		results := make(map[string][]*screamer.PartitionMetadata)
		mu := sync.Mutex{}

		for _, rid := range []string{runner1, runner2} {
			wg.Add(1)
			go func(runnerID string) {
				defer wg.Done()
				partitions, err := storage.GetAndSchedulePartitions(ctx, baseTime, runnerID, 100)
				s.NoError(err)
				mu.Lock()
				results[runnerID] = partitions
				mu.Unlock()
			}(rid)
		}
		wg.Wait()

		// Verify no duplicate assignments
		allScheduled := append(results[runner1], results[runner2]...)
		s.Len(allScheduled, 20, "All partitions should be scheduled")

		seen := make(map[string]bool)
		for _, p := range allScheduled {
			s.False(seen[p.PartitionToken], "Partition %s scheduled multiple times", p.PartitionToken)
			seen[p.PartitionToken] = true
		}
	})
}

func (s *SpannerTestSuite) TestGetInterruptedPartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "Interrupted")
	defer cleanup()

	runnerID := uuid.NewString()
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	staleTime := baseTime.Add(-10 * time.Second)
	liveTime := baseTime.Add(-1 * time.Second)

	// Setup stale and live runners
	staleRunner := uuid.NewString()
	liveRunner := uuid.NewString()
	s.setupRunner(ctx, storage, staleRunner, staleTime)
	s.setupRunner(ctx, storage, liveRunner, liveTime)

	// Create partitions
	s.insertPartitions(ctx, storage, map[string]screamer.State{
		"p_stale":   screamer.StateRunning,
		"p_orphan":  screamer.StateScheduled,
		"p_live":    screamer.StateRunning,
		"p_created": screamer.StateCreated,
	}, baseTime)

	s.assignPartition(ctx, storage, "p_stale", staleRunner, staleTime)
	s.assignPartition(ctx, storage, "p_live", liveRunner, liveTime)

	// Get interrupted partitions
	interrupted, err := storage.GetInterruptedPartitions(ctx, runnerID, 100)
	s.NoError(err)
	s.Len(interrupted, 2, "Should find stale and orphaned partitions")

	tokens := make(map[string]bool)
	for _, p := range interrupted {
		tokens[p.PartitionToken] = true
	}
	s.True(tokens["p_stale"])
	s.True(tokens["p_orphan"])
}

func (s *SpannerTestSuite) TestGetInterruptedPartitionsLimit() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "InterruptedLimit")
	defer cleanup()

	runnerID := uuid.NewString()
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	staleTime := baseTime.Add(-10 * time.Second)
	staleRunner := uuid.NewString()
	s.setupRunner(ctx, storage, staleRunner, staleTime)

	// Create 150 partitions
	for i := 0; i < 150; i++ {
		token := fmt.Sprintf("p%d", i)
		s.insertPartitions(ctx, storage, map[string]screamer.State{token: screamer.StateRunning}, baseTime)
	}

	// First batch
	batch1, err := storage.GetInterruptedPartitions(ctx, runnerID, 100)
	s.NoError(err)
	s.Len(batch1, 100)

	// Second batch
	batch2, err := storage.GetInterruptedPartitions(ctx, runnerID, 100)
	s.NoError(err)
	s.Len(batch2, 50)

	// Third batch should be empty
	batch3, err := storage.GetInterruptedPartitions(ctx, runnerID, 100)
	s.NoError(err)
	s.Empty(batch3)
}

func (s *SpannerTestSuite) TestAddChildPartitions() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ChildPartitions")
	defer cleanup()

	childStart := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent",
		EndTimestamp:    endTime,
		HeartbeatMillis: 10000,
		State:           screamer.StateRunning,
	}

	record := &screamer.ChildPartitionsRecord{
		StartTimestamp: childStart,
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child1", ParentPartitionTokens: []string{"parent"}},
			{Token: "child2", ParentPartitionTokens: []string{"parent"}},
		},
	}

	s.NoError(storage.AddChildPartitions(ctx, parent, record))

	// Verify children created
	iter := storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(),
		[]string{columnPartitionToken, columnState, columnWatermark})
	defer iter.Stop()

	var children []string
	err := iter.Do(func(row *spanner.Row) error {
		var token string
		var state screamer.State
		var watermark time.Time
		if err := row.Columns(&token, &state, &watermark); err != nil {
			return err
		}
		children = append(children, token)
		s.Equal(screamer.StateCreated, state)
		s.Equal(childStart, watermark)
		return nil
	})
	s.NoError(err)
	s.ElementsMatch([]string{"child1", "child2"}, children)
}

func (s *SpannerTestSuite) TestUpdatePartitionStates() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "UpdateStates")
	defer cleanup()

	runnerID := uuid.NewString()
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	s.insertPartitions(ctx, storage, map[string]screamer.State{"p1": screamer.StateCreated}, baseTime)

	partition := &screamer.PartitionMetadata{PartitionToken: "p1"}

	// Update to running
	s.NoError(storage.UpdateToRunning(ctx, partition))
	s.Equal(screamer.StateRunning, s.getPartitionState(ctx, storage, "p1"))

	// Update to finished
	s.NoError(storage.UpdateToFinished(ctx, partition, runnerID))
	s.Equal(screamer.StateFinished, s.getPartitionState(ctx, storage, "p1"))

	// Update watermark
	newWatermark := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	s.NoError(storage.UpdateWatermark(ctx, partition, newWatermark))

	row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"p1"}, []string{columnWatermark})
	s.NoError(err)
	var watermark time.Time
	s.NoError(row.Columns(&watermark))
	s.Equal(newWatermark, watermark)
}

func (s *SpannerTestSuite) TestGetActiveRunnerCount() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ActiveRunners")
	defer cleanup()

	now := time.Now().UTC()
	staleTime := now.Add(-10 * time.Second)
	activeTime := now.Add(-2 * time.Second)

	// Create runners with different update times
	s.setupRunner(ctx, storage, "stale1", staleTime)
	s.setupRunner(ctx, storage, "stale2", staleTime)
	s.setupRunner(ctx, storage, "active1", activeTime)
	s.setupRunner(ctx, storage, "active2", activeTime)

	count, err := storage.GetActiveRunnerCount(ctx)
	s.NoError(err)
	s.Equal(2, count, "Should count only active runners")
}

func (s *SpannerTestSuite) TestAddChildPartitions_Split() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ChildSplit")
	defer cleanup()

	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	splitTime := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)

	// Create parent partition
	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent1",
		StartTimestamp:  startTime,
		EndTimestamp:    endTime,
		HeartbeatMillis: 3000,
		State:           screamer.StateRunning,
		Watermark:       splitTime,
	}

	// Simulate partition split: 1 parent -> 2 children
	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: splitTime,
		RecordSequence: "seq-001",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child1", ParentPartitionTokens: []string{"parent1"}},
			{Token: "child2", ParentPartitionTokens: []string{"parent1"}},
		},
	}

	s.NoError(storage.AddChildPartitions(ctx, parent, childRecord))

	// Verify both children were created
	for _, child := range childRecord.ChildPartitions {
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName,
			spanner.Key{child.Token}, []string{
				columnPartitionToken,
				columnParentTokens,
				columnStartTimestamp,
				columnEndTimestamp,
				columnHeartbeatMillis,
				columnState,
				columnWatermark,
			})
		s.NoError(err)

		var p screamer.PartitionMetadata
		s.NoError(row.ToStruct(&p))

		// Verify child inherits correct metadata from parent
		s.Equal(child.Token, p.PartitionToken)
		s.Equal(child.ParentPartitionTokens, p.ParentTokens)
		s.Equal(splitTime, p.StartTimestamp)
		s.Equal(endTime, p.EndTimestamp)
		s.Equal(int64(3000), p.HeartbeatMillis)
		s.Equal(screamer.StateCreated, p.State)
		s.Equal(splitTime, p.Watermark)
	}
}

func (s *SpannerTestSuite) TestAddChildPartitions_Merge() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ChildMerge")
	defer cleanup()

	endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	mergeTime := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)

	// Create parent partition (used for metadata)
	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent1",
		EndTimestamp:    endTime,
		HeartbeatMillis: 5000,
	}

	// Simulate partition merge: 2 parents -> 1 child
	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: mergeTime,
		RecordSequence: "seq-002",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "merged-child", ParentPartitionTokens: []string{"parent1", "parent2"}},
		},
	}

	s.NoError(storage.AddChildPartitions(ctx, parent, childRecord))

	// Verify merged child was created with both parent tokens
	row, err := storage.client.Single().ReadRow(ctx, storage.tableName,
		spanner.Key{"merged-child"}, []string{columnPartitionToken, columnParentTokens})
	s.NoError(err)

	var token string
	var parentTokens []string
	s.NoError(row.Columns(&token, &parentTokens))

	s.Equal("merged-child", token)
	s.Equal([]string{"parent1", "parent2"}, parentTokens)
}

func (s *SpannerTestSuite) TestAddChildPartitions_Idempotent() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ChildIdempotent")
	defer cleanup()

	parent := &screamer.PartitionMetadata{
		PartitionToken:  "parent",
		EndTimestamp:    time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
		HeartbeatMillis: 3000,
	}

	childRecord := &screamer.ChildPartitionsRecord{
		StartTimestamp: time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
		RecordSequence: "seq-003",
		ChildPartitions: []*screamer.ChildPartition{
			{Token: "child1", ParentPartitionTokens: []string{"parent"}},
		},
	}

	// Add child partition first time
	s.NoError(storage.AddChildPartitions(ctx, parent, childRecord))

	// Add same child partition again (can happen during merge scenarios)
	// Should not return error due to AlreadyExists handling
	s.NoError(storage.AddChildPartitions(ctx, parent, childRecord))

	// Verify only one partition exists
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE PartitionToken = @token", storage.tableName),
		Params: map[string]interface{}{
			"token": "child1",
		},
	}
	iter := storage.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	s.NoError(err)

	var count int64
	s.NoError(row.Columns(&count))
	s.Equal(int64(1), count, "Should only have one child partition despite duplicate adds")
}
