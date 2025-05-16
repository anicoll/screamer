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
	_, err := s.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(s.tableName, spanner.AllKeys()),
	})
	assert.NoError(s.t, err)
}

func (s *SpannerTestSuite) setupSpannerPartitionStorage(ctx context.Context, tableName string) *testStorage {
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

	return &testStorage{
		t:                       s.T(),
		SpannerPartitionStorage: storage,
	}
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_InitializeRootPartition() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "InitializeRootPartition")
	defer storage.CleanupData(ctx)

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

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "Read")
	defer storage.CleanupData(ctx)

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
		s.T().Skip("Skipping test because Spanner emulator cannot handle FOR UPDATE query.")
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

func (s *SpannerTestSuite) TestSpannerPartitionStorage_Read_race() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "Read")
	defer storage.CleanupData(ctx)

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
	s.T().Skip("This test is skipped because Spanner emulator cannot handle FOR UPDATE query.")
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "Read")
	defer storage.CleanupData(ctx)

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

	s.Run("GetAndSchedulePartitions", func() {
		partitions, err := storage.GetAndSchedulePartitions(ctx, timestamp, runnerID)
		s.NoError(err)

		s.Len(partitions, 1)
		p := partitions[0]
		s.Equal(p.PartitionToken, "created1")
	})
}

func (s *SpannerTestSuite) TestSpannerPartitionStorage_AddChildPartitions() {
	ctx := context.Background()
	storage := s.setupSpannerPartitionStorage(ctx, "AddChildPartitions")
	defer storage.CleanupData(ctx)

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
	storage := s.setupSpannerPartitionStorage(ctx, "Update")
	defer storage.CleanupData(ctx)

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

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string         `spanner:"PartitionToken"`
			State          screamer.State `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", State: screamer.StateRunning}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateToRunning(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	s.Run("UpdateToFinished", func() {
		err := storage.UpdateToFinished(ctx, partitions[0])
		s.NoError(err)

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string         `spanner:"PartitionToken"`
			State          screamer.State `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", State: screamer.StateFinished}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateToFinished(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	s.Run("UpdateWatermark", func() {
		timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

		err := storage.UpdateWatermark(ctx, partitions[0], timestamp)
		s.NoError(err)

		columns := []string{columnPartitionToken, columnWatermark}

		type partition struct {
			PartitionToken string    `spanner:"PartitionToken"`
			Watermark      time.Time `spanner:"Watermark"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		s.NoError(err)

		got := partition{}
		err = r.ToStruct(&got)
		s.NoError(err)

		want := partition{PartitionToken: "token1", Watermark: timestamp}
		if !reflect.DeepEqual(got, want) {
			s.T().Errorf("UpdateWatermark(ctx, %+v, %q): got = %+v, want %+v", partitions[0], timestamp, got, want)
		}
	})
}
