package screamer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/internal/helper"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	projectID  = "project-id"
	instanceID = "instance-id"
	databaseID = "stream-database"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	client    *spanner.Client
	dsn       string
	proxy     *interceptor.QueueInterceptor
	ddlMutex  sync.Mutex // Serialize DDL operations to avoid Spanner emulator conflicts
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.proxy = interceptor.NewQueueInterceptor(100)

	var err error
	s.container, err = helper.NewTestContainer(
		s.ctx,
		"gcr.io/cloud-spanner-emulator/emulator",
		nil,
		[]string{"9010/tcp"},
		wait.ForLog("gRPC server listening at"),
	)
	s.NoError(err)

	mappedPort, err := s.container.MappedPort(s.ctx, "9010")
	s.NoError(err)
	hostIP, err := s.container.Host(s.ctx)
	s.NoError(err)

	os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%s", hostIP, mappedPort.Port()))

	instanceName, err := helper.CreateInstance(s.ctx, projectID, instanceID)
	s.NoError(err)
	s.dsn, err = helper.CreateDatabase(s.ctx, instanceName, databaseID)
	s.NoError(err)
}

func (s *IntegrationTestSuite) TearDownSuite() {
	if s.container != nil {
		s.container.Terminate(s.ctx)
	}
}

func (s *IntegrationTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *IntegrationTestSuite) BeforeTest(suiteName, testName string) {
	client, err := spanner.NewClient(s.ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(s.proxy.UnaryInterceptor)))
	s.NoError(err)
	s.client = client
}

// --- Test Helpers ---

type testConsumer struct {
	mu      sync.Mutex
	changes []json.RawMessage
}

func (c *testConsumer) Consume(change []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.changes = append(c.changes, change)
	return nil
}

func (c *testConsumer) getChanges() []json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]json.RawMessage, len(c.changes))
	copy(result, c.changes)
	return result
}

func (s *IntegrationTestSuite) createTableAndStream(ctx context.Context, name string) (string, string) {
	// Serialize DDL operations to avoid conflicts with Spanner emulator
	s.ddlMutex.Lock()
	defer s.ddlMutex.Unlock()

	adminClient, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(s.proxy.UnaryInterceptor)))
	s.NoError(err)
	defer adminClient.Close()

	streamName := name + "Stream"
	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: s.client.DatabaseName(),
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				Bool       BOOL,
				Int64      INT64,
				Float64    FLOAT64,
				String     STRING(MAX),
				Timestamp  TIMESTAMP,
			) PRIMARY KEY (Int64)`, name),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, name),
		},
	})
	s.NoError(err)
	s.NoError(op.Wait(ctx))
	return name, streamName
}

func (s *IntegrationTestSuite) setupSpannerStorage(ctx context.Context, runnerID, tableName string) *partitionstorage.SpannerPartitionStorage {
	storage := partitionstorage.NewSpanner(s.client, tableName)

	// Serialize DDL operations to avoid conflicts with Spanner emulator
	s.ddlMutex.Lock()
	err := storage.RunMigrations(ctx)
	s.ddlMutex.Unlock()
	s.NoError(err)

	s.NoError(storage.RegisterRunner(ctx, runnerID))

	// Keep runner alive
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				storage.RefreshRunner(ctx, runnerID)
			}
		}
	}()

	return storage
}

func (s *IntegrationTestSuite) executeDML(ctx context.Context, statements ...string) {
	for _, stmt := range statements {
		_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.Update(ctx, spanner.NewStatement(stmt))
			return err
		})
		s.NoError(err)
	}
}

// --- Tests ---

func (s *IntegrationTestSuite) TestInMemoryStorage() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tableName, streamName := s.createTableAndStream(ctx, "inmem")
	storage := partitionstorage.NewInmemory()
	consumer := &testConsumer{}

	subscriber := screamer.NewSubscriber(
		s.client,
		streamName,
		uuid.NewString(),
		storage,
		screamer.WithSerializedConsumer(true),
	)

	go subscriber.Subscribe(ctx, consumer)
	time.Sleep(time.Second)

	// Execute test data
	s.executeDML(ctx,
		fmt.Sprintf(`INSERT INTO %s (Bool, Int64, Float64, String, Timestamp) VALUES (TRUE, 1, 0.5, 'test', CURRENT_TIMESTAMP())`, tableName),
		fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 1`, tableName),
		fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 1`, tableName),
	)

	time.Sleep(5 * time.Second)
	cancel()

	changes := consumer.getChanges()
	s.GreaterOrEqual(len(changes), 3, "Should receive at least 3 change records")
}

func (s *IntegrationTestSuite) TestSpannerStorage() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runnerID := uuid.NewString()
	tableName, streamName := s.createTableAndStream(ctx, "spanner")
	storage := s.setupSpannerStorage(ctx, runnerID, tableName+"_meta")
	consumer := &testConsumer{}

	subscriber := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID,
		storage,
		screamer.WithSerializedConsumer(true),
	)

	go subscriber.Subscribe(ctx, consumer)
	time.Sleep(time.Second)

	s.executeDML(ctx,
		fmt.Sprintf(`INSERT INTO %s (Bool, Int64, Float64, String, Timestamp) VALUES (TRUE, 1, 0.5, 'test', CURRENT_TIMESTAMP())`, tableName),
		fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 1`, tableName),
		fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 1`, tableName),
	)

	time.Sleep(5 * time.Second)
	cancel()

	changes := consumer.getChanges()
	s.GreaterOrEqual(len(changes), 3, "Should receive at least 3 change records")
}

func (s *IntegrationTestSuite) TestInterruptedPartitions() {
	ctx := context.Background()
	runnerID := uuid.NewString()
	tableName, streamName := s.createTableAndStream(ctx, "interrupted")
	metaTable := tableName + "_meta"

	// First runner starts processing
	firstCtx, firstCancel := context.WithCancel(ctx)
	storage1 := s.setupSpannerStorage(firstCtx, runnerID, metaTable)
	consumer1 := &testConsumer{}

	subscriber1 := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID,
		storage1,
		screamer.WithHeartbeatInterval(time.Second),
		screamer.WithSerializedConsumer(true),
	)

	go subscriber1.Subscribe(firstCtx, consumer1)
	time.Sleep(time.Second)

	s.executeDML(ctx, fmt.Sprintf(`INSERT INTO %s (Bool, Int64, Float64, String, Timestamp) VALUES (TRUE, 1, 0.5, 'test', CURRENT_TIMESTAMP())`, tableName))
	time.Sleep(3 * time.Second)

	// Verify first runner received changes
	s.NotEmpty(consumer1.getChanges(), "First runner should receive changes")

	// Cancel first runner (simulate failure)
	firstCancel()
	time.Sleep(time.Second)

	// Second runner takes over
	newRunnerID := uuid.NewString()
	storage2 := s.setupSpannerStorage(ctx, newRunnerID, metaTable)
	consumer2 := &testConsumer{}

	subscriber2 := screamer.NewSubscriber(
		s.client,
		streamName,
		newRunnerID,
		storage2,
		screamer.WithHeartbeatInterval(time.Second),
		screamer.WithSerializedConsumer(true),
	)

	go subscriber2.Subscribe(ctx, consumer2)
	time.Sleep(10 * time.Second) // Wait for interrupted partitions to become stale

	s.executeDML(ctx, fmt.Sprintf(`INSERT INTO %s (Bool, Int64, Float64, String, Timestamp) VALUES (TRUE, 2, 1.5, 'test2', CURRENT_TIMESTAMP())`, tableName))
	time.Sleep(time.Second)

	// Verify second runner receives new changes
	s.NotEmpty(consumer2.getChanges(), "Second runner should receive changes")
}

func (s *IntegrationTestSuite) TestMultipleRunners() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tableName, streamName := s.createTableAndStream(ctx, "multi")
	metaTable := tableName + "_meta"

	// Create 3 runners
	runners := []struct {
		id         string
		storage    *partitionstorage.SpannerPartitionStorage
		subscriber *screamer.Subscriber
		consumer   *testConsumer
	}{}

	for i := 0; i < 3; i++ {
		runnerID := uuid.NewString()
		storage := s.setupSpannerStorage(ctx, runnerID, metaTable)
		consumer := &testConsumer{}
		subscriber := screamer.NewSubscriber(
			s.client,
			streamName,
			runnerID,
			storage,
			screamer.WithStartTimestamp(time.Now()),
			screamer.WithHeartbeatInterval(time.Second),
			screamer.WithSerializedConsumer(true),
		)
		runners = append(runners, struct {
			id         string
			storage    *partitionstorage.SpannerPartitionStorage
			subscriber *screamer.Subscriber
			consumer   *testConsumer
		}{runnerID, storage, subscriber, consumer})

		go subscriber.Subscribe(ctx, consumer)
	}

	time.Sleep(time.Second)

	// Generate test data
	for i := 0; i < 5; i++ {
		s.executeDML(ctx, fmt.Sprintf(`INSERT INTO %s (Bool, Int64, Float64, String, Timestamp) VALUES (TRUE, %d, 0.5, 'test', CURRENT_TIMESTAMP())`, tableName, i))
	}

	time.Sleep(5 * time.Second)
	cancel()

	// Verify at least one runner received changes
	totalChanges := 0
	for _, runner := range runners {
		totalChanges += len(runner.consumer.getChanges())
	}
	s.Greater(totalChanges, 0, "At least one runner should receive changes")
}
