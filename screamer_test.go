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
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	projectID  = "test-project"
	instanceID = "test-instance"
	databaseID = "test-database"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx    context.Context
	client *spanner.Client
	dsn    string
	proxy  *interceptor.QueueInterceptor
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.proxy = interceptor.NewQueueInterceptor(100)
	os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
	host := os.Getenv("SPANNER_EMULATOR_HOST")
	if host == "" {
		s.T().Skip("SPANNER_EMULATOR_HOST is not set, skipping integration tests")
		return
	}

	s.setupInfrastructure()
}

func (s *IntegrationTestSuite) TearDownSuite() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *IntegrationTestSuite) setupInfrastructure() {
	// Create Instance
	instanceAdmin, err := instance.NewInstanceAdminClient(s.ctx)
	s.Require().NoError(err)
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(s.ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err == nil {
		_, err = op.Wait(s.ctx)
		s.Require().NoError(err)
	}

	// Create Database
	dbAdmin, err := database.NewDatabaseAdminClient(s.ctx)
	s.Require().NoError(err)
	defer dbAdmin.Close()

	dbOp, err := dbAdmin.CreateDatabase(s.ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err == nil {
		_, err = dbOp.Wait(s.ctx)
		s.Require().NoError(err)
	}

	s.dsn = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	s.client, err = spanner.NewClient(s.ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(s.proxy.UnaryInterceptor)))
	s.Require().NoError(err)
}

func (s *IntegrationTestSuite) createTableAndStream(tableName string) (string, error) {
	dbAdmin, err := database.NewDatabaseAdminClient(s.ctx)
	if err != nil {
		return "", err
	}
	defer dbAdmin.Close()

	streamName := tableName + "Stream"

	// Check if table exists (simple check or just try create and ignore already exists error if we were reusing, but we are creating per test potentially)
	// For simplicity in this suite, we assume unique table names per test case

	op, err := dbAdmin.UpdateDatabaseDdl(s.ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: s.dsn,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				ID INT64,
				Value STRING(MAX),
				Bool BOOL,
			) PRIMARY KEY (ID)`, tableName),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		},
	})
	if err != nil {
		return "", err
	}
	if err := op.Wait(s.ctx); err != nil {
		return "", err
	}
	return streamName, nil
}

// -- Reusable Components --

func (s *IntegrationTestSuite) newSubscriber(streamName string, storage screamer.PartitionStorage, opts ...screamer.Option) *screamer.Subscriber {
	// Defaults
	defaultOpts := []screamer.Option{
		screamer.WithSerializedConsumer(true),
		screamer.WithLogLevel("debug"),
	}
	return screamer.NewSubscriber(s.client, streamName, uuid.NewString(), storage, append(defaultOpts, opts...)...)
}

// setupSpannerStorage prepares a Spanner-backed PartitionStorage, runs migrations,
// registers a runner and starts a background refresher. It returns the storage,
// the runnerID, and a stop function to cancel the refresher.
func (s *IntegrationTestSuite) setupSpannerStorage(metaTable string) (screamer.PartitionStorage, string, func()) {
	storage := partitionstorage.NewSpanner(s.client, metaTable)
	s.Require().NoError(storage.RunMigrations(s.ctx))

	runnerID := uuid.NewString()
	s.Require().NoError(storage.RegisterRunner(s.ctx, runnerID))

	// start refresher
	refreshCtx, refreshCancel := context.WithCancel(s.ctx)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				_ = storage.RefreshRunner(refreshCtx, runnerID)
			}
		}
	}()

	stop := func() { refreshCancel() }
	return storage, runnerID, stop
}

// startSubscriber starts the given subscriber with a test context and returns a cancel func.
// It sleeps briefly to allow the subscription to start.
func (s *IntegrationTestSuite) startSubscriber(sub *screamer.Subscriber, consumer *recordingConsumer) context.CancelFunc {
	ctx, cancel := context.WithCancel(s.ctx)
	go func() {
		_ = sub.Subscribe(ctx, consumer)
	}()
	time.Sleep(1 * time.Second)
	return cancel
}

type recordingConsumer struct {
	mu      sync.Mutex
	records []*screamer.DataChangeRecord
}

func (c *recordingConsumer) Consume(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var dcr screamer.DataChangeRecord
	if err := json.Unmarshal(data, &dcr); err != nil {
		return err
	}
	c.records = append(c.records, &dcr)
	return nil
}

func (c *recordingConsumer) getRecords() []*screamer.DataChangeRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	copied := make([]*screamer.DataChangeRecord, len(c.records))
	copy(copied, c.records)
	return copied
}

func (s *IntegrationTestSuite) insertData(tableName string, id int, value string) {
	_, err := s.client.Apply(s.ctx, []*spanner.Mutation{
		spanner.Insert(tableName, []string{"ID", "Value", "Bool"}, []interface{}{id, value, true}),
	})
	s.Require().NoError(err)
}

func (s *IntegrationTestSuite) updateData(tableName string, id int, value string) {
	_, err := s.client.Apply(s.ctx, []*spanner.Mutation{
		spanner.Update(tableName, []string{"ID", "Value"}, []interface{}{id, value}),
	})
	s.Require().NoError(err)
}

// -- Tests --

func (s *IntegrationTestSuite) TestBasicFlow_Inmemory() {
	tableName := "test_basic_inmem_" + uuid.NewString()[:8]
	streamName, err := s.createTableAndStream(tableName)
	s.Require().NoError(err)

	storage := partitionstorage.NewInmemory()
	consumer := &recordingConsumer{}
	sub := s.newSubscriber(streamName, storage)

	cancel := s.startSubscriber(sub, consumer)
	defer cancel()

	s.insertData(tableName, 1, "hello")
	s.updateData(tableName, 1, "world")

	s.Eventually(func() bool {
		return len(consumer.getRecords()) >= 2
	}, 10*time.Second, 500*time.Millisecond)

	records := consumer.getRecords()
	s.Require().Len(records, 2)
	s.Equal("hello", records[0].Mods[0].NewValues["Value"])
	s.Equal("world", records[1].Mods[0].NewValues["Value"])
}

func (s *IntegrationTestSuite) TestBasicFlow_SpannerStorage() {
	tableName := "test_basic_spanner_" + uuid.NewString()[:8]
	streamName, err := s.createTableAndStream(tableName)
	s.Require().NoError(err)
	metaTable := tableName + "_meta"

	storage, runnerID, stop := s.setupSpannerStorage(metaTable)
	defer stop()

	consumer := &recordingConsumer{}
	sub := screamer.NewSubscriber(s.client, streamName, runnerID, storage, screamer.WithSerializedConsumer(true))
	cancel := s.startSubscriber(sub, consumer)
	defer cancel()

	time.Sleep(1 * time.Second)

	s.insertData(tableName, 100, "spanner-data")

	s.Eventually(func() bool {
		return len(consumer.getRecords()) >= 1
	}, 10*time.Second, 500*time.Millisecond)

	records := consumer.getRecords()
	s.Require().NotEmpty(records)
	s.Equal("spanner-data", records[0].Mods[0].NewValues["Value"])
}

func (s *IntegrationTestSuite) TestResumption() {
	// This tests that if we stop and start, we verify we don't miss data or process duplicates (if handled)
	// For now, let's just ensure clean shutdown and restart works
	tableName := "test_resumption_" + uuid.NewString()[:8]
	streamName, err := s.createTableAndStream(tableName)
	s.Require().NoError(err)

	storage, runnerID, stop := s.setupSpannerStorage(tableName + "_meta")
	defer stop()

	consumer := &recordingConsumer{}

	sub1 := screamer.NewSubscriber(s.client, streamName, runnerID, storage, screamer.WithSerializedConsumer(true))
	cancel1 := s.startSubscriber(sub1, consumer)

	s.insertData(tableName, 1, "run1")
	time.Sleep(1 * time.Second)

	cancel1()
	// Wait for shutdown
	time.Sleep(1 * time.Second)

	s.insertData(tableName, 2, "run2") // This happens while "offline"

	// New subscriber using same runnerID (resumes using same registered runner)
	sub2 := screamer.NewSubscriber(s.client, streamName, runnerID, storage, screamer.WithSerializedConsumer(true))
	cancel2 := s.startSubscriber(sub2, consumer)
	defer cancel2()

	s.Eventually(func() bool {
		return len(consumer.getRecords()) >= 2
	}, 10*time.Second, 500*time.Millisecond)

	records := consumer.getRecords()
	var values []string
	for _, r := range records {
		if v, ok := r.Mods[0].NewValues["Value"].(string); ok {
			values = append(values, v)
		}
	}
	s.Contains(values, "run1")
	s.Contains(values, "run2")
}
