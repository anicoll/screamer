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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testTableName = "PartitionMetadata"
	projectID     = "project-id"
	instanceID    = "instance-id"
	databaseID    = "stream-database"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	client    *spanner.Client
	dsn       string
	proxy     *interceptor.QueueInterceptor
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	s.ctx = context.Background()
	image := "gcr.io/cloud-spanner-emulator/emulator"
	ports := []string{"9010/tcp"}
	s.proxy = interceptor.NewQueueInterceptor(100)

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
	intanceName, err := helper.CreateInstance(s.ctx, projectID, instanceID) // create instance
	s.NoError(err)

	s.dsn, err = helper.CreateDatabase(s.ctx, intanceName, databaseID) // create database
	s.NoError(err)
}

func (s *IntegrationTestSuite) TearDownSuite() {
	if s.container != nil {
		err := s.container.Terminate(s.ctx)
		s.NoError(err)
	}
}

func (s *IntegrationTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *IntegrationTestSuite) BeforeTest(suiteName, testName string) {
	s.createSpannerClient(s.T().Context())
}

// testConsumer is a generic consumer for both V1 and V2 data change records
type testConsumer struct {
	mu           sync.Mutex
	changesV1    []*screamer.DataChangeRecord
	changesV2    []*screamer.DataChangeRecordWithPartitionMeta
	isV2Consumer bool
}

func newTestConsumer(v2 bool) *testConsumer {
	return &testConsumer{isV2Consumer: v2}
}

func (c *testConsumer) Consume(change []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isV2Consumer {
		dcr := &screamer.DataChangeRecordWithPartitionMeta{}
		if err := json.Unmarshal(change, dcr); err != nil {
			return err
		}
		c.changesV2 = append(c.changesV2, dcr)
	} else {
		dcr := &screamer.DataChangeRecord{}
		if err := json.Unmarshal(change, dcr); err != nil {
			return err
		}
		c.changesV1 = append(c.changesV1, dcr)
	}
	return nil
}

func (c *testConsumer) getChangesV1() []*screamer.DataChangeRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*screamer.DataChangeRecord, len(c.changesV1))
	copy(result, c.changesV1)
	c.changesV1 = nil
	return result
}

func (c *testConsumer) getChangesV2() []*screamer.DataChangeRecordWithPartitionMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*screamer.DataChangeRecordWithPartitionMeta, len(c.changesV2))
	copy(result, c.changesV2)
	return result
}

func (s *IntegrationTestSuite) createTableAndChangeStream(ctx context.Context, databaseName, tableName string) (string, string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(s.proxy.UnaryInterceptor)))
	if err != nil {
		return "", "", err
	}
	defer databaseAdminClient.Close()

	streamName := tableName + "Stream"

	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				Bool            BOOL,
				Int64           INT64,
				Float64         FLOAT64,
				Timestamp       TIMESTAMP,
				Date            DATE,
				String          STRING(MAX),
				Bytes           BYTES(MAX),
				Numeric         NUMERIC,
				Json            JSON,
				BoolArray       ARRAY<BOOL>,
				Int64Array      ARRAY<INT64>,
				Float64Array    ARRAY<FLOAT64>,
				TimestampArray  ARRAY<TIMESTAMP>,
				DateArray       ARRAY<DATE>,
				StringArray     ARRAY<STRING(MAX)>,
				BytesArray      ARRAY<BYTES(MAX)>,
				NumericArray    ARRAY<NUMERIC>,
				JsonArray       ARRAY<JSON>,
			) PRIMARY KEY (Int64)`, tableName),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		},
	})
	if err != nil {
		return "", "", err
	}

	if err := op.Wait(ctx); err != nil {
		return "", "", err
	}

	return tableName, streamName, err
}

// Helper methods for common test operations

func (s *IntegrationTestSuite) createSpannerClient(ctx context.Context) {
	spannerClient, err := spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(s.proxy.UnaryInterceptor)))
	s.NoError(err)
	s.client = spannerClient
}

func (s *IntegrationTestSuite) setupSpannerStorage(ctx context.Context, runnerID, metaTableName string) *partitionstorage.SpannerPartitionStorage {
	storage := partitionstorage.NewSpanner(s.client, metaTableName)
	s.Require().NoError(storage.RunMigrations(ctx))
	s.Require().NoError(storage.RegisterRunner(ctx, runnerID))
	go func(rid string) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(time.Second)
				_ = storage.RefreshRunner(ctx, rid)
			}
		}
	}(runnerID)
	return storage
}

type partition struct {
	PartitionToken string
	State          string
}

func (s *IntegrationTestSuite) getPartitions(ctx context.Context, table string) []partition {
	iter := s.client.Single().Read(ctx, table, spanner.AllKeys(), []string{"PartitionToken", "State"})
	var partitions []partition
	err := iter.Do(func(row *spanner.Row) error {
		var p partition
		if err := row.Columns(&p.PartitionToken, &p.State); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	})
	s.NoError(err)
	return partitions
}

func (s *IntegrationTestSuite) createSubscriber(streamName string, runnerID string, storage screamer.PartitionStorage, opts ...screamer.Option) *screamer.Subscriber {
	defaultOpts := []screamer.Option{
		screamer.WithSerializedConsumer(true),
	}
	allOpts := append(defaultOpts, opts...)
	return screamer.NewSubscriber(s.client, streamName, runnerID, storage, allOpts...)
}

func (s *IntegrationTestSuite) executeDMLStatements(ctx context.Context, client *spanner.Client, statements []string) {
	for _, stmt := range statements {
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.Update(ctx, spanner.NewStatement(stmt))
			return err
		})
		s.NoError(err)
	}
}

func (s *IntegrationTestSuite) getExpectedColumnTypes() []*screamer.ColumnType {
	return []*screamer.ColumnType{
		{Name: "Bool", Type: screamer.Type{Code: screamer.TypeCode_BOOL}, OrdinalPosition: 1},
		{Name: "Int64", Type: screamer.Type{Code: screamer.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
		{Name: "Float64", Type: screamer.Type{Code: screamer.TypeCode_FLOAT64}, OrdinalPosition: 3},
		{Name: "Timestamp", Type: screamer.Type{Code: screamer.TypeCode_TIMESTAMP}, OrdinalPosition: 4},
		{Name: "Date", Type: screamer.Type{Code: screamer.TypeCode_DATE}, OrdinalPosition: 5},
		{Name: "String", Type: screamer.Type{Code: screamer.TypeCode_STRING}, OrdinalPosition: 6},
		{Name: "Bytes", Type: screamer.Type{Code: screamer.TypeCode_BYTES}, OrdinalPosition: 7},
		{Name: "Numeric", Type: screamer.Type{Code: screamer.TypeCode_NUMERIC}, OrdinalPosition: 8},
		{Name: "Json", Type: screamer.Type{Code: screamer.TypeCode_JSON}, OrdinalPosition: 9},
		{Name: "BoolArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_BOOL}, OrdinalPosition: 10},
		{Name: "Int64Array", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_INT64}, OrdinalPosition: 11},
		{Name: "Float64Array", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_FLOAT64}, OrdinalPosition: 12},
		{Name: "TimestampArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_TIMESTAMP}, OrdinalPosition: 13},
		{Name: "DateArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_DATE}, OrdinalPosition: 14},
		{Name: "StringArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_STRING}, OrdinalPosition: 15},
		{Name: "BytesArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_BYTES}, OrdinalPosition: 16},
		{Name: "NumericArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_NUMERIC}, OrdinalPosition: 17},
		{Name: "JsonArray", Type: screamer.Type{Code: screamer.TypeCode_ARRAY, ArrayElementType: screamer.TypeCode_JSON}, OrdinalPosition: 18},
	}
}

func (s *IntegrationTestSuite) getTestDMLStatements(tableName string) []string {
	return []string{
		fmt.Sprintf(`
			INSERT INTO %s
				(Bool, Int64, Float64, Timestamp, Date, String, Bytes, Numeric, Json, BoolArray, Int64Array, Float64Array, TimestampArray, DateArray, StringArray, BytesArray, NumericArray, JsonArray)
			VALUES (
				TRUE,
				1,
				0.5,
				'2023-12-31T23:59:59.999999999Z',
				'2023-01-01',
				'string',
				B'bytes',
				NUMERIC '123.456',
				JSON '{"name":"foobar"}',
				[TRUE, FALSE],
				[1, 2],
				[0.5, 0.25],
				[TIMESTAMP '2023-12-31T23:59:59.999999999Z', TIMESTAMP '2023-01-01T00:00:00Z'],
				[DATE '2023-01-01', DATE '2023-02-01'],
				['string1', 'string2'],
				[B'bytes1', B'bytes2'],
				[NUMERIC '12.345', NUMERIC '67.89'],
				[JSON '{"name":"foobar"}', JSON '{"name":"barbaz"}']
			)
		`, tableName),
		fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 1`, tableName),
		fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 1`, tableName),
	}
}

func (s *IntegrationTestSuite) getExpectedDataChangeRecords(tableName string) []*screamer.DataChangeRecord {
	columnTypes := s.getExpectedColumnTypes()

	return []*screamer.DataChangeRecord{
		// INSERT record
		{
			RecordSequence:                       "00000000",
			IsLastRecordInTransactionInPartition: true,
			TableName:                            tableName,
			ServerTransactionID:                  "1",
			ColumnTypes:                          columnTypes,
			Mods: []*screamer.Mod{
				{
					Keys: map[string]interface{}{"Int64": "1"},
					NewValues: map[string]interface{}{
						"Bool":           true,
						"BoolArray":      []interface{}{true, false},
						"Bytes":          "Ynl0ZXM=",
						"BytesArray":     []interface{}{"Ynl0ZXMx", "Ynl0ZXMy"},
						"Date":           "2023-01-01",
						"DateArray":      []interface{}{"2023-01-01", "2023-02-01"},
						"Float64":        0.5,
						"Float64Array":   []interface{}{0.5, 0.25},
						"Int64Array":     []interface{}{"1", "2"},
						"Json":           "{\"name\":\"foobar\"}",
						"JsonArray":      []interface{}{"{\"name\":\"foobar\"}", "{\"name\":\"barbaz\"}"},
						"Numeric":        "123.456",
						"NumericArray":   []interface{}{"12.345", "67.89"},
						"String":         "string",
						"StringArray":    []interface{}{"string1", "string2"},
						"Timestamp":      "2023-12-31T23:59:59.999999999Z",
						"TimestampArray": []interface{}{"2023-12-31T23:59:59.999999999Z", "2023-01-01T00:00:00Z"},
					},
					OldValues: map[string]interface{}{},
				},
			},
			ModType:                         screamer.ModType_INSERT,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    3,
			NumberOfPartitionsInTransaction: 1,
			TransactionTag:                  "",
			IsSystemTransaction:             false,
		},
		// UPDATE record
		{
			RecordSequence:                       "00000001",
			IsLastRecordInTransactionInPartition: true,
			TableName:                            tableName,
			ServerTransactionID:                  "2",
			ColumnTypes:                          columnTypes,
			Mods: []*screamer.Mod{
				{
					Keys:      map[string]interface{}{"Int64": "1"},
					NewValues: map[string]interface{}{"Bool": false},
					OldValues: map[string]interface{}{"Bool": true},
				},
			},
			ModType:                         screamer.ModType_UPDATE,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    3,
			NumberOfPartitionsInTransaction: 1,
			TransactionTag:                  "",
			IsSystemTransaction:             false,
		},
		// DELETE record
		{
			RecordSequence:                       "00000002",
			IsLastRecordInTransactionInPartition: true,
			TableName:                            tableName,
			ServerTransactionID:                  "3",
			ColumnTypes:                          columnTypes,
			Mods: []*screamer.Mod{
				{
					Keys:      map[string]interface{}{"Int64": "1"},
					NewValues: map[string]interface{}{},
					OldValues: map[string]interface{}{
						"Bool":           false,
						"BoolArray":      []interface{}{true, false},
						"Bytes":          "Ynl0ZXM=",
						"BytesArray":     []interface{}{"Ynl0ZXMx", "Ynl0ZXMy"},
						"Date":           "2023-01-01",
						"DateArray":      []interface{}{"2023-01-01", "2023-02-01"},
						"Float64":        0.5,
						"Float64Array":   []interface{}{0.5, 0.25},
						"Int64Array":     []interface{}{"1", "2"},
						"Json":           "{\"name\":\"foobar\"}",
						"JsonArray":      []interface{}{"{\"name\":\"foobar\"}", "{\"name\":\"barbaz\"}"},
						"Numeric":        "123.456",
						"NumericArray":   []interface{}{"12.345", "67.89"},
						"String":         "string",
						"StringArray":    []interface{}{"string1", "string2"},
						"Timestamp":      "2023-12-31T23:59:59.999999999Z",
						"TimestampArray": []interface{}{"2023-12-31T23:59:59.999999999Z", "2023-01-01T00:00:00Z"},
					},
				},
			},
			ModType:                         screamer.ModType_DELETE,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    3,
			NumberOfPartitionsInTransaction: 1,
			TransactionTag:                  "",
			IsSystemTransaction:             false,
		},
	}
}

func (s *IntegrationTestSuite) validateChanges(changes []*screamer.DataChangeRecord, expected []*screamer.DataChangeRecord) {
	opts := []cmp.Option{
		cmpopts.IgnoreFields(screamer.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID", "RecordSequence", "NumberOfRecordsInTransaction"),
		cmpopts.IgnoreFields(screamer.Mod{}, "OldValues", "NewValues"),
	}

	s.Len(changes, len(expected))
	for _, change := range changes {
		switch change.ModType {
		case screamer.ModType_INSERT:
			diff := cmp.Diff(expected[0], change, opts...)
			s.Empty(diff)
		case screamer.ModType_UPDATE:
			diff := cmp.Diff(expected[1], change, opts...)
			s.Empty(diff)
		case screamer.ModType_DELETE:
			diff := cmp.Diff(expected[2], change, opts...)
			s.Empty(diff)
		}
	}
}

func (s *IntegrationTestSuite) TestSubscriber_inmemstorage() {
	ctx := context.Background()

	runnerID := uuid.NewString()

	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := s.createTableAndChangeStream(ctx, s.client.DatabaseName(), "inmemstorage")
	s.NoError(err)
	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	// Test data
	statements := s.getTestDMLStatements(tableName)
	expected := s.getExpectedDataChangeRecords(tableName)

	// Set up storage and subscriber
	storage := partitionstorage.NewInmemory()
	subscriber := s.createSubscriber(streamName, runnerID, storage)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumer := newTestConsumer(false)
	go func() {
		_ = subscriber.Subscribe(ctx, consumer)
	}()
	s.T().Log("Subscribe started.")

	s.T().Log("Executing DML statements...")
	time.Sleep(time.Second)

	s.executeDMLStatements(ctx, s.client, statements)

	s.T().Log("Waiting subscription...")
	time.Sleep(time.Second * 5)
	cancel()

	changes := consumer.getChangesV1()
	s.validateChanges(changes, expected)
}

func (s *IntegrationTestSuite) TestSubscriber_spannerstorage() {
	ctx := context.Background()
	runnerID := uuid.NewString()

	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := s.createTableAndChangeStream(ctx, s.client.DatabaseName(), "spannerstorage")
	metaTableName := "spannerstorage_metadata"
	s.NoError(err)
	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	// Test data
	statements := s.getTestDMLStatements(tableName)
	expected := s.getExpectedDataChangeRecords(tableName)

	// Set up storage and subscriber
	storage := s.setupSpannerStorage(ctx, runnerID, metaTableName)

	subscriber := s.createSubscriber(streamName, runnerID, storage)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumer := newTestConsumer(false)
	go func() {
		_ = subscriber.Subscribe(ctx, consumer)
	}()
	s.T().Log("Subscribe started.")

	s.T().Log("Executing DML statements...")
	time.Sleep(time.Second)

	s.executeDMLStatements(ctx, s.client, statements)

	s.T().Log("Waiting subscription...")
	time.Sleep(time.Second * 5)
	cancel()

	changes := consumer.getChangesV1()
	s.validateChanges(changes, expected)
}

func (s *IntegrationTestSuite) TestSubscriber_spannerstorage_interrupted() {
	ctx := context.Background()
	runnerID := uuid.NewString()
	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := s.createTableAndChangeStream(ctx, s.client.DatabaseName(), "interrupted")
	s.NoError(err)
	metaTableName := "interrupted_metadata"

	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	contextForCancellation, cancelFunc := context.WithCancel(context.Background())
	// Create metadata table
	storage := s.setupSpannerStorage(contextForCancellation, runnerID, metaTableName)

	// Test with SpannerStorage
	subscriber := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID,
		storage,
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)

	v1consumer := newTestConsumer(false)

	go func() {
		_ = subscriber.Subscribe(contextForCancellation, v1consumer)
	}()
	s.T().Log("Subscribe started.")

	s.T().Log("Executing DML statements...")
	time.Sleep(time.Second)

	s.createTestData(ctx, tableName)

	s.T().Log("Waiting for subscription processing...")
	time.Sleep(time.Second * 3)

	// Verify metadata in partition storage table
	partitions := s.getPartitions(ctx, metaTableName)
	// Should have at least one partition still running
	s.NotEmpty(partitions)
	hasRunningPartition := false
	for _, p := range partitions {
		if p.State == "RUNNING" {
			hasRunningPartition = true
			break
		}
	}
	s.True(hasRunningPartition, "Should have at least one running partition")
	cancelFunc() // cancel running subscription
	time.Sleep(time.Second)

	// Verify we received change records
	v1changes := v1consumer.getChangesV1()
	s.NotEmpty(v1changes, "Should have received at least one change record")
	// At least one record should be for our table
	var tableRecords int
	for _, r := range v1changes {
		if r.TableName == tableName {
			tableRecords++
		}
	}
	s.Greater(tableRecords, 0, "Should have received records for our table")

	newRunnerID := uuid.NewString()
	newStorage := s.setupSpannerStorage(ctx, newRunnerID, metaTableName)
	v2consumer := newTestConsumer(true)
	newSubscriber := screamer.NewSubscriber(
		s.client,
		streamName,
		newRunnerID,
		newStorage,
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)

	go func() {
		_ = newSubscriber.Subscribe(ctx, v2consumer)
	}()

	time.Sleep(time.Second * 10) // wait for old records to become stale.
	s.createTestData(ctx, tableName)
	time.Sleep(time.Second)

	// Verify we received change records
	changes := v2consumer.getChangesV2()
	s.T().Logf("Received %d changes", len(changes))
	s.NotEmpty(changes, "Should have received at least one change record")

	// At least one record should be for our table
	var newTableRecords int
	for _, r := range changes {
		if r.TableName == tableName {
			newTableRecords++
		}
	}
	s.Greater(newTableRecords, 0, "Should have received records for our table")
}

func (s *IntegrationTestSuite) TestSubscriber_spannerstorage_multiple_runners() {
	ctx := context.Background()

	runnerID1 := uuid.NewString()
	runnerID2 := uuid.NewString()
	runnerID3 := uuid.NewString()
	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := s.createTableAndChangeStream(ctx, s.client.DatabaseName(), "multiple_runners")
	s.NoError(err)
	metaTableName := "multiple_runners_metadata"

	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	cancelCtx, cancel := context.WithCancel(context.Background())

	// multiple storages will be added to subscribers to simulate multiple runners.
	storage1 := s.setupSpannerStorage(cancelCtx, runnerID1, metaTableName)
	storage2 := s.setupSpannerStorage(cancelCtx, runnerID2, metaTableName)
	storage3 := s.setupSpannerStorage(cancelCtx, runnerID3, metaTableName)

	// Test with SpannerStorage
	subscriber1 := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID1,
		storage1,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)
	subscriber2 := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID2,
		storage2,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)
	subscriber3 := screamer.NewSubscriber(
		s.client,
		streamName,
		runnerID3,
		storage3,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)

	consumerr := newTestConsumer(false)
	go func() {
		_ = subscriber1.Subscribe(cancelCtx, consumerr)
	}()

	go func() {
		_ = subscriber2.Subscribe(cancelCtx, consumerr)
	}()
	go func() {
		_ = subscriber3.Subscribe(cancelCtx, consumerr)
	}()
	s.T().Log("Subscribe started.")

	s.T().Log("Executing DML statements...")
	time.Sleep(time.Second)

	for range 5 {
		s.createTestData(ctx, tableName)
	}

	s.T().Log("Waiting for subscription processing...")
	time.Sleep(time.Second * 5)

	partitions := s.getPartitions(ctx, metaTableName)

	// Should have at least one partition still running
	s.NotEmpty(partitions)
	hasRunningPartition := false
	for _, p := range partitions {
		if p.State == "RUNNING" {
			hasRunningPartition = true
			break
		}
	}
	s.True(hasRunningPartition, "Should have at least one running partition")
	cancel() // cancel running subscriptions

	// At least one record should be for our table
	var tableRecords int
	for _, r := range consumerr.getChangesV1() {
		if r.TableName == tableName {
			tableRecords++
		}
	}
	s.Greater(tableRecords, 0, "Should have received records for our table")

	newRunnerID := uuid.NewString()
	storage4 := s.setupSpannerStorage(ctx, newRunnerID, metaTableName)

	newConsumer := newTestConsumer(true)
	newSubscriber := screamer.NewSubscriber(
		s.client,
		streamName,
		newRunnerID,
		storage4,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
		screamer.WithSerializedConsumer(true),
	)

	go func() {
		_ = newSubscriber.Subscribe(ctx, newConsumer)
	}()

	time.Sleep(time.Second * 11) // wait for old records to become stale.
	s.createTestData(ctx, tableName)
	time.Sleep(time.Second * 1)

	// Verify we received change records
	changes := newConsumer.getChangesV2()
	s.T().Logf("Received %d changes", len(changes))
	s.NotEmpty(changes, "Should have received at least one change record")

	// At least one record should be for our table
	var newTableRecords int
	for _, r := range changes {
		if r.TableName == tableName {
			newTableRecords++
		}
	}
	s.Greater(newTableRecords, 0, "Should have received records for our table")
	cancel()
}

func (s *IntegrationTestSuite) createTestData(ctx context.Context, tableName string) {
	// Insert data
	stmt := fmt.Sprintf(`
		INSERT INTO %s
			(Bool, Int64, Float64, Timestamp, Date, String, Bytes, Numeric, Json)
		VALUES (
			TRUE,
			42,
			3.14,
			'2023-12-31T23:59:59Z',
			'2023-01-01',
			'test string',
			B'test bytes',
			NUMERIC '123.45',
			JSON '{"test":"value"}'
		)
	`, tableName)
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)

	// Update data
	stmt = fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 42`, tableName)
	_, err = s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)

	// Delete data
	stmt = fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 42`, tableName)
	_, err = s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)
}
