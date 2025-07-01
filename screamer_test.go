package screamer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/internal/helper"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/go-faker/faker/v4"
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
}

func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) SetupSuite() {
	s.ctx = context.Background()
	image := "gcr.io/cloud-spanner-emulator/emulator"
	ports := []string{"9010/tcp"}

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

func createTableAndChangeStream(ctx context.Context, databaseName string) (string, string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", "", err
	}
	defer databaseAdminClient.Close()

	tableName := faker.Word()
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

type consumerV2 struct {
	changes []*screamer.DataChangeRecordWithPartitionMeta
}

func (c *consumerV2) Consume(change []byte) error {
	dcr := &screamer.DataChangeRecordWithPartitionMeta{}
	if err := json.Unmarshal(change, dcr); err != nil {
		return err
	}
	c.changes = append(c.changes, dcr)
	return nil
}

type consumerV1 struct {
	changes []*screamer.DataChangeRecord
}

func (c *consumerV1) Consume(change []byte) error {
	dcr := &screamer.DataChangeRecord{}
	if err := json.Unmarshal(change, dcr); err != nil {
		return err
	}
	c.changes = append(c.changes, dcr)
	return nil
}

func (s *IntegrationTestSuite) TestSubscriber_inmemstorage() {
	ctx := context.Background()
	proxy := interceptor.NewQueueInterceptor(10)
	spannerClient, err := spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(proxy.UnaryInterceptor)))
	s.NoError(err)
	runnerID := uuid.NewString()
	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := createTableAndChangeStream(ctx, spannerClient.DatabaseName())
	s.NoError(err)

	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	tests := map[string]struct {
		statements []string
		expected   []*screamer.DataChangeRecord
	}{
		"change": {
			statements: []string{
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
			},
			expected: []*screamer.DataChangeRecord{
				{
					RecordSequence:                       "00000000",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "1",
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
				{
					RecordSequence:                       "00000001",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "2",
					// This is NOT how spanner behavioes but the emulator returns all values regardless.... just FYI
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
				{
					RecordSequence:                       "00000002",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "3",
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
			},
		},
	}
	for _, test := range tests {
		storage := partitionstorage.NewInmemory()
		subscriber := screamer.NewSubscriber(spannerClient, streamName, runnerID, storage)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		consumer := &consumerV1{}
		go func() {
			_ = subscriber.Subscribe(ctx, consumer)
		}()
		s.T().Log("Subscribe started.")

		s.T().Log("Executing DML statements...")
		time.Sleep(time.Second)

		for _, stmt := range test.statements {
			_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				if _, err := tx.Update(ctx, spanner.NewStatement(stmt)); err != nil {
					return err
				}
				return nil
			})
			s.NoError(err)
		}

		s.T().Log("Waiting subscription...")
		time.Sleep(time.Second * 5)
		cancel()

		opts := []cmp.Option{
			cmpopts.IgnoreFields(screamer.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID", "RecordSequence", "NumberOfRecordsInTransaction"),
			cmpopts.IgnoreFields(screamer.Mod{}, "OldValues", "NewValues"),
		}
		s.Assert().Len(consumer.changes, len(test.expected))
		for _, change := range consumer.changes {
			switch change.ModType {
			case screamer.ModType_INSERT:
				diff := cmp.Diff(test.expected[0], change, opts...)
				s.Empty(diff)
			case screamer.ModType_UPDATE:
				diff := cmp.Diff(test.expected[1], change, opts...)
				s.Empty(diff)
			case screamer.ModType_DELETE:
				diff := cmp.Diff(test.expected[2], change, opts...)
				s.Empty(diff)
			}
		}
	}
}

func (s *IntegrationTestSuite) TestSubscriber_spannerstorage() {
	ctx := context.Background()
	proxy := interceptor.NewQueueInterceptor(10)
	spannerClient, err := spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(proxy.UnaryInterceptor)))
	s.NoError(err)
	runnerID := uuid.NewString()
	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := createTableAndChangeStream(ctx, spannerClient.DatabaseName())
	s.NoError(err)
	metaTableName := faker.Word()

	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	tests := map[string]struct {
		statements []string
		expected   []*screamer.DataChangeRecord
	}{
		"change": {
			statements: []string{
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
			},
			expected: []*screamer.DataChangeRecord{
				{
					RecordSequence:                       "00000000",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "1",
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
				{
					RecordSequence:                       "00000001",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "2",
					// This is NOT how spanner behavioes but the emulator returns all values regardless.... just FYI
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
				{
					RecordSequence:                       "00000002",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ServerTransactionID:                  "3",
					ColumnTypes: []*screamer.ColumnType{
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
					},
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
			},
		},
	}
	for _, test := range tests {
		storage := partitionstorage.NewSpanner(spannerClient, metaTableName)
		s.NoError(storage.RunMigrations(ctx))
		s.NoError(storage.RegisterRunner(ctx, runnerID))
		subscriber := screamer.NewSubscriber(spannerClient, streamName, runnerID, storage)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		consumer := &consumerV1{}
		go func() {
			_ = subscriber.Subscribe(ctx, consumer)
		}()
		s.T().Log("Subscribe started.")

		s.T().Log("Executing DML statements...")
		time.Sleep(time.Second)

		for _, stmt := range test.statements {
			_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				if _, err := tx.Update(ctx, spanner.NewStatement(stmt)); err != nil {
					return err
				}
				return nil
			})
			s.NoError(err)
		}

		s.T().Log("Waiting subscription...")
		time.Sleep(time.Second * 5)
		cancel()

		opts := []cmp.Option{
			cmpopts.IgnoreFields(screamer.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID", "RecordSequence", "NumberOfRecordsInTransaction"),
			cmpopts.IgnoreFields(screamer.Mod{}, "OldValues", "NewValues"),
		}
		s.Assert().Len(consumer.changes, len(test.expected))
		for _, change := range consumer.changes {
			switch change.ModType {
			case screamer.ModType_INSERT:
				diff := cmp.Diff(test.expected[0], change, opts...)
				s.Empty(diff)
			case screamer.ModType_UPDATE:
				diff := cmp.Diff(test.expected[1], change, opts...)
				s.Empty(diff)
			case screamer.ModType_DELETE:
				diff := cmp.Diff(test.expected[2], change, opts...)
				s.Empty(diff)
			}
		}
	}
}

func (s *IntegrationTestSuite) TestSubscriber_spannerstorage_interrupted() {
	ctx := context.Background()
	proxy := interceptor.NewQueueInterceptor(10)
	spannerClient, err := spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(proxy.UnaryInterceptor)))
	s.NoError(err)
	runnerID := uuid.NewString()
	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := createTableAndChangeStream(ctx, spannerClient.DatabaseName())
	s.NoError(err)
	metaTableName := testTableName

	s.T().Logf("Created table: %q, change stream: %q", tableName, streamName)

	// Create the metadata table for SpannerStorage
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	s.NoError(err)
	defer adminClient.Close()

	// Create metadata table
	storage := partitionstorage.NewSpanner(spannerClient, metaTableName)
	s.NoError(storage.RunMigrations(ctx))
	s.NoError(storage.RegisterRunner(ctx, runnerID))

	// Test with SpannerStorage
	subscriber := screamer.NewSubscriber(
		spannerClient,
		streamName,
		runnerID,
		storage,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumerr := &consumerV1{}
	contextForCancellation, cancelFunc := context.WithCancel(context.Background())
	go func() {
		_ = subscriber.Subscribe(contextForCancellation, consumerr)
	}()
	s.T().Log("Subscribe started.")

	s.T().Log("Executing DML statements...")
	time.Sleep(time.Second)

	s.createTestData(ctx, spannerClient, tableName)

	s.T().Log("Waiting for subscription processing...")
	time.Sleep(time.Second * 5)

	// Verify metadata in partition storage table
	iter := spannerClient.Single().Read(ctx, metaTableName, spanner.AllKeys(), []string{"PartitionToken", "State"})
	var partitions []struct {
		PartitionToken string
		State          string
	}
	err = iter.Do(func(row *spanner.Row) error {
		var p struct {
			PartitionToken string
			State          string
		}
		if err := row.Columns(&p.PartitionToken, &p.State); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	})
	s.NoError(err)

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

	newRunnerID := uuid.NewString()
	err = storage.RegisterRunner(ctx, newRunnerID)
	s.NoError(err)

	newConsumer := &consumerV1{}
	newSubscriber := screamer.NewSubscriber(
		spannerClient,
		streamName,
		newRunnerID,
		storage,
		screamer.WithStartTimestamp(time.Now()),
		screamer.WithHeartbeatInterval(1*time.Second),
	)

	go func() {
		_ = newSubscriber.Subscribe(ctx, newConsumer)
	}()
	// Verify we received change records
	s.T().Logf("Received %d changes", len(consumerr.changes))
	s.NotEmpty(consumerr.changes, "Should have received at least one change record")

	// At least one record should be for our table
	var tableRecords int
	for _, r := range consumerr.changes {
		if r.TableName == tableName {
			tableRecords++
		}
	}
	s.Greater(tableRecords, 0, "Should have received records for our table")

	time.Sleep(time.Second * 10) // wait for old records to become stale.
	s.createTestData(ctx, spannerClient, tableName)
	time.Sleep(time.Second * 1)

	// Verify we received change records
	s.T().Logf("Received %d changes", len(newConsumer.changes))
	s.NotEmpty(newConsumer.changes, "Should have received at least one change record")

	// At least one record should be for our table
	var newTableRecords int
	for _, r := range newConsumer.changes {
		if r.TableName == tableName {
			newTableRecords++
		}
	}
	s.Greater(newTableRecords, 0, "Should have received records for our table")

	cancel()
}

func (s *IntegrationTestSuite) createTestData(ctx context.Context, spannerClient *spanner.Client, tableName string) {
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
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)

	// Update data
	stmt = fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 42`, tableName)
	_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)

	// Delete data
	stmt = fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 42`, tableName)
	_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{SQL: stmt})
		return err
	})
	s.NoError(err)
}
