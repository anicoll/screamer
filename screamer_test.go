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
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/go-faker/faker/v4"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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

type consumer struct {
	changes []*screamer.DataChangeRecord
}

func (c *consumer) Consume(change []byte) error {
	dcr := &screamer.DataChangeRecord{}
	if err := json.Unmarshal(change, dcr); err != nil {
		return err
	}
	c.changes = append(c.changes, dcr)
	return nil
}

func (s *IntegrationTestSuite) TestSubscriber() {
	s.T().Skip("Skip integration test until resolution for emulator issue")
	ctx := context.Background()

	spannerClient, err := spanner.NewClient(ctx, s.dsn)
	s.NoError(err)

	s.T().Log("Creating table and change stream...")
	tableName, streamName, err := createTableAndChangeStream(ctx, spannerClient.DatabaseName())
	s.NoError(err)
	timeToStart := time.Now()
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
					IsLastRecordInTransactionInPartition: false,
					TableName:                            tableName,
					ColumnTypes: []*screamer.ColumnType{
						{Name: "Int64", Type: screamer.Type{Code: screamer.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: screamer.Type{Code: screamer.TypeCode_BOOL}, OrdinalPosition: 1},
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
					IsLastRecordInTransactionInPartition: false,
					TableName:                            tableName,
					ColumnTypes: []*screamer.ColumnType{
						{Name: "Int64", Type: screamer.Type{Code: screamer.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: screamer.Type{Code: screamer.TypeCode_BOOL}, OrdinalPosition: 1},
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
					ColumnTypes: []*screamer.ColumnType{
						{Name: "Int64", Type: screamer.Type{Code: screamer.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: screamer.Type{Code: screamer.TypeCode_BOOL}, OrdinalPosition: 1},
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
		subscriber := screamer.NewSubscriber(spannerClient, streamName, storage, screamer.WithStartTimestamp(timeToStart))

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		consumer := &consumer{}
		go func() {
			_ = subscriber.Subscribe(ctx, consumer)
		}()
		s.T().Log("Subscribe started.")

		s.T().Log("Executing DML statements...")
		time.Sleep(time.Second)
		_, err = spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			for _, stmt := range test.statements {
				if _, err := tx.Update(ctx, spanner.NewStatement(stmt)); err != nil {
					return err
				}
			}
			return nil
		})
		s.NoError(err)

		s.T().Log("Waiting subscription...")
		time.Sleep(time.Second * 5)
		cancel()

		opt := cmpopts.IgnoreFields(screamer.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID")

		diff := cmp.Diff(test.expected, consumer.changes, opt)
		s.Empty(diff)

	}
}
