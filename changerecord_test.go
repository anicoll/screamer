package screamer

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
)

func TestDataChangeRecord_DecodeToNonSpannerType(t *testing.T) {
	commitTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	startTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	watermark := time.Date(2023, 1, 1, 11, 0, 0, 0, time.UTC)
	createdTime := time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC)
	scheduledTime := time.Date(2023, 1, 1, 9, 30, 0, 0, time.UTC)
	runningTime := time.Date(2023, 1, 1, 10, 30, 0, 0, time.UTC)

	partition := &PartitionMetadata{
		PartitionToken: "token-001",
		StartTimestamp: startTime,
		CreatedAt:      createdTime,
		ScheduledAt:    &scheduledTime,
		RunningAt:      &runningTime,
	}

	tests := []struct {
		name          string
		record        *dataChangeRecord
		expectedTable string
		expectedType  ModType
		expectedMods  int
		expectedCols  int
	}{
		{
			name: "insert_basic_types",
			record: &dataChangeRecord{
				CommitTimestamp:                      commitTime,
				RecordSequence:                       "seq-001",
				ServerTransactionID:                  "txn-001",
				IsLastRecordInTransactionInPartition: true,
				TableName:                            "users",
				ColumnTypes: []*columnType{
					{Name: "id", Type: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"code": "INT64"}}, IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "name", Type: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"code": "STRING"}}, IsPrimaryKey: false, OrdinalPosition: 2},
				},
				Mods: []*mod{
					{
						Keys:      spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": int64(1)}},
						NewValues: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": int64(1), "name": "John"}},
						OldValues: spanner.NullJSON{Valid: false},
					},
				},
				ModType:                         "INSERT",
				ValueCaptureType:                "OLD_AND_NEW_VALUES",
				NumberOfRecordsInTransaction:    1,
				NumberOfPartitionsInTransaction: 1,
				TransactionTag:                  "tag-001",
				IsSystemTransaction:             false,
			},
			expectedTable: "users",
			expectedType:  ModType_INSERT,
			expectedMods:  1,
			expectedCols:  2,
		},
		{
			name: "update_with_array",
			record: &dataChangeRecord{
				CommitTimestamp: commitTime,
				RecordSequence:  "seq-002",
				TableName:       "products",
				ColumnTypes: []*columnType{
					{
						Name: "tags",
						Type: spanner.NullJSON{Valid: true, Value: map[string]interface{}{
							"code":               "ARRAY",
							"array_element_type": map[string]interface{}{"code": "STRING"},
						}},
						IsPrimaryKey:    false,
						OrdinalPosition: 1,
					},
				},
				Mods: []*mod{
					{
						Keys:      spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": int64(1)}},
						NewValues: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"tags": []string{"tag1", "tag2"}}},
						OldValues: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"tags": []string{"tag1"}}},
					},
				},
				ModType: "UPDATE",
			},
			expectedTable: "products",
			expectedType:  ModType_UPDATE,
			expectedMods:  1,
			expectedCols:  1,
		},
		{
			name: "delete_with_nulls",
			record: &dataChangeRecord{
				CommitTimestamp: commitTime,
				RecordSequence:  "seq-003",
				TableName:       "deleted_items",
				ColumnTypes:     []*columnType{},
				Mods: []*mod{
					{
						Keys:      spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": int64(1)}},
						NewValues: spanner.NullJSON{Valid: false},
						OldValues: spanner.NullJSON{Valid: false},
					},
				},
				ModType: "DELETE",
			},
			expectedTable: "deleted_items",
			expectedType:  ModType_DELETE,
			expectedMods:  1,
			expectedCols:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.record.DecodeToNonSpannerType(partition, watermark)

			// Verify basic fields
			require.Equal(t, tt.expectedTable, result.DataChangeRecord.TableName)
			require.Equal(t, tt.expectedType, result.DataChangeRecord.ModType)
			require.Len(t, result.DataChangeRecord.Mods, tt.expectedMods)
			require.Len(t, result.DataChangeRecord.ColumnTypes, tt.expectedCols)

			// Verify partition metadata
			require.Equal(t, partition.PartitionToken, result.PartitionToken)
			require.Equal(t, partition.StartTimestamp, result.StartTimestamp)
			require.Equal(t, watermark, result.Watermark)

			// Verify array types have element type
			for _, col := range result.DataChangeRecord.ColumnTypes {
				if col.Type.Code == TypeCode_ARRAY {
					require.NotEmpty(t, col.Type.ArrayElementType)
				}
			}

			// Verify mod values match null validity
			for i, mod := range result.DataChangeRecord.Mods {
				if tt.record.Mods[i].NewValues.Valid {
					require.NotNil(t, mod.NewValues)
				} else {
					require.Nil(t, mod.NewValues)
				}
			}
		})
	}
}
