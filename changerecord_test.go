package screamer

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
)

func TestDataChangeRecord_DecodeToNonSpannerType(t *testing.T) {
	commitTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	startTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	watermarkTime := time.Date(2023, 1, 1, 11, 0, 0, 0, time.UTC)
	createdTime := time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC)
	scheduledTime := time.Date(2023, 1, 1, 9, 30, 0, 0, time.UTC)
	runningTime := time.Date(2023, 1, 1, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name                string
		record              *dataChangeRecord
		partitionMetadata   *PartitionMetadata
		recordWatermark     time.Time
		expectedTableName   string
		expectedModType     ModType
		expectedModsCount   int
		expectedColumnCount int
	}{
		{
			name: "INSERT record with basic types",
			record: &dataChangeRecord{
				CommitTimestamp:                      commitTime,
				RecordSequence:                       "seq-001",
				ServerTransactionID:                  "txn-001",
				IsLastRecordInTransactionInPartition: true,
				TableName:                            "users",
				ColumnTypes: []*columnType{
					{
						Name:            "id",
						Type:            spanner.NullJSON{Valid: true, Value: map[string]interface{}{"code": "INT64"}},
						IsPrimaryKey:    true,
						OrdinalPosition: 1,
					},
					{
						Name:            "name",
						Type:            spanner.NullJSON{Valid: true, Value: map[string]interface{}{"code": "STRING"}},
						IsPrimaryKey:    false,
						OrdinalPosition: 2,
					},
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
			partitionMetadata: &PartitionMetadata{
				PartitionToken: "token-001",
				StartTimestamp: startTime,
				CreatedAt:      createdTime,
				ScheduledAt:    &scheduledTime,
				RunningAt:      &runningTime,
			},
			recordWatermark:     watermarkTime,
			expectedTableName:   "users",
			expectedModType:     ModType_INSERT,
			expectedModsCount:   1,
			expectedColumnCount: 2,
		},
		{
			name: "UPDATE record with array type",
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
			partitionMetadata: &PartitionMetadata{
				PartitionToken: "token-002",
				StartTimestamp: startTime,
				CreatedAt:      createdTime,
			},
			recordWatermark:     watermarkTime,
			expectedTableName:   "products",
			expectedModType:     ModType_UPDATE,
			expectedModsCount:   1,
			expectedColumnCount: 1,
		},
		{
			name: "DELETE record with null values",
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
			partitionMetadata: &PartitionMetadata{
				PartitionToken: "token-003",
				StartTimestamp: startTime,
				CreatedAt:      createdTime,
			},
			recordWatermark:     watermarkTime,
			expectedTableName:   "deleted_items",
			expectedModType:     ModType_DELETE,
			expectedModsCount:   1,
			expectedColumnCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.record.DecodeToNonSpannerType(tt.partitionMetadata, tt.recordWatermark)

			// Test basic fields
			assert.Equal(t, result.DataChangeRecord.TableName, tt.expectedTableName)
			assert.Equal(t, result.DataChangeRecord.ModType, tt.expectedModType)
			assert.Equal(t, len(result.DataChangeRecord.Mods), tt.expectedModsCount)
			assert.Equal(t, len(result.DataChangeRecord.ColumnTypes), tt.expectedColumnCount)

			// Test partition metadata
			assert.Equal(t, result.PartitionToken, tt.partitionMetadata.PartitionToken)
			assert.Equal(t, result.StartTimestamp, tt.partitionMetadata.StartTimestamp)
			assert.Equal(t, result.Watermark, tt.recordWatermark)

			// Test array element type handling
			for i, columnType := range result.DataChangeRecord.ColumnTypes {
				if columnType.Type.Code == TypeCode_ARRAY {
					assert.NotEmpty(t, columnType.Type.ArrayElementType, "column %d: array type should have array_element_type set", i)
				}
			}

			// Test null JSON handling in mods
			for i, mod := range result.DataChangeRecord.Mods {
				if tt.record.Mods[i].NewValues.Valid && mod.NewValues == nil {
					t.Errorf("mod %d: expected non-nil new values", i)
				}
				if !tt.record.Mods[i].NewValues.Valid && mod.NewValues != nil {
					t.Errorf("mod %d: expected nil new values", i)
				}
			}
		})
	}
}
