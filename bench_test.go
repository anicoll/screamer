package screamer

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

// Benchmarks for the per-record path: decoding a change stream row and encoding
// the JSON delivered to the consumer. Run with:
//
//	go test -run XXX -bench 'Record' -benchmem .
//
// The fixture is a 10-column UPDATE with one mod, a typical OLTP row.

func benchRecord(cols int) *RawDataChangeRecord {
	ct := make([]*RawColumnType, 0, cols)
	keys := map[string]interface{}{"Id": "1234567890"}
	nv := map[string]interface{}{}
	codes := []string{"STRING", "INT64", "FLOAT64", "BOOL", "TIMESTAMP"}
	for i := 0; i < cols; i++ {
		name := fmt.Sprintf("Column%02d", i)
		code := codes[i%len(codes)]
		ct = append(ct, &RawColumnType{Name: name, Type: nj(map[string]interface{}{"code": code}), IsPrimaryKey: i == 0, OrdinalPosition: int64(i + 1)})
		switch code {
		case "STRING":
			nv[name] = "some moderately long string value for the column"
		case "INT64":
			nv[name] = "987654321"
		case "FLOAT64":
			nv[name] = 3.14159
		case "BOOL":
			nv[name] = true
		case "TIMESTAMP":
			nv[name] = "2026-11-01T08:00:00.123456Z"
		}
	}
	return &RawDataChangeRecord{
		CommitTimestamp: time.Now(), RecordSequence: "00000001", ServerTransactionID: "MTAxNzQ5NjU0MDY1OTk3NTU2MzA=",
		IsLastRecordInTransactionInPartition: true, TableName: "Users", ColumnTypes: ct,
		Mods:    []*RawMod{{Keys: nj(keys), NewValues: nj(nv)}},
		ModType: "UPDATE", ValueCaptureType: "NEW_ROW", NumberOfRecordsInTransaction: 1, NumberOfPartitionsInTransaction: 1,
	}
}

func benchRow(b *testing.B) *spanner.Row {
	return fixtureRowOf(b, []*ChangeRecord{{
		DataChangeRecords:      []*RawDataChangeRecord{benchRecord(10)},
		HeartbeatRecords:       []*HeartbeatRecord{},
		ChildPartitionsRecords: []*ChildPartitionsRecord{},
	}})
}

var benchPartition = &PartitionMetadata{PartitionToken: "token", StartTimestamp: time.Now(), CreatedAt: time.Now()}

// BenchmarkRecord_Before is the path before the fast decoder and encoder:
// row.Columns with reflection, then json.Marshal of the decoded record.
func BenchmarkRecord_Before(b *testing.B) {
	row := benchRow(b)
	b.ReportAllocs()
	for b.Loop() {
		var records []*ChangeRecord
		if err := row.Columns(&records); err != nil {
			b.Fatal(err)
		}
		if _, err := json.Marshal(records[0].DataChangeRecords[0].DecodeToNonSpannerType(benchPartition, time.Now())); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecord_After is the current path.
func BenchmarkRecord_After(b *testing.B) {
	row := benchRow(b)
	b.ReportAllocs()
	for b.Loop() {
		records, err := decodeChangeRecordRow(row)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := encodeDataChangeRecord(records[0].DataChangeRecords[0], benchPartition, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordDecode_Client(b *testing.B) {
	row := benchRow(b)
	b.ReportAllocs()
	for b.Loop() {
		var records []*ChangeRecord
		if err := row.Columns(&records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordDecode_Fast(b *testing.B) {
	row := benchRow(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := decodeChangeRecordRow(row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordEncode_EncodingJSON(b *testing.B) {
	r := benchRecord(10)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(r.DecodeToNonSpannerType(benchPartition, time.Now())); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordEncode_Fast(b *testing.B) {
	records, err := decodeChangeRecordRow(benchRow(b))
	if err != nil {
		b.Fatal(err)
	}
	r := records[0].DataChangeRecords[0]
	b.ReportAllocs()
	for b.Loop() {
		if _, err := encodeDataChangeRecord(r, benchPartition, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
