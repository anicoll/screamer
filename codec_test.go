package screamer

import (
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
)

func nj(v interface{}) spanner.NullJSON { return spanner.NullJSON{Value: v, Valid: true} }

// codecFixture covers the shapes the decoder and encoder must handle: several
// mods, NULL and empty old_values (NEW_ROW and NEW_VALUES capture), ARRAY column
// types, strings that need escaping, heartbeats and a merge.
func codecFixture() []*ChangeRecord {
	ts := time.Date(2026, 11, 1, 8, 0, 0, 123456789, time.UTC)
	return []*ChangeRecord{
		{
			DataChangeRecords: []*RawDataChangeRecord{{
				CommitTimestamp:                      ts,
				RecordSequence:                       "00000001",
				ServerTransactionID:                  "MTAxNzQ5NjU0MDY1OTk3NTU2MzA=",
				IsLastRecordInTransactionInPartition: true,
				TableName:                            "Users",
				ColumnTypes: []*RawColumnType{
					{Name: "Id", Type: nj(map[string]interface{}{"code": "INT64"}), IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "Tags", Type: nj(map[string]interface{}{"code": "ARRAY", "array_element_type": map[string]interface{}{"code": "STRING"}}), OrdinalPosition: 2},
					{Name: "Bio <html> & \"quotes\"", Type: nj(map[string]interface{}{"code": "STRING"}), OrdinalPosition: 3},
				},
				Mods: []*RawMod{
					{
						Keys:      nj(map[string]interface{}{"Id": "1"}),
						NewValues: nj(map[string]interface{}{"Tags": []interface{}{"a", "b"}, "Bio": "line\nbreak <b>   ünïcödé", "Score": 1.5}),
						OldValues: spanner.NullJSON{},
					},
					{
						Keys:      nj(map[string]interface{}{"Id": "2"}),
						NewValues: nj(map[string]interface{}{"Tags": nil}),
						OldValues: nj(map[string]interface{}{}),
					},
				},
				ModType:                         "UPDATE",
				ValueCaptureType:                "NEW_VALUES",
				NumberOfRecordsInTransaction:    2,
				NumberOfPartitionsInTransaction: 1,
				TransactionTag:                  "app=web",
				IsSystemTransaction:             false,
			}},
			HeartbeatRecords:       []*HeartbeatRecord{},
			ChildPartitionsRecords: []*ChildPartitionsRecord{},
		},
		{
			DataChangeRecords:      []*RawDataChangeRecord{},
			HeartbeatRecords:       []*HeartbeatRecord{{Timestamp: ts.Add(time.Second)}},
			ChildPartitionsRecords: []*ChildPartitionsRecord{},
		},
		{
			DataChangeRecords: []*RawDataChangeRecord{},
			HeartbeatRecords:  []*HeartbeatRecord{},
			ChildPartitionsRecords: []*ChildPartitionsRecord{{
				StartTimestamp: ts.Add(2 * time.Second),
				RecordSequence: "00000002",
				ChildPartitions: []*ChildPartition{
					{Token: "child-a", ParentPartitionTokens: []string{"parent-1"}},
					{Token: "child-b", ParentPartitionTokens: []string{"parent-1", "parent-2"}},
				},
			}},
		},
	}
}

func fixtureRowOf(t testing.TB, records []*ChangeRecord) *spanner.Row {
	row, err := spanner.NewRow([]string{"ChangeRecord"}, []interface{}{records})
	require.NoError(t, err)
	return row
}

func normalizeJSON(t *testing.T, b []byte) interface{} {
	t.Helper()
	var v interface{}
	require.NoError(t, json.Unmarshal(b, &v), "invalid JSON: %s", b)
	return v
}

// The fast decoder must read every field the Spanner client's reflection
// decoder reads, and the record a consumer receives must hold the same JSON.
func TestDecodeChangeRecordColumn_MatchesClientDecoder(t *testing.T) {
	row := fixtureRowOf(t, codecFixture())

	var slow []*ChangeRecord
	require.NoError(t, row.Columns(&slow))
	var col spanner.GenericColumnValue
	require.NoError(t, row.Column(0, &col))
	fast, err := decodeChangeRecordColumn(col)
	require.NoError(t, err)
	require.Len(t, fast, len(slow))

	p := &PartitionMetadata{PartitionToken: "tok", StartTimestamp: time.Unix(10, 0).UTC(), CreatedAt: time.Unix(5, 0).UTC()}
	wm := time.Unix(20, 0).UTC()
	for i := range slow {
		require.Len(t, fast[i].DataChangeRecords, len(slow[i].DataChangeRecords))
		for j, s := range slow[i].DataChangeRecords {
			want, err := json.Marshal(s.DecodeToNonSpannerType(p, wm))
			require.NoError(t, err)
			got, err := encodeDataChangeRecord(fast[i].DataChangeRecords[j], p, wm)
			require.NoError(t, err)
			require.Equal(t, normalizeJSON(t, want), normalizeJSON(t, got))

			// DecodeToNonSpannerType gives the same result from either form.
			require.Equal(t, s.DecodeToNonSpannerType(p, wm), fast[i].DataChangeRecords[j].DecodeToNonSpannerType(p, wm))
		}

		require.Len(t, fast[i].HeartbeatRecords, len(slow[i].HeartbeatRecords))
		for j := range slow[i].HeartbeatRecords {
			require.True(t, slow[i].HeartbeatRecords[j].Timestamp.Equal(fast[i].HeartbeatRecords[j].Timestamp))
		}
		require.Len(t, fast[i].ChildPartitionsRecords, len(slow[i].ChildPartitionsRecords))
		for j, s := range slow[i].ChildPartitionsRecords {
			f := fast[i].ChildPartitionsRecords[j]
			require.True(t, s.StartTimestamp.Equal(f.StartTimestamp))
			require.Equal(t, s.RecordSequence, f.RecordSequence)
			require.Equal(t, s.ChildPartitions, f.ChildPartitions)
		}
	}
}

// A row the fast decoder does not understand is decoded by the client instead.
func TestDecodeChangeRecordRow_FallsBackOnUnexpectedShape(t *testing.T) {
	row, err := spanner.NewRow([]string{"ChangeRecord"}, []interface{}{"not a change record"})
	require.NoError(t, err)
	var col spanner.GenericColumnValue
	require.NoError(t, row.Column(0, &col))
	_, err = decodeChangeRecordColumn(col)
	require.Error(t, err)

	_, err = decodeChangeRecordRow(row)
	require.Error(t, err, "the reflection decoder rejects it too")

	records, err := decodeChangeRecordRow(fixtureRowOf(t, codecFixture()))
	require.NoError(t, err)
	require.Len(t, records, 3)
}

// For records built by other readers (such as the simulator), which carry
// NullJSON values, the output is byte for byte what encoding/json produced
// before.
func TestEncodeDataChangeRecord_ByteIdenticalForNullJSON(t *testing.T) {
	sched := time.Unix(30, 0).UTC()
	for _, p := range []*PartitionMetadata{
		{PartitionToken: "tok", StartTimestamp: time.Unix(10, 0).UTC(), CreatedAt: time.Unix(5, 0).UTC()},
		{PartitionToken: "tok<&>", StartTimestamp: time.Unix(10, 5).UTC(), ScheduledAt: &sched, RunningAt: &sched},
	} {
		for _, cr := range codecFixture() {
			for _, r := range cr.DataChangeRecords {
				wm := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
				want, err := json.Marshal(r.DecodeToNonSpannerType(p, wm))
				require.NoError(t, err)
				got, err := encodeDataChangeRecord(r, p, wm)
				require.NoError(t, err)
				require.Equal(t, string(want), string(got))
			}
		}
	}
}

// Mods copied as JSON text are left out when NULL or empty, like the omitempty
// maps they replace, and kept verbatim otherwise.
func TestEncodeDataChangeRecord_RawMods(t *testing.T) {
	r := &RawDataChangeRecord{Mods: []*RawMod{
		{keysJSON: rawJSON{`{"Id":"1"}`, true}, newValuesJSON: rawJSON{` { "b":1, "a":2 } `, true}, oldValuesJSON: rawJSON{`{}`, true}},
		{keysJSON: rawJSON{`{"Id":"2"}`, true}, newValuesJSON: rawJSON{}, oldValuesJSON: rawJSON{` { } `, true}},
		{keysJSON: rawJSON{`null`, true}},
	}}
	out, err := encodeDataChangeRecord(r, &PartitionMetadata{}, time.Time{})
	require.NoError(t, err)

	var doc struct {
		Mods []json.RawMessage `json:"mods"`
	}
	require.NoError(t, json.Unmarshal(out, &doc))
	require.Equal(t, []string{`{"keys":{"Id":"1"},"new_values": { "b":1, "a":2 } }`, `{"keys":{"Id":"2"}}`, `{}`},
		[]string{string(doc.Mods[0]), string(doc.Mods[1]), string(doc.Mods[2])})
}

func FuzzAppendString(f *testing.F) {
	for _, s := range []string{"", "plain", "\"\\/\b\f\n\r\t", "<script>&</script>", "  ", "\x00\x1f\x7f", "\xff\xfe invalid", "ünïcödé 🎉"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		want, err := json.Marshal(s)
		require.NoError(t, err)
		require.Equal(t, string(want), string(appendString(nil, s)))
	})
}

func FuzzAppendTime(f *testing.F) {
	f.Add(int64(0), int64(0))
	f.Add(int64(1_762_000_000), int64(123_456_789))
	f.Add(int64(253_402_300_799), int64(999_999_999))
	f.Fuzz(func(t *testing.T, sec, nsec int64) {
		ts := time.Unix(sec%253_402_300_800, nsec%1_000_000_000).UTC()
		want, err := json.Marshal(ts)
		if err != nil {
			t.Skip() // out of the range time.Time can marshal
		}
		require.Equal(t, string(want), string(appendTime(nil, ts)))
	})
}
