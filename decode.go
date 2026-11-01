package screamer

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// This file decodes the ChangeRecord column of a change stream row by walking
// the protobuf values directly. The Spanner client's row.Columns decodes the
// same column with reflection and unmarshals every JSON value (each mod's keys
// and values, and each column's type) into maps that are then marshalled again
// for the consumer. Walking the values and keeping JSON as text is several times
// faster and allocates a fraction of the memory. If the column does not have
// the expected shape, the reader falls back to row.Columns.

// decodeChangeRecordColumn decodes an ARRAY<STRUCT<data_change_record ...,
// heartbeat_record ..., child_partitions_record ...>> column.
func decodeChangeRecordColumn(col spanner.GenericColumnValue) ([]*ChangeRecord, error) {
	t := col.Type
	if t.GetCode() != sppb.TypeCode_ARRAY || t.GetArrayElementType().GetCode() != sppb.TypeCode_STRUCT {
		return nil, fmt.Errorf("screamer: change record column has type %v, want ARRAY<STRUCT>", t.GetCode())
	}
	st := t.GetArrayElementType().GetStructType()
	f := fieldsOf(st, "data_change_record", "heartbeat_record", "child_partitions_record")

	list, err := listOf(col.Value)
	if err != nil || list == nil {
		return nil, err
	}

	var (
		dcr = structArrayDecoder{t: fieldType(st, f[0])}
		hb  = structArrayDecoder{t: fieldType(st, f[1])}
		cpr = structArrayDecoder{t: fieldType(st, f[2])}
	)
	out := make([]*ChangeRecord, 0, len(list))
	for _, v := range list {
		row, err := listOf(v)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		cr := &ChangeRecord{}
		if cr.DataChangeRecords, err = decodeDataChangeRecords(&dcr, field(row, f[0])); err != nil {
			return nil, err
		}
		if cr.HeartbeatRecords, err = decodeHeartbeatRecords(&hb, field(row, f[1])); err != nil {
			return nil, err
		}
		if cr.ChildPartitionsRecords, err = decodeChildPartitionsRecords(&cpr, field(row, f[2])); err != nil {
			return nil, err
		}
		out = append(out, cr)
	}
	return out, nil
}

// structArrayDecoder holds an ARRAY<STRUCT> type and, once looked up, the
// positions of the struct fields the decoder reads.
type structArrayDecoder struct {
	t      *sppb.Type
	fields []int
}

func (d *structArrayDecoder) structType() *sppb.StructType {
	return d.t.GetArrayElementType().GetStructType()
}

func (d *structArrayDecoder) lookup(names ...string) []int {
	if d.fields == nil {
		d.fields = fieldsOf(d.structType(), names...)
	}
	return d.fields
}

var dataChangeRecordFields = []string{
	"commit_timestamp", "record_sequence", "server_transaction_id",
	"is_last_record_in_transaction_in_partition", "table_name", "column_types", "mods",
	"mod_type", "value_capture_type", "number_of_records_in_transaction",
	"number_of_partitions_in_transaction", "transaction_tag", "is_system_transaction",
}

func decodeDataChangeRecords(d *structArrayDecoder, v *structpb.Value) ([]*RawDataChangeRecord, error) {
	list, err := listOf(v)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	f := d.lookup(dataChangeRecordFields...)
	st := d.structType()
	ct := structArrayDecoder{t: fieldType(st, f[5])}
	mods := structArrayDecoder{t: fieldType(st, f[6])}

	out := make([]*RawDataChangeRecord, 0, len(list))
	for _, item := range list {
		row, err := listOf(item)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		r := &RawDataChangeRecord{}
		if r.CommitTimestamp, err = timestampOf(field(row, f[0])); err != nil {
			return nil, err
		}
		r.RecordSequence = stringOf(field(row, f[1]))
		r.ServerTransactionID = stringOf(field(row, f[2]))
		r.IsLastRecordInTransactionInPartition = field(row, f[3]).GetBoolValue()
		r.TableName = stringOf(field(row, f[4]))
		if r.ColumnTypes, err = decodeColumnTypes(&ct, field(row, f[5])); err != nil {
			return nil, err
		}
		if r.Mods, err = decodeMods(&mods, field(row, f[6])); err != nil {
			return nil, err
		}
		r.ModType = stringOf(field(row, f[7]))
		r.ValueCaptureType = stringOf(field(row, f[8]))
		if r.NumberOfRecordsInTransaction, err = int64Of(field(row, f[9])); err != nil {
			return nil, err
		}
		if r.NumberOfPartitionsInTransaction, err = int64Of(field(row, f[10])); err != nil {
			return nil, err
		}
		r.TransactionTag = stringOf(field(row, f[11]))
		r.IsSystemTransaction = field(row, f[12]).GetBoolValue()
		out = append(out, r)
	}
	return out, nil
}

func decodeColumnTypes(d *structArrayDecoder, v *structpb.Value) ([]*RawColumnType, error) {
	list, err := listOf(v)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	f := d.lookup("name", "type", "is_primary_key", "ordinal_position")
	out := make([]*RawColumnType, 0, len(list))
	for _, item := range list {
		row, err := listOf(item)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		c := &RawColumnType{
			Name:         stringOf(field(row, f[0])),
			typeJSON:     stringOf(field(row, f[1])),
			IsPrimaryKey: field(row, f[2]).GetBoolValue(),
		}
		if c.OrdinalPosition, err = int64Of(field(row, f[3])); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func decodeMods(d *structArrayDecoder, v *structpb.Value) ([]*RawMod, error) {
	list, err := listOf(v)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	f := d.lookup("keys", "new_values", "old_values")
	out := make([]*RawMod, 0, len(list))
	for _, item := range list {
		row, err := listOf(item)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		out = append(out, &RawMod{
			keysJSON:      rawJSONOf(field(row, f[0])),
			newValuesJSON: rawJSONOf(field(row, f[1])),
			oldValuesJSON: rawJSONOf(field(row, f[2])),
		})
	}
	return out, nil
}

func decodeHeartbeatRecords(d *structArrayDecoder, v *structpb.Value) ([]*HeartbeatRecord, error) {
	list, err := listOf(v)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	f := d.lookup("timestamp")
	out := make([]*HeartbeatRecord, 0, len(list))
	for _, item := range list {
		row, err := listOf(item)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		h := &HeartbeatRecord{}
		if h.Timestamp, err = timestampOf(field(row, f[0])); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, nil
}

func decodeChildPartitionsRecords(d *structArrayDecoder, v *structpb.Value) ([]*ChildPartitionsRecord, error) {
	list, err := listOf(v)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	f := d.lookup("start_timestamp", "record_sequence", "child_partitions")
	children := structArrayDecoder{t: fieldType(d.structType(), f[2])}
	out := make([]*ChildPartitionsRecord, 0, len(list))
	for _, item := range list {
		row, err := listOf(item)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		r := &ChildPartitionsRecord{RecordSequence: stringOf(field(row, f[1]))}
		if r.StartTimestamp, err = timestampOf(field(row, f[0])); err != nil {
			return nil, err
		}
		cl, err := listOf(field(row, f[2]))
		if err != nil {
			return nil, err
		}
		cf := children.lookup("token", "parent_partition_tokens")
		for _, c := range cl {
			crow, err := listOf(c)
			if err != nil {
				return nil, err
			}
			if crow == nil {
				continue
			}
			child := &ChildPartition{Token: stringOf(field(crow, cf[0]))}
			parents, err := listOf(field(crow, cf[1]))
			if err != nil {
				return nil, err
			}
			child.ParentPartitionTokens = make([]string, 0, len(parents))
			for _, p := range parents {
				child.ParentPartitionTokens = append(child.ParentPartitionTokens, stringOf(p))
			}
			r.ChildPartitions = append(r.ChildPartitions, child)
		}
		out = append(out, r)
	}
	return out, nil
}

// fieldsOf returns the position of each named field in st, or -1 if st has no such field.
func fieldsOf(st *sppb.StructType, names ...string) []int {
	out := make([]int, len(names))
	for i, name := range names {
		out[i] = -1
		for j, f := range st.GetFields() {
			if f.GetName() == name {
				out[i] = j
				break
			}
		}
	}
	return out
}

func fieldType(st *sppb.StructType, i int) *sppb.Type {
	if i < 0 || i >= len(st.GetFields()) {
		return nil
	}
	return st.GetFields()[i].GetType()
}

// field returns the i'th value of a struct, or nil if the struct has no such field.
func field(row []*structpb.Value, i int) *structpb.Value {
	if i < 0 || i >= len(row) {
		return nil
	}
	return row[i]
}

// listOf returns the values of an ARRAY or STRUCT value, or nil if it is NULL or missing.
func listOf(v *structpb.Value) ([]*structpb.Value, error) {
	switch k := v.GetKind().(type) {
	case nil, *structpb.Value_NullValue:
		return nil, nil
	case *structpb.Value_ListValue:
		return k.ListValue.GetValues(), nil
	default:
		return nil, fmt.Errorf("screamer: unexpected %T in change record, want a list", k)
	}
}

func stringOf(v *structpb.Value) string {
	return v.GetStringValue()
}

func rawJSONOf(v *structpb.Value) rawJSON {
	if s, ok := v.GetKind().(*structpb.Value_StringValue); ok {
		return rawJSON{text: s.StringValue, valid: true}
	}
	return rawJSON{}
}

// int64Of decodes an INT64, which Spanner sends as a decimal string.
func int64Of(v *structpb.Value) (int64, error) {
	switch k := v.GetKind().(type) {
	case nil, *structpb.Value_NullValue:
		return 0, nil
	case *structpb.Value_StringValue:
		return strconv.ParseInt(k.StringValue, 10, 64)
	case *structpb.Value_NumberValue:
		return int64(k.NumberValue), nil
	default:
		return 0, fmt.Errorf("screamer: unexpected %T in change record, want an INT64", k)
	}
}

func timestampOf(v *structpb.Value) (time.Time, error) {
	switch k := v.GetKind().(type) {
	case nil, *structpb.Value_NullValue:
		return time.Time{}, nil
	case *structpb.Value_StringValue:
		t, err := time.Parse(time.RFC3339Nano, k.StringValue)
		if err != nil {
			return time.Time{}, fmt.Errorf("screamer: bad timestamp in change record: %w", err)
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("screamer: unexpected %T in change record, want a TIMESTAMP", k)
	}
}
