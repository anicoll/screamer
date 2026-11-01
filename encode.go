package screamer

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
)

// This file writes the JSON that consumers receive for each data change record.
// It produces the same document as json.Marshal(r.DecodeToNonSpannerType(p, wm))
// without reflection, and copies the mods' keys and values straight from the
// JSON text Spanner sent instead of unmarshalling and re-marshalling them. The
// only visible difference is inside keys, new_values and old_values: object keys
// keep Spanner's order and characters keep Spanner's escaping, rather than being
// sorted and HTML-escaped by encoding/json. The JSON values are the same.

// encodeDataChangeRecord returns the JSON document delivered to consumers.
func encodeDataChangeRecord(r *RawDataChangeRecord, p *PartitionMetadata, watermark time.Time) ([]byte, error) {
	dst := make([]byte, 0, r.sizeHint())
	dst = append(dst, `{"commit_timestamp":`...)
	dst = appendTime(dst, r.CommitTimestamp)
	dst = append(dst, `,"record_sequence":`...)
	dst = appendString(dst, r.RecordSequence)
	dst = append(dst, `,"server_transaction_id":`...)
	dst = appendString(dst, r.ServerTransactionID)
	dst = append(dst, `,"is_last_record_in_transaction_in_partition":`...)
	dst = strconv.AppendBool(dst, r.IsLastRecordInTransactionInPartition)
	dst = append(dst, `,"table_name":`...)
	dst = appendString(dst, r.TableName)

	dst = append(dst, `,"column_types":[`...)
	for i, t := range r.ColumnTypes {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"name":`...)
		dst = appendString(dst, t.Name)
		dst = append(dst, `,"type":`...)
		if t.typeJSON != "" {
			dst = append(dst, columnTypes.get(t.typeJSON).json...)
		} else {
			dst = appendType(dst, decodeColumnTypeJSONToType(t.Type))
		}
		if t.IsPrimaryKey {
			dst = append(dst, `,"is_primary_key":true`...)
		}
		dst = append(dst, `,"ordinal_position":`...)
		dst = strconv.AppendInt(dst, t.OrdinalPosition, 10)
		dst = append(dst, '}')
	}

	dst = append(dst, `],"mods":[`...)
	for i, m := range r.Mods {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '{')
		first := true
		var err error
		if dst, first, err = appendModField(dst, first, `"keys":`, m.Keys, m.keysJSON); err != nil {
			return nil, err
		}
		if dst, first, err = appendModField(dst, first, `"new_values":`, m.NewValues, m.newValuesJSON); err != nil {
			return nil, err
		}
		if dst, _, err = appendModField(dst, first, `"old_values":`, m.OldValues, m.oldValuesJSON); err != nil {
			return nil, err
		}
		dst = append(dst, '}')
	}

	dst = append(dst, `],"mod_type":`...)
	dst = appendString(dst, r.ModType)
	dst = append(dst, `,"value_capture_type":`...)
	dst = appendString(dst, r.ValueCaptureType)
	dst = append(dst, `,"number_of_records_in_transaction":`...)
	dst = strconv.AppendInt(dst, r.NumberOfRecordsInTransaction, 10)
	dst = append(dst, `,"number_of_partitions_in_transaction":`...)
	dst = strconv.AppendInt(dst, r.NumberOfPartitionsInTransaction, 10)
	dst = append(dst, `,"transaction_tag":`...)
	dst = appendString(dst, r.TransactionTag)
	dst = append(dst, `,"is_system_transaction":`...)
	dst = strconv.AppendBool(dst, r.IsSystemTransaction)

	dst = append(dst, `,"partition_token":`...)
	dst = appendString(dst, p.PartitionToken)
	dst = append(dst, `,"start_timestamp":`...)
	dst = appendTime(dst, p.StartTimestamp)
	dst = append(dst, `,"watermark":`...)
	dst = appendTime(dst, watermark)
	dst = append(dst, `,"created_at":`...)
	dst = appendTime(dst, p.CreatedAt)
	if p.ScheduledAt != nil {
		dst = append(dst, `,"scheduled_at":`...)
		dst = appendTime(dst, *p.ScheduledAt)
	}
	if p.RunningAt != nil {
		dst = append(dst, `,"running_at":`...)
		dst = appendTime(dst, *p.RunningAt)
	}
	return append(dst, '}'), nil
}

// sizeHint estimates the encoded size, so the output is allocated once.
func (r *RawDataChangeRecord) sizeHint() int {
	n := 600 + len(r.ColumnTypes)*80
	for _, m := range r.Mods {
		n += len(m.keysJSON.text) + len(m.newValuesJSON.text) + len(m.oldValuesJSON.text) + 48
		if !m.newValuesJSON.valid && m.NewValues.Valid {
			n += 256 // re-marshalled below; a guess is enough
		}
	}
	return n
}

// appendModField writes one of a mod's keys, new_values or old_values, and
// leaves it out when it is NULL or an empty object, like the omitempty map it
// replaces.
func appendModField(dst []byte, first bool, name string, j spanner.NullJSON, raw rawJSON) ([]byte, bool, error) {
	if raw.valid {
		if !isNonEmptyObject(raw.text) {
			return dst, first, nil
		}
		if !first {
			dst = append(dst, ',')
		}
		dst = append(dst, name...)
		return append(dst, raw.text...), false, nil
	}

	m := decodeNullJSONToMap(j)
	if len(m) == 0 {
		return dst, first, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, first, err
	}
	if !first {
		dst = append(dst, ',')
	}
	dst = append(dst, name...)
	return append(dst, b...), false, nil
}

// isNonEmptyObject reports whether s is a JSON object with at least one member.
func isNonEmptyObject(s string) bool {
	i := skipSpace(s, 0)
	if i >= len(s) || s[i] != '{' {
		return false
	}
	i = skipSpace(s, i+1)
	return i < len(s) && s[i] != '}'
}

func skipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func appendType(dst []byte, t Type) []byte {
	dst = append(dst, `{"code":`...)
	dst = appendString(dst, string(t.Code))
	if t.ArrayElementType != "" {
		dst = append(dst, `,"array_element_type":`...)
		dst = appendString(dst, string(t.ArrayElementType))
	}
	return append(dst, '}')
}

// appendTime writes t the way time.Time.MarshalJSON does.
func appendTime(dst []byte, t time.Time) []byte {
	dst = append(dst, '"')
	dst = t.AppendFormat(dst, time.RFC3339Nano)
	return append(dst, '"')
}

const hexDigits = "0123456789abcdef"

// appendString writes s as a JSON string, escaped exactly as encoding/json
// escapes it (including its HTML-safe escaping of <, > and &).
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `\ufffd`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// columnTypeEntry is a decoded column type and its encoded output.
type columnTypeEntry struct {
	typ  Type
	json []byte
}

// columnTypeCache maps the JSON text of a column type, as Spanner sends it, to
// its decoded form. Every record of a table repeats the same handful of type
// strings, so each is parsed once.
type columnTypeCache struct {
	mu sync.RWMutex
	m  map[string]*columnTypeEntry
}

// maxCachedColumnTypes bounds the cache. Distinct column types are few; past
// this, types are decoded on every use rather than cached.
const maxCachedColumnTypes = 4096

var columnTypes = &columnTypeCache{m: make(map[string]*columnTypeEntry)}

func (c *columnTypeCache) get(raw string) *columnTypeEntry {
	c.mu.RLock()
	e, ok := c.m[raw]
	c.mu.RUnlock()
	if ok {
		return e
	}

	var v interface{}
	_ = json.Unmarshal([]byte(raw), &v)
	t := decodeColumnTypeJSONToType(spanner.NullJSON{Value: v, Valid: v != nil})
	e = &columnTypeEntry{typ: t, json: appendType(nil, t)}

	c.mu.Lock()
	if len(c.m) < maxCachedColumnTypes {
		c.m[raw] = e
	}
	c.mu.Unlock()
	return e
}
