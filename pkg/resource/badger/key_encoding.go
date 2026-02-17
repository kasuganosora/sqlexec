package badger

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// KeyEncoder encodes keys for Badger storage
type KeyEncoder struct{}

// NewKeyEncoder creates a new KeyEncoder
func NewKeyEncoder() *KeyEncoder {
	return &KeyEncoder{}
}

// EncodeTableKey encodes table metadata key
// Format: table:{table_name}
func (e *KeyEncoder) EncodeTableKey(tableName string) []byte {
	return []byte(PrefixTable + tableName)
}

// DecodeTableKey decodes table name from key
func (e *KeyEncoder) DecodeTableKey(key []byte) (tableName string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixTable) {
		return "", false
	}
	return s[len(PrefixTable):], true
}

// EncodeRowKey encodes row data key
// Format: row:{table_name}:{primary_key}
func (e *KeyEncoder) EncodeRowKey(tableName, primaryKey string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", PrefixRow, tableName, primaryKey))
}

// DecodeRowKey decodes table name and primary key from row key
func (e *KeyEncoder) DecodeRowKey(key []byte) (tableName, primaryKey string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixRow) {
		return "", "", false
	}
	parts := strings.SplitN(s[len(PrefixRow):], ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// EncodeRowPrefix encodes row key prefix for table scan
// Format: row:{table_name}:
func (e *KeyEncoder) EncodeRowPrefix(tableName string) []byte {
	return []byte(fmt.Sprintf("%s%s:", PrefixRow, tableName))
}

// EncodeIndexKey encodes index key
// Format: idx:{table_name}:{column_name}:{value}
func (e *KeyEncoder) EncodeIndexKey(tableName, columnName, value string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s:%s", PrefixIndex, tableName, columnName, value))
}

// EncodeCompositeIndexKey encodes composite index key for multiple columns
// Format: idx:{table_name}:{col1_col2_col3}:{value1|value2|value3}
func (e *KeyEncoder) EncodeCompositeIndexKey(tableName string, columnNames []string, values []string) []byte {
	columnKey := strings.Join(columnNames, "_")
	valueKey := strings.Join(values, "|")
	return []byte(fmt.Sprintf("%s%s:%s:%s", PrefixIndex, tableName, columnKey, valueKey))
}

// DecodeIndexKey decodes index key components
func (e *KeyEncoder) DecodeIndexKey(key []byte) (tableName, columnName, value string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixIndex) {
		return "", "", "", false
	}
	parts := strings.SplitN(s[len(PrefixIndex):], ":", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// DecodeCompositeIndexKey decodes composite index key into column names and values
func (e *KeyEncoder) DecodeCompositeIndexKey(key []byte) (tableName string, columnNames []string, values []string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixIndex) {
		return "", nil, nil, false
	}
	parts := strings.SplitN(s[len(PrefixIndex):], ":", 3)
	if len(parts) != 3 {
		return "", nil, nil, false
	}
	columnNames = strings.Split(parts[1], "_")
	values = strings.Split(parts[2], "|")
	return parts[0], columnNames, values, true
}

// EncodeIndexPrefix encodes index key prefix for column scan
// Format: idx:{table_name}:{column_name}:
func (e *KeyEncoder) EncodeIndexPrefix(tableName, columnName string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s:", PrefixIndex, tableName, columnName))
}

// EncodeSeqKey encodes sequence key
// Format: seq:{table_name}:{column_name}
func (e *KeyEncoder) EncodeSeqKey(tableName, columnName string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", PrefixSeq, tableName, columnName))
}

// DecodeSeqKey decodes sequence key components
func (e *KeyEncoder) DecodeSeqKey(key []byte) (tableName, columnName string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixSeq) {
		return "", "", false
	}
	parts := strings.SplitN(s[len(PrefixSeq):], ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// EncodeConfigKey encodes table config key
// Format: config:{table_name}
func (e *KeyEncoder) EncodeConfigKey(tableName string) []byte {
	return []byte(PrefixConfig + tableName)
}

// DecodeConfigKey decodes table name from config key
func (e *KeyEncoder) DecodeConfigKey(key []byte) (tableName string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, PrefixConfig) {
		return "", false
	}
	return s[len(PrefixConfig):], true
}

// ValueEncoder encodes values for keys
type ValueEncoder struct{}

// NewValueEncoder creates a new ValueEncoder
func NewValueEncoder() *ValueEncoder {
	return &ValueEncoder{}
}

// EncodeInt64 encodes int64 value
func (e *ValueEncoder) EncodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

// DecodeInt64 decodes int64 value
func (e *ValueEncoder) DecodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// EncodeUint64 encodes uint64 value
func (e *ValueEncoder) EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

// DecodeUint64 decodes uint64 value
func (e *ValueEncoder) DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// PrimaryKeyGenerator generates primary keys
type PrimaryKeyGenerator struct {
	encoder *KeyEncoder
}

// NewPrimaryKeyGenerator creates a new PrimaryKeyGenerator
func NewPrimaryKeyGenerator() *PrimaryKeyGenerator {
	return &PrimaryKeyGenerator{
		encoder: NewKeyEncoder(),
	}
}

// GenerateFromRow generates primary key from row data using the primary key column
func (g *PrimaryKeyGenerator) GenerateFromRow(tableInfo *domain.TableInfo, row domain.Row) (string, error) {
	for _, col := range tableInfo.Columns {
		if col.Primary {
			val := row[col.Name]
			if val == nil {
				return "", fmt.Errorf("primary key column %s is nil", col.Name)
			}
			return fmt.Sprintf("%v", val), nil
		}
	}
	return "", fmt.Errorf("no primary key column found in table %s", tableInfo.Name)
}

// FormatIntKey formats an integer as a zero-padded key for correct ordering
func (g *PrimaryKeyGenerator) FormatIntKey(id int64) string {
	return fmt.Sprintf("%020d", id)
}

// ParseIntKey parses a zero-padded integer key
func (g *PrimaryKeyGenerator) ParseIntKey(key string) (int64, error) {
	return strconv.ParseInt(key, 10, 64)
}
