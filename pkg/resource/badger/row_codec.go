package badger

import (
	"encoding/json"
	"fmt"
	"time"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// RowCodec handles serialization and deserialization of row data
type RowCodec struct{}

// NewRowCodec creates a new RowCodec
func NewRowCodec() *RowCodec {
	return &RowCodec{}
}

// Encode serializes a row to bytes
func (c *RowCodec) Encode(row domain.Row) ([]byte, error) {
	if row == nil {
		return nil, nil
	}
	return json.Marshal(row)
}

// Decode deserializes bytes to a row
func (c *RowCodec) Decode(data []byte) (domain.Row, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var row domain.Row
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("failed to decode row: %w", err)
	}
	return row, nil
}

// EncodeBatch encodes multiple rows
func (c *RowCodec) EncodeBatch(rows []domain.Row) ([][]byte, error) {
	result := make([][]byte, len(rows))
	for i, row := range rows {
		data, err := c.Encode(row)
		if err != nil {
			return nil, fmt.Errorf("failed to encode row %d: %w", i, err)
		}
		result[i] = data
	}
	return result, nil
}

// DecodeBatch decodes multiple rows
func (c *RowCodec) DecodeBatch(data [][]byte) ([]domain.Row, error) {
	result := make([]domain.Row, len(data))
	for i, d := range data {
		row, err := c.Decode(d)
		if err != nil {
			return nil, fmt.Errorf("failed to decode row %d: %w", i, err)
		}
		result[i] = row
	}
	return result, nil
}

// TableInfoCodec handles serialization of table metadata
type TableInfoCodec struct{}

// NewTableInfoCodec creates a new TableInfoCodec
func NewTableInfoCodec() *TableInfoCodec {
	return &TableInfoCodec{}
}

// Encode serializes TableInfo to bytes
func (c *TableInfoCodec) Encode(info *domain.TableInfo) ([]byte, error) {
	if info == nil {
		return nil, nil
	}
	return json.Marshal(info)
}

// Decode deserializes bytes to TableInfo
func (c *TableInfoCodec) Decode(data []byte) (*domain.TableInfo, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var info domain.TableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to decode table info: %w", err)
	}
	return &info, nil
}

// IndexValueCodec handles serialization of index values (lists of row keys)
type IndexValueCodec struct{}

// NewIndexValueCodec creates a new IndexValueCodec
func NewIndexValueCodec() *IndexValueCodec {
	return &IndexValueCodec{}
}

// Encode serializes list of row keys to bytes
func (c *IndexValueCodec) Encode(keys []string) ([]byte, error) {
	if keys == nil {
		return []byte("[]"), nil
	}
	return json.Marshal(keys)
}

// Decode deserializes bytes to list of row keys
func (c *IndexValueCodec) Decode(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("failed to decode index keys: %w", err)
	}
	return keys, nil
}

// TableConfigCodec handles serialization of table config
type TableConfigCodec struct{}

// NewTableConfigCodec creates a new TableConfigCodec
func NewTableConfigCodec() *TableConfigCodec {
	return &TableConfigCodec{}
}

// Encode serializes TableConfig to bytes
func (c *TableConfigCodec) Encode(cfg *TableConfig) ([]byte, error) {
	if cfg == nil {
		return nil, nil
	}
	return json.Marshal(cfg)
}

// Decode deserializes bytes to TableConfig
func (c *TableConfigCodec) Decode(data []byte) (*TableConfig, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var cfg TableConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to decode table config: %w", err)
	}
	return &cfg, nil
}

// ValueConverter converts between Go types and storage format
type ValueConverter struct{}

// NewValueConverter creates a new ValueConverter
func NewValueConverter() *ValueConverter {
	return &ValueConverter{}
}

// ToStorageValue converts a Go value to storage format
func (c *ValueConverter) ToStorageValue(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool, []byte:
		return val, nil
	case time.Time:
		return val.Format(time.RFC3339Nano), nil
	default:
		// For complex types, convert to JSON
		return json.Marshal(val)
	}
}

// FromStorageValue converts storage value to Go type
func (c *ValueConverter) FromStorageValue(v interface{}, targetType string) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch targetType {
	case "string", "VARCHAR", "TEXT", "CHAR":
		return fmt.Sprintf("%v", v), nil
	case "int", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		return toInt64(v)
	case "float", "FLOAT", "DOUBLE", "DECIMAL":
		return toFloat64(v)
	case "bool", "BOOLEAN", "BOOL":
		return toBool(v)
	case "time", "TIMESTAMP", "DATETIME", "DATE":
		return toTime(v)
	default:
		return v, nil
	}
}

// Helper conversion functions
func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case string:
		var i int64
		_, err := fmt.Sscanf(val, "%d", &i)
		return i, err
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		var f float64
		_, err := fmt.Sscanf(val, "%f", &f)
		return f, err
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int, int8, int16, int32, int64:
		return val.(int64) != 0, nil
	case string:
		return val == "true" || val == "1" || val == "TRUE", nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func toTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		// Try common formats
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, val); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time: %s", val)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}
