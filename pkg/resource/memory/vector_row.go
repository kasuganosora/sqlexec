package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ParseVectorValue converts a stored row value into a float32 vector.
func ParseVectorValue(val interface{}) ([]float32, error) {
	if val == nil {
		return nil, fmt.Errorf("vector value is nil")
	}
	switch v := val.(type) {
	case []float32:
		out := make([]float32, len(v))
		copy(out, v)
		return out, nil
	case []float64:
		out := make([]float32, len(v))
		for i, x := range v {
			out[i] = float32(x)
		}
		return out, nil
	case []interface{}:
		out := make([]float32, len(v))
		for i, elem := range v {
			f, ok := toFloat32(elem)
			if !ok {
				return nil, fmt.Errorf("unsupported vector element type %T", elem)
			}
			out[i] = f
		}
		return out, nil
	case string:
		return parseVectorJSON(v)
	default:
		return nil, fmt.Errorf("unsupported vector type %T", val)
	}
}

func parseVectorJSON(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty vector string")
	}
	var floats []float64
	if err := json.Unmarshal([]byte(s), &floats); err != nil {
		return nil, fmt.Errorf("invalid vector JSON: %w", err)
	}
	out := make([]float32, len(floats))
	for i, x := range floats {
		out[i] = float32(x)
	}
	return out, nil
}

func toFloat32(val interface{}) (float32, bool) {
	switch v := val.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case int:
		return float32(v), true
	case int32:
		return float32(v), true
	case int64:
		return float32(v), true
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return float32(f), true
	default:
		return 0, false
	}
}

func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

// VectorIDFromRow picks a stable vector ID: integer primary key, then "id", else 1-based ordinal.
func VectorIDFromRow(row domain.Row, schema *domain.TableInfo, ordinal int) int64 {
	if schema != nil {
		for _, col := range schema.Columns {
			if !col.Primary {
				continue
			}
			if id, ok := toInt64(row[col.Name]); ok {
				return id
			}
		}
	}
	if id, ok := toInt64(row["id"]); ok {
		return id
	}
	return int64(ordinal + 1)
}

// ExtractVectorRecords reads a vector column out of table rows.
func ExtractVectorRecords(schema *domain.TableInfo, rows []domain.Row, columnName string, dimension int) []VectorRecord {
	records := make([]VectorRecord, 0, len(rows))
	for i, row := range rows {
		raw, ok := row[columnName]
		if !ok || raw == nil {
			continue
		}
		vec, err := ParseVectorValue(raw)
		if err != nil {
			continue
		}
		if dimension > 0 && len(vec) != dimension {
			continue
		}
		records = append(records, VectorRecord{
			ID:     VectorIDFromRow(row, schema, i),
			Vector: vec,
		})
	}
	return records
}

type sliceVectorLoader struct {
	records []VectorRecord
}

func (l *sliceVectorLoader) Load(ctx context.Context) ([]VectorRecord, error) {
	return l.records, nil
}

func (l *sliceVectorLoader) Count() int64 {
	return int64(len(l.records))
}
