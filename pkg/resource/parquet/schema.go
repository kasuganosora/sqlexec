package parquet

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	pq "github.com/parquet-go/parquet-go"
)

// domainTypeToParquetNode converts a domain.ColumnInfo to a parquet node.
func domainTypeToParquetNode(col domain.ColumnInfo) pq.Node {
	var node pq.Node

	switch strings.ToLower(col.Type) {
	case "int64", "bigint":
		node = pq.Leaf(pq.Int64Type)
	case "int32", "int", "integer":
		node = pq.Leaf(pq.Int32Type)
	case "float64", "double":
		node = pq.Leaf(pq.DoubleType)
	case "float32", "float":
		node = pq.Leaf(pq.FloatType)
	case "bool", "boolean":
		node = pq.Leaf(pq.BooleanType)
	case "string", "varchar", "text":
		node = pq.String()
	case "bytes", "blob", "binary", "varbinary":
		node = pq.Leaf(pq.ByteArrayType)
	case "time", "datetime", "timestamp":
		// Store timestamps as INT64 milliseconds
		node = pq.Leaf(pq.Int64Type)
	default:
		// Default to string for unknown types
		node = pq.String()
	}

	if col.Nullable {
		node = pq.Optional(node)
	}

	return node
}

// domainSchemaToParquet converts domain columns to a parquet schema.
func domainSchemaToParquet(tableName string, columns []domain.ColumnInfo) *pq.Schema {
	group := make(pq.Group)
	for _, col := range columns {
		group[col.Name] = domainTypeToParquetNode(col)
	}
	return pq.NewSchema(tableName, group)
}

// parquetFieldToDomain converts a parquet schema field to a domain.ColumnInfo.
func parquetFieldToDomain(field pq.Field) domain.ColumnInfo {
	col := domain.ColumnInfo{
		Name:     field.Name(),
		Nullable: field.Optional(),
	}

	if field.Leaf() {
		col.Type = parquetNodeTypeToString(field)
	} else {
		col.Type = "string"
	}

	return col
}

// parquetNodeTypeToString maps a leaf parquet node to a domain type string.
func parquetNodeTypeToString(node pq.Node) string {
	t := node.Type()
	switch t.Kind() {
	case pq.Boolean:
		return "bool"
	case pq.Int32:
		return "int32"
	case pq.Int64:
		return "int64"
	case pq.Float:
		return "float32"
	case pq.Double:
		return "float64"
	case pq.ByteArray:
		if lt := t.LogicalType(); lt != nil && lt.UTF8 != nil {
			return "string"
		}
		return "bytes"
	case pq.FixedLenByteArray:
		return "bytes"
	default:
		return "string"
	}
}

// parquetSchemaToDomain converts a parquet schema to domain column infos.
func parquetSchemaToDomain(schema *pq.Schema) []domain.ColumnInfo {
	fields := schema.Fields()
	columns := make([]domain.ColumnInfo, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, parquetFieldToDomain(field))
	}
	return columns
}

// domainRowToParquetRow converts a domain.Row to a parquet.Row using the given schema.
func domainRowToParquetRow(schema *pq.Schema, columns []domain.ColumnInfo, row domain.Row) pq.Row {
	values := make([]pq.Value, len(columns))
	for i, col := range columns {
		v, ok := row[col.Name]
		if !ok || v == nil {
			values[i] = pq.NullValue().Level(0, 0, i)
			continue
		}
		values[i] = goValueToParquet(col, v, i)
	}
	return pq.Row(values)
}

// goValueToParquet converts a Go value to a parquet.Value based on column type.
func goValueToParquet(col domain.ColumnInfo, v interface{}, colIndex int) pq.Value {
	defLevel := 0
	if col.Nullable {
		defLevel = 1
	}

	switch strings.ToLower(col.Type) {
	case "int64", "bigint", "time", "datetime", "timestamp":
		switch val := v.(type) {
		case int64:
			return pq.Int64Value(val).Level(0, defLevel, colIndex)
		case int:
			return pq.Int64Value(int64(val)).Level(0, defLevel, colIndex)
		case int32:
			return pq.Int64Value(int64(val)).Level(0, defLevel, colIndex)
		case float64:
			return pq.Int64Value(int64(val)).Level(0, defLevel, colIndex)
		default:
			return pq.Int64Value(0).Level(0, defLevel, colIndex)
		}
	case "int32", "int", "integer":
		switch val := v.(type) {
		case int32:
			return pq.Int32Value(val).Level(0, defLevel, colIndex)
		case int:
			return pq.Int32Value(int32(val)).Level(0, defLevel, colIndex)
		case int64:
			return pq.Int32Value(int32(val)).Level(0, defLevel, colIndex)
		case float64:
			return pq.Int32Value(int32(val)).Level(0, defLevel, colIndex)
		default:
			return pq.Int32Value(0).Level(0, defLevel, colIndex)
		}
	case "float64", "double":
		switch val := v.(type) {
		case float64:
			return pq.DoubleValue(val).Level(0, defLevel, colIndex)
		case int64:
			return pq.DoubleValue(float64(val)).Level(0, defLevel, colIndex)
		case int:
			return pq.DoubleValue(float64(val)).Level(0, defLevel, colIndex)
		default:
			return pq.DoubleValue(0).Level(0, defLevel, colIndex)
		}
	case "float32", "float":
		switch val := v.(type) {
		case float32:
			return pq.FloatValue(val).Level(0, defLevel, colIndex)
		case float64:
			return pq.FloatValue(float32(val)).Level(0, defLevel, colIndex)
		default:
			return pq.FloatValue(0).Level(0, defLevel, colIndex)
		}
	case "bool", "boolean":
		switch val := v.(type) {
		case bool:
			return pq.BooleanValue(val).Level(0, defLevel, colIndex)
		default:
			return pq.BooleanValue(false).Level(0, defLevel, colIndex)
		}
	case "string", "varchar", "text":
		switch val := v.(type) {
		case string:
			return pq.ByteArrayValue([]byte(val)).Level(0, defLevel, colIndex)
		default:
			return pq.ByteArrayValue([]byte(fmt.Sprintf("%v", v))).Level(0, defLevel, colIndex)
		}
	case "bytes", "blob", "binary", "varbinary":
		switch val := v.(type) {
		case []byte:
			return pq.ByteArrayValue(val).Level(0, defLevel, colIndex)
		case string:
			return pq.ByteArrayValue([]byte(val)).Level(0, defLevel, colIndex)
		default:
			return pq.ByteArrayValue(nil).Level(0, defLevel, colIndex)
		}
	default:
		return pq.ByteArrayValue([]byte(fmt.Sprintf("%v", v))).Level(0, defLevel, colIndex)
	}
}

// parquetValueToGo converts a parquet.Value to a Go value based on column type.
func parquetValueToGo(col domain.ColumnInfo, v pq.Value) interface{} {
	if v.IsNull() {
		return nil
	}

	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		if strings.ToLower(col.Type) == "int64" {
			return int64(v.Int32())
		}
		return int64(v.Int32()) // Store as int64 for consistency
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		if strings.ToLower(col.Type) == "float64" || strings.ToLower(col.Type) == "double" {
			return float64(v.Float())
		}
		return float64(v.Float()) // Store as float64 for consistency
	case pq.Double:
		return v.Double()
	case pq.ByteArray:
		data := v.ByteArray()
		switch strings.ToLower(col.Type) {
		case "string", "varchar", "text", "":
			return string(data)
		default:
			// Make a copy of the byte array
			cp := make([]byte, len(data))
			copy(cp, data)
			return cp
		}
	default:
		return string(v.ByteArray())
	}
}
