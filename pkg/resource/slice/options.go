package slice

import (
	"reflect"
	"strings"
	"time"
)

// Option 配置 SliceAdapter 的选项函数
type Option func(*sliceConfig)

type sliceConfig struct {
	databaseName  string
	writable      bool
	mvccSupported bool
}

func defaultConfig() sliceConfig {
	return sliceConfig{
		databaseName:  "default",
		writable:      true,
		mvccSupported: true,
	}
}

// WithWritable 设置是否可写
func WithWritable(w bool) Option {
	return func(c *sliceConfig) { c.writable = w }
}

// WithMVCC 设置是否支持 MVCC
func WithMVCC(m bool) Option {
	return func(c *sliceConfig) { c.mvccSupported = m }
}

// WithDatabaseName 设置数据库名
func WithDatabaseName(name string) Option {
	return func(c *sliceConfig) { c.databaseName = name }
}

// ============ Struct Tag 解析 ============

// fieldMapping 描述一个 struct 字段到列的映射关系
type fieldMapping struct {
	ColumnName string // 列名（从 tag 或字段名解析）
	FieldIndex int    // 字段在 struct 中的索引（用于 reflect.Value.Field()）
	Skip       bool   // 是否跳过此字段（tag 为 "-"）
}

// resolveFieldMappings 解析 struct 类型的字段映射
// 优先级：db tag > json tag > 字段名
// 支持 db:"-" 或 json:"-" 跳过字段
// 仅处理可导出字段
func resolveFieldMappings(t reflect.Type) []fieldMapping {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	mappings := make([]fieldMapping, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// 跳过非导出字段
		if field.PkgPath != "" {
			continue
		}

		fm := fieldMapping{
			FieldIndex: i,
			ColumnName: field.Name, // 默认使用字段名
		}

		// 优先检查 db tag
		if dbTag, ok := field.Tag.Lookup("db"); ok {
			name := parseTagName(dbTag)
			if name == "-" {
				fm.Skip = true
				mappings = append(mappings, fm)
				continue
			}
			if name != "" {
				fm.ColumnName = name
			}
		} else if jsonTag, ok := field.Tag.Lookup("json"); ok {
			// 其次检查 json tag
			name := parseTagName(jsonTag)
			if name == "-" {
				fm.Skip = true
				mappings = append(mappings, fm)
				continue
			}
			if name != "" {
				fm.ColumnName = name
			}
		}

		mappings = append(mappings, fm)
	}
	return mappings
}

// parseTagName 从 tag 值中提取名称部分（逗号前的部分）
// 例如 "name,omitempty" → "name"，"-" → "-"，"" → ""
func parseTagName(tag string) string {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx]
	}
	return tag
}

// ============ 增强类型映射 ============

var timeType = reflect.TypeOf(time.Time{})

// getFieldType 获取 struct 字段的列类型
func getFieldType(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 特殊类型优先检查
	if t == timeType {
		return "datetime"
	}
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "blob"
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int64"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int64"
	case reflect.Float32, reflect.Float64:
		return "float64"
	case reflect.Bool:
		return "bool"
	case reflect.String:
		return "string"
	case reflect.Interface:
		return "any"
	default:
		return "string"
	}
}

// inferColumnType 从值推断列类型
func inferColumnType(value interface{}) string {
	if value == nil {
		return "any"
	}

	// 特殊类型优先检查
	if _, ok := value.(time.Time); ok {
		return "datetime"
	}
	if _, ok := value.(*time.Time); ok {
		return "datetime"
	}
	if _, ok := value.([]byte); ok {
		return "blob"
	}

	switch value.(type) {
	case int, int8, int16, int32, int64:
		return "int64"
	case uint, uint8, uint16, uint32, uint64:
		return "int64"
	case float32, float64:
		return "float64"
	case bool:
		return "bool"
	case string:
		return "string"
	default:
		return "any"
	}
}

// isFieldNullable 检查字段是否可空
func isFieldNullable(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr
}

// isMapStringAny 检查值是否为 map[string]any
func isMapStringAny(v interface{}) bool {
	if v == nil {
		return false
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String
}

// isMapStringAnyType 检查类型是否为 map[string]any
func isMapStringAnyType(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String
}

// getStructFields 获取结构体的所有可导出字段
func getStructFields(t reflect.Type) []reflect.StructField {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	fields := make([]reflect.StructField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath == "" { // 可导出字段
			fields = append(fields, field)
		}
	}
	return fields
}
