package slice

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ============ resolveFieldMappings 测试 ============

func TestResolveFieldMappings_DbTag(t *testing.T) {
	type User struct {
		ID   int    `db:"user_id"`
		Name string `db:"user_name"`
		Age  int    `db:"age"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 3)
	assert.Equal(t, "user_id", mappings[0].ColumnName)
	assert.Equal(t, "user_name", mappings[1].ColumnName)
	assert.Equal(t, "age", mappings[2].ColumnName)
	assert.Equal(t, 0, mappings[0].FieldIndex)
	assert.Equal(t, 1, mappings[1].FieldIndex)
	assert.Equal(t, 2, mappings[2].FieldIndex)
}

func TestResolveFieldMappings_JsonFallback(t *testing.T) {
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 2)
	assert.Equal(t, "id", mappings[0].ColumnName)
	assert.Equal(t, "name", mappings[1].ColumnName)
}

func TestResolveFieldMappings_DbPriority(t *testing.T) {
	// db tag 优先于 json tag
	type User struct {
		ID int `db:"user_id" json:"id"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 1)
	assert.Equal(t, "user_id", mappings[0].ColumnName)
}

func TestResolveFieldMappings_NoTag(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 2)
	assert.Equal(t, "ID", mappings[0].ColumnName)
	assert.Equal(t, "Name", mappings[1].ColumnName)
}

func TestResolveFieldMappings_DbSkip(t *testing.T) {
	type User struct {
		ID       int    `db:"id"`
		Name     string `db:"name"`
		Internal string `db:"-"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 3)
	assert.False(t, mappings[0].Skip)
	assert.False(t, mappings[1].Skip)
	assert.True(t, mappings[2].Skip)
}

func TestResolveFieldMappings_JsonSkip(t *testing.T) {
	type User struct {
		ID       int    `json:"id"`
		Internal string `json:"-"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 2)
	assert.False(t, mappings[0].Skip)
	assert.True(t, mappings[1].Skip)
}

func TestResolveFieldMappings_TagWithOptions(t *testing.T) {
	type User struct {
		Name string `db:"name,omitempty"`
		Age  int    `json:"age,string"`
	}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 2)
	assert.Equal(t, "name", mappings[0].ColumnName)
	assert.Equal(t, "age", mappings[1].ColumnName)
}

func TestResolveFieldMappings_UnexportedFieldsSkipped(t *testing.T) {
	type User struct {
		ID       int `db:"id"`
		internal string
	}
	_ = User{internal: "hidden"}
	mappings := resolveFieldMappings(reflect.TypeOf(User{}))
	assert.Len(t, mappings, 1)
	assert.Equal(t, "id", mappings[0].ColumnName)
}

func TestResolveFieldMappings_Pointer(t *testing.T) {
	type User struct {
		ID int `db:"id"`
	}
	// 传入指针类型也应能正确解析
	mappings := resolveFieldMappings(reflect.TypeOf(&User{}))
	assert.Len(t, mappings, 1)
	assert.Equal(t, "id", mappings[0].ColumnName)
}

func TestResolveFieldMappings_NonStruct(t *testing.T) {
	mappings := resolveFieldMappings(reflect.TypeOf(42))
	assert.Nil(t, mappings)
}

// ============ parseTagName 测试 ============

func TestParseTagName(t *testing.T) {
	tests := []struct {
		tag      string
		expected string
	}{
		{"name", "name"},
		{"name,omitempty", "name"},
		{"-", "-"},
		{"-,", "-"},
		{"", ""},
		{",omitempty", ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, parseTagName(tt.tag), "tag=%q", tt.tag)
	}
}

// ============ 增强类型映射测试 ============

func TestGetFieldType_TimeTime(t *testing.T) {
	assert.Equal(t, "datetime", getFieldType(reflect.TypeOf(time.Time{})))
}

func TestGetFieldType_TimeTimePtr(t *testing.T) {
	var tp *time.Time
	assert.Equal(t, "datetime", getFieldType(reflect.TypeOf(tp)))
}

func TestGetFieldType_ByteSlice(t *testing.T) {
	assert.Equal(t, "blob", getFieldType(reflect.TypeOf([]byte{})))
}

func TestGetFieldType_BasicTypes(t *testing.T) {
	assert.Equal(t, "int64", getFieldType(reflect.TypeOf(0)))
	assert.Equal(t, "int64", getFieldType(reflect.TypeOf(int64(0))))
	assert.Equal(t, "int64", getFieldType(reflect.TypeOf(uint(0))))
	assert.Equal(t, "float64", getFieldType(reflect.TypeOf(0.0)))
	assert.Equal(t, "float64", getFieldType(reflect.TypeOf(float32(0))))
	assert.Equal(t, "bool", getFieldType(reflect.TypeOf(false)))
	assert.Equal(t, "string", getFieldType(reflect.TypeOf("")))
}

func TestInferColumnType_TimeTime(t *testing.T) {
	assert.Equal(t, "datetime", inferColumnType(time.Now()))
}

func TestInferColumnType_TimeTimePtr(t *testing.T) {
	now := time.Now()
	assert.Equal(t, "datetime", inferColumnType(&now))
}

func TestInferColumnType_ByteSlice(t *testing.T) {
	assert.Equal(t, "blob", inferColumnType([]byte{1, 2, 3}))
}

func TestInferColumnType_Nil(t *testing.T) {
	assert.Equal(t, "any", inferColumnType(nil))
}

func TestInferColumnType_BasicTypes(t *testing.T) {
	assert.Equal(t, "int64", inferColumnType(42))
	assert.Equal(t, "int64", inferColumnType(int64(42)))
	assert.Equal(t, "int64", inferColumnType(uint(42)))
	assert.Equal(t, "float64", inferColumnType(3.14))
	assert.Equal(t, "float64", inferColumnType(float32(3.14)))
	assert.Equal(t, "bool", inferColumnType(true))
	assert.Equal(t, "string", inferColumnType("hello"))
}

// ============ Option 测试 ============

func TestWithWritable(t *testing.T) {
	cfg := defaultConfig()
	assert.True(t, cfg.writable)

	WithWritable(false)(&cfg)
	assert.False(t, cfg.writable)
}

func TestWithMVCC(t *testing.T) {
	cfg := defaultConfig()
	assert.True(t, cfg.mvccSupported)

	WithMVCC(false)(&cfg)
	assert.False(t, cfg.mvccSupported)
}

func TestWithDatabaseName(t *testing.T) {
	cfg := defaultConfig()
	assert.Equal(t, "default", cfg.databaseName)

	WithDatabaseName("mydb")(&cfg)
	assert.Equal(t, "mydb", cfg.databaseName)
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	assert.Equal(t, "default", cfg.databaseName)
	assert.True(t, cfg.writable)
	assert.True(t, cfg.mvccSupported)
}

// ============ 辅助函数测试 ============

func TestIsMapStringAny(t *testing.T) {
	assert.True(t, isMapStringAny(map[string]any{"a": 1}))
	assert.True(t, isMapStringAny(map[string]interface{}{"a": 1}))
	assert.False(t, isMapStringAny(nil))
	assert.False(t, isMapStringAny(42))
	assert.False(t, isMapStringAny(map[int]string{1: "a"}))
}

func TestIsMapStringAnyType(t *testing.T) {
	assert.True(t, isMapStringAnyType(reflect.TypeOf(map[string]any{})))
	assert.False(t, isMapStringAnyType(reflect.TypeOf(42)))
	assert.False(t, isMapStringAnyType(reflect.TypeOf(map[int]string{})))
}

func TestGetStructFields(t *testing.T) {
	type User struct {
		ID       int
		Name     string
		internal string
	}
	_ = User{internal: "hidden"}
	fields := getStructFields(reflect.TypeOf(User{}))
	assert.Len(t, fields, 2)
	assert.Equal(t, "ID", fields[0].Name)
	assert.Equal(t, "Name", fields[1].Name)
}

func TestIsFieldNullable(t *testing.T) {
	assert.True(t, isFieldNullable(reflect.TypeOf((*int)(nil))))
	assert.False(t, isFieldNullable(reflect.TypeOf(0)))
	assert.False(t, isFieldNullable(reflect.TypeOf("")))
}
