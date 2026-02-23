package generated

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestIsGeneratedColumn(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
			{Name: "col3", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1+10", GeneratedDepends: []string{"col1"}},
		},
	}

	assert.True(t, IsGeneratedColumn("col2", schema))
	assert.True(t, IsGeneratedColumn("col3", schema))
	assert.False(t, IsGeneratedColumn("col1", schema))
	assert.False(t, IsGeneratedColumn("col4", schema))
}

func TestGetAffectedGeneratedColumns_Simple(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
		},
	}

	affected := GetAffectedGeneratedColumns([]string{"col1"}, schema)
	assert.Equal(t, []string{"col2"}, affected)
}

func TestGetAffectedGeneratedColumns_Multiple(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
			{Name: "col3", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1+10", GeneratedDepends: []string{"col1"}},
		},
	}

	affected := GetAffectedGeneratedColumns([]string{"col1"}, schema)
	assert.Contains(t, affected, "col2")
	assert.Contains(t, affected, "col3")
}

func TestGetAffectedGeneratedColumns_Cascading(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
			{Name: "col3", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col2+col1", GeneratedDepends: []string{"col2", "col1"}},
		},
	}

	affected := GetAffectedGeneratedColumns([]string{"col1"}, schema)
	assert.Contains(t, affected, "col2")
	assert.Contains(t, affected, "col3")
}

func TestGetAffectedGeneratedColumns_NoAffected(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true},
		},
	}

	affected := GetAffectedGeneratedColumns([]string{"col1"}, schema)
	assert.Equal(t, 0, len(affected))
}

func TestSetGeneratedColumnsToNULL(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
		},
	}

	row := domain.Row{
		"col1": 10,
		"col2": 20,
	}

	result := SetGeneratedColumnsToNULL(row, schema)
	assert.Equal(t, 10, result["col1"])
	assert.Nil(t, result["col2"])
}

func TestFilterGeneratedColumns(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
		},
	}

	row := domain.Row{
		"col1": 10,
		"col2": 20,
	}

	result := FilterGeneratedColumns(row, schema)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 10, result["col1"])
	assert.NotContains(t, result, "col2")
}

func TestGetGeneratedColumnValues(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
		},
	}

	row := domain.Row{
		"col1": 10,
		"col2": 20,
	}

	result := GetGeneratedColumnValues(row, schema)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 20, result["col2"])
	assert.NotContains(t, result, "col1")
}

func TestRemoveGeneratedColumns(t *testing.T) {
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT", Nullable: true},
			{Name: "col2", Type: "INT", Nullable: true, IsGenerated: true, GeneratedExpr: "col1*2", GeneratedDepends: []string{"col1"}},
		},
	}

	row := domain.Row{
		"col1": 10,
		"col2": 20,
	}

	result := RemoveGeneratedColumns(row, schema)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 10, result["col1"])
	assert.NotContains(t, result, "col2")
}

func TestCastToInt(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected int64
		hasError bool
	}{
		{"int", 10, int64(10), false},
		{"int8", int8(10), int64(10), false},
		{"int64", int64(10), int64(10), false},
		{"float64", 10.5, int64(10), false},
		{"bool true", true, int64(1), false},
		{"bool false", false, int64(0), false},
		{"string valid", "10", int64(10), false},
		{"string invalid", "abc", int64(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToInt(tt.value)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCastToFloat(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected float64
		hasError bool
	}{
		{"int", 10, 10.0, false},
		{"float64", 10.5, 10.5, false},
		{"bool true", true, 1.0, false},
		{"bool false", false, 0.0, false},
		{"string valid", "10.5", 10.5, false},
		{"string invalid", "abc", 0.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToFloat(tt.value)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCastToString(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int", 10, "10"},
		{"float64", 10.5, "10.500000"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"string", "hello", "hello"},
		{"nil", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToString(tt.value)
			assert.NoError(t, err)
			// nil 应该返回空字符串
			if tt.value == nil {
				assert.Equal(t, tt.expected, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCastToBool(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"int non-zero", 10, true},
		{"int zero", 0, false},
		{"float64 non-zero", 10.5, true},
		{"float64 zero", 0.0, false},
		{"bool true", true, true},
		{"bool false", false, false},
		{"string true", "true", true},
		{"string false", "false", false},
		{"string 1", "1", true},
		{"string 0", "0", false},
		{"empty string", "", false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToBool(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCastToType_DefaultType 测试默认类型分支
func TestCastToType_DefaultType(t *testing.T) {
	// 测试未知类型，应该返回原值
	result, err := CastToType(123, "UNKNOWN_TYPE")
	assert.NoError(t, err)
	assert.Equal(t, 123, result)
}

// TestCastToType_DateTime 测试时间类型转换
func TestCastToType_DateTime(t *testing.T) {
	result, err := CastToType("2024-01-01", "DATETIME")
	assert.NoError(t, err)
	assert.Equal(t, "2024-01-01", result)
}

// TestCastToType_Varchar 测试 VARCHAR 类型转换
func TestCastToType_Varchar(t *testing.T) {
	result, err := CastToType(123, "VARCHAR")
	assert.NoError(t, err)
	assert.Equal(t, "123", result)
}

// TestCastToType_Decimal 测试 DECIMAL 类型转换
func TestCastToType_Decimal(t *testing.T) {
	result, err := CastToType("123.45", "DECIMAL")
	assert.NoError(t, err)
	assert.Equal(t, 123.45, result)
}

// TestCastToType_Nil 测试 NULL 值转换
func TestCastToType_Nil(t *testing.T) {
	result, err := CastToType(nil, "INT")
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestCastToString_Bytes 测试字节数组转字符串
func TestCastToString_Bytes(t *testing.T) {
	result, err := castToString([]byte{'h', 'e', 'l', 'l', 'o'})
	assert.NoError(t, err)
	assert.Equal(t, "hello", result)
}

// TestCastToString_Nil 测试 nil 转字符串
func TestCastToString_Nil(t *testing.T) {
	result, err := castToString(nil)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

// TestCastToString_MoreTypes 测试更多类型转字符串
func TestCastToString_MoreTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"int", 123, "123"},
		{"int64", int64(456), "456"},
		{"float32", float32(3.14), "3.140000"},
		{"float64", float64(2.718), "2.718000"},
		{"int8", int8(100), "100"},
		{"uint", uint(999), "999"},
		{"uint64", uint64(888), "888"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToString(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

// TestCastToType_MoreVariants 测试更多类型变体
func TestCastToType_MoreVariants(t *testing.T) {
	tests := []struct {
		name       string
		value      interface{}
		targetType string
		expectErr  bool
	}{
		{"TINYINT", 123, "TINYINT", false},
		{"SMALLINT", 123, "SMALLINT", false},
		{"MEDIUMINT", 123, "MEDIUMINT", false},
		{"BIGINT", 123, "BIGINT", false},
		{"NUMERIC", 123.45, "NUMERIC", false},
		{"REAL", 123.45, "REAL", false},
		{"TEXT", 123, "TEXT", false},
		{"TINYTEXT", 123, "TINYTEXT", false},
		{"MEDIUMTEXT", 123, "MEDIUMTEXT", false},
		{"LONGTEXT", 123, "LONGTEXT", false},
		{"BOOL", 1, "BOOL", false},
		{"DATE", "2024-01-01", "DATE", false},
		{"TIMESTAMP", "2024-01-01", "TIMESTAMP", false},
		{"TIME", "12:30:00", "TIME", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CastToType(tt.value, tt.targetType)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

// TestCastToFloat_MoreIntTypes 测试更多整数类型转浮点
func TestCastToFloat_MoreIntTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  float64
	}{
		{"int8", int8(42), 42.0},
		{"int16", int16(42), 42.0},
		{"int32", int32(42), 42.0},
		{"int", 42, 42.0},
		{"uint8", uint8(42), 42.0},
		{"uint16", uint16(42), 42.0},
		{"uint32", uint32(42), 42.0},
		{"uint64", uint64(42), 42.0},
		{"uint", uint(42), 42.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := castToFloat(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}
