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
		expected int
		hasError bool
	}{
		{"int", 10, 10, false},
		{"int8", int8(10), 10, false},
		{"int64", int64(10), 10, false},
		{"float64", 10.5, 10, false},
		{"bool true", true, 1, false},
		{"bool false", false, 0, false},
		{"string valid", "10", 10, false},
		{"string invalid", "abc", 0, true},
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
