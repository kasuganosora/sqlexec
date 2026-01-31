package generated

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexHelper(t *testing.T) {
	helper := NewIndexHelper()
	assert.NotNil(t, helper)
	assert.NotNil(t, helper.evaluator)
}

func TestNewIndexHelperWithEvaluator(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()
	helper := NewIndexHelperWithEvaluator(evaluator)
	assert.NotNil(t, helper)
	assert.Equal(t, evaluator, helper.evaluator)
}

func TestIndexHelper_CanIndexGeneratedColumn_STORED(t *testing.T) {
	helper := NewIndexHelper()

	col := &domain.ColumnInfo{
		Name:         "total",
		Type:         "INT",
		IsGenerated:  true,
		GeneratedType: "STORED",
		GeneratedExpr: "price * quantity",
	}

	canIndex, reason := helper.CanIndexGeneratedColumn(col)
	assert.True(t, canIndex)
	assert.Empty(t, reason)
}

func TestIndexHelper_CanIndexGeneratedColumn_VIRTUAL(t *testing.T) {
	helper := NewIndexHelper()

	col := &domain.ColumnInfo{
		Name:         "full_name",
		Type:         "VARCHAR",
		IsGenerated:  true,
		GeneratedType: "VIRTUAL",
		GeneratedExpr: "CONCAT(first_name, ' ', last_name)",
	}

	canIndex, reason := helper.CanIndexGeneratedColumn(col)
	// 简单表达式应该可索引
	assert.True(t, canIndex)
	assert.Empty(t, reason)
}

func TestIndexHelper_CanIndexGeneratedColumn_VIRTUAL_TooComplex(t *testing.T) {
	helper := NewIndexHelper()

	// 创建一个超长表达式
	longExpr := ""
	for i := 0; i < 1001; i++ {
		longExpr += "a"
	}

	col := &domain.ColumnInfo{
		Name:         "complex_col",
		Type:         "VARCHAR",
		IsGenerated:  true,
		GeneratedType: "VIRTUAL",
		GeneratedExpr: longExpr,
	}

	canIndex, reason := helper.CanIndexGeneratedColumn(col)
	assert.False(t, canIndex)
	assert.Contains(t, reason, "too complex")
}

func TestIndexHelper_CanIndexGeneratedColumn_NonGenerated(t *testing.T) {
	helper := NewIndexHelper()

	col := &domain.ColumnInfo{
		Name:        "name",
		Type:        "VARCHAR",
		IsGenerated: false,
	}

	canIndex, reason := helper.CanIndexGeneratedColumn(col)
	assert.False(t, canIndex)
	assert.Contains(t, reason, "not a generated column")
}

func TestIndexHelper_CanIndexGeneratedColumn_Nil(t *testing.T) {
	helper := NewIndexHelper()

	canIndex, reason := helper.CanIndexGeneratedColumn(nil)
	assert.False(t, canIndex)
	assert.Contains(t, reason, "not a generated column")
}

func TestIndexHelper_CanIndexGeneratedColumn_UnsupportedType(t *testing.T) {
	helper := NewIndexHelper()

	col := &domain.ColumnInfo{
		Name:         "test",
		Type:         "INT",
		IsGenerated:  true,
		GeneratedType: "UNKNOWN_TYPE",
		GeneratedExpr: "id * 2",
	}

	canIndex, reason := helper.CanIndexGeneratedColumn(col)
	assert.False(t, canIndex)
	assert.Contains(t, reason, "unsupported generated column type")
}

func TestIndexHelper_GetIndexValueForGenerated_STORED(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "price", Type: "DECIMAL"},
			{Name: "quantity", Type: "INT"},
			{Name: "total", Type: "DECIMAL", IsGenerated: true, GeneratedType: "STORED"},
		},
	}

	row := domain.Row{
		"id":       1,
		"price":    10.50,
		"quantity": 2,
		"total":    21.00,
	}

	val, err := helper.GetIndexValueForGenerated("total", row, schema)
	require.NoError(t, err)
	assert.Equal(t, 21.00, val)
}

func TestIndexHelper_GetIndexValueForGenerated_NonGenerated(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "price", Type: "DECIMAL"},
		},
	}

	row := domain.Row{
		"id":    1,
		"price": 10.50,
	}

	val, err := helper.GetIndexValueForGenerated("price", row, schema)
	require.NoError(t, err)
	assert.Equal(t, 10.50, val)
}

func TestIndexHelper_GetIndexValueForGenerated_ColumnNotFound(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
	}

	row := domain.Row{"id": 1}

	val, err := helper.GetIndexValueForGenerated("nonexistent", row, schema)
	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Contains(t, err.Error(), "column not found")
}

func TestIndexHelper_ValidateIndexDefinition_NonGenerated(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
	}

	err := helper.ValidateIndexDefinition("id", schema)
	assert.NoError(t, err)
}

func TestIndexHelper_ValidateIndexDefinition_STORED(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "total", Type: "DECIMAL", IsGenerated: true, GeneratedType: "STORED"},
		},
	}

	err := helper.ValidateIndexDefinition("total", schema)
	assert.NoError(t, err)
}

func TestIndexHelper_ValidateIndexDefinition_VIRTUAL(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "full_name", Type: "VARCHAR", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: "CONCAT(first, ' ', last)"},
		},
	}

	err := helper.ValidateIndexDefinition("full_name", schema)
	assert.NoError(t, err)
}

func TestIndexHelper_ValidateIndexDefinition_VIRTUAL_TooComplex(t *testing.T) {
	helper := NewIndexHelper()

	longExpr := ""
	for i := 0; i < 1001; i++ {
		longExpr += "a"
	}

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "complex", Type: "VARCHAR", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: longExpr},
		},
	}

	err := helper.ValidateIndexDefinition("complex", schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-indexable")
}

func TestIndexHelper_ValidateIndexDefinition_ColumnNotFound(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
	}

	err := helper.ValidateIndexDefinition("nonexistent", schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column not found")
}

func TestIndexHelper_GetIndexableGeneratedColumns(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
			{Name: "total", Type: "DECIMAL", IsGenerated: true, GeneratedType: "STORED"},
			{Name: "full_name", Type: "VARCHAR", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: "first + last"},
			{Name: "complex", Type: "TEXT", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: "a" + string(make([]byte, 1000))},
		},
	}

	indexable := helper.GetIndexableGeneratedColumns(schema)
	
	// 应该包含total和full_name，不包含complex
	assert.Contains(t, indexable, "total")
	assert.Contains(t, indexable, "full_name")
	assert.NotContains(t, indexable, "complex")
	assert.Len(t, indexable, 2)
}

func TestIndexHelper_GetIndexableGeneratedColumns_None(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	indexable := helper.GetIndexableGeneratedColumns(schema)
	assert.Len(t, indexable, 0)
}

func TestIndexHelper_isIndexableExpression_Empty(t *testing.T) {
	helper := NewIndexHelper()
	assert.False(t, helper.isIndexableExpression(""))
}

func TestIndexHelper_isIndexableExpression_Simple(t *testing.T) {
	helper := NewIndexHelper()
	assert.True(t, helper.isIndexableExpression("a + b"))
	assert.True(t, helper.isIndexableExpression("CONCAT(a, b)"))
}

func TestIndexHelper_isIndexableExpression_TooLong(t *testing.T) {
	helper := NewIndexHelper()
	
	longExpr := ""
	for i := 0; i < 1001; i++ {
		longExpr += "a"
	}
	
	assert.False(t, helper.isIndexableExpression(longExpr))
}

func TestIndexHelper_getColumnInfo(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	col := helper.getColumnInfo("id", schema)
	assert.NotNil(t, col)
	assert.Equal(t, "id", col.Name)
	assert.Equal(t, "INT", col.Type)
}

func TestIndexHelper_getColumnInfo_NotFound(t *testing.T) {
	helper := NewIndexHelper()

	schema := &domain.TableInfo{
		Name: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
	}

	col := helper.getColumnInfo("nonexistent", schema)
	assert.Nil(t, col)
}
