package generated

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// TestNewVirtualCalculatorWithCache 测试使用缓存创建计算器
func TestNewVirtualCalculatorWithCache(t *testing.T) {
	cache := NewExpressionCache()
	calc := NewVirtualCalculatorWithCache(cache)
	
	assert.NotNil(t, calc)
	assert.Equal(t, cache, calc.exprCache)
}

// TestNewGeneratedColumnEvaluatorWithCache 测试使用缓存创建求值器
func TestNewGeneratedColumnEvaluatorWithCache(t *testing.T) {
	cache := NewExpressionCache()
	evaluator := NewGeneratedColumnEvaluatorWithCache(cache)
	
	assert.NotNil(t, evaluator)
	assert.NotNil(t, evaluator.functionAPI)
}

// TestGetVirtualColumnNames 测试获取所有 VIRTUAL 列名称
func TestGetVirtualColumnNames(t *testing.T) {
	calc := NewVirtualCalculator()
	
	schema := &domain.TableInfo{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", IsGenerated: false},
			{Name: "stored", Type: "INT", IsGenerated: true, GeneratedType: "STORED"},
			{Name: "virtual1", Type: "INT", IsGenerated: true, GeneratedType: "VIRTUAL"},
			{Name: "virtual2", Type: "INT", IsGenerated: true, GeneratedType: "VIRTUAL"},
		},
	}
	
	names := calc.GetVirtualColumnNames(schema)
	assert.Len(t, names, 2)
	assert.Contains(t, names, "virtual1")
	assert.Contains(t, names, "virtual2")
}

// TestCalculateColumnErrorCases 测试 CalculateColumn 的错误情况
func TestCalculateColumnErrorCases(t *testing.T) {
	calc := NewVirtualCalculator()
	schema := &domain.TableInfo{
		Columns: []domain.ColumnInfo{
			{Name: "price", Type: "DECIMAL"},
			{Name: "quantity", Type: "INT"},
			{Name: "total", Type: "DECIMAL", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: "price * quantity"},
		},
	}
	row := domain.Row{"price": 10.5, "quantity": int64(2)}
	
	tests := []struct{
		name string
		col *domain.ColumnInfo
		expectError bool
	}{
		{"nil column", nil, true},
		{"non-virtual column", &schema.Columns[0], true},
		{"stored column", &domain.ColumnInfo{Name: "x", IsGenerated: true, GeneratedType: "STORED"}, true},
		{"empty expression", &domain.ColumnInfo{Name: "x", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: ""}, true},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calc.CalculateColumn(tt.col, row, schema)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCastTypeCoverage 测试类型转换的覆盖率
func TestCastTypeCoverage(t *testing.T) {
	tests := []struct{
		name string
		value interface{}
		targetType string
		expectError bool
	}{
		// castToInt 的更多测试
		{"int8 to int", int8(42), "INT", false},
		{"int16 to int", int16(42), "INT", false},
		{"int32 to int", int32(42), "INT", false},
		{"uint to int", uint(42), "INT", false},
		{"uint8 to int", uint8(42), "INT", false},
		{"uint16 to int", uint16(42), "INT", false},
		{"uint32 to int", uint32(42), "INT", false},
		{"uint64 to int", uint64(42), "INT", false},
		{"float32 to int", float32(3.14), "INT", false},
		{"string to int error", "abc", "INT", true},
		
		// castToFloat 的更多测试
		{"int to float", int(42), "FLOAT", false},
		{"int8 to float", int8(42), "FLOAT", false},
		{"int16 to float", int16(42), "FLOAT", false},
		{"int32 to float", int32(42), "FLOAT", false},
		{"int64 to float", int64(42), "FLOAT", false},
		{"uint to float", uint(42), "FLOAT", false},
		{"uint8 to float", uint8(42), "FLOAT", false},
		{"uint16 to float", uint16(42), "FLOAT", false},
		{"uint32 to float", uint32(42), "FLOAT", false},
		{"uint64 to float", uint64(42), "FLOAT", false},
		{"float32 to float", float32(3.14), "FLOAT", false},
		{"string to float error", "abc", "FLOAT", true},
		
		// 未知类型
		{"unknown type", struct{}{}, "UNKNOWN", false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CastToType(tt.value, tt.targetType)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

// TestHasVirtualColumnsCoverage 测试 HasVirtualColumns 的边界情况
func TestHasVirtualColumnsCoverage(t *testing.T) {
	calc := NewVirtualCalculator()
	
	// 空 schema
	emptySchema := &domain.TableInfo{}
	assert.False(t, calc.HasVirtualColumns(emptySchema))
	
	// 只有非 VIRTUAL 列
	noVirtualSchema := &domain.TableInfo{
		Columns: []domain.ColumnInfo{
			{Name: "id", IsGenerated: false},
			{Name: "stored", IsGenerated: true, GeneratedType: "STORED"},
		},
	}
	assert.False(t, calc.HasVirtualColumns(noVirtualSchema))
}

// TestGetColumnInfoCoverage 测试 getColumnInfo 的边界情况
func TestGetColumnInfoCoverage(t *testing.T) {
	calc := NewVirtualCalculator()
	
	schema := &domain.TableInfo{
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "INT"},
			{Name: "col2", Type: "STRING"},
		},
	}
	
	// 找到存在的列
	col1 := calc.getColumnInfo("col1", schema)
	assert.NotNil(t, col1)
	assert.Equal(t, "col1", col1.Name)
	
	// 找到存在的列
	col2 := calc.getColumnInfo("col2", schema)
	assert.NotNil(t, col2)
	assert.Equal(t, "col2", col2.Name)
	
	// 不存在的列
	col3 := calc.getColumnInfo("col3", schema)
	assert.Nil(t, col3)
}

// TestParseNumericLiteral 测试数字字面量解析
func TestParseNumericLiteral(t *testing.T) {
	tests := []struct{
		input string
		expected interface{}
		expectError bool
	}{
		{"123", float64(123), false}, // parseNumericLiteral 返回 float64
		{"-123", float64(-123), false},
		{"123.45", 123.45, false},
		{"-123.45", -123.45, false},
		{"abc", nil, true},
		{"", nil, true},
	}
	
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseNumericLiteral(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestToFloat64Coverage 测试 toFloat64 的边界情况
func TestToFloat64Coverage(t *testing.T) {
	tests := []struct{
		input interface{}
		expected float64
		ok bool
	}{
		// 已经测试过的类型
		{int(42), 42.0, true},
		{int8(42), 42.0, true},
		{int16(42), 42.0, true},
		{int32(42), 42.0, true},
		{int64(42), 42.0, true},
		{uint(42), 42.0, true},
		{uint8(42), 42.0, true},
		{uint16(42), 42.0, true},
		{uint32(42), 42.0, true},
		{uint64(42), 42.0, true},
		{float64(3.14), 3.14, true},
		
		// 边界情况
		{"123", 123.0, true},
		{"123.45", 123.45, true},
		{"abc", 0.0, false},
		{"", 0.0, false},
		{struct{}{}, 0.0, false},
	}
	
	for i, tt := range tests {
		t.Run(string(rune(i)), func(t *testing.T) {
			result, ok := toFloat64(tt.input)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				// 对于浮点数，使用近似比较
				if tt.input == float32(3.14) {
					assert.InDelta(t, tt.expected, result, 0.001)
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}
}
