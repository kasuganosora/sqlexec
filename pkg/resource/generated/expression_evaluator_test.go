package generated

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// TestEvaluateExpressionWithConstants 测试包含常量的表达式求值
func TestEvaluateExpressionWithConstants(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()

	t.Run("arithmetic with constants", func(t *testing.T) {
		testCases := []struct {
			expr     string
			row      domain.Row
			expected interface{}
			err      bool
		}{
			{
				expr:     "2 * (width + height)",
				row:      domain.Row{"width": 5.0, "height": 3.0},
				expected: 16.0,
				err:       false,
			},
			{
				expr:     "2 * width + height",
				row:      domain.Row{"width": 5.0, "height": 3.0},
				expected: 13.0,
				err:       false,
			},
			{
				expr:     "(width + height) * 2",
				row:      domain.Row{"width": 5.0, "height": 3.0},
				expected: 16.0,
				err:       false,
			},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, tc.row, nil)
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		}
	})

	t.Run("mixed numeric constants", func(t *testing.T) {
		row := domain.Row{
			"price":    10.50,
			"quantity": 5,
			"discount":  0.1,
		}

		testCases := []struct {
			expr     string
			expected interface{}
		}{
			{"price * quantity * (1 - discount)", 10.50 * 5 * 0.9},
			{"2 * (price * quantity)", 2 * (10.50 * 5)},
			{"100 + price", 110.50},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})

	t.Run("decimal literals in expressions", func(t *testing.T) {
		row := domain.Row{
			"sidea": 3.0,
			"sideb": 4.0,
		}

		result, err := evaluator.Evaluate("sidea + sideb > 5.0", row, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, result)
	})
}

// TestEvaluateBooleanExpressions 测试布尔表达式求值
func TestEvaluateBooleanExpressions(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()

	t.Run("comparison operations", func(t *testing.T) {
		row := domain.Row{
			"sidea": 3.0,
			"sideb": 4.0,
			"sidec": 5.0,
		}

		testCases := []struct {
			expr     string
			expected interface{}
		}{
			{"sidea + sideb > sidec", true},
			{"sidea + sidec > sideb", true},
			{"sideb + sidec > sidea", true},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})

	t.Run("boolean literals", func(t *testing.T) {
		row := domain.Row{"value": 10}

		testCases := []struct {
			expr     string
			expected interface{}
		}{
			{"true", true},
			{"false", false},
			{"TRUE", true},
			{"FALSE", false},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})
}

// TestEvaluateWithComplexParentheses 测试复杂括号表达式
func TestEvaluateWithComplexParentheses(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()

	t.Run("nested parentheses", func(t *testing.T) {
		row := domain.Row{
			"a": 2,
			"b": 3,
			"c": 4,
		}

		testCases := []struct {
			expr     string
			expected interface{}
		}{
			{"((a + b) * c)", 20.0},
			{"(a + (b * c))", 14.0},
			{"a * (b + c)", 14.0},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})

	t.Run("operators with different precedence", func(t *testing.T) {
		row := domain.Row{
			"width": 5,
			"height": 3,
			"depth": 2,
		}

		testCases := []struct {
			expr     string
			expected interface{}
		}{
			{"width * height + depth", 17},
			{"width * (height + depth)", 25},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})
}

// TestEvaluateWithNullHandling 测试NULL处理
func TestEvaluateWithNullHandling(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()

	t.Run("NULL propagation in arithmetic", func(t *testing.T) {
		testCases := []struct {
			expr     string
			row      domain.Row
			expected interface{}
		}{
			{
				expr:     "price * quantity",
				row:      domain.Row{"price": 100.0, "quantity": nil},
				expected: nil, // NULL * anything = NULL
			},
			{
				expr:     "price + quantity",
				row:      domain.Row{"price": 100.0, "quantity": nil},
				expected: nil, // NULL + anything = NULL
			},
		}

		for _, tc := range testCases {
			result, err := evaluator.Evaluate(tc.expr, tc.row, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})

	t.Run("division by zero", func(t *testing.T) {
		row := domain.Row{"value": 100, "divisor": 0}

		_, err := evaluator.Evaluate("value / divisor", row, nil)
		assert.Error(t, err) // 除以零应该返回错误
	})
}

// TestEvaluateWithDecimalTypeConversion 测试DECIMAL类型转换
func TestEvaluateWithDecimalTypeConversion(t *testing.T) {
	evaluator := NewGeneratedColumnEvaluator()
	schema := &domain.TableInfo{
		Name: "test",
		Columns: []domain.ColumnInfo{
			{Name: "price", Type: "DECIMAL(10,2)", Nullable: true},
			{Name: "quantity", Type: "INT", Nullable: true},
			{Name: "total", Type: "DECIMAL(10,2)", IsGenerated: true, GeneratedType: "VIRTUAL", GeneratedExpr: "price * quantity"},
		},
	}

	t.Run("decimal multiplication should produce decimal", func(t *testing.T) {
		row := domain.Row{
			"price":    10.50,
			"quantity": 5,
		}

		result, err := evaluator.Evaluate("price * quantity", row, schema)
		assert.NoError(t, err)
		assert.Equal(t, 52.5, result)

		// Cast to column type
		castResult, err := CastToType(result, "DECIMAL(10,2)")
		assert.NoError(t, err)
		assert.Equal(t, 52.5, castResult)
	})
}
