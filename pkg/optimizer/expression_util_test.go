package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestExtractConditions tests condition extraction from expressions
func TestExtractConditions(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name     string
		expr     *parser.Expression
		expected int
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: 0,
		},
		{
			name: "simple condition",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
			},
			expected: 1,
		},
		{
			name: "AND condition",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "and",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "name"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "test"},
				},
			},
			expected: 2,
		},
		{
			name: "OR condition (should not be split)",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "name"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "test"},
				},
			},
			expected: 1,
		},
		{
			name: "nested AND conditions",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "and",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "and",
					Left: &parser.Expression{
						Type:     parser.ExprTypeOperator,
						Operator: "eq",
						Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
						Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
					},
					Right: &parser.Expression{
						Type:     parser.ExprTypeOperator,
						Operator: "eq",
						Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "name"},
						Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "test"},
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 30},
				},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.extractConditions(tt.expr)
			if len(result) != tt.expected {
				t.Errorf("Expected %d conditions, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestExtractAggFuncs tests aggregation function extraction
func TestExtractAggFuncs(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name     string
		cols     []parser.SelectColumn
		expected int
	}{
		{
			name:     "no columns",
			cols:     []parser.SelectColumn{},
			expected: 0,
		},
		{
			name: "wildcard column",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
			},
			expected: 0,
		},
		{
			name: "simple column without aggregation",
			cols: []parser.SelectColumn{
				{
					Expr: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
				},
			},
			expected: 0,
		},
		{
			name: "COUNT aggregation",
			cols: []parser.SelectColumn{
				{
					Expr: &parser.Expression{
						Type:     parser.ExprTypeFunction,
						Function: "COUNT",
						Value:    "COUNT",
					},
				},
			},
			expected: 1,
		},
		{
			name: "multiple aggregations",
			cols: []parser.SelectColumn{
				{
					Expr: &parser.Expression{
						Type:     parser.ExprTypeFunction,
						Function: "COUNT",
						Value:    "COUNT",
					},
				},
				{
					Expr: &parser.Expression{
						Type:     parser.ExprTypeFunction,
						Function: "SUM",
						Value:    "SUM",
					},
				},
				{
					Expr: &parser.Expression{
						Type:     parser.ExprTypeFunction,
						Function: "AVG",
						Value:    "AVG",
					},
				},
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.extractAggFuncs(tt.cols)
			if len(result) != tt.expected {
				t.Errorf("Expected %d aggregation functions, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestParseAggregationFunction tests parsing aggregation functions
func TestParseAggregationFunction(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name    string
		expr    *parser.Expression
		wantNil bool
	}{
		{
			name:    "nil expression",
			expr:    nil,
			wantNil: true,
		},
		{
			name: "COUNT function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "COUNT",
				Value:    "COUNT",
			},
			wantNil: false,
		},
		{
			name: "SUM function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "SUM",
				Value:    "SUM",
			},
			wantNil: false,
		},
		{
			name: "AVG function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "AVG",
				Value:    "AVG",
			},
			wantNil: false,
		},
		{
			name: "MAX function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "MAX",
				Value:    "MAX",
			},
			wantNil: false,
		},
		{
			name: "MIN function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "MIN",
				Value:    "MIN",
			},
			wantNil: false,
		},
		{
			name: "non-aggregation function",
			expr: &parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "NOW",
				Value:    "NOW",
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.parseAggregationFunction(tt.expr)
			if tt.wantNil && result != nil {
				t.Errorf("Expected nil, got aggregation item")
			}
			if !tt.wantNil && result == nil {
				t.Errorf("Expected aggregation item, got nil")
			}
			if !tt.wantNil && result != nil {
				if result.Alias == "" {
					// Check that Distinct is false by default
					if result.Distinct != false {
						t.Errorf("Expected Distinct to be false by default")
					}
				}
			}
		})
	}
}

// TestExpressionToString tests expression to string conversion
func TestExpressionToString(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name     string
		expr     *parser.Expression
		expected string
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: "",
		},
		{
			name: "column expression",
			expr: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "id",
			},
			expected: "id",
		},
		{
			name: "value expression - string",
			expr: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: "test",
			},
			expected: "test",
		},
		{
			name: "value expression - number",
			expr: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 42,
			},
			expected: "42",
		},
		{
			name: "operator expression",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
			},
			expected: "id eq 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.expressionToString(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestIsWildcard tests wildcard detection
func TestIsWildcard(t *testing.T) {
	tests := []struct {
		name     string
		cols     []parser.SelectColumn
		expected bool
	}{
		{
			name:     "empty columns",
			cols:     []parser.SelectColumn{},
			expected: false,
		},
		{
			name: "wildcard column",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
			},
			expected: true,
		},
		{
			name: "non-wildcard column",
			cols: []parser.SelectColumn{
				{IsWildcard: false},
			},
			expected: false,
		},
		{
			name: "multiple columns with wildcard",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
				{IsWildcard: false},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isWildcard(tt.cols)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestValueToExpression tests value to expression conversion
func TestValueToExpression(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name         string
		val          interface{}
		expectedType parser.ExprType
	}{
		{
			name:         "nil value",
			val:          nil,
			expectedType: "VALUE",
		},
		{
			name:         "int value",
			val:          42,
			expectedType: "VALUE",
		},
		{
			name:         "int64 value",
			val:          int64(42),
			expectedType: "VALUE",
		},
		{
			name:         "float value",
			val:          3.14,
			expectedType: "VALUE",
		},
		{
			name:         "string value",
			val:          "test",
			expectedType: "VALUE",
		},
		{
			name:         "bool value",
			val:          true,
			expectedType: "VALUE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.valueToExpression(tt.val)
			if result.Type != tt.expectedType {
				t.Errorf("Expected type '%s', got '%s'", tt.expectedType, result.Type)
			}
		})
	}
}
