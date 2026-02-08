package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestConvertConditionsToFilters tests converting conditions to filters
func TestConvertConditionsToFilters(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name     string
		conds    []*parser.Expression
		expected int
	}{
		{
			name:     "nil conditions",
			conds:    []*parser.Expression{nil},
			expected: 0,
		},
		{
			name: "single condition",
			conds: []*parser.Expression{
				{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				},
			},
			expected: 1,
		},
		{
			name: "multiple conditions",
			conds: []*parser.Expression{
				{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				},
				{
					Type:     parser.ExprTypeOperator,
					Operator: "gt",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 18},
				},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.convertConditionsToFilters(tt.conds)
			if len(result) != tt.expected {
				t.Errorf("Expected %d filters, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestExtractFiltersFromCondition tests extracting filters from conditions
func TestExtractFiltersFromCondition(t *testing.T) {
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
			name: "single filter",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
			},
			expected: 1,
		},
		{
			name: "AND condition - multiple filters",
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
					Operator: "gt",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 18},
				},
			},
			expected: 2,
		},
		{
			name: "OR condition - single filter (OR cannot be converted to simple filter)",
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
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 2},
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.extractFiltersFromCondition(tt.expr)
			if len(result) != tt.expected {
				t.Errorf("Expected %d filters, got %d", tt.expected, len(result))
			}
		})
	}
}

// TestConvertExpressionToFilter tests converting expression to filter
func TestConvertExpressionToFilter(t *testing.T) {
	optimizer := NewOptimizer(nil)

	tests := []struct {
		name     string
		expr     *parser.Expression
		wantNil  bool
	}{
		{
			name:    "nil expression",
			expr:    nil,
			wantNil: true,
		},
		{
			name: "non-operator expression",
			expr: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "id",
			},
			wantNil: true,
		},
		{
			name: "valid equality filter",
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
			wantNil: false,
		},
		{
			name: "valid greater than filter",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "age",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 18,
				},
			},
			wantNil: false,
		},
		{
			name: "operator with missing left operand",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
			},
			wantNil: true,
		},
		{
			name: "operator with missing right operand",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
			},
			wantNil: true,
		},
		{
			name: "left operand not a column",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 2,
				},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := optimizer.convertExpressionToFilter(tt.expr)
			if tt.wantNil {
				if result != nil {
					t.Errorf("Expected nil, got filter: %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("Expected filter, got nil")
				} else {
					// Verify filter has required fields
					if result.Field == "" {
						t.Errorf("Expected Field to be set")
					}
					if result.Operator == "" {
						t.Errorf("Expected Operator to be set")
					}
				}
			}
		})
	}
}

// TestNestedANDConditions tests deeply nested AND conditions
func TestNestedANDConditions(t *testing.T) {
	optimizer := NewOptimizer(nil)

	// Create a deeply nested AND expression
	// (id = 1 AND (name = 'test' AND (age > 18 AND active = true)))
	expr := &parser.Expression{
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
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "name"},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "test"},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "and",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "gt",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 18},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "eq",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "active"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: true},
				},
			},
		},
	}

	result := optimizer.extractFiltersFromCondition(expr)
	if len(result) != 4 {
		t.Errorf("Expected 4 filters from nested AND, got %d", len(result))
	}
}
