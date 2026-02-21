package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewViewExecutor tests creating a view executor
func TestNewViewExecutor(t *testing.T) {
	// Create with nil data source for basic test
	executor := NewViewExecutor(nil)

	if executor == nil {
		t.Fatal("Expected executor to be created")
	}
	// Executor was created successfully
}

// TestIsTruthy tests truthiness evaluation
func TestIsTruthy(t *testing.T) {
	executor := &ViewExecutor{}

	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"nil value", nil, false},
		{"zero int", 0, false},
		{"zero float64", 0.0, false},
		{"empty string", "", false},
		{"false boolean", false, false},
		{"non-zero int", 1, true},
		{"negative int", -1, true},
		{"non-zero float", 0.5, true},
		{"non-empty string", "test", true},
		{"true boolean", true, true},
		{"non-empty slice", []int{1, 2, 3}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.isTruthy(tt.value)
			if result != tt.expected {
				t.Errorf("Expected %v for %v, got %v", tt.expected, tt.value, result)
			}
		})
	}
}

// TestIsSelectAll_ViewExecutor tests checking for SELECT *
func TestIsSelectAll_ViewExecutor(t *testing.T) {
	executor := &ViewExecutor{}

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
			name: "single wildcard",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
			},
			expected: true,
		},
		{
			name: "specific columns",
			cols: []parser.SelectColumn{
				{Name: "id"},
				{Name: "name"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.isSelectAll(tt.cols)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestEvaluateWhere tests evaluating WHERE expression
func TestEvaluateWhere(t *testing.T) {
	executor := &ViewExecutor{}

	row := domain.Row{
		"id":   int64(1),
		"name": "Alice",
		"age":  30,
	}

	tests := []struct {
		name     string
		expr     *parser.Expression
		expected bool
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: true,
		},
		{
			name: "column reference (truthy)",
			expr: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "name",
			},
			expected: true,
		},
		{
			name: "column reference (falsy)",
			expr: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "nonexistent",
			},
			expected: false,
		},
		{
			name: "constant value (true)",
			expr: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: true,
			},
			expected: true,
		},
		{
			name: "constant value (false)",
			expr: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.evaluateWhere(row, tt.expr)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestApplyOuterQuery_Where tests applying WHERE clause
func TestApplyOuterQuery_Where(t *testing.T) {
	executor := &ViewExecutor{}

	viewResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Alice"},
		},
		Total: 3,
	}

	outerQuery := &parser.SelectStatement{
		Where: &parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: "name",
		},
	}

	result := executor.applyOuterQuery(viewResult, outerQuery, &domain.ViewInfo{})

	// All rows have truthy name values, so all should be kept
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows after filtering, got %d", len(result.Rows))
	}
}

// TestApplyOuterQuery_Limit tests applying LIMIT clause
func TestApplyOuterQuery_Limit(t *testing.T) {
	executor := &ViewExecutor{}

	viewResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
		Rows: []domain.Row{
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
			{"id": int64(4)},
			{"id": int64(5)},
		},
		Total: 5,
	}

	limit := int64(2)
	outerQuery := &parser.SelectStatement{
		Limit: &limit,
	}

	result := executor.applyOuterQuery(viewResult, outerQuery, &domain.ViewInfo{})

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after limiting, got %d", len(result.Rows))
	}

	if result.Total != 2 {
		t.Errorf("Expected total to be 2, got %d", result.Total)
	}
}
