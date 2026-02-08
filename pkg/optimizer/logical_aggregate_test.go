package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestNewLogicalAggregate(t *testing.T) {
	child := &MockLogicalPlan{}

	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "count_id"},
		{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Alias: "total_salary"},
	}

	groupByCols := []string{"department", "team"}

	aggPlan := NewLogicalAggregate(aggFuncs, groupByCols, child)

	if aggPlan == nil {
		t.Fatal("Expected non-nil LogicalAggregate")
	}

	children := aggPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}

	funcs := aggPlan.GetAggFuncs()
	if len(funcs) != len(aggFuncs) {
		t.Errorf("Expected %d aggregate functions, got %d", len(aggFuncs), len(funcs))
	}

	groupBy := aggPlan.GetGroupByCols()
	if len(groupBy) != len(groupByCols) {
		t.Errorf("Expected %d group by columns, got %d", len(groupByCols), len(groupBy))
	}
}

func TestLogicalAggregate_Children(t *testing.T) {
	child := &MockLogicalPlan{}

	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}},
	}

	aggPlan := NewLogicalAggregate(aggFuncs, []string{}, child)

	children := aggPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}
}

func TestLogicalAggregate_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}

	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}},
	}

	aggPlan := NewLogicalAggregate(aggFuncs, []string{}, child1)

	// Set new children
	aggPlan.SetChildren(child2)

	children := aggPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalAggregate_Schema(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name        string
		aggFuncs    []*AggregationItem
		groupByCols []string
		expectedCols int
	}{
		{
			name: "only aggregate functions",
			aggFuncs: []*AggregationItem{
				{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "count_id"},
				{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Alias: "total_salary"},
			},
			groupByCols:  []string{},
			expectedCols: 2,
		},
		{
			name: "only group by columns",
			aggFuncs: []*AggregationItem{},
			groupByCols: []string{"department", "team"},
			expectedCols: 2,
		},
		{
			name: "both aggregate and group by",
			aggFuncs: []*AggregationItem{
				{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "count_id"},
			},
			groupByCols: []string{"department"},
			expectedCols: 2,
		},
		{
			name:          "empty aggregate",
			aggFuncs:      []*AggregationItem{},
			groupByCols:   []string{},
			expectedCols:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggPlan := NewLogicalAggregate(tt.aggFuncs, tt.groupByCols, child)

			schema := aggPlan.Schema()
			if len(schema) != tt.expectedCols {
				t.Errorf("Expected %d columns in schema, got %d", tt.expectedCols, len(schema))
			}
		})
	}
}

func TestLogicalAggregate_GetAggFuncs(t *testing.T) {
	child := &MockLogicalPlan{}

	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "count_id"},
		{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Alias: "total_salary"},
		{Type: Avg, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Alias: "avg_age"},
	}

	aggPlan := NewLogicalAggregate(aggFuncs, []string{}, child)

	funcs := aggPlan.GetAggFuncs()
	if len(funcs) != len(aggFuncs) {
		t.Errorf("Expected %d aggregate functions, got %d", len(aggFuncs), len(funcs))
	}

	// Verify the functions are returned correctly
	for i, agg := range funcs {
		if agg.Type != aggFuncs[i].Type {
			t.Errorf("Function %d: expected type %v, got %v", i, aggFuncs[i].Type, agg.Type)
		}
		if agg.Alias != aggFuncs[i].Alias {
			t.Errorf("Function %d: expected alias %s, got %s", i, aggFuncs[i].Alias, agg.Alias)
		}
	}
}

func TestLogicalAggregate_GetGroupByCols(t *testing.T) {
	child := &MockLogicalPlan{}

	groupByCols := []string{"department", "team", "location"}

	aggPlan := NewLogicalAggregate([]*AggregationItem{}, groupByCols, child)

	groupBy := aggPlan.GetGroupByCols()
	if len(groupBy) != len(groupByCols) {
		t.Errorf("Expected %d group by columns, got %d", len(groupByCols), len(groupBy))
	}

	// Verify group by columns are returned correctly
	for i, col := range groupBy {
		if col != groupByCols[i] {
			t.Errorf("Group by column %d: expected %s, got %s", i, groupByCols[i], col)
		}
	}
}

func TestLogicalAggregate_GetGroupBy(t *testing.T) {
	child := &MockLogicalPlan{}

	groupByCols := []string{"department", "team"}

	aggPlan := NewLogicalAggregate([]*AggregationItem{}, groupByCols, child)

	// Test GetGroupBy method (should be same as GetGroupByCols)
	groupBy := aggPlan.GetGroupBy()
	if len(groupBy) != len(groupByCols) {
		t.Errorf("Expected %d group by columns from GetGroupBy, got %d", len(groupByCols), len(groupBy))
	}

	// Verify they return the same data
	groupByCols2 := aggPlan.GetGroupByCols()
	if len(groupBy) != len(groupByCols2) {
		t.Error("GetGroupBy() and GetGroupByCols() returned different lengths")
	}

	for i := range groupBy {
		if groupBy[i] != groupByCols2[i] {
			t.Errorf("Group by column %d: GetGroupBy=%s, GetGroupByCols=%s", i, groupBy[i], groupByCols2[i])
		}
	}
}

func TestLogicalAggregate_Explain(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name        string
		aggFuncs    []*AggregationItem
		groupByCols []string
		contains    []string
	}{
		{
			name: "with aggregate functions only",
			aggFuncs: []*AggregationItem{
				{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}},
				{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}},
			},
			groupByCols: []string{},
			contains:    []string{"Aggregate", "COUNT", "SUM", "id", "salary"},
		},
		{
			name:        "with group by only",
			aggFuncs:    []*AggregationItem{},
			groupByCols: []string{"department", "team"},
			contains:    []string{"Aggregate", "GROUP BY", "department", "team"},
		},
		{
			name: "with both aggregate and group by",
			aggFuncs: []*AggregationItem{
				{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}},
			},
			groupByCols: []string{"department"},
			contains:    []string{"Aggregate", "COUNT", "GROUP BY", "department"},
		},
		{
			name:        "empty aggregate",
			aggFuncs:    []*AggregationItem{},
			groupByCols: []string{},
			contains:    []string{"Aggregate"},
		},
		{
			name: "with aliases",
			aggFuncs: []*AggregationItem{
				{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "total_count"},
			},
			groupByCols: []string{},
			contains:    []string{"Aggregate", "COUNT", "total_count"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggPlan := NewLogicalAggregate(tt.aggFuncs, tt.groupByCols, child)
			explain := aggPlan.Explain()

			for _, expected := range tt.contains {
				if !containsSubstring(explain, expected) {
					t.Errorf("Explain() = %s, should contain %s", explain, expected)
				}
			}
		})
	}
}

func TestLogicalAggregate_AllAggTypes(t *testing.T) {
	child := &MockLogicalPlan{}

	// Test all aggregation types
	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Alias: "count_id"},
		{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Alias: "sum_salary"},
		{Type: Avg, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Alias: "avg_age"},
		{Type: Min, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "score"}, Alias: "min_score"},
		{Type: Max, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "rating"}, Alias: "max_rating"},
	}

	aggPlan := NewLogicalAggregate(aggFuncs, []string{"department"}, child)

	schema := aggPlan.Schema()
	expectedCols := len(aggFuncs) + 1 // +1 for group by column
	if len(schema) != expectedCols {
		t.Errorf("Expected %d columns in schema, got %d", expectedCols, len(schema))
	}

	explain := aggPlan.Explain()
	expectedStrings := []string{"Aggregate", "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP BY"}
	for _, expected := range expectedStrings {
		if !containsSubstring(explain, expected) {
			t.Errorf("Explain() should contain %s, got: %s", expected, explain)
		}
	}
}

func TestLogicalAggregate_WithoutAliases(t *testing.T) {
	child := &MockLogicalPlan{}

	// Test aggregate functions without aliases
	aggFuncs := []*AggregationItem{
		{Type: Count, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}}, // No alias
		{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}}, // No alias
	}

	aggPlan := NewLogicalAggregate(aggFuncs, []string{}, child)

	schema := aggPlan.Schema()
	if len(schema) != len(aggFuncs) {
		t.Errorf("Expected %d columns in schema, got %d", len(aggFuncs), len(schema))
	}

	explain := aggPlan.Explain()
	if !containsSubstring(explain, "COUNT") || !containsSubstring(explain, "SUM") {
		t.Errorf("Explain() should contain aggregate function names, got: %s", explain)
	}
}
