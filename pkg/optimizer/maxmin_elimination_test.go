package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestMaxMinEliminationSingleMax(t *testing.T) {
	// Create a simple logical plan: SELECT MAX(a) FROM t
	// Apply MaxMinEliminationRule
	// Verify the plan is transformed correctly
	rule := NewMaxMinEliminationRule(nil)

	// Create test plan - column "a" must be indexed for optimization to apply
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "INT64", Primary: true},
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	// Create aggregation node
	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Max, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}, Alias: "max_a"},
		},
		[]string{},
		dataSource,
	)

	// Check if rule matches
	if !rule.Match(agg) {
		t.Error("Rule should match single MAX aggregation")
	}

	// Create optimization context
	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	// Apply rule
	result, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Verify result is not nil (optimization was applied)
	if result == nil {
		t.Error("Expected optimization to be applied")
	}
}

func TestMaxMinEliminationSingleMin(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	// Column "b" must be indexed for optimization to apply
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "b", Type: "INT64", Unique: true},
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Min, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "b"}, Alias: "min_b"},
		},
		[]string{},
		dataSource,
	)

	if !rule.Match(agg) {
		t.Error("Rule should match single MIN aggregation")
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	result, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if result == nil {
		t.Error("Expected optimization to be applied")
	}
}

func TestMaxMinEliminationMultiple(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	// Column "a" must be indexed for optimization to apply
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "INT64", Primary: true},
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	// Create aggregation with both MAX and MIN
	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Max, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}, Alias: "max_a"},
			{Type: Min, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}, Alias: "min_a"},
		},
		[]string{},
		dataSource,
	)

	if !rule.Match(agg) {
		t.Error("Rule should match multiple MAX/MIN aggregations")
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	result, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if result == nil {
		t.Error("Expected optimization to be applied")
	}
}

func TestMaxMinEliminationWithGroupBy(t *testing.T) {
	// Rule should NOT match when GROUP BY is present
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "INT64"},
			{Name: "b", Type: "INT64"},
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	// Create aggregation with GROUP BY
	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Max, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}, Alias: "max_a"},
		},
		[]string{"b"}, // GROUP BY b
		dataSource,
	)

	if rule.Match(agg) {
		t.Error("Rule should NOT match aggregation with GROUP BY")
	}
}

func TestMaxMinEliminationNonMaxMin(t *testing.T) {
	// Rule should NOT match for non-MAX/MIN aggregations
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "INT64"},
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	// Create aggregation with SUM (not MAX/MIN)
	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Sum, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}, Alias: "sum_a"},
		},
		[]string{},
		dataSource,
	)

	if rule.Match(agg) {
		t.Error("Rule should NOT match non-MAX/MIN aggregation")
	}
}

func TestMaxMinEliminationEmptyAggregation(t *testing.T) {
	// Rule should NOT match for empty aggregation
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name:    "test_table",
		Columns: []domain.ColumnInfo{},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	// Create empty aggregation
	agg := NewLogicalAggregate(
		[]*AggregationItem{},
		[]string{},
		dataSource,
	)

	if rule.Match(agg) {
		t.Error("Rule should NOT match empty aggregation")
	}
}

func TestCheckColumnHasIndex_PrimaryKey(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	// Primary key column should have an index
	if !rule.checkColumnHasIndex("test_table", "id", optCtx) {
		t.Error("Primary key column should have an index")
	}
}

func TestCheckColumnHasIndex_UniqueColumn(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "email", Type: "VARCHAR", Unique: true},
		},
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	// Unique column should have an index
	if !rule.checkColumnHasIndex("test_table", "email", optCtx) {
		t.Error("Unique column should have an index")
	}
}

func TestCheckColumnHasIndex_RegularColumn(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	// Regular column without any index should return false
	if rule.checkColumnHasIndex("test_table", "name", optCtx) {
		t.Error("Regular column without index should return false")
	}
}

func TestCheckColumnHasIndex_WithExplicitIndex(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "age", Type: "INT"},
		},
		Atts: map[string]interface{}{
			"__indexes__": []*domain.Index{
				{Name: "idx_age", Table: "test_table", Columns: []string{"age"}},
			},
		},
	}

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	// Column with explicit index in Atts should have an index
	if !rule.checkColumnHasIndex("test_table", "age", optCtx) {
		t.Error("Column with explicit index should have an index")
	}
}

func TestCheckColumnHasIndex_NonExistentTable(t *testing.T) {
	rule := NewMaxMinEliminationRule(nil)

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{},
	}

	if rule.checkColumnHasIndex("nonexistent", "id", optCtx) {
		t.Error("Non-existent table should return false")
	}
}

func TestMaxMinElimination_NoIndex_NoOptimization(t *testing.T) {
	// When a column has no index, MAX/MIN elimination should NOT apply
	rule := NewMaxMinEliminationRule(nil)

	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "value", Type: "INT"}, // No index
		},
	}

	dataSource := NewLogicalDataSource("test_table", tableInfo)

	agg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: Max, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"}, Alias: "max_value"},
		},
		[]string{},
		dataSource,
	)

	optCtx := &OptimizationContext{
		TableInfo: map[string]*domain.TableInfo{
			"test_table": tableInfo,
		},
	}

	result, err := rule.Apply(context.Background(), agg, optCtx)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Since "value" has no index, the optimization should return the original plan
	if result != agg {
		t.Error("Expected original plan when column has no index")
	}
}
