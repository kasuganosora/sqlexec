package optimizer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =============================================================================
// P0-1: Shared aggregate accumulator bug
// SELECT SUM(a), SUM(b) => both SUM accumulators share state.sum
// =============================================================================

func TestOptimizedAggregate_MultipleSumColumns(t *testing.T) {
	// Setup: rows with two numeric columns
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "int"},
			{Name: "b", Type: "int"},
		},
		Rows: []domain.Row{
			{"a": int64(10), "b": int64(100)},
			{"a": int64(20), "b": int64(200)},
			{"a": int64(30), "b": int64(300)},
		},
		Total: 3,
	}

	aggFuncs := []*AggregationItem{
		{Type: Sum, Expr: &parser.Expression{Column: "a"}, Alias: "sum_a"},
		{Type: Sum, Expr: &parser.Expression{Column: "b"}, Alias: "sum_b"},
	}

	agg := &OptimizedAggregate{
		AggFuncs:    aggFuncs,
		GroupByCols: []string{},
	}

	result, err := agg.executeHashAggregate(input)
	if err != nil {
		t.Fatalf("executeHashAggregate failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	sumA := result.Rows[0]["sum_a"]
	sumB := result.Rows[0]["sum_b"]

	// sum_a should be 10+20+30 = 60
	// sum_b should be 100+200+300 = 600
	// BUG: both would be 660 (sum of all values in shared accumulator)
	if toFloat(sumA) != 60.0 {
		t.Errorf("sum_a: expected 60, got %v", sumA)
	}
	if toFloat(sumB) != 600.0 {
		t.Errorf("sum_b: expected 600, got %v", sumB)
	}
}

func TestOptimizedAggregate_MultipleMinMaxColumns(t *testing.T) {
	input := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "a", Type: "int"},
			{Name: "b", Type: "int"},
		},
		Rows: []domain.Row{
			{"a": int64(10), "b": int64(300)},
			{"a": int64(20), "b": int64(200)},
			{"a": int64(30), "b": int64(100)},
		},
		Total: 3,
	}

	aggFuncs := []*AggregationItem{
		{Type: Min, Expr: &parser.Expression{Column: "a"}, Alias: "min_a"},
		{Type: Min, Expr: &parser.Expression{Column: "b"}, Alias: "min_b"},
		{Type: Max, Expr: &parser.Expression{Column: "a"}, Alias: "max_a"},
		{Type: Max, Expr: &parser.Expression{Column: "b"}, Alias: "max_b"},
	}

	agg := &OptimizedAggregate{
		AggFuncs:    aggFuncs,
		GroupByCols: []string{},
	}

	result, err := agg.executeHashAggregate(input)
	if err != nil {
		t.Fatalf("executeHashAggregate failed: %v", err)
	}

	row := result.Rows[0]
	// min_a=10, min_b=100, max_a=30, max_b=300
	if row["min_a"] != int64(10) {
		t.Errorf("min_a: expected 10, got %v", row["min_a"])
	}
	if row["min_b"] != int64(100) {
		t.Errorf("min_b: expected 100, got %v", row["min_b"])
	}
	if row["max_a"] != int64(30) {
		t.Errorf("max_a: expected 30, got %v", row["max_a"])
	}
	if row["max_b"] != int64(300) {
		t.Errorf("max_b: expected 300, got %v", row["max_b"])
	}
}

// =============================================================================
// P0-2: BatchExecutor deadlock
// Add() calls flush() which also locks be.mu => deadlock
// =============================================================================

func TestBatchExecutor_AddFlushDeadlock(t *testing.T) {
	flushed := make(chan []interface{}, 10)
	be := NewBatchExecutor(2, time.Hour, func(items []interface{}) error {
		flushed <- items
		return nil
	})
	defer be.Close()

	done := make(chan struct{})
	go func() {
		// Add 2 items: the second should trigger flush
		_ = be.Add("a")
		_ = be.Add("b") // This triggers flush => deadlock on non-reentrant mutex
		close(done)
	}()

	select {
	case <-done:
		// Success: no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("BatchExecutor.Add deadlocked when triggering flush")
	}
}

// =============================================================================
// P0-3: PlanCache data race on entry.LastHit
// =============================================================================

func TestPlanCache_ConcurrentGetNoRace(t *testing.T) {
	cache := NewPlanCache(100)
	cache.Put(42, nil) // nil plan is fine for this test

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cache.Get(42)
			}
		}()
	}
	wg.Wait()
	// Test passes if no data race detected (run with -race)
}

// =============================================================================
// P0-4: merge_join duplicate-key handling (INNER JOIN)
// left=[1,1] right=[1,1] should produce 4 rows, not 1
// =============================================================================

func TestMergeJoin_DuplicateKeysInnerJoin(t *testing.T) {
	mj := &PhysicalMergeJoin{JoinType: InnerJoin}

	leftRows := []domain.Row{
		{"id": int64(1), "left_val": "a"},
		{"id": int64(1), "left_val": "b"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "right_val": "x"},
		{"id": int64(1), "right_val": "y"},
	}

	result := mj.mergeRows(leftRows, rightRows, "id", "id", InnerJoin)

	// Should produce 2x2 = 4 rows (cartesian product for matching keys)
	if len(result) != 4 {
		t.Errorf("INNER JOIN with duplicate keys: expected 4 rows, got %d", len(result))
		for i, r := range result {
			t.Logf("  row %d: %v", i, r)
		}
	}
}

func TestMergeJoin_MultipleMatchGroups(t *testing.T) {
	mj := &PhysicalMergeJoin{JoinType: InnerJoin}

	leftRows := []domain.Row{
		{"id": int64(1), "left_val": "a"},
		{"id": int64(2), "left_val": "b"},
		{"id": int64(3), "left_val": "c"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "right_val": "x"},
		{"id": int64(2), "right_val": "y"},
		{"id": int64(3), "right_val": "z"},
	}

	result := mj.mergeRows(leftRows, rightRows, "id", "id", InnerJoin)

	if len(result) != 3 {
		t.Errorf("INNER JOIN with unique keys: expected 3 rows, got %d", len(result))
	}
}

// =============================================================================
// P0-6: LIMIT pushed below WHERE
// SELECT * FROM t WHERE x > 5 LIMIT 10 should not push LIMIT below WHERE
// =============================================================================

func TestLimitPushDown_NotPushedBelowSelection(t *testing.T) {
	rule := &LimitPushDownRule{}

	// Create: LIMIT -> Selection -> DataSource
	ds := &LogicalDataSource{TableName: "test"}
	cond := &parser.Expression{Type: parser.ExprTypeOperator, Operator: ">"}
	selection := NewLogicalSelection([]*parser.Expression{cond}, ds)
	limit := NewLogicalLimit(10, 0, selection)

	result, err := rule.Apply(context.Background(), limit, nil)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// The result should still be LIMIT -> Selection, NOT Selection -> LIMIT
	if sel, ok := result.(*LogicalSelection); ok {
		if len(sel.Children()) > 0 {
			if _, isLimit := sel.Children()[0].(*LogicalLimit); isLimit {
				t.Error("LIMIT was incorrectly pushed below Selection (WHERE clause)")
			}
		}
	}
}

// =============================================================================
// P0-7: isTrue zero handling
// isTrue(int64(0)) should return false, but multi-type case breaks this
// =============================================================================

func TestIsTrue_IntZero(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"nil", nil, false},
		{"true", true, true},
		{"false", false, false},
		{"int_zero", int(0), false},
		{"int_nonzero", int(1), true},
		{"int64_zero", int64(0), false},
		{"int64_nonzero", int64(42), true},
		{"int32_zero", int32(0), false},
		{"int8_zero", int8(0), false},
		{"uint_zero", uint(0), false},
		{"float64_zero", float64(0.0), false},
		{"float64_nonzero", float64(3.14), true},
		{"float32_zero", float32(0.0), false},
		{"string_empty", "", false},
		{"string_zero", "0", false},
		{"string_nonzero", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTrue(tt.value)
			if got != tt.expected {
				t.Errorf("isTrue(%v [%T]) = %v, want %v", tt.value, tt.value, got, tt.expected)
			}
		})
	}
}

// =============================================================================
// P0-8: Range selectivity inversion
// val <= minFloat for ">" should return ~1.0 (all rows match), not 0.0
// =============================================================================

func TestRangeSelectivity_BoundaryValues(t *testing.T) {
	estimator := &SimpleCardinalityEstimator{}

	// Create column stats with range [10, 100]
	colStats := &ColumnStatistics{
		MinValue: float64(10),
		MaxValue: float64(100),
	}

	// Range [10, 100], query: val > 5 (val below min)
	// ALL rows satisfy > 5 when data is in [10,100], so selectivity ≈ 1.0
	sel := estimator.estimateRangeSelectivity(">", float64(5), colStats)
	if sel < 0.9 {
		t.Errorf("> below min: expected selectivity ~1.0, got %f", sel)
	}

	// Range [10, 100], query: val > 200 (val above max)
	// NO rows satisfy > 200 when data is in [10,100], so selectivity ≈ 0.0
	sel = estimator.estimateRangeSelectivity(">", float64(200), colStats)
	if sel > 0.1 {
		t.Errorf("> above max: expected selectivity ~0.0, got %f", sel)
	}

	// Range [10, 100], query: val < 200 (val above max)
	// ALL rows satisfy < 200 when data is in [10,100], so selectivity ≈ 1.0
	sel = estimator.estimateRangeSelectivity("<", float64(200), colStats)
	if sel < 0.9 {
		t.Errorf("< above max: expected selectivity ~1.0, got %f", sel)
	}

	// Range [10, 100], query: val < 5 (val below min)
	// NO rows satisfy < 5 when data is in [10,100], so selectivity ≈ 0.0
	sel = estimator.estimateRangeSelectivity("<", float64(5), colStats)
	if sel > 0.1 {
		t.Errorf("< below min: expected selectivity ~0.0, got %f", sel)
	}
}

// =============================================================================
// P0-10: SELECT NULL returns version string instead of nil
// =============================================================================

func TestExpressionExecutor_SelectNullLiteral(t *testing.T) {
	evaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("testdb", nil, evaluator)

	// SELECT NULL - Value is nil, but it's a NULL literal, not a system variable
	expr := &parser.Expression{
		Type:  parser.ExprTypeValue,
		Value: nil,
	}
	row := make(parser.Row)

	val, err := executor.evaluateNoFromExpression(expr, row)
	if err != nil {
		t.Fatalf("evaluateNoFromExpression failed: %v", err)
	}

	// Should be nil, not "sqlexec MySQL-compatible database"
	if val != nil {
		t.Errorf("SELECT NULL returned %v (%T), expected nil", val, val)
	}
}

// =============================================================================
// P0-11: WriteTrigger Stop() panic - close(channel) while senders active
// =============================================================================

func TestWriteTrigger_StopNoPanic(t *testing.T) {
	// Create a collector - needs a base collector
	collector := NewIncrementalStatisticsCollector(nil, 1000)
	wtm := NewWriteTriggerManager(collector, 1000, 2)

	// Register a table
	wtm.RegisterTable("test_table")

	// Stop should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("WriteTriggerManager.Stop() panicked: %v", r)
		}
	}()

	wtm.Stop()
}

// =============================================================================
// P0-12: canPushDownToDataSource returns true if ANY column matches
// Should require ALL columns to be in the schema
// =============================================================================

func TestCanPushDownToDataSource_RequiresAllColumns(t *testing.T) {
	rule := &EnhancedPredicatePushdownRule{}

	// DataSource with columns: id, name
	ds := &LogicalDataSource{
		TableName: "users",
		TableInfo: &domain.TableInfo{
			Name: "users",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "VARCHAR"},
			},
		},
	}

	// Condition referencing columns from TWO tables: users.id = orders.user_id
	// This should NOT be pushable to "users" datasource because "user_id" is not in schema
	crossTableExpr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "eq",
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
		Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "user_id"}, // not in schema
	}

	canPush := rule.canPushDownToDataSource(crossTableExpr, ds)
	if canPush {
		t.Error("canPushDownToDataSource should return false when not all columns are in schema")
	}
}

// =============================================================================
// P0-13: physical_scan NewPhysicalProjection OOB when aliases < exprs
// =============================================================================

func TestPhysicalProjection_ShortAliases(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewPhysicalProjection panicked with short aliases: %v", r)
		}
	}()

	exprs := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "a"},
		{Type: parser.ExprTypeColumn, Column: "b"},
		{Type: parser.ExprTypeColumn, Column: "c"},
	}
	aliases := []string{"alias_a"} // shorter than exprs

	child := &mockPhysicalPlan{cost: 100}

	_ = NewPhysicalProjection(exprs, aliases, child)
}

// mockPhysicalPlan is a minimal PhysicalPlan for testing
type mockPhysicalPlan struct {
	cost float64
}

func (m *mockPhysicalPlan) Children() []PhysicalPlan               { return nil }
func (m *mockPhysicalPlan) SetChildren(children ...PhysicalPlan)    {}
func (m *mockPhysicalPlan) Schema() []ColumnInfo                    { return nil }
func (m *mockPhysicalPlan) Cost() float64                           { return m.cost }
func (m *mockPhysicalPlan) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, nil
}
func (m *mockPhysicalPlan) Explain() string { return "mock" }

// =============================================================================
// P0-5: ConstantFoldingRule SetChildren ordering bug
// =============================================================================

func TestConstantFoldingRule_ChildrenOrdering(t *testing.T) {
	// Create: Selection -> DataSource with a constant condition
	ds := &LogicalDataSource{TableName: "test"}
	// Constant true condition: 1 = 1
	cond := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "eq",
		Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: int64(1)},
		Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: int64(1)},
	}
	selection := NewLogicalSelection([]*parser.Expression{cond}, ds)

	rule := &ConstantFoldingRule{}
	result, err := rule.Apply(context.Background(), selection, nil)
	if err != nil {
		t.Fatalf("ConstantFoldingRule.Apply failed: %v", err)
	}

	// The result should not be the same plan with lost children
	if result == nil {
		t.Fatal("result should not be nil")
	}
}

// =============================================================================
// Helpers
// =============================================================================

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}
