package optimizer

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	sqlparser "github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// mockCardinalityEstimatorFlex is a flexible mock that supports per-table estimates.
type mockCardinalityEstimatorFlex struct {
	tableScanEstimates map[string]int64
	filterEstimates    map[string]int64
}

func (m *mockCardinalityEstimatorFlex) EstimateTableScan(tableName string) int64 {
	if v, ok := m.tableScanEstimates[tableName]; ok {
		return v
	}
	return 1000
}

func (m *mockCardinalityEstimatorFlex) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	if v, ok := m.filterEstimates[tableName]; ok {
		return v
	}
	return 1000
}

func (m *mockCardinalityEstimatorFlex) EstimateJoin(left, right LogicalPlan, joinType JoinType) int64 {
	return 1000
}

func (m *mockCardinalityEstimatorFlex) EstimateDistinct(table string, columns []string) int64 {
	return 100
}

func (m *mockCardinalityEstimatorFlex) UpdateStatistics(tableName string, stats *TableStatistics) {
	// no-op
}

// createDataSourceWithColumns creates a LogicalDataSource with specific columns for testing.
func createDataSourceWithColumns(tableName string, columns []ColumnInfo) *LogicalDataSource {
	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i] = c.Name
	}
	tableInfo := createMockTableInfoForJoin(tableName, colNames)
	// Update types and nullable in the tableInfo to match
	for i, c := range columns {
		if i < len(tableInfo.Columns) {
			tableInfo.Columns[i].Type = c.Type
			tableInfo.Columns[i].Nullable = c.Nullable
		}
	}
	ds := NewLogicalDataSource(tableName, tableInfo)
	return ds
}

// ---------------------------------------------------------------------------
// Fix #19: LogicalJoin.Schema() column conflict
//   Right table columns with the same name as left table columns should get
//   a "right_" prefix to avoid ambiguity.
// ---------------------------------------------------------------------------

func TestLogicalJoin_Schema_NoConflict(t *testing.T) {
	// Left: (id, name), Right: (order_id, amount) -- no overlapping names.
	left := createDataSourceWithColumns("users", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
	})
	right := createDataSourceWithColumns("orders", []ColumnInfo{
		{Name: "order_id", Type: "INT", Nullable: false},
		{Name: "amount", Type: "DECIMAL", Nullable: true},
	})

	join := NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "order_id"},
			Operator: "=",
		},
	})

	schema := join.Schema()

	assert.Len(t, schema, 4, "should return all 4 columns")

	expectedNames := []string{"id", "name", "order_id", "amount"}
	for i, expected := range expectedNames {
		assert.Equal(t, expected, schema[i].Name,
			"column %d should be %q, got %q", i, expected, schema[i].Name)
	}
}

func TestLogicalJoin_Schema_WithConflict(t *testing.T) {
	// Left: (id, name), Right: (id, value) -- "id" conflicts.
	left := createDataSourceWithColumns("users", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
	})
	right := createDataSourceWithColumns("orders", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "value", Type: "DECIMAL", Nullable: true},
	})

	join := NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Operator: "=",
		},
	})

	schema := join.Schema()

	assert.Len(t, schema, 4, "should return all 4 columns")

	// Right "id" should become "right_id"
	expectedNames := []string{"id", "name", "right_id", "value"}
	for i, expected := range expectedNames {
		assert.Equal(t, expected, schema[i].Name,
			"column %d should be %q, got %q", i, expected, schema[i].Name)
	}
}

func TestLogicalJoin_Schema_MultipleConflicts(t *testing.T) {
	// Left: (id, name, status), Right: (id, name, email) -- "id" and "name" conflict.
	left := createDataSourceWithColumns("users", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
		{Name: "status", Type: "VARCHAR", Nullable: true},
	})
	right := createDataSourceWithColumns("profiles", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
		{Name: "email", Type: "VARCHAR", Nullable: true},
	})

	join := NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Operator: "=",
		},
	})

	schema := join.Schema()

	assert.Len(t, schema, 6, "should return all 6 columns")

	expectedNames := []string{"id", "name", "status", "right_id", "right_name", "email"}
	for i, expected := range expectedNames {
		assert.Equal(t, expected, schema[i].Name,
			"column %d should be %q, got %q", i, expected, schema[i].Name)
	}
}

// ---------------------------------------------------------------------------
// Fix #20: JoinElimination FK detection
//   isForeignKeyPrimaryKeyJoin should detect FK-PK patterns by naming
//   conventions: left.id = right.<left_table>_id,
//   left.<right_table>_id = right.id, or left.id = right.id.
//
//   These tests exercise the FK detection through the public Apply method.
//   The estimator returns high cardinality (>1) for both tables so that the
//   cardinality shortcut does NOT fire; the only way the join gets eliminated
//   is if FK-PK detection succeeds.
// ---------------------------------------------------------------------------

// buildFKJoinPlan creates a LogicalJoin between two data sources with a single
// join condition referencing the given column names.
func buildFKJoinPlan(leftTable string, leftCol string, rightTable string, rightCol string, operator string) *LogicalJoin {
	left := createDataSourceWithColumns(leftTable, []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: leftTable + "_col", Type: "VARCHAR", Nullable: true},
	})
	right := createDataSourceWithColumns(rightTable, []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: rightTable + "_col", Type: "VARCHAR", Nullable: true},
	})

	join := NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: leftCol},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: rightCol},
			Operator: operator,
		},
	})
	return join
}

func TestJoinElimination_FKDetection_LeftIdRightFK(t *testing.T) {
	// Pattern: left.id = right.users_id  (left table is "users")
	// This should be detected as a FK-PK relationship.
	join := buildFKJoinPlan("users", "id", "orders", "users_id", "=")

	estimator := &mockCardinalityEstimatorFlex{
		tableScanEstimates: map[string]int64{"users": 1000, "orders": 5000},
		filterEstimates:    map[string]int64{"users": 1000, "orders": 5000},
	}
	rule := NewJoinEliminationRule(estimator)

	result, err := rule.Apply(context.Background(), join, &OptimizationContext{})
	assert.NoError(t, err)

	// The join should be eliminated: result is the left child (users data source).
	_, isDataSource := result.(*LogicalDataSource)
	assert.True(t, isDataSource,
		"join should be eliminated (FK pattern: left.id = right.<left_table>_id), but got %T", result)
}

func TestJoinElimination_FKDetection_LeftFKRightId(t *testing.T) {
	// Pattern: left.orders_id = right.id  (right table is "orders")
	// This should be detected as a FK-PK relationship.
	join := buildFKJoinPlan("users", "orders_id", "orders", "id", "=")

	// We need left data source to have an "orders_id" column for the expression.
	// Rebuild with correct columns.
	left := createDataSourceWithColumns("users", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "orders_id", Type: "INT", Nullable: true},
	})
	right := createDataSourceWithColumns("orders", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "amount", Type: "DECIMAL", Nullable: true},
	})
	join = NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "orders_id"},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "id"},
			Operator: "=",
		},
	})

	estimator := &mockCardinalityEstimatorFlex{
		tableScanEstimates: map[string]int64{"users": 1000, "orders": 500},
		filterEstimates:    map[string]int64{"users": 1000, "orders": 500},
	}
	rule := NewJoinEliminationRule(estimator)

	result, err := rule.Apply(context.Background(), join, &OptimizationContext{})
	assert.NoError(t, err)

	_, isDataSource := result.(*LogicalDataSource)
	assert.True(t, isDataSource,
		"join should be eliminated (FK pattern: left.<right_table>_id = right.id), but got %T", result)
}

func TestJoinElimination_FKDetection_BothId(t *testing.T) {
	// Pattern: left.id = right.id  (1:1 relationship)
	join := buildFKJoinPlan("users", "id", "user_profiles", "id", "=")

	estimator := &mockCardinalityEstimatorFlex{
		tableScanEstimates: map[string]int64{"users": 1000, "user_profiles": 1000},
		filterEstimates:    map[string]int64{"users": 1000, "user_profiles": 1000},
	}
	rule := NewJoinEliminationRule(estimator)

	result, err := rule.Apply(context.Background(), join, &OptimizationContext{})
	assert.NoError(t, err)

	_, isDataSource := result.(*LogicalDataSource)
	assert.True(t, isDataSource,
		"join should be eliminated (FK pattern: left.id = right.id), but got %T", result)
}

func TestJoinElimination_FKDetection_NoMatch(t *testing.T) {
	// Pattern: left.name = right.email  -- NOT a FK-PK pattern.
	left := createDataSourceWithColumns("users", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "VARCHAR", Nullable: true},
	})
	right := createDataSourceWithColumns("contacts", []ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "email", Type: "VARCHAR", Nullable: true},
	})
	join := NewLogicalJoin(InnerJoin, left, right, []*JoinCondition{
		{
			Left:     &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "name"},
			Right:    &sqlparser.Expression{Type: sqlparser.ExprTypeColumn, Column: "email"},
			Operator: "=",
		},
	})

	estimator := &mockCardinalityEstimatorFlex{
		tableScanEstimates: map[string]int64{"users": 1000, "contacts": 2000},
		filterEstimates:    map[string]int64{"users": 1000, "contacts": 2000},
	}
	rule := NewJoinEliminationRule(estimator)

	result, err := rule.Apply(context.Background(), join, &OptimizationContext{})
	assert.NoError(t, err)

	_, isJoin := result.(*LogicalJoin)
	assert.True(t, isJoin,
		"join should NOT be eliminated (no FK-PK pattern), but got %T", result)
}

func TestJoinElimination_FKDetection_NonEqualityOperator(t *testing.T) {
	// Pattern: left.id > right.id -- not an equality condition, should not eliminate.
	join := buildFKJoinPlan("users", "id", "orders", "id", ">")

	estimator := &mockCardinalityEstimatorFlex{
		tableScanEstimates: map[string]int64{"users": 1000, "orders": 1000},
		filterEstimates:    map[string]int64{"users": 1000, "orders": 1000},
	}
	rule := NewJoinEliminationRule(estimator)

	result, err := rule.Apply(context.Background(), join, &OptimizationContext{})
	assert.NoError(t, err)

	_, isJoin := result.(*LogicalJoin)
	assert.True(t, isJoin,
		"join should NOT be eliminated (non-equality operator '>'), but got %T", result)
}

// ---------------------------------------------------------------------------
// Fix #17: hint_join.go uses debugf/debugln instead of fmt.Printf/Println
//   This is a code quality test. We parse the hint_join.go source with Go's
//   AST to verify that no direct fmt.Printf or fmt.Println calls exist
//   (fmt.Errorf is allowed since it returns an error value, not console output).
// ---------------------------------------------------------------------------

func TestHintJoin_NoFmtPrintCalls(t *testing.T) {
	// Parse hint_join.go using Go's AST parser to detect fmt.Printf / fmt.Println calls.
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "hint_join.go", nil, parser.AllErrors)
	if err != nil {
		t.Fatalf("failed to parse hint_join.go: %v", err)
	}

	var violations []string

	ast.Inspect(node, func(n ast.Node) bool {
		callExpr, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		selExpr, ok := callExpr.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		ident, ok := selExpr.X.(*ast.Ident)
		if !ok {
			return true
		}

		if ident.Name == "fmt" {
			funcName := selExpr.Sel.Name
			// fmt.Errorf and fmt.Sprintf are acceptable; Printf/Println/Print are not.
			if strings.HasPrefix(funcName, "Print") {
				pos := fset.Position(callExpr.Pos())
				violations = append(violations, pos.String()+": fmt."+funcName)
			}
		}
		return true
	})

	assert.Empty(t, violations,
		"hint_join.go should not contain direct fmt.Print/Printf/Println calls "+
			"(Fix #17: use debugf/debugln instead); found: %v", violations)
}
