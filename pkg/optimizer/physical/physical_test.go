package physical

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestPhysicalOperatorInterface(t *testing.T) {
	// Test that all physical operators implement the interface
	var _ PhysicalOperator = (*PhysicalTableScan)(nil)
	var _ PhysicalOperator = (*PhysicalSelection)(nil)
	var _ PhysicalOperator = (*PhysicalProjection)(nil)
	var _ PhysicalOperator = (*PhysicalHashJoin)(nil)
	var _ PhysicalOperator = (*PhysicalHashAggregate)(nil)
	var _ PhysicalOperator = (*PhysicalLimit)(nil)
}

func TestTableScan(t *testing.T) {
	scan := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	if scan.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got %s", scan.TableName)
	}

	if len(scan.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(scan.Columns))
	}

	if scan.Cost() != 100.0 {
		t.Errorf("Expected cost 100.0, got %f", scan.Cost())
	}

	// Test Children
	children := scan.Children()
	if len(children) != 0 {
		t.Errorf("Expected 0 children, got %d", len(children))
	}

	// Test Schema
	schema := scan.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}
}

func TestTableScan_SetChildren(t *testing.T) {
	scan := &PhysicalTableScan{
		TableName: "test_table",
		Columns:   []optimizer.ColumnInfo{},
		cost:      0,
		children:  []PhysicalOperator{},
	}

	// TableScan should not accept children
	child := &PhysicalTableScan{
		TableName: "child_table",
		Columns:   []optimizer.ColumnInfo{},
		children:  []PhysicalOperator{},
	}

	scan.SetChildren(child)

	// TableScan should still have no children (SetChildren replaces the list)
	children := scan.Children()
	if len(children) != 1 {  // SetChildren replaces, not appends
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestSelection(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "age", Type: "int"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	selection := &PhysicalSelection{
		cost: 50.0,
		children: []PhysicalOperator{},
	}

	selection.SetChildren(child)

	if selection.Cost() != 50.0 {
		t.Errorf("Expected cost 50.0, got %f", selection.Cost())
	}

	if selection.Cost() != 50.0 {
		t.Errorf("Expected cost 50.0, got %f", selection.Cost())
	}

	children := selection.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}

	schema := selection.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}
}

func TestProjection(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
			{Name: "age", Type: "int"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	projection := &PhysicalProjection{
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
		cost: 20.0,
		children: []PhysicalOperator{},
	}

	projection.SetChildren(child)

	if len(projection.Columns) != 2 {
		t.Errorf("Expected 2 projected columns, got %d", len(projection.Columns))
	}

	if projection.Cost() != 20.0 {
		t.Errorf("Expected cost 20.0, got %f", projection.Cost())
	}

	schema := projection.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}
}

func TestJoin(t *testing.T) {
	left := &PhysicalTableScan{
		TableName: "users",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	right := &PhysicalTableScan{
		TableName: "orders",
		Columns: []optimizer.ColumnInfo{
			{Name: "order_id", Type: "int"},
			{Name: "user_id", Type: "int"},
		},
		cost: 150.0,
		children: []PhysicalOperator{},
	}

	join := &PhysicalHashJoin{
		JoinType: optimizer.InnerJoin,
		Conditions: []*optimizer.JoinCondition{},
		cost: 250.0,
		children: []PhysicalOperator{},
	}

	join.SetChildren(left, right)

	if join.JoinType != optimizer.InnerJoin {
		t.Errorf("Expected join type INNER, got %v", join.JoinType)
	}

	if join.Cost() != 250.0 {
		t.Errorf("Expected cost 250.0, got %f", join.Cost())
	}

	children := join.Children()
	if len(children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(children))
	}

	schema := join.Schema()
	if len(schema) != 4 {
		t.Errorf("Expected schema with 4 columns, got %d", len(schema))
	}
}

func TestAggregate(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "sales",
		Columns: []optimizer.ColumnInfo{
			{Name: "category", Type: "varchar"},
			{Name: "amount", Type: "decimal"},
		},
		cost: 200.0,
		children: []PhysicalOperator{},
	}

	aggregate := &PhysicalHashAggregate{
		GroupByCols: []string{"category"},
		AggFuncs: []*optimizer.AggregationItem{
			{Type: optimizer.Sum, Expr: nil, Alias: "total_amount"},
		},
		cost: 80.0,
		children: []PhysicalOperator{},
	}

	aggregate.SetChildren(child)

	if len(aggregate.GroupByCols) != 1 {
		t.Errorf("Expected 1 group by column, got %d", len(aggregate.GroupByCols))
	}

	if len(aggregate.AggFuncs) != 1 {
		t.Errorf("Expected 1 aggregate function, got %d", len(aggregate.AggFuncs))
	}

	if aggregate.Cost() != 80.0 {
		t.Errorf("Expected cost 80.0, got %f", aggregate.Cost())
	}
}

func TestLimit(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		cost: 1000.0,
		children: []PhysicalOperator{},
	}

	limit := &PhysicalLimit{
		Limit:  10,
		Offset: 20,
		cost:   5.0,
		children: []PhysicalOperator{},
	}

	limit.SetChildren(child)

	if limit.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", limit.Limit)
	}

	if limit.Offset != 20 {
		t.Errorf("Expected offset 20, got %d", limit.Offset)
	}

	if limit.Cost() != 5.0 {
		t.Errorf("Expected cost 5.0, got %f", limit.Cost())
	}
}

func TestPhysicalOperatorExplain(t *testing.T) {
	scan := &PhysicalTableScan{
		TableName: "users",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	explanation := scan.Explain()
	if explanation == "" {
		t.Error("Explain should return a non-empty string")
	}

	// Test Selection Explain
	selection := &PhysicalSelection{
		cost: 50.0,
		children: []PhysicalOperator{},
	}
	selection.SetChildren(scan)

	selectionExplain := selection.Explain()
	if selectionExplain == "" {
		t.Error("PhysicalSelection Explain should return a non-empty string")
	}
}

func TestOperatorChaining(t *testing.T) {
	// Build a complex plan: TableScan -> Selection -> Projection -> Limit
	scan := &PhysicalTableScan{
		TableName: "users",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
			{Name: "age", Type: "int"},
		},
		cost: 1000.0,
		children: []PhysicalOperator{},
	}

	selection := &PhysicalSelection{
		cost: 100.0,
		children: []PhysicalOperator{},
	}
	selection.SetChildren(scan)

	projection := &PhysicalProjection{
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
		cost: 20.0,
		children: []PhysicalOperator{},
	}
	projection.SetChildren(selection)

	limit := &PhysicalLimit{
		Limit: 10,
		Offset: 0,
		cost:   5.0,
		children: []PhysicalOperator{},
	}
	limit.SetChildren(projection)

	// Verify the chain
	if len(limit.Children()) != 1 {
		t.Fatalf("Expected limit to have 1 child, got %d", len(limit.Children()))
	}

	projChild, ok := limit.Children()[0].(*PhysicalProjection)
	if !ok {
		t.Fatal("Expected limit's child to be PhysicalProjection")
	}

	if len(projChild.Children()) != 1 {
		t.Fatalf("Expected projection to have 1 child, got %d", len(projChild.Children()))
	}

	selChild, ok := projChild.Children()[0].(*PhysicalSelection)
	if !ok {
		t.Fatal("Expected projection's child to be PhysicalSelection")
	}

	if len(selChild.Children()) != 1 {
		t.Fatalf("Expected selection to have 1 child, got %d", len(selChild.Children()))
	}

	_, ok = selChild.Children()[0].(*PhysicalTableScan)
	if !ok {
		t.Fatal("Expected selection's child to be PhysicalTableScan")
	}

	// Verify total cost
	totalCost := scan.Cost() + selection.Cost() + projection.Cost() + limit.Cost()
	if totalCost != 1125.0 {
		t.Errorf("Expected total cost 1125.0, got %f", totalCost)
	}
}

func TestNewPhysicalHashAggregate(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "category", Type: "varchar"},
			{Name: "amount", Type: "decimal"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	aggFuncs := []*optimizer.AggregationItem{
		{Type: optimizer.Sum, Expr: nil, Alias: "total"},
		{Type: optimizer.Count, Expr: nil, Alias: "cnt"},
	}
	groupByCols := []string{"category"}

	agg := NewPhysicalHashAggregate(aggFuncs, groupByCols, child)

	if agg == nil {
		t.Fatal("NewPhysicalHashAggregate should return non-nil")
	}

	if len(agg.GetAggFuncs()) != 2 {
		t.Errorf("Expected 2 agg funcs, got %d", len(agg.GetAggFuncs()))
	}

	if len(agg.GetGroupByCols()) != 1 {
		t.Errorf("Expected 1 group by col, got %d", len(agg.GetGroupByCols()))
	}

	schema := agg.Schema()
	if len(schema) != 3 { // 1 group by + 2 agg funcs
		t.Errorf("Expected schema with 3 columns, got %d", len(schema))
	}

	explanation := agg.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestPhysicalAggregate_NoGroupBy(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns:   []optimizer.ColumnInfo{{Name: "amount", Type: "decimal"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	aggFuncs := []*optimizer.AggregationItem{
		{Type: optimizer.Sum, Expr: nil, Alias: "total"},
	}

	agg := NewPhysicalHashAggregate(aggFuncs, []string{}, child)
	schema := agg.Schema()

	if len(schema) != 1 {
		t.Errorf("Expected 1 column (no group by), got %d", len(schema))
	}

	explanation := agg.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestNewPhysicalLimit(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	limit := NewPhysicalLimit(10, 5, child)

	if limit == nil {
		t.Fatal("NewPhysicalLimit should return non-nil")
	}

	if limit.GetLimit() != 10 {
		t.Errorf("Expected limit 10, got %d", limit.GetLimit())
	}

	if limit.GetOffset() != 5 {
		t.Errorf("Expected offset 5, got %d", limit.GetOffset())
	}

	schema := limit.Schema()
	if len(schema) != 1 {
		t.Errorf("Expected schema with 1 column, got %d", len(schema))
	}

	explanation := limit.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestLimitInfo(t *testing.T) {
	info := NewLimitInfo(100, 10)

	if info.Limit != 100 {
		t.Errorf("Expected limit 100, got %d", info.Limit)
	}

	if info.Offset != 10 {
		t.Errorf("Expected offset 10, got %d", info.Offset)
	}
}

func TestPhysicalLimit_NoChild(t *testing.T) {
	limit := &PhysicalLimit{
		Limit:  10,
		Offset: 0,
		cost:   5.0,
		children: []PhysicalOperator{},
	}

	schema := limit.Schema()
	if len(schema) != 0 {
		t.Errorf("Expected empty schema for limit with no child, got %d", len(schema))
	}

	explanation := limit.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string even with no child")
	}
}

func TestNewPhysicalProjection(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	exprs := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
		{Type: parser.ExprTypeColumn, Column: "name"},
	}
	aliases := []string{"user_id", "user_name"}

	proj := NewPhysicalProjection(exprs, aliases, child)

	if proj == nil {
		t.Fatal("NewPhysicalProjection should return non-nil")
	}

	if len(proj.GetExprs()) != 2 {
		t.Errorf("Expected 2 exprs, got %d", len(proj.GetExprs()))
	}

	if len(proj.GetAliases()) != 2 {
		t.Errorf("Expected 2 aliases, got %d", len(proj.GetAliases()))
	}

	schema := proj.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}

	if schema[0].Name != "user_id" {
		t.Errorf("Expected first column name 'user_id', got %s", schema[0].Name)
	}

	explanation := proj.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestPhysicalProjection_NoAlias(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	exprs := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	proj := NewPhysicalProjection(exprs, []string{}, child)
	schema := proj.Schema()

	if len(schema) != 1 {
		t.Errorf("Expected 1 column, got %d", len(schema))
	}

	if schema[0].Name != "id" {
		t.Errorf("Expected column name 'id' when no alias, got %s", schema[0].Name)
	}
}

func TestNewPhysicalSelection(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "age", Type: "int"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "age"},
	}

	sel := NewPhysicalSelection(conditions, []domain.Filter{}, child, nil)

	if sel == nil {
		t.Fatal("NewPhysicalSelection should return non-nil")
	}

	if len(sel.GetConditions()) != 1 {
		t.Errorf("Expected 1 condition, got %d", len(sel.GetConditions()))
	}

	schema := sel.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}
}

func TestPhysicalSelection_NoConditions(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	sel := NewPhysicalSelection([]*parser.Expression{}, []domain.Filter{}, child, nil)

	if len(sel.GetConditions()) != 0 {
		t.Errorf("Expected 0 conditions, got %d", len(sel.GetConditions()))
	}

	if len(sel.GetFilters()) != 0 {
		t.Errorf("Expected 0 filters, got %d", len(sel.GetFilters()))
	}
}

func TestNewPhysicalHashJoin(t *testing.T) {
	left := &PhysicalTableScan{
		TableName: "users",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	right := &PhysicalTableScan{
		TableName: "orders",
		Columns:   []optimizer.ColumnInfo{{Name: "user_id", Type: "int"}},
		cost:      150.0,
		children:  []PhysicalOperator{},
	}

	conditions := []*optimizer.JoinCondition{}

	join := NewPhysicalHashJoin(optimizer.InnerJoin, left, right, conditions)

	if join == nil {
		t.Fatal("NewPhysicalHashJoin should return non-nil")
	}

	if join.GetJoinType() != optimizer.InnerJoin {
		t.Errorf("Expected InnerJoin, got %v", join.GetJoinType())
	}

	schema := join.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}

	explanation := join.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestPhysicalHashJoin_OuterJoins(t *testing.T) {
	left := &PhysicalTableScan{
		TableName: "a",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	right := &PhysicalTableScan{
		TableName: "b",
		Columns:   []optimizer.ColumnInfo{{Name: "a_id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	joinTypes := []optimizer.JoinType{
		optimizer.LeftOuterJoin,
		optimizer.RightOuterJoin,
		optimizer.FullOuterJoin,
	}

	for _, joinType := range joinTypes {
		join := NewPhysicalHashJoin(joinType, left, right, []*optimizer.JoinCondition{})

		if join.GetJoinType() != joinType {
			t.Errorf("Expected %v, got %v", joinType, join.GetJoinType())
		}

		explanation := join.Explain()
		if explanation == "" {
			t.Errorf("Explain should return non-empty string for %v", joinType)
		}
	}
}

func TestNewPhysicalTableScan(t *testing.T) {
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar", Nullable: true},
		},
	}

	scan := NewPhysicalTableScan("test_table", tableInfo, nil, []domain.Filter{}, nil)

	if scan == nil {
		t.Fatal("NewPhysicalTableScan should return non-nil")
	}

	if scan.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got %s", scan.TableName)
	}

	if len(scan.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(scan.Columns))
	}

	schema := scan.Schema()
	if len(schema) != 2 {
		t.Errorf("Expected schema with 2 columns, got %d", len(schema))
	}

	explanation := scan.Explain()
	if explanation == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestPhysicalTableScan_ParallelScanning(t *testing.T) {
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
		},
	}

	scan := NewPhysicalTableScan("test_table", tableInfo, nil, []domain.Filter{}, nil)

	// Test parallel scanner access
	scanner := scan.GetParallelScanner()
	if scanner == nil {
		t.Fatal("GetParallelScanner should return non-nil")
	}

	// Test parallel scan enabled
	if !scan.IsParallelScanEnabled() {
		t.Error("Parallel scan should be enabled by default")
	}

	// Disable parallel scan
	scan.enableParallelScan = false
	if scan.IsParallelScanEnabled() {
		t.Error("Parallel scan should be disabled")
	}

	// Re-enable parallel scan
	scan.enableParallelScan = true
	if !scan.IsParallelScanEnabled() {
		t.Error("Parallel scan should be enabled again")
	}
}

func TestPhysicalHashAggregate_ExplainFormatting(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test",
		Columns:   []optimizer.ColumnInfo{{Name: "x", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	// Single agg function, no group by
	agg1 := NewPhysicalHashAggregate(
		[]*optimizer.AggregationItem{{Type: optimizer.Count, Alias: "cnt"}},
		[]string{},
		child,
	)
	explain1 := agg1.Explain()
	if explain1 == "" {
		t.Error("Explain should return non-empty string")
	}

	// Multiple agg functions, with group by
	agg2 := NewPhysicalHashAggregate(
		[]*optimizer.AggregationItem{
			{Type: optimizer.Sum, Alias: "total"},
			{Type: optimizer.Avg, Alias: "avg"},
		},
		[]string{"category"},
		child,
	)
	explain2 := agg2.Explain()
	if explain2 == "" {
		t.Error("Explain should return non-empty string")
	}
}

func TestPhysicalLimit_EdgeCases(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	// Zero limit
	limit1 := NewPhysicalLimit(0, 0, child)
	if limit1.GetLimit() != 0 {
		t.Errorf("Expected limit 0, got %d", limit1.GetLimit())
	}

	// Large offset
	limit2 := NewPhysicalLimit(1000, 5000, child)
	if limit2.GetOffset() != 5000 {
		t.Errorf("Expected offset 5000, got %d", limit2.GetOffset())
	}
}

func TestPhysicalProjection_MultipleExprs(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test",
		Columns: []optimizer.ColumnInfo{
			{Name: "a", Type: "int"},
			{Name: "b", Type: "int"},
			{Name: "c", Type: "int"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	exprs := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "a"},
		{Type: parser.ExprTypeColumn, Column: "b"},
		{Type: parser.ExprTypeColumn, Column: "c"},
	}
	aliases := []string{"x", "y", "z"}

	proj := NewPhysicalProjection(exprs, aliases, child)
	schema := proj.Schema()

	if len(schema) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(schema))
	}

	if schema[0].Name != "x" || schema[1].Name != "y" || schema[2].Name != "z" {
		t.Error("Column names should match aliases")
	}
}

func TestPhysicalSelection_MultipleConditions(t *testing.T) {
	child := &PhysicalTableScan{
		TableName: "test",
		Columns: []optimizer.ColumnInfo{
			{Name: "age", Type: "int"},
			{Name: "status", Type: "varchar"},
		},
		cost: 100.0,
		children: []PhysicalOperator{},
	}

	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "age"},
		{Type: parser.ExprTypeColumn, Column: "status"},
	}

	sel := NewPhysicalSelection(conditions, []domain.Filter{}, child, nil)

	if len(sel.GetConditions()) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(sel.GetConditions()))
	}
}

func TestPhysicalHashJoin_NoConditions(t *testing.T) {
	left := &PhysicalTableScan{
		TableName: "a",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	right := &PhysicalTableScan{
		TableName: "b",
		Columns:   []optimizer.ColumnInfo{{Name: "id", Type: "int"}},
		cost:      100.0,
		children:  []PhysicalOperator{},
	}

	join := NewPhysicalHashJoin(optimizer.CrossJoin, left, right, []*optimizer.JoinCondition{})

	if len(join.GetConditions()) != 0 {
		t.Errorf("Expected 0 conditions for cross join, got %d", len(join.GetConditions()))
	}
}
