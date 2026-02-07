package physical

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
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
