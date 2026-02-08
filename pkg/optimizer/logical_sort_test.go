package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestNewLogicalSort(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name    string
		orderBy []*parser.OrderItem
	}{
		{
			name:    "empty order by",
			orderBy: []*parser.OrderItem{},
		},
		{
			name: "single order item",
			orderBy: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
			},
		},
		{
			name: "multiple order items",
			orderBy: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "name"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Direction: "DESC"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortPlan := NewLogicalSort(tt.orderBy, child)

			if sortPlan == nil {
				t.Fatal("Expected non-nil LogicalSort")
			}

			children := sortPlan.Children()
			if len(children) != 1 {
				t.Errorf("Expected 1 child, got %d", len(children))
			}

			orderBy := sortPlan.OrderBy()
			if len(orderBy) != len(tt.orderBy) {
				t.Errorf("Expected %d order items, got %d", len(tt.orderBy), len(orderBy))
			}
		})
	}
}

func TestLogicalSort_Children(t *testing.T) {
	child := &MockLogicalPlan{}
	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	children := sortPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}
}

func TestLogicalSort_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}
	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child1)

	// Set new children
	sortPlan.SetChildren(child2)

	children := sortPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalSort_Schema(t *testing.T) {
	// Test with child that has schema
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	child := &MockLogicalPlan{schema: childSchema}
	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	schema := sortPlan.Schema()
	if len(schema) != len(childSchema) {
		t.Errorf("Expected schema length %d, got %d", len(childSchema), len(schema))
	}

	// Test without child
	emptySort := &LogicalSort{
		orderBy:  []*parser.OrderItem{},
		children: []LogicalPlan{},
	}

	emptySchema := emptySort.Schema()
	if len(emptySchema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(emptySchema))
	}
}

func TestLogicalSort_OrderBy(t *testing.T) {
	child := &MockLogicalPlan{}

	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "name"}, Direction: "ASC"},
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Direction: "DESC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	// Test OrderBy method
	result := sortPlan.OrderBy()
	if len(result) != len(orderBy) {
		t.Errorf("Expected %d order items, got %d", len(orderBy), len(result))
	}

	// Verify order items
	for i, item := range result {
		if item.Direction != orderBy[i].Direction {
			t.Errorf("Order item %d: expected direction %s, got %s", i, orderBy[i].Direction, item.Direction)
		}
	}
}

func TestLogicalSort_GetOrderBy(t *testing.T) {
	child := &MockLogicalPlan{}

	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	// Test GetOrderBy method (should be same as OrderBy)
	result := sortPlan.GetOrderBy()
	if len(result) != len(orderBy) {
		t.Errorf("Expected %d order items from GetOrderBy, got %d", len(orderBy), len(result))
	}

	// Verify they return the same data
	if len(sortPlan.OrderBy()) != len(sortPlan.GetOrderBy()) {
		t.Error("OrderBy() and GetOrderBy() returned different lengths")
	}
}

func TestLogicalSort_OrderByDirections(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name      string
		direction string
	}{
		{
			name:      "ASC direction",
			direction: "ASC",
		},
		{
			name:      "DESC direction",
			direction: "DESC",
		},
		{
			name:      "empty direction",
			direction: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: tt.direction},
			}

			sortPlan := NewLogicalSort(orderBy, child)

			result := sortPlan.OrderBy()
			if len(result) != 1 {
				t.Fatal("Expected 1 order item")
			}

			if result[0].Direction != tt.direction {
				t.Errorf("Expected direction %s, got %s", tt.direction, result[0].Direction)
			}
		})
	}
}

func TestLogicalSort_OrderByExpressions(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name string
		expr parser.Expression
	}{
		{
			name: "column expression",
			expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
		},
		{
			name: "function expression",
			expr: parser.Expression{Type: parser.ExprTypeFunction, Function: "max"},
		},
		{
			name: "value expression",
			expr: parser.Expression{Type: parser.ExprTypeValue, Value: 42},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := []*parser.OrderItem{
				{Expr: tt.expr, Direction: "ASC"},
			}

			sortPlan := NewLogicalSort(orderBy, child)

			result := sortPlan.OrderBy()
			if len(result) != 1 {
				t.Fatal("Expected 1 order item")
			}

			if result[0].Expr.Type != tt.expr.Type {
				t.Errorf("Expected expression type %v, got %v", tt.expr.Type, result[0].Expr.Type)
			}
		})
	}
}

func TestLogicalSort_Explain(t *testing.T) {
	child := &MockLogicalPlan{}

	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	explain := sortPlan.Explain()
	if explain != "LogicalSort" {
		t.Errorf("Expected 'LogicalSort', got '%s'", explain)
	}
}

func TestLogicalSort_EmptyOrderBy(t *testing.T) {
	child := &MockLogicalPlan{}

	// Test with empty order by
	sortPlan := NewLogicalSort([]*parser.OrderItem{}, child)

	orderBy := sortPlan.OrderBy()
	if len(orderBy) != 0 {
		t.Errorf("Expected 0 order items, got %d", len(orderBy))
	}

	explain := sortPlan.Explain()
	if explain != "LogicalSort" {
		t.Errorf("Expected 'LogicalSort', got '%s'", explain)
	}
}

func TestLogicalSort_MultipleOrderItems(t *testing.T) {
	child := &MockLogicalPlan{}

	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "department"}, Direction: "ASC"},
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Direction: "DESC"},
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "name"}, Direction: "ASC"},
	}

	sortPlan := NewLogicalSort(orderBy, child)

	result := sortPlan.OrderBy()
	if len(result) != 3 {
		t.Errorf("Expected 3 order items, got %d", len(result))
	}

	// Verify order is preserved
	for i, item := range result {
		if item.Expr.Column != orderBy[i].Expr.Column {
			t.Errorf("Order item %d: expected column %s, got %s", i, orderBy[i].Expr.Column, item.Expr.Column)
		}
	}
}
