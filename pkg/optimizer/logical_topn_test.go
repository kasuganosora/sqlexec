package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestNewLogicalTopN(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name   string
		items  []*parser.OrderItem
		limit  int64
		offset int64
	}{
		{
			name:   "simple topn",
			items:  []*parser.OrderItem{},
			limit:  10,
			offset: 0,
		},
		{
			name: "topn with order",
			items: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
			},
			limit:  20,
			offset: 5,
		},
		{
			name: "topn with multiple orders",
			items: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "name"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Direction: "DESC"},
			},
			limit:  100,
			offset: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topNPlan := NewLogicalTopN(tt.items, tt.limit, tt.offset, child)

			if topNPlan == nil {
				t.Fatal("Expected non-nil LogicalTopN")
			}

			if topNPlan.GetLimit() != tt.limit {
				t.Errorf("Expected limit %d, got %d", tt.limit, topNPlan.GetLimit())
			}

			if topNPlan.GetOffset() != tt.offset {
				t.Errorf("Expected offset %d, got %d", tt.offset, topNPlan.GetOffset())
			}

			children := topNPlan.Children()
			if len(children) != 1 {
				t.Errorf("Expected 1 child, got %d", len(children))
			}

			sortItems := topNPlan.SortItems()
			if len(sortItems) != len(tt.items) {
				t.Errorf("Expected %d sort items, got %d", len(tt.items), len(sortItems))
			}
		})
	}
}

func TestLogicalTopN_Children(t *testing.T) {
	child := &MockLogicalPlan{}
	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	topNPlan := NewLogicalTopN(items, 10, 0, child)

	children := topNPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}
}

func TestLogicalTopN_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}
	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	topNPlan := NewLogicalTopN(items, 10, 0, child1)

	// Set new children
	topNPlan.SetChildren(child2)

	children := topNPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalTopN_Schema(t *testing.T) {
	// Test with child that has schema
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	child := &MockLogicalPlan{schema: childSchema}
	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	topNPlan := NewLogicalTopN(items, 10, 0, child)

	schema := topNPlan.Schema()
	if len(schema) != len(childSchema) {
		t.Errorf("Expected schema length %d, got %d", len(childSchema), len(schema))
	}

	// Test without child
	emptyTopN := &LogicalTopN{
		sortItems: []*parser.OrderItem{},
		limit:     10,
		offset:    0,
		children:  []LogicalPlan{},
	}

	emptySchema := emptyTopN.Schema()
	if len(emptySchema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(emptySchema))
	}
}

func TestLogicalTopN_SortItems(t *testing.T) {
	child := &MockLogicalPlan{}

	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "name"}, Direction: "ASC"},
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "age"}, Direction: "DESC"},
	}

	topNPlan := NewLogicalTopN(items, 10, 0, child)

	// Test SortItems method
	result := topNPlan.SortItems()
	if len(result) != len(items) {
		t.Errorf("Expected %d sort items, got %d", len(items), len(result))
	}

	// Verify sort items
	for i, item := range result {
		if item.Direction != items[i].Direction {
			t.Errorf("Sort item %d: expected direction %s, got %s", i, items[i].Direction, item.Direction)
		}
	}
}

func TestLogicalTopN_GetLimit_SetLimit(t *testing.T) {
	child := &MockLogicalPlan{}
	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	topNPlan := NewLogicalTopN(items, 42, 5, child)

	// Test GetLimit
	if topNPlan.GetLimit() != 42 {
		t.Errorf("Expected limit 42, got %d", topNPlan.GetLimit())
	}

	// Test SetLimit
	topNPlan.SetLimit(100)
	if topNPlan.GetLimit() != 100 {
		t.Errorf("Expected limit 100 after SetLimit, got %d", topNPlan.GetLimit())
	}
}

func TestLogicalTopN_GetOffset_SetOffset(t *testing.T) {
	child := &MockLogicalPlan{}
	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}

	topNPlan := NewLogicalTopN(items, 42, 5, child)

	// Test GetOffset
	if topNPlan.GetOffset() != 5 {
		t.Errorf("Expected offset 5, got %d", topNPlan.GetOffset())
	}

	// Test SetOffset
	topNPlan.SetOffset(20)
	if topNPlan.GetOffset() != 20 {
		t.Errorf("Expected offset 20 after SetOffset, got %d", topNPlan.GetOffset())
	}
}

func TestLogicalTopN_Explain(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name     string
		limit    int64
		offset   int64
		contains []string
	}{
		{
			name:     "simple topn",
			limit:    10,
			offset:   0,
			contains: []string{"TopN", "Limit=10", "Offset=0"},
		},
		{
			name:     "topn with offset",
			limit:    20,
			offset:   5,
			contains: []string{"TopN", "Limit=20", "Offset=5"},
		},
		{
			name:     "large values",
			limit:    999999,
			offset:   111111,
			contains: []string{"TopN", "Limit=999999", "Offset=111111"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topNPlan := NewLogicalTopN([]*parser.OrderItem{}, tt.limit, tt.offset, child)
			explain := topNPlan.Explain()

			for _, expected := range tt.contains {
				if !containsSubstring(explain, expected) {
					t.Errorf("Explain() = %s, should contain %s", explain, expected)
				}
			}
		})
	}
}

func TestLogicalTopN_WithOrderItems(t *testing.T) {
	child := &MockLogicalPlan{}

	items := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "department"}, Direction: "ASC"},
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "salary"}, Direction: "DESC"},
	}

	topNPlan := NewLogicalTopN(items, 10, 0, child)

	sortItems := topNPlan.SortItems()
	if len(sortItems) != 2 {
		t.Errorf("Expected 2 sort items, got %d", len(sortItems))
	}

	// Verify order is preserved
	for i, item := range sortItems {
		if item.Expr.Column != items[i].Expr.Column {
			t.Errorf("Sort item %d: expected column %s, got %s", i, items[i].Expr.Column, item.Expr.Column)
		}
	}

	// Verify limit and offset
	if topNPlan.GetLimit() != 10 {
		t.Errorf("Expected limit 10, got %d", topNPlan.GetLimit())
	}
	if topNPlan.GetOffset() != 0 {
		t.Errorf("Expected offset 0, got %d", topNPlan.GetOffset())
	}
}

func TestLogicalTopN_EmptyOrderItems(t *testing.T) {
	child := &MockLogicalPlan{}

	// Test with empty order items (just limit and offset)
	topNPlan := NewLogicalTopN([]*parser.OrderItem{}, 10, 5, child)

	sortItems := topNPlan.SortItems()
	if len(sortItems) != 0 {
		t.Errorf("Expected 0 sort items, got %d", len(sortItems))
	}

	if topNPlan.GetLimit() != 10 {
		t.Errorf("Expected limit 10, got %d", topNPlan.GetLimit())
	}

	if topNPlan.GetOffset() != 5 {
		t.Errorf("Expected offset 5, got %d", topNPlan.GetOffset())
	}

	explain := topNPlan.Explain()
	if !containsSubstring(explain, "TopN") {
		t.Errorf("Expected 'TopN' in explain, got: %s", explain)
	}
}
