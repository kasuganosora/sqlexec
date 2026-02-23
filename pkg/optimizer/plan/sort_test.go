package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestSortConfig(t *testing.T) {
	tests := []struct {
		name         string
		orderByItems []*parser.OrderItem
	}{
		{
			name:         "Empty sort",
			orderByItems: []*parser.OrderItem{},
		},
		{
			name: "Single ascending",
			orderByItems: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "name"}, Direction: "ASC"},
			},
		},
		{
			name: "Single descending",
			orderByItems: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "created_at"}, Direction: "DESC"},
			},
		},
		{
			name: "Multiple columns",
			orderByItems: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "department"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "salary"}, Direction: "DESC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "employee_id"}, Direction: "ASC"},
			},
		},
		{
			name: "All descending",
			orderByItems: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "priority"}, Direction: "DESC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "urgency"}, Direction: "DESC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "date"}, Direction: "DESC"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SortConfig{
				OrderByItems: tt.orderByItems,
			}

			if len(config.OrderByItems) != len(tt.orderByItems) {
				t.Errorf("OrderByItems length = %v, want %v", len(config.OrderByItems), len(tt.orderByItems))
			}

			// Verify OrderByItems content (basic length check, full struct comparison not possible due to Expression complexity)
			for i := range tt.orderByItems {
				if config.OrderByItems[i].Direction != tt.orderByItems[i].Direction {
					t.Errorf("OrderByItems[%d].Direction = %v, want %v", i, config.OrderByItems[i].Direction, tt.orderByItems[i].Direction)
				}
			}
		})
	}
}

func TestSortConfigWithPlan(t *testing.T) {
	orderByItems := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "price"}, Direction: "ASC"},
		{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "rating"}, Direction: "DESC"},
	}

	sortConfig := &SortConfig{
		OrderByItems: orderByItems,
	}

	plan := &Plan{
		ID:     "sort_001",
		Type:   TypeSort,
		Config: sortConfig,
	}

	if plan.Type != TypeSort {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeSort)
	}

	retrievedConfig, ok := plan.Config.(*SortConfig)
	if !ok {
		t.Fatal("Failed to retrieve SortConfig from Plan")
	}

	if len(retrievedConfig.OrderByItems) != 2 {
		t.Errorf("OrderByItems length = %v, want 2", len(retrievedConfig.OrderByItems))
	}

	if retrievedConfig.OrderByItems[0].Direction != "ASC" {
		t.Error("First sort should be ascending")
	}

	if retrievedConfig.OrderByItems[1].Direction != "DESC" {
		t.Error("Second sort should be descending")
	}

	if plan.Explain() != "Sort[sort_001]" {
		t.Errorf("Plan.Explain() = %v, want Sort[sort_001]", plan.Explain())
	}
}

func TestSortConfigNilItems(t *testing.T) {
	config := &SortConfig{
		OrderByItems: nil,
	}

	if config.OrderByItems != nil {
		t.Errorf("Expected OrderByItems to be nil, got %v", config.OrderByItems)
	}
}

func TestSortConfigEmptyArray(t *testing.T) {
	config := &SortConfig{
		OrderByItems: []*parser.OrderItem{},
	}

	if len(config.OrderByItems) != 0 {
		t.Errorf("OrderByItems length = %v, want 0", len(config.OrderByItems))
	}
}

func TestSortConfigSingleItem(t *testing.T) {
	config := &SortConfig{
		OrderByItems: []*parser.OrderItem{
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "id"}, Direction: "ASC"},
		},
	}

	if len(config.OrderByItems) != 1 {
		t.Errorf("OrderByItems length = %v, want 1", len(config.OrderByItems))
	}

	// Basic check - just verify the item exists since full Expression comparison is complex
	if len(config.OrderByItems) != 1 {
		t.Errorf("OrderByItems length = %v, want 1", len(config.OrderByItems))
	}

	if config.OrderByItems[0].Direction != "ASC" {
		t.Error("Sort should be ascending")
	}
}

func TestSortConfigComplexOrdering(t *testing.T) {
	// Test complex real-world ordering scenarios
	tests := []struct {
		name      string
		items     []*parser.OrderItem
		descCount int
		ascCount  int
	}{
		{
			name: "Mixed ASC/DESC",
			items: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "date"}, Direction: "DESC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "time"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "priority"}, Direction: "DESC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "id"}, Direction: "ASC"},
			},
			descCount: 2,
			ascCount:  2,
		},
		{
			name: "All ascending",
			items: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "first_name"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "last_name"}, Direction: "ASC"},
			},
			descCount: 0,
			ascCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SortConfig{
				OrderByItems: tt.items,
			}

			descFound := 0
			ascFound := 0

			for _, item := range config.OrderByItems {
				if item.Direction == "DESC" {
					descFound++
				} else {
					ascFound++
				}
			}

			if descFound != tt.descCount {
				t.Errorf("DESC count = %v, want %v", descFound, tt.descCount)
			}

			if ascFound != tt.ascCount {
				t.Errorf("ASC count = %v, want %v", ascFound, tt.ascCount)
			}
		})
	}
}
