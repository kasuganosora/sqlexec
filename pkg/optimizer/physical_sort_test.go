package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewPhysicalSort tests creating a physical sort operator
func TestNewPhysicalSort(t *testing.T) {
	mockChild := &mockPhysicalPlanForSort{cost: 100.0}
	orderByItems := []*OrderByItem{
		{Column: "id", Direction: "ASC"},
		{Column: "name", Direction: "DESC"},
	}

	sort := NewPhysicalSort(orderByItems, mockChild)

	if sort == nil {
		t.Fatal("Expected sort to be created")
	}

	if len(sort.OrderByItems) != 2 {
		t.Errorf("Expected 2 order by items, got %d", len(sort.OrderByItems))
	}

	if sort.OrderByItems[0].Column != "id" || sort.OrderByItems[0].Direction != "ASC" {
		t.Errorf("First order by item mismatch")
	}

	if sort.OrderByItems[1].Column != "name" || sort.OrderByItems[1].Direction != "DESC" {
		t.Errorf("Second order by item mismatch")
	}

	if sort.cost <= mockChild.Cost() {
		t.Errorf("Expected sort cost to be greater than child cost")
	}

	if len(sort.children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(sort.children))
	}
}

// TestPhysicalSort_Children tests getting children
func TestPhysicalSort_Children(t *testing.T) {
	mockChild := &mockPhysicalPlanForSort{cost: 100.0}
	sort := NewPhysicalSort([]*OrderByItem{{Column: "id"}}, mockChild)

	children := sort.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}

	if children[0] != mockChild {
		t.Error("Child should be the mockChild")
	}
}

// TestPhysicalSort_SetChildren tests setting children
func TestPhysicalSort_SetChildren(t *testing.T) {
	mockChild1 := &mockPhysicalPlanForSort{cost: 100.0}
	mockChild2 := &mockPhysicalPlanForSort{cost: 200.0}
	sort := NewPhysicalSort([]*OrderByItem{{Column: "id"}}, mockChild1)

	newChildren := []PhysicalPlan{mockChild2}
	sort.SetChildren(newChildren...)

	children := sort.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}

	if children[0] != mockChild2 {
		t.Error("Child should be mockChild2 after setting")
	}
}

// TestPhysicalSort_Schema tests getting schema
func TestPhysicalSort_Schema(t *testing.T) {
	mockSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	mockChild := &mockPhysicalPlanForSort{cost: 100.0, schema: mockSchema}
	sort := NewPhysicalSort([]*OrderByItem{{Column: "id"}}, mockChild)

	schema := sort.Schema()
	if len(schema) != len(mockSchema) {
		t.Errorf("Expected schema length %d, got %d", len(mockSchema), len(schema))
	}

	for i, col := range schema {
		if col.Name != mockSchema[i].Name || col.Type != mockSchema[i].Type {
			t.Errorf("Schema column %d mismatch", i)
		}
	}
}

// TestPhysicalSort_Schema_NoChild tests schema with no child
func TestPhysicalSort_Schema_NoChild(t *testing.T) {
	sort := &PhysicalSort{
		OrderByItems: []*OrderByItem{{Column: "id"}},
		children:     []PhysicalPlan{},
	}

	schema := sort.Schema()
	if len(schema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(schema))
	}
}

// TestPhysicalSort_Cost tests getting cost
func TestPhysicalSort_Cost(t *testing.T) {
	mockChild := &mockPhysicalPlanForSort{cost: 100.0}
	sort := NewPhysicalSort([]*OrderByItem{{Column: "id"}}, mockChild)

	cost := sort.Cost()
	if cost <= mockChild.Cost() {
		t.Errorf("Expected sort cost (%f) to be greater than child cost (%f)", cost, mockChild.Cost())
	}
}

// TestPhysicalSort_Execute tests deprecated execute method
func TestPhysicalSort_Execute(t *testing.T) {
	mockChild := &mockPhysicalPlanForSort{cost: 100.0}
	sort := NewPhysicalSort([]*OrderByItem{{Column: "id"}}, mockChild)
	ctx := context.Background()

	_, err := sort.Execute(ctx)
	if err == nil {
		t.Error("Expected error for deprecated Execute method")
	}

	if err.Error() != "PhysicalSort.Execute is deprecated. Please use pkg/executor instead" {
		t.Errorf("Expected deprecation error message, got: %v", err)
	}
}

// TestPhysicalSort_Explain tests explain method
func TestPhysicalSort_Explain(t *testing.T) {
	tests := []struct {
		name            string
		orderByItems    []*OrderByItem
		expectedPattern string
	}{
		{
			name: "single ASC column",
			orderByItems: []*OrderByItem{
				{Column: "id", Direction: "ASC"},
			},
			expectedPattern: "Sort(id ASC",
		},
		{
			name: "single DESC column",
			orderByItems: []*OrderByItem{
				{Column: "name", Direction: "DESC"},
			},
			expectedPattern: "Sort(name DESC",
		},
		{
			name: "multiple columns",
			orderByItems: []*OrderByItem{
				{Column: "id", Direction: "ASC"},
				{Column: "name", Direction: "DESC"},
				{Column: "age", Direction: "ASC"},
			},
			expectedPattern: "Sort(id ASC, name DESC, age ASC",
		},
		{
			name: "empty order by",
			orderByItems: []*OrderByItem{},
			expectedPattern: "Sort(",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChild := &mockPhysicalPlanForSort{cost: 100.0}
			sort := NewPhysicalSort(tt.orderByItems, mockChild)

			explanation := sort.Explain()
			if explanation == "" {
				t.Error("Expected non-empty explanation")
			}

			// Check if it contains the expected pattern
			if !containsSubstring(explanation, tt.expectedPattern) {
				t.Errorf("Expected explanation to contain '%s', got: %s", tt.expectedPattern, explanation)
			}

			// Check if it contains cost information
			if !containsSubstring(explanation, "cost=") {
				t.Error("Expected explanation to contain cost information")
			}
		})
	}
}

// TestLog2 tests log2 helper function
func TestLog2(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{1, 0},
		{2, 1},
		{4, 2},
		{8, 3},
		{1024, 10},
		{0, 0},
		{-1, 0},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := log2(tt.input)
			if result != tt.expected {
				t.Errorf("log2(%f) = %f, expected %f", tt.input, result, tt.expected)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && contains(s, substr))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// mockPhysicalPlanForSort is a mock implementation of PhysicalPlan for testing
type mockPhysicalPlanForSort struct {
	cost   float64
	schema []ColumnInfo
}

func (m *mockPhysicalPlanForSort) Children() []PhysicalPlan {
	return []PhysicalPlan{}
}

func (m *mockPhysicalPlanForSort) SetChildren(children ...PhysicalPlan) {
	// No-op for mock
}

func (m *mockPhysicalPlanForSort) Schema() []ColumnInfo {
	return m.schema
}

func (m *mockPhysicalPlanForSort) Cost() float64 {
	return m.cost
}

func (m *mockPhysicalPlanForSort) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, nil
}

func (m *mockPhysicalPlanForSort) Explain() string {
	return "MockPlanForSort"
}
