package optimizer

import (
	"testing"
)

func TestNewLogicalUnion(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}

	tests := []struct {
		name     string
		children []LogicalPlan
		expected int
	}{
		{
			name:     "empty union",
			children: []LogicalPlan{},
			expected: 0,
		},
		{
			name:     "single child",
			children: []LogicalPlan{child1},
			expected: 1,
		},
		{
			name:     "two children",
			children: []LogicalPlan{child1, child2},
			expected: 2,
		},
		{
			name:     "multiple children",
			children: []LogicalPlan{child1, child2, child1, child2},
			expected: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unionPlan := NewLogicalUnion(tt.children)

			if unionPlan == nil {
				t.Fatal("Expected non-nil LogicalUnion")
			}

			children := unionPlan.Children()
			if len(children) != tt.expected {
				t.Errorf("Expected %d children, got %d", tt.expected, len(children))
			}

			// Check default isAll value
			if !unionPlan.IsAll() {
				t.Error("Expected default IsAll to be true")
			}
		})
	}
}

func TestLogicalUnion_Children(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}

	unionPlan := NewLogicalUnion([]LogicalPlan{child1, child2})

	children := unionPlan.Children()
	if len(children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(children))
	}
}

func TestLogicalUnion_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}
	child3 := &MockLogicalPlan{}

	unionPlan := NewLogicalUnion([]LogicalPlan{child1, child2})

	// Set new children
	unionPlan.SetChildren(child3)

	children := unionPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalUnion_Schema(t *testing.T) {
	// Test with children that have schema
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	child1 := &MockLogicalPlan{schema: childSchema}
	child2 := &MockLogicalPlan{schema: childSchema}

	unionPlan := NewLogicalUnion([]LogicalPlan{child1, child2})

	schema := unionPlan.Schema()
	if len(schema) != len(childSchema) {
		t.Errorf("Expected schema length %d, got %d", len(childSchema), len(schema))
	}

	// Test without children
	emptyUnion := &LogicalUnion{
		children: []LogicalPlan{},
		isAll:    true,
	}

	emptySchema := emptyUnion.Schema()
	if len(emptySchema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(emptySchema))
	}
}

func TestLogicalUnion_IsAll_SetAll(t *testing.T) {
	child := &MockLogicalPlan{}
	unionPlan := NewLogicalUnion([]LogicalPlan{child})

	// Test default isAll (should be true)
	if !unionPlan.IsAll() {
		t.Error("Expected default IsAll to be true")
	}

	// Test SetAll(false)
	unionPlan.SetAll(false)
	if unionPlan.IsAll() {
		t.Error("Expected IsAll to be false after SetAll(false)")
	}

	// Test SetAll(true)
	unionPlan.SetAll(true)
	if !unionPlan.IsAll() {
		t.Error("Expected IsAll to be true after SetAll(true)")
	}
}

func TestLogicalUnion_Explain(t *testing.T) {
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
	}
	child1 := &MockLogicalPlan{schema: childSchema}
	child2 := &MockLogicalPlan{schema: childSchema}

	tests := []struct {
		name     string
		isAll    bool
		children []LogicalPlan
		contains []string
	}{
		{
			name:     "UNION ALL with single child",
			isAll:    true,
			children: []LogicalPlan{child1},
			contains: []string{"UNION ALL", "MockLogicalPlan"},
		},
		{
			name:     "UNION ALL with multiple children",
			isAll:    true,
			children: []LogicalPlan{child1, child2},
			contains: []string{"UNION ALL", "MockLogicalPlan", "MockLogicalPlan"},
		},
		{
			name:     "UNION (without ALL)",
			isAll:    false,
			children: []LogicalPlan{child1},
			contains: []string{"UNION", "MockLogicalPlan"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unionPlan := NewLogicalUnion(tt.children)
			unionPlan.SetAll(tt.isAll)
			explain := unionPlan.Explain()

			for _, expected := range tt.contains {
				if !contains(explain, expected) {
					t.Errorf("Explain() = %s, should contain %s", explain, expected)
				}
			}

			// Check for "UNION ALL" or "UNION" based on isAll
			if tt.isAll && !contains(explain, "UNION ALL") {
				t.Errorf("Expected 'UNION ALL' in explain for isAll=true, got: %s", explain)
			}
			if !tt.isAll && contains(explain, "UNION ALL") {
				t.Errorf("Did not expect 'UNION ALL' in explain for isAll=false, got: %s", explain)
			}
		})
	}
}

func TestLogicalUnion_DefaultIsAll(t *testing.T) {
	child := &MockLogicalPlan{}
	unionPlan := NewLogicalUnion([]LogicalPlan{child})

	// Verify default is UNION ALL for performance
	if !unionPlan.IsAll() {
		t.Error("Expected NewLogicalUnion to default to UNION ALL (isAll=true)")
	}

	explain := unionPlan.Explain()
	if !contains(explain, "UNION ALL") {
		t.Errorf("Expected 'UNION ALL' in default explain, got: %s", explain)
	}
}
