package optimizer

import (
	"testing"
)

// MockLogicalPlan is a simple mock for LogicalPlan interface
type MockLogicalPlan struct {
	schema []ColumnInfo
}

func (m *MockLogicalPlan) Children() []LogicalPlan {
	return nil
}

func (m *MockLogicalPlan) SetChildren(children ...LogicalPlan) {
}

func (m *MockLogicalPlan) Schema() []ColumnInfo {
	if m.schema == nil {
		return []ColumnInfo{}
	}
	return m.schema
}

func (m *MockLogicalPlan) Explain() string {
	return "MockLogicalPlan"
}

func TestNewLogicalLimit(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name   string
		limit  int64
		offset int64
	}{
		{
			name:   "simple limit",
			limit:  10,
			offset: 0,
		},
		{
			name:   "limit with offset",
			limit:  20,
			offset: 5,
		},
		{
			name:   "zero limit",
			limit:  0,
			offset: 0,
		},
		{
			name:   "large values",
			limit:  1000000,
			offset: 100000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limitPlan := NewLogicalLimit(tt.limit, tt.offset, child)

			if limitPlan == nil {
				t.Fatal("Expected non-nil LogicalLimit")
			}

			if limitPlan.GetLimit() != tt.limit {
				t.Errorf("Expected limit %d, got %d", tt.limit, limitPlan.GetLimit())
			}

			if limitPlan.GetOffset() != tt.offset {
				t.Errorf("Expected offset %d, got %d", tt.offset, limitPlan.GetOffset())
			}

			children := limitPlan.Children()
			if len(children) != 1 {
				t.Errorf("Expected 1 child, got %d", len(children))
			}
		})
	}
}

func TestLogicalLimit_Children(t *testing.T) {
	child := &MockLogicalPlan{}
	limitPlan := NewLogicalLimit(10, 0, child)

	children := limitPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}
}

func TestLogicalLimit_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}

	limitPlan := NewLogicalLimit(10, 0, child1)

	// Set new children
	limitPlan.SetChildren(child2)

	children := limitPlan.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalLimit_Schema(t *testing.T) {
	// Test with child that has schema
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	child := &MockLogicalPlan{schema: childSchema}
	limitPlan := NewLogicalLimit(10, 0, child)

	schema := limitPlan.Schema()
	if len(schema) != len(childSchema) {
		t.Errorf("Expected schema length %d, got %d", len(childSchema), len(schema))
	}

	// Test without child
	emptyLimit := &LogicalLimit{
		limitVal:  10,
		offsetVal: 0,
		children:  []LogicalPlan{},
	}

	emptySchema := emptyLimit.Schema()
	if len(emptySchema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(emptySchema))
	}
}

func TestLogicalLimit_Explain(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name     string
		limit    int64
		offset   int64
		contains []string
	}{
		{
			name:     "simple limit",
			limit:    10,
			offset:   0,
			contains: []string{"Limit", "limit=10", "offset=0"},
		},
		{
			name:     "limit with offset",
			limit:    20,
			offset:   5,
			contains: []string{"Limit", "limit=20", "offset=5"},
		},
		{
			name:     "large values",
			limit:    999999,
			offset:   111111,
			contains: []string{"Limit", "limit=999999", "offset=111111"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limitPlan := NewLogicalLimit(tt.limit, tt.offset, child)
			explain := limitPlan.Explain()

			for _, expected := range tt.contains {
				if !contains(explain, expected) {
					t.Errorf("Explain() = %s, should contain %s", explain, expected)
				}
			}
		})
	}
}

func TestLogicalLimit_GetLimit(t *testing.T) {
	child := &MockLogicalPlan{}
	limitPlan := NewLogicalLimit(42, 5, child)

	if limitPlan.GetLimit() != 42 {
		t.Errorf("Expected limit 42, got %d", limitPlan.GetLimit())
	}
}

func TestLogicalLimit_GetOffset(t *testing.T) {
	child := &MockLogicalPlan{}
	limitPlan := NewLogicalLimit(42, 5, child)

	if limitPlan.GetOffset() != 5 {
		t.Errorf("Expected offset 5, got %d", limitPlan.GetOffset())
	}
}
