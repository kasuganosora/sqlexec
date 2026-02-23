package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestNewLogicalSelection(t *testing.T) {
	child := &MockLogicalPlan{}

	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
		{Type: parser.ExprTypeValue, Value: 42},
	}

	selection := NewLogicalSelection(conditions, child)

	if selection == nil {
		t.Fatal("Expected non-nil LogicalSelection")
	}

	children := selection.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}

	conds := selection.Conditions()
	if len(conds) != len(conditions) {
		t.Errorf("Expected %d conditions, got %d", len(conditions), len(conds))
	}
}

func TestLogicalSelection_Children(t *testing.T) {
	child := &MockLogicalPlan{}
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	selection := NewLogicalSelection(conditions, child)

	children := selection.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(children))
	}
}

func TestLogicalSelection_SetChildren(t *testing.T) {
	child1 := &MockLogicalPlan{}
	child2 := &MockLogicalPlan{}
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	selection := NewLogicalSelection(conditions, child1)

	// Set new children
	selection.SetChildren(child2)

	children := selection.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

func TestLogicalSelection_Schema(t *testing.T) {
	// Test with child that has schema
	childSchema := []ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR"},
	}
	child := &MockLogicalPlan{schema: childSchema}
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	selection := NewLogicalSelection(conditions, child)

	schema := selection.Schema()
	if len(schema) != len(childSchema) {
		t.Errorf("Expected schema length %d, got %d", len(childSchema), len(schema))
	}

	// Test without child
	emptySelection := &LogicalSelection{
		filterConditions: conditions,
		children:         []LogicalPlan{},
	}

	emptySchema := emptySelection.Schema()
	if len(emptySchema) != 0 {
		t.Errorf("Expected empty schema, got %d columns", len(emptySchema))
	}
}

func TestLogicalSelection_Conditions(t *testing.T) {
	child := &MockLogicalPlan{}

	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
		{Type: parser.ExprTypeValue, Value: 42},
		{Type: parser.ExprTypeFunction, Function: "max"},
	}

	selection := NewLogicalSelection(conditions, child)

	conds := selection.Conditions()
	if len(conds) != len(conditions) {
		t.Errorf("Expected %d conditions, got %d", len(conditions), len(conds))
	}

	// Verify conditions are returned correctly
	for i, cond := range conds {
		if cond.Type != conditions[i].Type {
			t.Errorf("Condition %d: expected type %v, got %v", i, conditions[i].Type, cond.Type)
		}
	}
}

func TestLogicalSelection_GetConditions(t *testing.T) {
	child := &MockLogicalPlan{}

	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "name"},
	}

	selection := NewLogicalSelection(conditions, child)

	// Test GetConditions method (should be same as Conditions)
	conds := selection.GetConditions()
	if len(conds) != len(conditions) {
		t.Errorf("Expected %d conditions from GetConditions, got %d", len(conditions), len(conds))
	}
}

func TestLogicalSelection_Selectivity(t *testing.T) {
	child := &MockLogicalPlan{}
	conditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	selection := NewLogicalSelection(conditions, child)

	// Test default selectivity (should be 0.1)
	selectivity := selection.Selectivity()
	if selectivity != 0.1 {
		t.Errorf("Expected selectivity 0.1, got %f", selectivity)
	}
}

func TestLogicalSelection_Explain(t *testing.T) {
	child := &MockLogicalPlan{}

	tests := []struct {
		name       string
		conditions []*parser.Expression
		contains   []string
	}{
		{
			name: "single condition",
			conditions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Column: "id"},
			},
			contains: []string{"Selection"},
		},
		{
			name: "multiple conditions",
			conditions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Column: "id"},
				{Type: parser.ExprTypeColumn, Column: "name"},
			},
			contains: []string{"Selection", "WHERE"},
		},
		{
			name:       "no conditions",
			conditions: []*parser.Expression{},
			contains:   []string{"Selection"},
		},
		{
			name: "condition with function",
			conditions: []*parser.Expression{
				{Type: parser.ExprTypeFunction, Function: "max"},
			},
			contains: []string{"Selection", "WHERE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selection := NewLogicalSelection(tt.conditions, child)
			explain := selection.Explain()

			for _, expected := range tt.contains {
				if !contains(explain, expected) {
					t.Errorf("Explain() = %s, should contain %s", explain, expected)
				}
			}
		})
	}
}

func TestLogicalSelection_ConditionsAccessor(t *testing.T) {
	child := &MockLogicalPlan{}

	originalConditions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
	}

	selection := NewLogicalSelection(originalConditions, child)

	// Test that Conditions and GetConditions return the same
	conds1 := selection.Conditions()
	conds2 := selection.GetConditions()

	if len(conds1) != len(conds2) {
		t.Errorf("Conditions() and GetConditions() returned different lengths: %d vs %d", len(conds1), len(conds2))
	}

	// Verify they point to the same underlying data
	for i := range conds1 {
		if conds1[i] != conds2[i] {
			t.Errorf("Conditions()[%d] and GetConditions()[%d] are different pointers", i, i)
		}
	}
}
