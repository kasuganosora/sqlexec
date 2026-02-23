package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
)

// MockPhysicalPlan is a mock implementation of PhysicalPlan for testing
type MockPhysicalPlan struct {
	explain string
}

func (m *MockPhysicalPlan) Explain() string {
	return m.explain
}

func (m *MockPhysicalPlan) Children() []PhysicalPlan {
	return nil
}

func (m *MockPhysicalPlan) Cost() float64 {
	return 0
}

func (m *MockPhysicalPlan) SetCost(cost float64) {
}

func (m *MockPhysicalPlan) Schema() []ColumnInfo {
	return nil
}

func (m *MockPhysicalPlan) SetChildren(children ...PhysicalPlan) {
}

func TestExplainPlan(t *testing.T) {
	// Test with a mock PhysicalPlan
	mockPlan := &MockPhysicalPlan{explain: "MockPhysicalPlan"}

	result := ExplainPlan(mockPlan)
	if result != "MockPhysicalPlan\n" {
		t.Errorf("Expected 'MockPhysicalPlan\\n', got '%s'", result)
	}
}

func TestExplainPlanV2(t *testing.T) {
	tests := []struct {
		name     string
		plan     *plan.Plan
		expected string
	}{
		{
			name:     "nil plan",
			plan:     nil,
			expected: "",
		},
		{
			name: "simple plan",
			plan: &plan.Plan{
				ID:   "Scan",
				Type: plan.TypeTableScan,
			},
			expected: "Scan [TableScan]\n",
		},
		{
			name: "plan with children",
			plan: &plan.Plan{
				ID:   "Join",
				Type: plan.TypeHashJoin,
				Children: []*plan.Plan{
					{
						ID:   "Scan1",
						Type: plan.TypeTableScan,
					},
					{
						ID:   "Scan2",
						Type: plan.TypeTableScan,
					},
				},
			},
			expected: "Join [HashJoin]\n  Scan1 [TableScan]\n  Scan2 [TableScan]\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExplainPlanV2(tt.plan)
			if result != tt.expected {
				t.Errorf("ExplainPlanV2() = %v, want %v", result, tt.expected)
			}
		})
	}
}
