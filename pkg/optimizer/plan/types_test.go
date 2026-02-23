package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/types"
)

func TestPlanExplain(t *testing.T) {
	tests := []struct {
		name     string
		planType PlanType
		id       string
		want     string
	}{
		{
			name:     "TableScan plan",
			planType: TypeTableScan,
			id:       "scan_001",
			want:     "TableScan[scan_001]",
		},
		{
			name:     "HashJoin plan",
			planType: TypeHashJoin,
			id:       "join_002",
			want:     "HashJoin[join_002]",
		},
		{
			name:     "Sort plan",
			planType: TypeSort,
			id:       "sort_003",
			want:     "Sort[sort_003]",
		},
		{
			name:     "Empty ID plan",
			planType: TypeTableScan,
			id:       "",
			want:     "TableScan[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plan{
				ID:   tt.id,
				Type: tt.planType,
			}
			if got := p.Explain(); got != tt.want {
				t.Errorf("Plan.Explain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanCost(t *testing.T) {
	tests := []struct {
		name string
		cost float64
		want float64
	}{
		{
			name: "Zero cost",
			cost: 0.0,
			want: 0.0,
		},
		{
			name: "Positive cost",
			cost: 100.5,
			want: 100.5,
		},
		{
			name: "High cost",
			cost: 999999.99,
			want: 999999.99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plan{
				EstimatedCost: tt.cost,
			}
			if got := p.Cost(); got != tt.want {
				t.Errorf("Plan.Cost() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlanTypeConstants(t *testing.T) {
	// Test that all plan type constants are defined correctly
	tests := []struct {
		name     string
		planType PlanType
		want     string
	}{
		{"TypeTableScan", TypeTableScan, "TableScan"},
		{"TypeHashJoin", TypeHashJoin, "HashJoin"},
		{"TypeSort", TypeSort, "Sort"},
		{"TypeAggregate", TypeAggregate, "Aggregate"},
		{"TypeProjection", TypeProjection, "Projection"},
		{"TypeSelection", TypeSelection, "Selection"},
		{"TypeLimit", TypeLimit, "Limit"},
		{"TypeInsert", TypeInsert, "Insert"},
		{"TypeUpdate", TypeUpdate, "Update"},
		{"TypeDelete", TypeDelete, "Delete"},
		{"TypeUnion", TypeUnion, "Union"},
		{"TypeVectorScan", TypeVectorScan, "VectorScan"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.planType) != tt.want {
				t.Errorf("PlanType %s = %v, want %v", tt.name, tt.planType, tt.want)
			}
		})
	}
}

func TestPlanExplainWithChildren(t *testing.T) {
	childPlan := &Plan{
		ID:   "child_001",
		Type: TypeTableScan,
		OutputSchema: []types.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	parentPlan := &Plan{
		ID:       "parent_001",
		Type:     TypeHashJoin,
		Children: []*Plan{childPlan},
		OutputSchema: []types.ColumnInfo{
			{Name: "name", Type: "varchar"},
		},
	}

	// Test parent plan explain
	got := parentPlan.Explain()
	want := "HashJoin[parent_001]"
	if got != want {
		t.Errorf("Parent Plan.Explain() = %v, want %v", got, want)
	}

	// Test child plan explain
	gotChild := childPlan.Explain()
	wantChild := "TableScan[child_001]"
	if gotChild != wantChild {
		t.Errorf("Child Plan.Explain() = %v, want %v", gotChild, wantChild)
	}

	// Test with multiple children
	child2 := &Plan{
		ID:   "child_002",
		Type: TypeTableScan,
	}
	parentPlan.Children = []*Plan{childPlan, child2}

	gotMulti := parentPlan.Explain()
	wantMulti := "HashJoin[parent_001]"
	if gotMulti != wantMulti {
		t.Errorf("Multi-child Plan.Explain() = %v, want %v", gotMulti, wantMulti)
	}
}

func TestPlanCostWithSchema(t *testing.T) {
	plan := &Plan{
		ID:            "test_001",
		Type:          TypeAggregate,
		EstimatedCost: 42.42,
		OutputSchema: []types.ColumnInfo{
			{Name: "count", Type: "bigint"},
			{Name: "sum", Type: "decimal"},
		},
	}

	cost := plan.Cost()
	if cost != 42.42 {
		t.Errorf("Plan.Cost() with schema = %v, want 42.42", cost)
	}

	explanation := plan.Explain()
	wantExplain := "Aggregate[test_001]"
	if explanation != wantExplain {
		t.Errorf("Plan.Explain() with schema = %v, want %v", explanation, wantExplain)
	}
}
