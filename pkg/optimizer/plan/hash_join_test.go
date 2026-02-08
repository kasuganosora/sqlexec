package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/types"
)

func TestHashJoinConfig(t *testing.T) {
	tests := []struct {
		name      string
		joinType  types.JoinType
		leftCond  *types.JoinCondition
		rightCond *types.JoinCondition
		buildSide string
	}{
		{
			name:      "Inner join",
			joinType:  types.InnerJoin,
			leftCond:  &types.JoinCondition{Left: &types.Expression{Column: "user_id"}, Right: &types.Expression{Column: "id"}, Operator: "="},
			rightCond: &types.JoinCondition{Left: &types.Expression{Column: "id"}, Right: &types.Expression{Column: "user_id"}, Operator: "="},
			buildSide: "right",
		},
		{
			name:      "Left outer join",
			joinType:  types.LeftOuterJoin,
			leftCond:  &types.JoinCondition{Left: &types.Expression{Column: "department_id"}, Right: &types.Expression{Column: "id"}, Operator: "="},
			rightCond: &types.JoinCondition{Left: &types.Expression{Column: "id"}, Right: &types.Expression{Column: "department_id"}, Operator: "="},
			buildSide: "right",
		},
		{
			name:      "Right outer join",
			joinType:  types.RightOuterJoin,
			leftCond:  &types.JoinCondition{Left: &types.Expression{Column: "product_id"}, Right: &types.Expression{Column: "id"}, Operator: "="},
			rightCond: &types.JoinCondition{Left: &types.Expression{Column: "id"}, Right: &types.Expression{Column: "product_id"}, Operator: "="},
			buildSide: "left",
		},
		{
			name:      "Full outer join",
			joinType:  types.FullOuterJoin,
			leftCond:  &types.JoinCondition{Left: &types.Expression{Column: "employee_id"}, Right: &types.Expression{Column: "id"}, Operator: "="},
			rightCond: &types.JoinCondition{Left: &types.Expression{Column: "id"}, Right: &types.Expression{Column: "employee_id"}, Operator: "="},
			buildSide: "left",
		},
		{
			name:      "Nil conditions",
			joinType:  types.InnerJoin,
			leftCond:  nil,
			rightCond: nil,
			buildSide: "left",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &HashJoinConfig{
				JoinType:  tt.joinType,
				LeftCond:  tt.leftCond,
				RightCond: tt.rightCond,
				BuildSide: tt.buildSide,
			}

			if config.JoinType != tt.joinType {
				t.Errorf("JoinType = %v, want %v", config.JoinType, tt.joinType)
			}

			if tt.leftCond != nil {
				if config.LeftCond != tt.leftCond {
					t.Errorf("LeftCond = %v, want %v", config.LeftCond, tt.leftCond)
				}
			} else {
				if config.LeftCond != nil {
					t.Errorf("Expected LeftCond to be nil, got %v", config.LeftCond)
				}
			}

			if tt.rightCond != nil {
				if config.RightCond != tt.rightCond {
					t.Errorf("RightCond = %v, want %v", config.RightCond, tt.rightCond)
				}
			} else {
				if config.RightCond != nil {
					t.Errorf("Expected RightCond to be nil, got %v", config.RightCond)
				}
			}

			if config.BuildSide != tt.buildSide {
				t.Errorf("BuildSide = %v, want %v", config.BuildSide, tt.buildSide)
			}
		})
	}
}

func TestHashJoinConfigWithPlan(t *testing.T) {
	leftCond := &types.JoinCondition{Left: &types.Expression{Column: "user_id"}, Right: &types.Expression{Column: "id"}, Operator: "="}
	rightCond := &types.JoinCondition{Left: &types.Expression{Column: "id"}, Right: &types.Expression{Column: "user_id"}, Operator: "="}

	hashJoinConfig := &HashJoinConfig{
		JoinType:  types.InnerJoin,
		LeftCond:  leftCond,
		RightCond: rightCond,
		BuildSide: "right",
	}

	plan := &Plan{
		ID:     "join_001",
		Type:   TypeHashJoin,
		Config: hashJoinConfig,
	}

	if plan.Type != TypeHashJoin {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeHashJoin)
	}

	retrievedConfig, ok := plan.Config.(*HashJoinConfig)
	if !ok {
		t.Fatal("Failed to retrieve HashJoinConfig from Plan")
	}

	if retrievedConfig.JoinType != types.InnerJoin {
		t.Errorf("JoinType = %v, want %v", retrievedConfig.JoinType, types.InnerJoin)
	}

	if retrievedConfig.LeftCond == nil || retrievedConfig.RightCond == nil {
		t.Error("Join conditions should not be nil")
	}

	if retrievedConfig.BuildSide != "right" {
		t.Errorf("BuildSide = %v, want right", retrievedConfig.BuildSide)
	}

	if plan.Explain() != "HashJoin[join_001]" {
		t.Errorf("Plan.Explain() = %v, want HashJoin[join_001]", plan.Explain())
	}
}

func TestHashJoinConfigNilFields(t *testing.T) {
	config := &HashJoinConfig{
		JoinType:  types.InnerJoin,
		LeftCond:  nil,
		RightCond: nil,
		BuildSide: "left",
	}

	if config.LeftCond != nil {
		t.Errorf("Expected LeftCond to be nil, got %v", config.LeftCond)
	}

	if config.RightCond != nil {
		t.Errorf("Expected RightCond to be nil, got %v", config.RightCond)
	}
}

func TestHashJoinConfigBuildSideValues(t *testing.T) {
	tests := []struct {
		buildSide string
	}{
		{"left"},
		{"right"},
		{"auto"},
		{"inner"},
		{"outer"},
	}

	for _, tt := range tests {
		t.Run(tt.buildSide, func(t *testing.T) {
			config := &HashJoinConfig{
				JoinType:  types.InnerJoin,
LeftCond:  &types.JoinCondition{Left: &types.Expression{Column: "a"}, Right: &types.Expression{Column: "b"}, Operator: "="},
			RightCond: &types.JoinCondition{Left: &types.Expression{Column: "b"}, Right: &types.Expression{Column: "a"}, Operator: "="},
				BuildSide: tt.buildSide,
			}

			if config.BuildSide != tt.buildSide {
				t.Errorf("BuildSide = %v, want %v", config.BuildSide, tt.buildSide)
			}
		})
	}
}
