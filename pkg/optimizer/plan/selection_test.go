package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestSelectionConfig(t *testing.T) {
	tests := []struct {
		name      string
		condition *parser.Expression
	}{
		{
			name:      "Nil condition",
			condition: nil,
		},
		{
			name: "Simple condition",
			condition: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: "age > 18",
			},
		},
		{
			name: "Complex condition",
			condition: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: "status = 'active' AND created_at > '2023-01-01'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SelectionConfig{
				Condition: tt.condition,
			}

			if tt.condition != nil {
				if config.Condition != tt.condition {
					t.Errorf("Condition = %v, want %v", config.Condition, tt.condition)
				}
			} else {
				if config.Condition != nil {
					t.Errorf("Expected Condition to be nil, got %v", config.Condition)
				}
			}
		})
	}
}

func TestSelectionConfigWithPlan(t *testing.T) {
	condition := &parser.Expression{
		Type:  parser.ExprTypeValue,
		Value: "salary > 50000",
	}

	selectionConfig := &SelectionConfig{
		Condition: condition,
	}

	plan := &Plan{
		ID:     "sel_001",
		Type:   TypeSelection,
		Config: selectionConfig,
	}

	if plan.Type != TypeSelection {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeSelection)
	}

	retrievedConfig, ok := plan.Config.(*SelectionConfig)
	if !ok {
		t.Fatal("Failed to retrieve SelectionConfig from Plan")
	}

	if retrievedConfig.Condition != condition {
		t.Error("Condition mismatch")
	}

	if retrievedConfig.Condition.Value != "salary > 50000" {
		t.Errorf("Condition.Value = %v, want salary > 50000", retrievedConfig.Condition.Value)
	}

	if plan.Explain() != "Selection[sel_001]" {
		t.Errorf("Plan.Explain() = %v, want Selection[sel_001]", plan.Explain())
	}
}

func TestSelectionConfigNilCondition(t *testing.T) {
	config := &SelectionConfig{
		Condition: nil,
	}

	if config.Condition != nil {
		t.Errorf("Expected Condition to be nil, got %v", config.Condition)
	}
}

func TestSelectionConfigMultipleExpressions(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"AND expression", "a = 1 AND b = 2"},
		{"OR expression", "x = 1 OR y = 2"},
		{"IN expression", "status IN ('active', 'pending')"},
		{"LIKE expression", "name LIKE 'John%'"},
		{"BETWEEN expression", "age BETWEEN 18 AND 65"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &SelectionConfig{
				Condition: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: tt.expr,
				},
			}

			if config.Condition.Value != tt.expr {
				t.Errorf("Condition.Value = %v, want %v", config.Condition.Value, tt.expr)
			}
		})
	}
}
