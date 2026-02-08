package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestProjectionConfig(t *testing.T) {
	tests := []struct {
		name        string
		expressions []*parser.Expression
		aliases     []string
	}{
		{
			name:        "Empty projection",
			expressions: []*parser.Expression{},
			aliases:     []string{},
		},
		{
			name: "Simple projection",
			expressions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Value: "name"},
			},
			aliases: []string{"name"},
		},
		{
			name: "Multiple expressions",
			expressions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Value: "first_name"},
				{Type: parser.ExprTypeColumn, Value: "last_name"},
				{Type: parser.ExprTypeColumn, Value: "age"},
			},
			aliases: []string{"first_name", "last_name", "age"},
		},
		{
			name: "Expression with alias",
			expressions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Value: "CONCAT(first_name, ' ', last_name)"},
				{Type: parser.ExprTypeColumn, Value: "YEAR(birth_date)"},
			},
			aliases: []string{"full_name", "birth_year"},
		},
		{
			name: "Different alias count",
			expressions: []*parser.Expression{
				{Type: parser.ExprTypeColumn, Value: "col1"},
				{Type: parser.ExprTypeColumn, Value: "col2"},
			},
			aliases: []string{"alias1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ProjectionConfig{
				Expressions: tt.expressions,
				Aliases:     tt.aliases,
			}

			if len(config.Expressions) != len(tt.expressions) {
				t.Errorf("Expressions length = %v, want %v", len(config.Expressions), len(tt.expressions))
			}

			if len(config.Aliases) != len(tt.aliases) {
				t.Errorf("Aliases length = %v, want %v", len(config.Aliases), len(tt.aliases))
			}

			// Verify Expressions content
			for i, expr := range tt.expressions {
				if config.Expressions[i].Type != expr.Type || config.Expressions[i].Value != expr.Value {
					t.Errorf("Expressions[%d] = %+v, want %+v", i, config.Expressions[i], expr)
				}
			}

			// Verify Aliases content
			for i, alias := range tt.aliases {
				if config.Aliases[i] != alias {
					t.Errorf("Aliases[%d] = %v, want %v", i, config.Aliases[i], alias)
				}
			}
		})
	}
}

func TestProjectionConfigWithPlan(t *testing.T) {
	expressions := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Value: "product_name"},
		{Type: parser.ExprTypeColumn, Value: "price * quantity"},
	}
	aliases := []string{"name", "total"}

	projectionConfig := &ProjectionConfig{
		Expressions: expressions,
		Aliases:     aliases,
	}

	plan := &Plan{
		ID:     "proj_001",
		Type:   TypeProjection,
		Config: projectionConfig,
	}

	if plan.Type != TypeProjection {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeProjection)
	}

	retrievedConfig, ok := plan.Config.(*ProjectionConfig)
	if !ok {
		t.Fatal("Failed to retrieve ProjectionConfig from Plan")
	}

	if len(retrievedConfig.Expressions) != 2 {
		t.Errorf("Expressions length = %v, want 2", len(retrievedConfig.Expressions))
	}

	if len(retrievedConfig.Aliases) != 2 {
		t.Errorf("Aliases length = %v, want 2", len(retrievedConfig.Aliases))
	}

	if retrievedConfig.Aliases[0] != "name" {
		t.Errorf("First alias = %v, want name", retrievedConfig.Aliases[0])
	}

	if plan.Explain() != "Projection[proj_001]" {
		t.Errorf("Plan.Explain() = %v, want Projection[proj_001]", plan.Explain())
	}
}

func TestProjectionConfigNilFields(t *testing.T) {
	config := &ProjectionConfig{
		Expressions: nil,
		Aliases:     nil,
	}

	if config.Expressions != nil {
		t.Errorf("Expected Expressions to be nil, got %v", config.Expressions)
	}

	if config.Aliases != nil {
		t.Errorf("Expected Aliases to be nil, got %v", config.Aliases)
	}
}

func TestProjectionConfigEmptyArrays(t *testing.T) {
	config := &ProjectionConfig{
		Expressions: []*parser.Expression{},
		Aliases:     []string{},
	}

	if len(config.Expressions) != 0 {
		t.Errorf("Expressions length = %v, want 0", len(config.Expressions))
	}

	if len(config.Aliases) != 0 {
		t.Errorf("Aliases length = %v, want 0", len(config.Aliases))
	}
}
