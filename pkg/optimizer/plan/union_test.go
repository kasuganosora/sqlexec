package plan

import (
	"testing"
)

func TestUnionConfig(t *testing.T) {
	tests := []struct {
		name     string
		distinct bool
		wantType string
	}{
		{
			name:     "UNION DISTINCT",
			distinct: true,
			wantType: "UNION",
		},
		{
			name:     "UNION ALL",
			distinct: false,
			wantType: "UNION ALL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &UnionConfig{
				Distinct: tt.distinct,
			}

			if config.Distinct != tt.distinct {
				t.Errorf("Distinct = %v, want %v", config.Distinct, tt.distinct)
			}
		})
	}
}

func TestUnionConfigWithPlan(t *testing.T) {
	unionConfig := &UnionConfig{
		Distinct: true, // UNION (not UNION ALL)
	}

	plan := &Plan{
		ID:     "union_001",
		Type:   TypeUnion,
		Config: unionConfig,
	}

	if plan.Type != TypeUnion {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeUnion)
	}

	retrievedConfig, ok := plan.Config.(*UnionConfig)
	if !ok {
		t.Fatal("Failed to retrieve UnionConfig from Plan")
	}

	if !retrievedConfig.Distinct {
		t.Error("Distinct should be true for UNION")
	}

	if plan.Explain() != "Union[union_001]" {
		t.Errorf("Plan.Explain() = %v, want Union[union_001]", plan.Explain())
	}
}

func TestUnionConfigAll(t *testing.T) {
	unionAllConfig := &UnionConfig{
		Distinct: false, // UNION ALL
	}

	plan := &Plan{
		ID:     "union_all_001",
		Type:   TypeUnion,
		Config: unionAllConfig,
	}

	retrievedConfig, ok := plan.Config.(*UnionConfig)
	if !ok {
		t.Fatal("Failed to retrieve UnionConfig from Plan")
	}

	if retrievedConfig.Distinct {
		t.Error("Distinct should be false for UNION ALL")
	}

	if plan.Explain() != "Union[union_all_001]" {
		t.Errorf("Plan.Explain() = %v, want Union[union_all_001]", plan.Explain())
	}
}
