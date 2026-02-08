package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/types"
)

func TestAggregateConfig(t *testing.T) {
	tests := []struct {
		name        string
		aggFuncs    []*types.AggregationItem
		groupByCols []string
	}{
		{
			name:        "Empty aggregate config",
			aggFuncs:    []*types.AggregationItem{},
			groupByCols: []string{},
		},
		{
			name: "Simple aggregate",
			aggFuncs: []*types.AggregationItem{
				{Type: types.Count, Alias: "count_id"},
			},
			groupByCols: []string{},
		},
		{
			name: "Aggregate with group by",
			aggFuncs: []*types.AggregationItem{
				{Type: types.Sum, Alias: "total_price"},
				{Type: types.Avg, Alias: "avg_quantity"},
			},
			groupByCols: []string{"category", "region"},
		},
		{
			name:        "Multiple group by columns",
			aggFuncs:    []*types.AggregationItem{},
			groupByCols: []string{"year", "month", "day", "hour"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &AggregateConfig{
				AggFuncs:    tt.aggFuncs,
				GroupByCols: tt.groupByCols,
			}

			if len(config.AggFuncs) != len(tt.aggFuncs) {
				t.Errorf("AggFuncs length = %v, want %v", len(config.AggFuncs), len(tt.aggFuncs))
			}

			if len(config.GroupByCols) != len(tt.groupByCols) {
				t.Errorf("GroupByCols length = %v, want %v", len(config.GroupByCols), len(tt.groupByCols))
			}

			// Verify GroupByCols content
			for i, col := range tt.groupByCols {
				if config.GroupByCols[i] != col {
					t.Errorf("GroupByCols[%d] = %v, want %v", i, config.GroupByCols[i], col)
				}
			}
		})
	}
}

func TestAggregateConfigWithPlan(t *testing.T) {
	aggConfig := &AggregateConfig{
		AggFuncs: []*types.AggregationItem{
			{Type: types.Count, Alias: "total_count"},
			{Type: types.Max, Alias: "max_price"},
		},
		GroupByCols: []string{"department", "team"},
	}

	plan := &Plan{
		ID:     "agg_001",
		Type:   TypeAggregate,
		Config: aggConfig,
	}

	if plan.Type != TypeAggregate {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeAggregate)
	}

	retrievedConfig, ok := plan.Config.(*AggregateConfig)
	if !ok {
		t.Fatal("Failed to retrieve AggregateConfig from Plan")
	}

	if len(retrievedConfig.AggFuncs) != 2 {
		t.Errorf("Retrieved AggFuncs length = %v, want 2", len(retrievedConfig.AggFuncs))
	}

	if len(retrievedConfig.GroupByCols) != 2 {
		t.Errorf("Retrieved GroupByCols length = %v, want 2", len(retrievedConfig.GroupByCols))
	}

	if plan.Explain() != "Aggregate[agg_001]" {
		t.Errorf("Plan.Explain() = %v, want Aggregate[agg_001]", plan.Explain())
	}
}

func TestAggregateConfigNilFields(t *testing.T) {
	config := &AggregateConfig{
		AggFuncs:    nil,
		GroupByCols: nil,
	}

	if config.AggFuncs != nil {
		t.Errorf("Expected AggFuncs to be nil, got %v", config.AggFuncs)
	}

	if config.GroupByCols != nil {
		t.Errorf("Expected GroupByCols to be nil, got %v", config.GroupByCols)
	}
}
