package join

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/stretchr/testify/assert"
)

func TestNewBushyJoinTreeBuilder(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	maxBushiness := 5
	builder := NewBushyJoinTreeBuilder(costModel, nil, maxBushiness)

	assert.NotNil(t, builder)
	assert.Equal(t, costModel, builder.costModel)
	// estimator can be nil in simplified implementation
	assert.Equal(t, maxBushiness, builder.maxBushiness)
}

func TestBushyJoinTreeBuilder_BuildBushyTree_FewTables(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	tests := []struct {
		name     string
		tables   []string
		expected interface{}
	}{
		{
			name:     "zero tables",
			tables:   []string{},
			expected: nil,
		},
		{
			name:     "one table",
			tables:   []string{"table1"},
			expected: nil,
		},
		{
			name:     "two tables",
			tables:   []string{"table1", "table2"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.BuildBushyTree(tt.tables, nil)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBushyJoinTreeBuilder_BuildBushyTree_ManyTables(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	tests := []struct {
		name         string
		tables       []string
		maxBushiness int
	}{
		{
			name:         "three tables",
			tables:       []string{"table1", "table2", "table3"},
			maxBushiness: 5,
		},
		{
			name:         "four tables",
			tables:       []string{"table1", "table2", "table3", "table4"},
			maxBushiness: 5,
		},
		{
			name:         "five tables",
			tables:       []string{"table1", "table2", "table3", "table4", "table5"},
			maxBushiness: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder.maxBushiness = tt.maxBushiness
			result := builder.BuildBushyTree(tt.tables, nil)
			// Simplified implementation returns nil
			assert.NotNil(t, result)
		})
	}
}

func TestBushyJoinTreeBuilder_Explain(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	explanation := builder.Explain()

	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "BushyJoinTreeBuilder")
	assert.Contains(t, explanation, "maxBushiness=5")
}

func TestBushyJoinTreeBuilder_BuilderProperties(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 10)

	assert.Equal(t, costModel, builder.costModel)
	// estimator can be nil in simplified implementation
	assert.Equal(t, 10, builder.maxBushiness)
}

func TestBushyJoinTreeBuilder_ZeroMaxBushiness(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 0)

	assert.Equal(t, 0, builder.maxBushiness)

	tables := []string{"table1", "table2", "table3"}
	result := builder.BuildBushyTree(tables, nil)
	// Should still handle gracefully
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_NegativeMaxBushiness(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, -1)

	assert.Equal(t, -1, builder.maxBushiness)

	tables := []string{"table1", "table2", "table3"}
	result := builder.BuildBushyTree(tables, nil)
	// Should still handle gracefully
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_DuplicateTables(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	tables := []string{"table1", "table2", "table1", "table3"}
	result := builder.BuildBushyTree(tables, nil)
	// Should handle duplicates gracefully
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_LargeTableSet(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 10)

	// Create many tables
	tables := make([]string, 20)
	for i := 0; i < 20; i++ {
		tables[i] = "table" + string(rune('0'+i))
	}

	result := builder.BuildBushyTree(tables, nil)
	// Should handle large table sets gracefully
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_NilCostModel(t *testing.T) {
	builder := NewBushyJoinTreeBuilder(nil, nil, 5)

	// Should handle nil cost model
	assert.NotNil(t, builder)

	tables := []string{"table1", "table2", "table3"}
	result := builder.BuildBushyTree(tables, nil)
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_NilEstimator(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	assert.Nil(t, builder.estimator)

	tables := []string{"table1", "table2", "table3"}
	result := builder.BuildBushyTree(tables, nil)
	// Should handle nil estimator
	assert.NotNil(t, result)
}

func TestBushyJoinTreeBuilder_ExplainFormats(t *testing.T) {
	tests := []struct {
		name         string
		maxBushiness int
		contains     []string
	}{
		{
			name:         "default bushiness",
			maxBushiness: 5,
			contains:     []string{"BushyJoinTreeBuilder", "maxBushiness=5"},
		},
		{
			name:         "high bushiness",
			maxBushiness: 10,
			contains:     []string{"BushyJoinTreeBuilder", "maxBushiness=10"},
		},
		{
			name:         "low bushiness",
			maxBushiness: 1,
			contains:     []string{"BushyJoinTreeBuilder", "maxBushiness=1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			costModel := cost.NewEnhancedCostModel(nil)
			builder := NewBushyJoinTreeBuilder(costModel, nil, tt.maxBushiness)

			explanation := builder.Explain()

			for _, str := range tt.contains {
				assert.Contains(t, explanation, str)
			}
		})
	}
}

func TestBushyJoinTreeBuilder_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		maxBushiness int
		tables       []string
	}{
		{
			name:         "max bushiness zero",
			maxBushiness: 0,
			tables:       []string{"table1", "table2", "table3"},
		},
		{
			name:         "max bushiness large",
			maxBushiness: 100,
			tables:       []string{"table1", "table2", "table3"},
		},
		{
			name:         "empty table list",
			maxBushiness: 5,
			tables:       []string{},
		},
		{
			name:         "single table",
			maxBushiness: 5,
			tables:       []string{"table1"},
		},
		{
			name:         "exactly three tables",
			maxBushiness: 5,
			tables:       []string{"table1", "table2", "table3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			costModel := cost.NewEnhancedCostModel(nil)
			builder := NewBushyJoinTreeBuilder(costModel, nil, tt.maxBushiness)

			result := builder.BuildBushyTree(tt.tables, nil)
			// Bushy tree is only beneficial for 3+ tables
			if len(tt.tables) >= 3 {
				assert.NotNil(t, result)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestBushyJoinTreeBuilder_MultipleBuilds(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	// Build multiple times with different tables
	tables1 := []string{"table1", "table2", "table3"}
	result1 := builder.BuildBushyTree(tables1, nil)
	assert.NotNil(t, result1)

	tables2 := []string{"table4", "table5", "table6"}
	result2 := builder.BuildBushyTree(tables2, nil)
	assert.NotNil(t, result2)
}

func TestBushyJoinTreeBuilder_ExplainConsistency(t *testing.T) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	// Explain should return consistent results
	expl1 := builder.Explain()
	expl2 := builder.Explain()

	assert.Equal(t, expl1, expl2, "explain should be consistent")
	assert.NotEmpty(t, expl1)
}

// Benchmark tests
func BenchmarkBushyJoinTreeBuilder_BuildBushyTree(b *testing.B) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	tables := make([]string, 10)
	for i := 0; i < 10; i++ {
		tables[i] = "table" + string(rune('0'+i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.BuildBushyTree(tables, nil)
	}
}

func BenchmarkBushyJoinTreeBuilder_Explain(b *testing.B) {
	costModel := cost.NewEnhancedCostModel(nil)
	builder := NewBushyJoinTreeBuilder(costModel, nil, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Explain()
	}
}

func BenchmarkBushyJoinTreeBuilder_NewBuilder(b *testing.B) {
	costModel := cost.NewEnhancedCostModel(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewBushyJoinTreeBuilder(costModel, nil, 5)
	}
}
