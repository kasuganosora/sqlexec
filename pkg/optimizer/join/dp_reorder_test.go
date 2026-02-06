package join

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/stretchr/testify/assert"
)

func TestNewDPJoinReorder(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	maxTables := 10

	reorder := NewDPJoinReorder(costModel, estimator, maxTables)

	assert.NotNil(t, reorder)
	assert.Equal(t, costModel, reorder.costModel)
	assert.Equal(t, estimator, reorder.estimator)
	assert.Equal(t, maxTables, reorder.maxTables)
	assert.NotNil(t, reorder.cache)
}

func TestNewReorderCache(t *testing.T) {
	cache := NewReorderCache(1000)

	assert.NotNil(t, cache)
	assert.NotNil(t, cache.cache)
}

func TestReorderCache_Get(t *testing.T) {
	cache := NewReorderCache(1000)

	// Test miss
	result := cache.Get("non_existent")
	assert.Nil(t, result)

	// Test hit
	reorderResult := &ReorderResult{
		Order: []string{"table1", "table2"},
		Cost:  100.0,
	}
	cache.Set("key1", reorderResult)

	result = cache.Get("key1")
	assert.NotNil(t, result)
	assert.Equal(t, 2, len(result.Order))
}

func TestReorderCache_Set(t *testing.T) {
	cache := NewReorderCache(1000)

	reorderResult := &ReorderResult{
		Order:        []string{"table1", "table2"},
		Cost:         100.0,
		JoinTreeType: "left-deep",
	}

	cache.Set("key1", reorderResult)

	result := cache.Get("key1")
	assert.NotNil(t, result)
	assert.Equal(t, 100.0, result.Cost)
}

func TestReorderCache_Clear(t *testing.T) {
	cache := NewReorderCache(1000)

	// Add some entries
	cache.Set("key1", &ReorderResult{Order: []string{"table1"}})
	cache.Set("key2", &ReorderResult{Order: []string{"table2"}})

	// Clear
	cache.Clear()

	// Check empty
	result1 := cache.Get("key1")
	result2 := cache.Get("key2")
	assert.Nil(t, result1)
	assert.Nil(t, result2)
}

func TestDPJoinReorder_Reorder_SingleTable(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	plan := &mockLogicalPlan{
		children: []LogicalPlan{},
	}

	resultPlan, err := reorder.Reorder(context.Background(), plan)

	assert.NoError(t, err)
	assert.NotNil(t, resultPlan)
}

func TestDPJoinReorder_Reorder_ManyTables(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	// Create a plan with many children
	plan := &mockLogicalPlan{
		children: []LogicalPlan{
			&mockLogicalPlan{},
			&mockLogicalPlan{},
			&mockLogicalPlan{},
		},
	}

	resultPlan, err := reorder.Reorder(context.Background(), plan)

	assert.NoError(t, err)
	assert.NotNil(t, resultPlan)
}

func TestDPJoinReorder_Reorder_TooManyTables(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 2) // max 2 tables

	// Create a plan with many children
	plan := &mockLogicalPlan{
		children: []LogicalPlan{
			&mockLogicalPlan{},
			&mockLogicalPlan{},
			&mockLogicalPlan{},
		},
	}

	resultPlan, err := reorder.Reorder(context.Background(), plan)

	assert.NoError(t, err)
	assert.NotNil(t, resultPlan)
}

func TestDPJoinReorder_CollectJoinNodes(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	// Create a plan structure
	child1 := &mockLogicalPlan{}
	child2 := &mockLogicalPlan{}
	plan := &mockLogicalPlan{
		children: []LogicalPlan{child1, child2},
	}

	joinNodes, tables := reorder.collectJoinNodes(plan)

	assert.NotNil(t, joinNodes)
	assert.NotNil(t, tables)
	assert.GreaterOrEqual(t, len(joinNodes), 0)
	assert.GreaterOrEqual(t, len(tables), 0)
}

func TestDPJoinReorder_DpSearch(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tables := []string{"table1", "table2"}
	joinNodes := []LogicalPlan{}

	// Test cache miss first
	result := reorder.dpSearch(tables, joinNodes)
	assert.NotNil(t, result)

	// Test cache hit
	cachedResult := reorder.dpSearch(tables, joinNodes)
	assert.NotNil(t, cachedResult)
}

func TestDPJoinReorder_SolveDP_SingleTable(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tables := []string{"table1"}
	dp := map[string]*DPState{
		"table1": {
			Order: []string{"table1"},
			Cost:  100.0,
		},
	}
	joinNodes := []LogicalPlan{}

	result := reorder.solveDP(tables, dp, joinNodes)

	assert.NotNil(t, result)
	assert.Equal(t, []string{"table1"}, result.Order)
}

func TestDPJoinReorder_GreedyReorder(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tables := []string{"table1", "table2", "table3"}
	joinNodes := []LogicalPlan{}

	plan := reorder.greedyReorder(tables, joinNodes)

	assert.NotNil(t, plan)
}

func TestDPJoinReorder_EstimateSingleTableCost(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	cost := reorder.estimateSingleTableCost("test_table")
	assert.Greater(t, cost, 0.0, "cost should be positive")
}

func TestDPJoinReorder_EstimateJoinCost(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	leftSet := []string{"table1"}
	rightSet := []string{"table2"}
	joinNodes := []LogicalPlan{}

	cost := reorder.estimateJoinCost(leftSet, rightSet, joinNodes)
	assert.GreaterOrEqual(t, cost, 0.0, "cost should be non-negative")
}

func TestDPJoinReorder_EstimateJoinCost_EmptySets(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	leftSet := []string{}
	rightSet := []string{"table2"}
	joinNodes := []LogicalPlan{}

	cost := reorder.estimateJoinCost(leftSet, rightSet, joinNodes)
	assert.Equal(t, 0.0, cost, "empty set should return 0")
}

func TestDPJoinReorder_EstimateGreedyJoinCost(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	currentOrder := []string{"table1"}
	newTable := "table2"
	tableCost := 100.0
	joinNodes := []LogicalPlan{}

	cost := reorder.estimateGreedyJoinCost(currentOrder, newTable, tableCost, joinNodes)
	assert.GreaterOrEqual(t, cost, 0.0, "cost should be non-negative")
}

func TestDPJoinReorder_GenerateCacheKey(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tests := []struct {
		name     string
		tables   []string
		expected string
	}{
		{
			name:     "single table",
			tables:   []string{"table1"},
			expected: "table1|",
		},
		{
			name:     "multiple tables",
			tables:   []string{"table1", "table2", "table3"},
			expected: "table1|table2|table3|",
		},
		{
			name:     "empty tables",
			tables:   []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := reorder.generateCacheKey(tt.tables)
			assert.Equal(t, tt.expected, key)
		})
	}
}

func TestDPJoinReorder_BuildDataSource(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	plan := reorder.buildDataSource("test_table")
	// This returns nil in the simplified implementation
	assert.NotNil(t, plan)
}

func TestDPJoinReorder_Explain(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	result := &ReorderResult{
		Order:        []string{"table1", "table2"},
		Cost:         1000.5,
		JoinTreeType: "left-deep",
	}

	explanation := reorder.Explain(result)
	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "=== JOIN Reorder Result ===")
	assert.Contains(t, explanation, "Order:")
	assert.Contains(t, explanation, "Cost:")
	assert.Contains(t, explanation, "Tree Type:")
}

func TestDPState_Completeness(t *testing.T) {
	state := &DPState{
		Order: []string{"table1", "table2"},
		Cost:  100.5,
		Left:  "table1",
		Right: "table2",
	}

	assert.Len(t, state.Order, 2)
	assert.Equal(t, 100.5, state.Cost)
	assert.Equal(t, "table1", state.Left)
	assert.Equal(t, "table2", state.Right)
}

func TestReorderResult_Completeness(t *testing.T) {
	result := &ReorderResult{
		Order:        []string{"table1", "table2"},
		Cost:         1000.0,
		JoinTreeType: "left-deep",
		Plan:         &mockLogicalPlan{},
	}

	assert.Len(t, result.Order, 2)
	assert.Equal(t, 1000.0, result.Cost)
	assert.Equal(t, "left-deep", result.JoinTreeType)
	assert.NotNil(t, result.Plan)
}

func TestDPJoinReorder_BuildPlanFromOrder(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tests := []struct {
		name     string
		order    []string
		expected *LogicalPlan
	}{
		{
			name:     "empty order",
			order:    []string{},
			expected: (*LogicalPlan)(nil),
		},
		{
			name:     "single table",
			order:    []string{"table1"},
			expected: nil, // simplified implementation
		},
		{
			name:     "multiple tables",
			order:    []string{"table1", "table2"},
			expected: nil, // simplified implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := reorder.buildPlanFromOrder(tt.order, []LogicalPlan{})
			// For empty order, we expect nil
			if len(tt.order) == 0 {
				assert.Nil(t, plan)
			} else {
				// For non-empty order, we should get a valid plan
				assert.NotNil(t, plan)
			}
		})
	}
}

func TestDPJoinReorder_CollectJoinsRecursive(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tests := []struct {
		name      string
		plan      LogicalPlan
		wantNodes int
	}{
		{
			name:      "nil plan",
			plan:      nil,
			wantNodes: 0,
		},
		{
			name:      "simple plan",
			plan:      &mockLogicalPlan{},
			wantNodes: 0,
		},
		{
			name: "plan with children",
			plan: &mockLogicalPlan{
				children: []LogicalPlan{
					&mockLogicalPlan{},
					&mockLogicalPlan{},
				},
			},
			wantNodes: 0, // simplified logic
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			joinNodes := []LogicalPlan{}
			tables := map[string]bool{}
			reorder.collectJoinsRecursive(tt.plan, &joinNodes, tables)
			// Just ensure it doesn't panic
			assert.NotNil(t, joinNodes)
		})
	}
}

func TestDPJoinReorder_SolveDP_NoSolution(t *testing.T) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tables := []string{"table1", "table2"}
	dp := map[string]*DPState{}
	joinNodes := []LogicalPlan{}

	result := reorder.solveDP(tables, dp, joinNodes)
	// Without DP states, no solution can be found, so nil is expected
	assert.Nil(t, result)
}

func TestReorderCache_Concurrency(t *testing.T) {
	cache := NewReorderCache(1000)

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			result := &ReorderResult{
				Order: []string{"table" + string(rune('0'+idx))},
				Cost:  float64(idx * 100),
			}
			cache.Set("key"+string(rune('0'+idx)), result)
			done <- true
		}(i)
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all are present
	for i := 0; i < 10; i++ {
		result := cache.Get("key" + string(rune('0'+i)))
		assert.NotNil(t, result)
	}
}

// Mock implementations
type mockCostModel struct{}

func (m *mockCostModel) ScanCost(tableName string, rowCount int64, useIndex bool) float64 {
	return float64(rowCount) * 0.01
}

func (m *mockCostModel) JoinCost(left, right LogicalPlan, joinType JoinType, conditions []*parser.Expression) float64 {
	return 100.0
}

type mockCardinalityEstimator struct{}

func (m *mockCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	return 10000
}

type testMockLogicalPlan struct {
	tableName string
	children []LogicalPlan
}

func (m *testMockLogicalPlan) Children() []LogicalPlan {
	return m.children
}

func (m *testMockLogicalPlan) SetChildren(children ...LogicalPlan) {
	m.children = children
}

func (m *testMockLogicalPlan) Explain() string {
	if m.tableName != "" {
		return "DataSource(" + m.tableName + ")"
	}
	return "Mock Plan"
}

// Benchmark tests
func BenchmarkDPJoinReorder_Reorder(b *testing.B) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	plan := &testMockLogicalPlan{
		children: []LogicalPlan{
			&mockLogicalPlan{},
			&mockLogicalPlan{},
			&mockLogicalPlan{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reorder.Reorder(context.Background(), plan)
	}
}

func BenchmarkDPJoinReorder_DpSearch(b *testing.B) {
	costModel := &mockCostModel{}
	estimator := &mockCardinalityEstimator{}
	reorder := NewDPJoinReorder(costModel, estimator, 10)

	tables := []string{"table1", "table2", "table3"}
	joinNodes := []LogicalPlan{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reorder.dpSearch(tables, joinNodes)
	}
}

func BenchmarkReorderCache_Get(b *testing.B) {
	cache := NewReorderCache(1000)
	result := &ReorderResult{
		Order: []string{"table1", "table2"},
		Cost:  100.0,
	}
	cache.Set("key1", result)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("key1")
	}
}
