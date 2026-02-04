package cost

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewAdaptiveCostModel(t *testing.T) {
	tests := []struct {
		name      string
		estimator CardinalityEstimator
		wantNil   bool
	}{
		{
			name:      "with simple estimator",
			estimator: &SimpleCardinalityEstimator{},
			wantNil:   false,
		},
		{
			name:      "with nil estimator",
			estimator: nil,
			wantNil:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := NewAdaptiveCostModel(tt.estimator)
			if tt.wantNil {
				assert.Nil(t, model)
			} else {
				assert.NotNil(t, model)
				assert.NotNil(t, model.hardware)
				assert.NotNil(t, model.factors)
				assert.NotNil(t, model.cacheHitInfo)
			}
		})
	}
}

func TestNewEnhancedCostModel(t *testing.T) {
	tests := []struct {
		name      string
		hardware  *HardwareProfile
		wantNil   bool
	}{
		{
			name:     "with hardware profile",
			hardware: DetectHardwareProfile(),
			wantNil:  false,
		},
		{
			name:     "with nil hardware",
			hardware: nil,
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := NewEnhancedCostModel(tt.hardware)
			if tt.wantNil {
				assert.Nil(t, model)
			} else {
				assert.NotNil(t, model)
				assert.NotNil(t, model.hardware)
				assert.NotNil(t, model.factors)
				assert.NotNil(t, model.estimator)
			}
		})
	}
}

func TestAdaptiveCostModel_ScanCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		tableName  string
		rowCount   int64
		useIndex   bool
		wantZero   bool
		wantNonNeg bool
	}{
		{
			name:       "zero rows",
			tableName:  "test_table",
			rowCount:   0,
			useIndex:   false,
			wantZero:   true,
			wantNonNeg: true,
		},
		{
			name:       "negative rows",
			tableName:  "test_table",
			rowCount:   -10,
			useIndex:   false,
			wantZero:   true,
			wantNonNeg: true,
		},
		{
			name:       "small table without index",
			tableName:  "test_table",
			rowCount:   100,
			useIndex:   false,
			wantZero:   false,
			wantNonNeg: true,
		},
		{
			name:       "small table with index",
			tableName:  "test_table",
			rowCount:   100,
			useIndex:   true,
			wantZero:   false,
			wantNonNeg: true,
		},
		{
			name:       "large table without index",
			tableName:  "test_table",
			rowCount:   1000000,
			useIndex:   false,
			wantZero:   false,
			wantNonNeg: true,
		},
		{
			name:       "large table with index",
			tableName:  "test_table",
			rowCount:   1000000,
			useIndex:   true,
			wantZero:   false,
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.ScanCost(tt.tableName, tt.rowCount, tt.useIndex)
			if tt.wantZero {
				assert.Equal(t, float64(0), cost, "expected zero cost")
			} else {
				assert.Greater(t, cost, float64(0), "expected positive cost")
			}
		})
	}
}

func TestAdaptiveCostModel_FilterCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name         string
		inputRows    int64
		selectivity  float64
		filters      []domain.Filter
		wantNonNeg   bool
	}{
		{
			name:       "no filters",
			inputRows:  1000,
			selectivity: 0.5,
			filters:    []domain.Filter{},
			wantNonNeg: true,
		},
		{
			name:       "single filter",
			inputRows:  1000,
			selectivity: 0.1,
			filters: []domain.Filter{
				{Field: "age", Operator: ">", Value: 18},
			},
			wantNonNeg: true,
		},
		{
			name:       "multiple filters",
			inputRows:  1000,
			selectivity: 0.05,
			filters: []domain.Filter{
				{Field: "age", Operator: ">", Value: 18},
				{Field: "score", Operator: "<", Value: 100},
			},
			wantNonNeg: true,
		},
		{
			name:       "zero selectivity",
			inputRows:  1000,
			selectivity: 0,
			filters:    []domain.Filter{},
			wantNonNeg: true,
		},
		{
			name:       "full selectivity",
			inputRows:  1000,
			selectivity: 1.0,
			filters:    []domain.Filter{},
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.FilterCost(tt.inputRows, tt.selectivity, tt.filters)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, cost, float64(0), "expected non-negative cost")
			}
		})
	}
}

func TestAdaptiveCostModel_JoinCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		left       interface{}
		right      interface{}
		joinType   JoinType
		wantNonNeg bool
	}{
		{
			name:       "inner join",
			left:       "left_table",
			right:      "right_table",
			joinType:   InnerJoin,
			wantNonNeg: true,
		},
		{
			name:       "left outer join",
			left:       "left_table",
			right:      "right_table",
			joinType:   LeftOuterJoin,
			wantNonNeg: true,
		},
		{
			name:       "right outer join",
			left:       "left_table",
			right:      "right_table",
			joinType:   RightOuterJoin,
			wantNonNeg: true,
		},
		{
			name:       "full outer join",
			left:       "left_table",
			right:      "right_table",
			joinType:   FullOuterJoin,
			wantNonNeg: true,
		},
		{
			name:       "unknown join type",
			left:       "left_table",
			right:      "right_table",
			joinType:   JoinType(100),
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.JoinCost(tt.left, tt.right, tt.joinType, nil)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, cost, float64(0), "expected non-negative cost")
			}
		})
	}
}

func TestAdaptiveCostModel_AggregateCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		inputRows  int64
		groupByCols int
		aggFuncs   int
		wantNonNeg bool
	}{
		{
			name:       "zero rows",
			inputRows:  0,
			groupByCols: 1,
			aggFuncs:   1,
			wantNonNeg: true,
		},
		{
			name:       "single group",
			inputRows:  1000,
			groupByCols: 1,
			aggFuncs:   1,
			wantNonNeg: true,
		},
		{
			name:       "multiple groups",
			inputRows:  1000,
			groupByCols: 3,
			aggFuncs:   2,
			wantNonNeg: true,
		},
		{
			name:       "large table",
			inputRows:  1000000,
			groupByCols: 2,
			aggFuncs:   3,
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.AggregateCost(tt.inputRows, tt.groupByCols, tt.aggFuncs)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, cost, float64(0), "expected non-negative cost")
			}
		})
	}
}

func TestAdaptiveCostModel_ProjectCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		inputRows  int64
		projCols   int
		wantNonNeg bool
	}{
		{
			name:       "zero columns",
			inputRows:  1000,
			projCols:   0,
			wantNonNeg: true,
		},
		{
			name:       "single column",
			inputRows:  1000,
			projCols:   1,
			wantNonNeg: true,
		},
		{
			name:       "multiple columns",
			inputRows:  1000,
			projCols:   5,
			wantNonNeg: true,
		},
		{
			name:       "many columns",
			inputRows:  1000000,
			projCols:   20,
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.ProjectCost(tt.inputRows, tt.projCols)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, cost, float64(0), "expected non-negative cost")
			}
		})
	}
}

func TestAdaptiveCostModel_SortCost(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		inputRows  int64
		wantZero   bool
		wantNonNeg bool
	}{
		{
			name:       "zero rows",
			inputRows:  0,
			wantZero:   true,
			wantNonNeg: true,
		},
		{
			name:       "single row",
			inputRows:  1,
			wantZero:   true,
			wantNonNeg: true,
		},
		{
			name:       "small table",
			inputRows:  100,
			wantZero:   false,
			wantNonNeg: true,
		},
		{
			name:       "large table",
			inputRows:  1000000,
			wantZero:   false,
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := model.SortCost(tt.inputRows)
			if tt.wantZero {
				assert.Equal(t, float64(0), cost, "expected zero cost")
			} else {
				assert.Greater(t, cost, float64(0), "expected positive cost")
			}
		})
	}
}

func TestAdaptiveCostModel_UpdateCacheHitInfo(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name     string
		table    string
		hitRate  float64
	}{
		{
			name:    "normal hit rate",
			table:   "test_table",
			hitRate: 0.8,
		},
		{
			name:    "zero hit rate",
			table:   "test_table",
			hitRate: 0.0,
		},
		{
			name:    "full hit rate",
			table:   "test_table",
			hitRate: 1.0,
		},
		{
			name:    "invalid hit rate",
			table:   "test_table",
			hitRate: 1.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model.UpdateCacheHitInfo(tt.table, tt.hitRate)
			rate := model.getTableCacheHitRate(tt.table)
			assert.Equal(t, tt.hitRate, rate)
		})
	}
}

func TestAdaptiveCostModel_GetHardwareProfile(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})
	hardware := model.GetHardwareProfile()

	assert.NotNil(t, hardware)
	assert.Greater(t, hardware.CPUCores, 0, "CPU cores should be positive")
	assert.Greater(t, hardware.TotalMemory, int64(0), "memory should be positive")
}

func TestAdaptiveCostModel_GetCostFactors(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})
	factors := model.GetCostFactors()

	assert.NotNil(t, factors)
	assert.Greater(t, factors.IOFactor, float64(0), "IO factor should be positive")
	assert.Greater(t, factors.CPUFactor, float64(0), "CPU factor should be positive")
	assert.Greater(t, factors.MemoryFactor, float64(0), "memory factor should be positive")
	assert.Greater(t, factors.NetworkFactor, float64(0), "network factor should be positive")
}

func TestAdaptiveCostModel_Explain(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})
	explain := model.Explain()

	assert.NotEmpty(t, explain, "explain should not be empty")
	assert.Contains(t, explain, "IO Factor", "explain should contain IO Factor")
	assert.Contains(t, explain, "CPU Factor", "explain should contain CPU Factor")
	assert.Contains(t, explain, "Memory Factor", "explain should contain Memory Factor")
	assert.Contains(t, explain, "Cache Hit Rate", "explain should contain Cache Hit Rate")
}

func TestSimpleCardinalityEstimator_EstimateTableScan(t *testing.T) {
	estimator := &SimpleCardinalityEstimator{}

	tests := []struct {
		name       string
		tableName  string
		wantResult int64
	}{
		{
			name:       "any table",
			tableName:  "test_table",
			wantResult: 10000,
		},
		{
			name:       "empty table name",
			tableName:  "",
			wantResult: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.EstimateTableScan(tt.tableName)
			assert.Equal(t, tt.wantResult, result)
		})
	}
}

func TestSimpleCardinalityEstimator_EstimateFilter(t *testing.T) {
	estimator := &SimpleCardinalityEstimator{}

	tests := []struct {
		name    string
		table   string
		filters []domain.Filter
		want    int64
	}{
		{
			name:    "no filters",
			table:   "test_table",
			filters: []domain.Filter{},
			want:    1000,
		},
		{
			name:  "with filters",
			table: "test_table",
			filters: []domain.Filter{
				{Field: "age", Operator: ">", Value: 18},
			},
			want: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.EstimateFilter(tt.table, tt.filters)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestAdaptiveCostModel_ScanCostForJoinModel(t *testing.T) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})

	tests := []struct {
		name       string
		tableName  string
		rowCount   int64
		useIndex   bool
	}{
		{
			name:      "without index",
			tableName: "test_table",
			rowCount:  1000,
			useIndex:  false,
		},
		{
			name:      "with index",
			tableName: "test_table",
			rowCount:  1000,
			useIndex:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost1 := model.ScanCost(tt.tableName, tt.rowCount, tt.useIndex)
			cost2 := model.ScanCostForJoinModel(tt.tableName, tt.rowCount, tt.useIndex)
			assert.Equal(t, cost1, cost2, "ScanCostForJoinModel should return same as ScanCost")
		})
	}
}

// Benchmark tests
func BenchmarkAdaptiveCostModel_ScanCost(b *testing.B) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		model.ScanCost("test_table", 10000, false)
	}
}

func BenchmarkAdaptiveCostModel_JoinCost(b *testing.B) {
	model := NewAdaptiveCostModel(&SimpleCardinalityEstimator{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		model.JoinCost("left", "right", InnerJoin, nil)
	}
}
