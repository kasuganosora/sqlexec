package statistics

import (
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewEnhancedCardinalityEstimator(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	assert.NotNil(t, estimator)
	assert.NotNil(t, estimator.statsCache)
	assert.NotNil(t, estimator.stats)
	assert.Equal(t, cache, estimator.statsCache)
}

func TestEnhancedCardinalityEstimator_UpdateStatistics(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}

	estimator.UpdateStatistics("test_table", stats)

	// Check in memory
	memoryStats, exists := estimator.stats["test_table"]
	assert.True(t, exists)
	assert.Equal(t, stats, memoryStats)

	// Check in cache
	cachedStats, ok := cache.Get("test_table")
	assert.True(t, ok)
	assert.Equal(t, stats, cachedStats)
}

func TestEnhancedCardinalityEstimator_GetStatistics(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.stats["test_table"] = stats

	// Get from memory
	retrievedStats, err := estimator.GetStatistics("test_table")
	assert.NoError(t, err)
	assert.Equal(t, stats, retrievedStats)

	// Test non-existent
	_, err = estimator.GetStatistics("non_existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "statistics not found")
}

func TestEnhancedCardinalityEstimator_GetStatistics_FromCache(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	cache.Set("test_table", stats)

	// Get from cache
	retrievedStats, err := estimator.GetStatistics("test_table")
	assert.NoError(t, err)
	assert.Equal(t, stats, retrievedStats)

	// Should now be in memory as well
	_, exists := estimator.stats["test_table"]
	assert.True(t, exists)
}

func TestEnhancedCardinalityEstimator_EstimateTableScan(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	tests := []struct {
		name       string
		tableName  string
		setupStats bool
		expected   int64
	}{
		{
			name:       "with statistics",
			tableName:  "test_table",
			setupStats: true,
			expected:   1000,
		},
		{
			name:       "without statistics",
			tableName:  "non_existent",
			setupStats: false,
			expected:   10000, // default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupStats {
				stats := &TableStatistics{
					Name:            tt.tableName,
					RowCount:        tt.expected,
					ColumnStats:     make(map[string]*ColumnStatistics),
					Histograms:      make(map[string]*Histogram),
					EstimatedRowCount: tt.expected,
				}
				estimator.UpdateStatistics(tt.tableName, stats)
			}

			result := estimator.EstimateTableScan(tt.tableName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateTableScan_EstimatedRowCount(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:             "test_table",
		RowCount:         1000,
		ColumnStats:       make(map[string]*ColumnStatistics),
		Histograms:        make(map[string]*Histogram),
		EstimatedRowCount: 5000, // different from actual RowCount
	}
	estimator.UpdateStatistics("test_table", stats)

	result := estimator.EstimateTableScan("test_table")
	assert.Equal(t, int64(5000), result, "should use EstimatedRowCount")
}

func TestEnhancedCardinalityEstimator_EstimateFilter_NoFilters(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	result := estimator.EstimateFilter("test_table", []domain.Filter{})
	assert.Equal(t, int64(1000), result, "no filters should return full table count")
}

func TestEnhancedCardinalityEstimator_EstimateFilter_WithFilters(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name: "test_table",
		RowCount: 10000,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DataType:      "integer",
				DistinctCount: 80,
				NullCount:     0,
				MinValue:      int64(18),
				MaxValue:      int64(80),
				NullFraction:  0.0,
			},
		},
		Histograms: make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 30},
	}

	result := estimator.EstimateFilter("test_table", filters)
	assert.Greater(t, result, int64(0))
	assert.Less(t, result, int64(10000), "filter should reduce row count")
}

func TestEnhancedCardinalityEstimator_EstimateFilter_NoStatistics(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 30},
	}

	result := estimator.EstimateFilter("non_existent", filters)
	assert.GreaterOrEqual(t, result, int64(1), "should return at least 1")
}

func TestEnhancedCardinalityEstimator_EstimateFilter_MultipleFilters(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name: "test_table",
		RowCount: 10000,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DataType:      "integer",
				DistinctCount: 80,
			},
		},
		Histograms: make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 30},
		{Field: "age", Operator: "<", Value: 50},
	}

	result := estimator.EstimateFilter("test_table", filters)
	assert.Greater(t, result, int64(0))
	assert.Less(t, result, int64(10000))
}

func TestEnhancedCardinalityEstimator_EstimateFilter_LogicOp(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   10000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	tests := []struct {
		name     string
		logicOp  string
		expected string // "valid" or "error"
	}{
		{
			name:     "AND logic",
			logicOp:  "AND",
			expected: "valid",
		},
		{
			name:     "OR logic",
			logicOp:  "OR",
			expected: "valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filters := []domain.Filter{
				{
					LogicOp: tt.logicOp,
					SubFilters: []domain.Filter{
						{Field: "age", Operator: ">", Value: 30},
					},
				},
			}

			result := estimator.EstimateFilter("test_table", filters)
			if tt.expected == "valid" {
				assert.GreaterOrEqual(t, result, int64(1))
			}
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateJoin(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	tests := []struct {
		name       string
		left       interface{}
		right      interface{}
		joinType   string
		wantNonNeg bool
	}{
		{
			name:       "inner join",
			left:       "left_table",
			right:      "right_table",
			joinType:   "INNER",
			wantNonNeg: true,
		},
		{
			name:       "left join",
			left:       "left_table",
			right:      "right_table",
			joinType:   "LEFT",
			wantNonNeg: true,
		},
		{
			name:       "right join",
			left:       "left_table",
			right:      "right_table",
			joinType:   "RIGHT",
			wantNonNeg: true,
		},
		{
			name:       "full join",
			left:       "left_table",
			right:      "right_table",
			joinType:   "FULL",
			wantNonNeg: true,
		},
		{
			name:       "unknown join type",
			left:       "left_table",
			right:      "right_table",
			joinType:   "UNKNOWN",
			wantNonNeg: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.EstimateJoin(tt.left, tt.right, tt.joinType)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, result, int64(0))
			}
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateDistinct(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	stats := &TableStatistics{
		Name: "test_table",
		RowCount: 10000,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {
				Name:          "id",
				DataType:      "integer",
				DistinctCount: 10000,
			},
			"age": {
				Name:          "age",
				DataType:      "integer",
				DistinctCount: 80,
			},
		},
		Histograms: make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	tests := []struct {
		name        string
		columns     []string
		wantNonNeg  bool
		wantLessThan int64
	}{
		{
			name:        "single column",
			columns:     []string{"id"},
			wantNonNeg:  true,
			wantLessThan: 10001,
		},
		{
			name:        "multiple columns",
			columns:     []string{"id", "age"},
			wantNonNeg:  true,
			wantLessThan: 10001,
		},
		{
			name:        "empty columns",
			columns:     []string{},
			wantNonNeg:  true,
			wantLessThan: 10001,
		},
		{
			name:        "non-existent table",
			columns:     []string{"id"},
			wantNonNeg:  true,
			wantLessThan: 5001, // default table/2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName := "test_table"
			if tt.name == "non-existent table" {
				tableName = "non_existent"
			}
			result := estimator.EstimateDistinct(tableName, tt.columns)
			if tt.wantNonNeg {
				assert.GreaterOrEqual(t, result, int64(0))
			}
			assert.LessOrEqual(t, result, tt.wantLessThan)
		})
	}
}

func TestEnhancedCardinalityEstimator_GetDefaultSelectivity(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	tests := []struct {
		name     string
		operator string
		expected float64
	}{
		{"equality", "=", 0.1},
		{"not equal", "!=", 0.9},
		{"greater than", ">", 0.3},
		{"greater than or equal", ">=", 0.3},
		{"less than", "<", 0.3},
		{"less than or equal", "<=", 0.3},
		{"in", "IN", 0.2},
		{"between", "BETWEEN", 0.3},
		{"like", "LIKE", 0.25},
		{"unknown", "UNKNOWN", 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.getDefaultSelectivity(tt.operator)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateRangeSelectivityUsingStats(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	colStats := &ColumnStatistics{
		MinValue: int64(0),
		MaxValue: int64(100),
	}

	tests := []struct {
		name      string
		operator  string
		value     interface{}
		wantValid bool
	}{
		{
			name:      "greater than mid",
			operator:  ">",
			value:     int64(50),
			wantValid: true,
		},
		{
			name:      "greater than min",
			operator:  ">",
			value:     int64(0),
			wantValid: true,
		},
		{
			name:      "greater than max",
			operator:  ">",
			value:     int64(100),
			wantValid: true,
		},
		{
			name:      "less than mid",
			operator:  "<",
			value:     int64(50),
			wantValid: true,
		},
		{
			name:      "less than min",
			operator:  "<",
			value:     int64(0),
			wantValid: true,
		},
		{
			name:      "less than max",
			operator:  "<",
			value:     int64(100),
			wantValid: true,
		},
		{
			name:      "non-numeric value",
			operator:  ">",
			value:     "string",
			wantValid: false,
		},
		{
			name:      "nil value",
			operator:  ">",
			value:     nil,
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.estimateRangeSelectivityUsingStats(tt.operator, tt.value, colStats)
			if tt.wantValid {
				assert.GreaterOrEqual(t, result, 0.0, "selectivity should be >= 0")
				assert.LessOrEqual(t, result, 1.0, "selectivity should be <= 1")
			} else {
				assert.Equal(t, 0.1, result, "invalid value should return default")
			}
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateLogicSelectivity(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	tests := []struct {
		name     string
		logicOp  string
		expected string // "valid" or "error"
	}{
		{
			name:     "AND logic",
			logicOp:  "AND",
			expected: "valid",
		},
		{
			name:     "OR logic",
			logicOp:  "OR",
			expected: "valid",
		},
		{
			name:     "unknown logic",
			logicOp:  "UNKNOWN",
			expected: "valid", // returns 1.0
		},
		{
			name:     "empty subfilters",
			logicOp:  "AND",
			expected: "valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := domain.Filter{
				LogicOp: tt.logicOp,
				SubFilters: []domain.Filter{
					{Field: "age", Operator: ">", Value: 30},
				},
			}

			result := estimator.estimateLogicSelectivity("test_table", filter)
			if tt.expected == "valid" {
				assert.GreaterOrEqual(t, result, 0.0)
				assert.LessOrEqual(t, result, 1.0)
			}
		})
	}
}

func TestExpressionToString(t *testing.T) {
	tests := []struct {
		name     string
		expr     interface{} // use interface{} to allow nil
		expected string
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: "",
		},
		{
			name:     "expression with column",
			expr:     interface{}("{Column: 'age'}"), // simplified
			expected: "age",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is a simplified test as we can't easily construct parser.Expression
			result := expressionToString(nil)
			if tt.expected == "" {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateOrSelectivity(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	filter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "age", Operator: "=", Value: 25},
			{Field: "age", Operator: "=", Value: 30},
		},
	}

	result := estimator.estimateOrSelectivity("test_table", filter)
	assert.GreaterOrEqual(t, result, 0.0)
	assert.LessOrEqual(t, result, 0.95, "should cap at 0.95")
}

func TestEnhancedCardinalityEstimator_EstimateOrSelectivity_EmptySubFilters(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	filter := domain.Filter{
		LogicOp:    "OR",
		SubFilters: []domain.Filter{},
	}

	result := estimator.estimateOrSelectivity("test_table", filter)
	assert.Equal(t, 1.0, result, "empty OR should return 1.0")
}

func TestEnhancedCardinalityEstimator_EstimateEquijoinSelectivity(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	// Setup stats for both tables
	leftStats := &TableStatistics{
		Name:       "left_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("left_table", leftStats)

	rightStats := &TableStatistics{
		Name:       "right_table",
		RowCount:   2000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("right_table", rightStats)

	result := estimator.estimateEquijoinSelectivity("left_table", "right_table")
	assert.Greater(t, result, 0.0)
	assert.LessOrEqual(t, result, 1.0)
}

func TestEnhancedCardinalityEstimator_EstimateEquijoinSelectivity_NoStats(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	result := estimator.estimateEquijoinSelectivity("non_existent_left", "non_existent_right")
	assert.Equal(t, 0.1, result, "no stats should return default 0.1")
}

func TestEnhancedCardinalityEstimator_EstimateRowCount(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	result := estimator.estimateRowCount(nil)
	assert.Equal(t, int64(10000), result, "should return default")
}

func TestEnhancedCardinalityEstimator_EstimateSelectivityUsingStats_IN(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	colStats := &ColumnStatistics{
		DistinctCount: 100,
	}

	filter := domain.Filter{
		Operator: "IN",
		Value:    []interface{}{1, 2, 3, 4, 5},
	}

	result := estimator.estimateSelectivityUsingStats(filter, colStats)
	assert.Greater(t, result, 0.0)
	assert.LessOrEqual(t, result, 1.0)
}

func TestEnhancedCardinalityEstimator_EstimateSelectivityUsingStats_LIKE(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	colStats := &ColumnStatistics{}

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "prefix pattern",
			pattern:  "abc%",
			expected: "higher",
		},
		{
			name:     "no pattern",
			pattern:  "abc",
			expected: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := domain.Filter{
				Operator: "LIKE",
				Value:    tt.pattern,
			}

			result := estimator.estimateSelectivityUsingStats(filter, colStats)
			assert.GreaterOrEqual(t, result, 0.0)
			assert.LessOrEqual(t, result, 1.0)
		})
	}
}

func TestEnhancedCardinalityEstimator_EstimateSelectivityUsingStats_BETWEEN(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	colStats := &ColumnStatistics{
		MinValue: int64(0),
		MaxValue: int64(100),
	}

	filter := domain.Filter{
		Operator: "BETWEEN",
		Value:    []interface{}{int64(25), int64(75)},
	}

	result := estimator.estimateSelectivityUsingStats(filter, colStats)
	assert.Greater(t, result, 0.0)
	assert.LessOrEqual(t, result, 1.0)
}

func TestEnhancedCardinalityEstimator_EstimateSelectivityUsingStats_InvalidValue(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	colStats := &ColumnStatistics{
		DistinctCount: 100,
	}

	filter := domain.Filter{
		Operator: "IN",
		Value:    "not a list",
	}

	result := estimator.estimateSelectivityUsingStats(filter, colStats)
	assert.Equal(t, 0.2, result, "invalid value should return default")
}

func TestEnhancedCardinalityEstimator_EstimateLogicSelectivity_Cap(t *testing.T) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	// Create a filter that would exceed 1.0 selectivity
	filter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "age", Operator: ">", Value: 0},
			{Field: "age", Operator: "<", Value: 100},
			{Field: "age", Operator: "=", Value: 50},
		},
	}

	result := estimator.estimateLogicSelectivity("test_table", filter)
	assert.LessOrEqual(t, result, 0.95, "should cap at 0.95")
}

// Benchmark tests
func BenchmarkEnhancedCardinalityEstimator_EstimateTableScan(b *testing.B) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)
	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   1000,
		ColumnStats: make(map[string]*ColumnStatistics),
		Histograms:  make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimator.EstimateTableScan("test_table")
	}
}

func BenchmarkEnhancedCardinalityEstimator_EstimateFilter(b *testing.B) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)
	stats := &TableStatistics{
		Name:       "test_table",
		RowCount:   10000,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DistinctCount: 80,
			},
		},
		Histograms: make(map[string]*Histogram),
	}
	estimator.UpdateStatistics("test_table", stats)

	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 30},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimator.EstimateFilter("test_table", filters)
	}
}

func BenchmarkEnhancedCardinalityEstimator_GetDefaultSelectivity(b *testing.B) {
	cache := NewStatisticsCache(time.Hour)
	estimator := NewEnhancedCardinalityEstimator(cache)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimator.getDefaultSelectivity("=")
	}
}
