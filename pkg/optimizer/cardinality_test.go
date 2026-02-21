package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestNewSimpleCardinalityEstimator(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	if estimator == nil {
		t.Fatal("NewSimpleCardinalityEstimator() returned nil")
	}

	if estimator.stats == nil {
		t.Error("stats map not initialized")
	}
}

func TestSimpleCardinalityEstimatorUpdateStatistics(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	stats := &TableStatistics{
		Name:     "test_table",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {
				Name:          "id",
				DataType:      "INT",
				DistinctCount: 1000,
				NullCount:     0,
			},
		},
	}

	estimator.UpdateStatistics("test_table", stats)

	retrieved, exists := estimator.stats["test_table"]
	if !exists {
		t.Error("Statistics not stored correctly")
	}

	if retrieved.RowCount != 1000 {
		t.Errorf("RowCount = %v, expected 1000", retrieved.RowCount)
	}
}

func TestSimpleCardinalityEstimatorEstimateTableScan(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	t.Run("with statistics", func(t *testing.T) {
		stats := &TableStatistics{
			Name:     "test_table",
			RowCount: 5000,
			ColumnStats: map[string]*ColumnStatistics{
				"id": {DistinctCount: 5000},
			},
		}
		estimator.UpdateStatistics("test_table", stats)

		result := estimator.EstimateTableScan("test_table")
		if result != 5000 {
			t.Errorf("EstimateTableScan() = %v, expected 5000", result)
		}
	})

	t.Run("without statistics", func(t *testing.T) {
		result := estimator.EstimateTableScan("unknown_table")
		if result != 1000 {
			t.Errorf("EstimateTableScan() = %v, expected 1000 (default)", result)
		}
	})
}

func TestSimpleCardinalityEstimatorEstimateFilter(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	stats := &TableStatistics{
		Name:     "test_table",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {
				Name:          "id",
				DataType:      "INT",
				DistinctCount: 1000,
				NullCount:     0,
			},
		},
	}
	estimator.UpdateStatistics("test_table", stats)

	t.Run("no filters", func(t *testing.T) {
		result := estimator.EstimateFilter("test_table", []domain.Filter{})
		if result != 1000 {
			t.Errorf("EstimateFilter() = %v, expected 1000", result)
		}
	})

	t.Run("equality filter", func(t *testing.T) {
		filters := []domain.Filter{
			{
				Field:    "id",
				Operator: "=",
				Value:    100,
			},
		}
		result := estimator.EstimateFilter("test_table", filters)
		// 1/NDV = 1/1000 = 0.001, * 1000 = 1
		if result < 1 {
			t.Errorf("EstimateFilter() = %v, expected >= 1", result)
		}
	})

	t.Run("AND filters", func(t *testing.T) {
		filters := []domain.Filter{
			{
				Field:    "id",
				Operator: "=",
				Value:    100,
				LogicOp:  "AND",
			},
		}
		result := estimator.EstimateFilter("test_table", filters)
		if result < 1 {
			t.Errorf("EstimateFilter() = %v, expected >= 1", result)
		}
	})

	t.Run("OR filters with subfilters", func(t *testing.T) {
		filters := []domain.Filter{
			{
				LogicOp: "OR",
				SubFilters: []domain.Filter{
					{Field: "id", Operator: "=", Value: 100},
					{Field: "id", Operator: "=", Value: 200},
				},
			},
		}
		result := estimator.EstimateFilter("test_table", filters)
		if result < 1 {
			t.Errorf("EstimateFilter() = %v, expected >= 1", result)
		}
	})

	t.Run("unknown table", func(t *testing.T) {
		filters := []domain.Filter{
			{Field: "id", Operator: "=", Value: 100},
		}
		result := estimator.EstimateFilter("unknown_table", filters)
		if result != 100 { // 1000 * 0.1 (default selectivity for =)
			t.Errorf("EstimateFilter() = %v, expected 100", result)
		}
	})
}

func TestSimpleCardinalityEstimatorEstimateFilterSelectivity(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	stats := &TableStatistics{
		Name:     "test_table",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DataType:      "INT",
				DistinctCount: 100,
				NullCount:     0,
				MinValue:      0,
				MaxValue:      100,
			},
		},
	}
	estimator.UpdateStatistics("test_table", stats)

	t.Run("equality operator", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: "=",
			Value:    50,
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		// 1/NDV = 1/100 = 0.01
		if sel <= 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("greater than operator", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: ">",
			Value:    50,
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		if sel < 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("IN operator", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: "IN",
			Value:    []interface{}{1, 2, 3, 4, 5},
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		// 5/100 = 0.05
		if sel <= 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("BETWEEN operator", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: "BETWEEN",
			Value:    []interface{}{20, 30},
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		if sel <= 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("LIKE operator", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: "LIKE",
			Value:    "test%",
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		// Default 0.25
		if sel != 0.25 {
			t.Errorf("Selectivity = %v, expected 0.25", sel)
		}
	})

	t.Run("unknown column", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "unknown_col",
			Operator: "=",
			Value:    50,
		}
		sel := estimator.estimateFilterSelectivity("test_table", filter)
		// Default 0.1
		if sel != 0.1 {
			t.Errorf("Selectivity = %v, expected 0.1", sel)
		}
	})

	t.Run("unknown table", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: "=",
			Value:    50,
		}
		sel := estimator.estimateFilterSelectivity("unknown_table", filter)
		// Default 0.1
		if sel != 0.1 {
			t.Errorf("Selectivity = %v, expected 0.1", sel)
		}
	})
}

func TestSimpleCardinalityEstimatorEstimateLogicSelectivity(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	stats := &TableStatistics{
		Name:     "test_table",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"age": {
				Name:          "age",
				DataType:      "INT",
				DistinctCount: 100,
				NullCount:     0,
			},
		},
	}
	estimator.UpdateStatistics("test_table", stats)

	t.Run("AND logic", func(t *testing.T) {
		filter := domain.Filter{
			LogicOp: "AND",
			SubFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: 50},
				{Field: "age", Operator: "=", Value: 50},
			},
		}
		sel := estimator.estimateLogicSelectivity("test_table", filter)
		if sel <= 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("OR logic", func(t *testing.T) {
		filter := domain.Filter{
			LogicOp: "OR",
			SubFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: 50},
				{Field: "age", Operator: "=", Value: 60},
			},
		}
		sel := estimator.estimateLogicSelectivity("test_table", filter)
		if sel <= 0 || sel > 1 {
			t.Errorf("Selectivity = %v, expected between 0 and 1", sel)
		}
	})

	t.Run("empty subfilters", func(t *testing.T) {
		filter := domain.Filter{
			LogicOp:     "AND",
			SubFilters:  []domain.Filter{},
		}
		sel := estimator.estimateLogicSelectivity("test_table", filter)
		if sel != 1.0 {
			t.Errorf("Selectivity = %v, expected 1.0", sel)
		}
	})
}

func TestSimpleCardinalityEstimatorEstimateRangeSelectivity(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	colStats := &ColumnStatistics{
		Name:     "age",
		DataType: "INT",
		MinValue: 0,
		MaxValue: 100,
	}

	t.Run("greater than", func(t *testing.T) {
		sel := estimator.estimateRangeSelectivity(">", 50, colStats)
		expected := 0.5 // (100-50)/100
		if sel != expected {
			t.Errorf("Selectivity = %v, expected %v", sel, expected)
		}
	})

	t.Run("greater than or equal", func(t *testing.T) {
		sel := estimator.estimateRangeSelectivity(">=", 50, colStats)
		expected := 0.500001 // (100-50+0.0001)/100
		if sel != expected {
			t.Errorf("Selectivity = %v, expected %v", sel, expected)
		}
	})

	t.Run("less than", func(t *testing.T) {
		sel := estimator.estimateRangeSelectivity("<", 50, colStats)
		expected := 0.5 // (50-0)/100
		if sel != expected {
			t.Errorf("Selectivity = %v, expected %v", sel, expected)
		}
	})

	t.Run("less than or equal", func(t *testing.T) {
		sel := estimator.estimateRangeSelectivity("<=", 50, colStats)
		expected := 0.500001 // (50-0+0.0001)/100
		if sel != expected {
			t.Errorf("Selectivity = %v, expected %v", sel, expected)
		}
	})

	t.Run("nil min max", func(t *testing.T) {
		colStats := &ColumnStatistics{
			Name:     "age",
			DataType: "INT",
			MinValue: nil,
			MaxValue: nil,
		}
		sel := estimator.estimateRangeSelectivity(">", 50, colStats)
		if sel != 0.1 {
			t.Errorf("Selectivity = %v, expected 0.1", sel)
		}
	})

	t.Run("equal min max", func(t *testing.T) {
		colStats := &ColumnStatistics{
			Name:     "age",
			DataType: "INT",
			MinValue: 100,
			MaxValue: 100,
		}
		sel := estimator.estimateRangeSelectivity(">", 50, colStats)
		if sel != 1.0 {
			t.Errorf("Selectivity = %v, expected 1.0", sel)
		}
	})
}

func TestSimpleCardinalityEstimatorGetDefaultSelectivity(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	tests := []struct {
		operator string
		expected float64
	}{
		{"=", 0.1},
		{"!=", 0.9},
		{">", 0.3},
		{">=", 0.3},
		{"<", 0.3},
		{"<=", 0.3},
		{"IN", 0.2},
		{"BETWEEN", 0.3},
		{"LIKE", 0.25},
		{"UNKNOWN", 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.operator, func(t *testing.T) {
			sel := estimator.getDefaultSelectivity(tt.operator)
			if sel != tt.expected {
				t.Errorf("getDefaultSelectivity(%s) = %v, expected %v", tt.operator, sel, tt.expected)
			}
		})
	}
}

func TestSimpleCardinalityEstimatorEstimateJoin(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	leftPlan := &LogicalDataSource{TableName: "left_table"}
	rightPlan := &LogicalDataSource{TableName: "right_table"}

	estimator.UpdateStatistics("left_table", &TableStatistics{RowCount: 1000})
	estimator.UpdateStatistics("right_table", &TableStatistics{RowCount: 2000})

	t.Run("inner join", func(t *testing.T) {
		result := estimator.EstimateJoin(leftPlan, rightPlan, InnerJoin)
		// min(1000, 2000) = 1000
		if result != 1000 {
			t.Errorf("EstimateJoin(InnerJoin) = %v, expected 1000", result)
		}
	})

	t.Run("left outer join", func(t *testing.T) {
		result := estimator.EstimateJoin(leftPlan, rightPlan, LeftOuterJoin)
		// left rows = 1000
		if result != 1000 {
			t.Errorf("EstimateJoin(LeftOuterJoin) = %v, expected 1000", result)
		}
	})

	t.Run("right outer join", func(t *testing.T) {
		result := estimator.EstimateJoin(leftPlan, rightPlan, RightOuterJoin)
		// right rows = 2000
		if result != 2000 {
			t.Errorf("EstimateJoin(RightOuterJoin) = %v, expected 2000", result)
		}
	})

	t.Run("full outer join", func(t *testing.T) {
		result := estimator.EstimateJoin(leftPlan, rightPlan, FullOuterJoin)
		// left + right/2 = 1000 + 1000 = 2000
		if result != 2000 {
			t.Errorf("EstimateJoin(FullOuterJoin) = %v, expected 2000", result)
		}
	})

	t.Run("zero rows", func(t *testing.T) {
		zeroPlan := &LogicalDataSource{TableName: "zero_table"}
		estimator.UpdateStatistics("zero_table", &TableStatistics{RowCount: 0})

		result := estimator.EstimateJoin(zeroPlan, rightPlan, InnerJoin)
		if result != 0 {
			t.Errorf("EstimateJoin() with zero rows = %v, expected 0", result)
		}
	})
}

func TestSimpleCardinalityEstimatorEstimateDistinct(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	t.Run("with column statistics", func(t *testing.T) {
		stats := &TableStatistics{
			Name:     "test_table",
			RowCount: 1000,
			ColumnStats: map[string]*ColumnStatistics{
				"id":   {DistinctCount: 1000},
				"name": {DistinctCount: 100},
				"age":  {DistinctCount: 50},
			},
		}
		estimator.UpdateStatistics("test_table", stats)

		result := estimator.EstimateDistinct("test_table", []string{"id", "name", "age"})
		// min NDV = 50
		if result != 50 {
			t.Errorf("EstimateDistinct() = %v, expected 50", result)
		}
	})

	t.Run("without columns", func(t *testing.T) {
		stats := &TableStatistics{
			Name:     "test_table",
			RowCount: 1000,
			ColumnStats: map[string]*ColumnStatistics{},
		}
		estimator.UpdateStatistics("test_table", stats)

		result := estimator.EstimateDistinct("test_table", []string{})
		if result != 1000 {
			t.Errorf("EstimateDistinct() = %v, expected 1000", result)
		}
	})

	t.Run("unknown table", func(t *testing.T) {
		estimator.UpdateStatistics("test_table", &TableStatistics{RowCount: 1000})
		result := estimator.EstimateDistinct("unknown_table", []string{"id"})
		// 1000/2 = 500
		if result != 500 {
			t.Errorf("EstimateDistinct() = %v, expected 500", result)
		}
	})

	t.Run("column without statistics", func(t *testing.T) {
		stats := &TableStatistics{
			Name:       "test_table",
			RowCount:   1000,
			ColumnStats: map[string]*ColumnStatistics{},
		}
		estimator.UpdateStatistics("test_table", stats)

		result := estimator.EstimateDistinct("test_table", []string{"unknown_col"})
		// 1000/2 = 500
		if result != 500 {
			t.Errorf("EstimateDistinct() = %v, expected 500", result)
		}
	})
}

func TestEstimateRowCount(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	t.Run("data source", func(t *testing.T) {
		estimator.UpdateStatistics("test_table", &TableStatistics{RowCount: 500})
		plan := &LogicalDataSource{TableName: "test_table"}

		result := estimator.estimateRowCount(plan)
		if result != 500 {
			t.Errorf("estimateRowCount() = %v, expected 500", result)
		}
	})

	t.Run("selection", func(t *testing.T) {
		estimator.UpdateStatistics("test_table", &TableStatistics{
			RowCount: 1000,
			ColumnStats: map[string]*ColumnStatistics{
				"id": {DistinctCount: 1000},
			},
		})
		sourcePlan := &LogicalDataSource{TableName: "test_table"}
		selectionPlan := &LogicalSelection{children: []LogicalPlan{sourcePlan}}

		result := estimator.estimateRowCount(selectionPlan)
		if result < 1 {
			t.Errorf("estimateRowCount() = %v, expected >= 1", result)
		}
	})

	t.Run("unknown plan type", func(t *testing.T) {
		plan := &LogicalProjection{children: []LogicalPlan{}}
		result := estimator.estimateRowCount(plan)
		if result != 1000 {
			t.Errorf("estimateRowCount() = %v, expected 1000 (default)", result)
		}
	})
}

func TestGetTableName(t *testing.T) {
	t.Run("data source", func(t *testing.T) {
		plan := &LogicalDataSource{TableName: "test_table"}
		result := getTableName(plan)
		if result != "test_table" {
			t.Errorf("getTableName() = %v, expected test_table", result)
		}
	})

	t.Run("nested plan", func(t *testing.T) {
		sourcePlan := &LogicalDataSource{TableName: "nested_table"}
		selectionPlan := &LogicalSelection{children: []LogicalPlan{sourcePlan}}

		result := getTableName(selectionPlan)
		if result != "nested_table" {
			t.Errorf("getTableName() = %v, expected nested_table", result)
		}
	})

	t.Run("plan without table name", func(t *testing.T) {
		plan := &LogicalSelection{children: []LogicalPlan{}}
		result := getTableName(plan)
		if result != "" {
			t.Errorf("getTableName() = %v, expected empty string", result)
		}
	})
}

func TestCollectColumnStatistics(t *testing.T) {
	rows := []domain.Row{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 25},
		{"id": 4, "name": nil, "age": 35},
		{"id": 5, "name": "Alice", "age": 30},
	}

	t.Run("integer column", func(t *testing.T) {
		stats := collectColumnStatistics(rows, "id", "INT")
		if stats.DistinctCount != 5 {
			t.Errorf("DistinctCount = %v, expected 5", stats.DistinctCount)
		}
		if stats.NullCount != 0 {
			t.Errorf("NullCount = %v, expected 0", stats.NullCount)
		}
		if stats.MinValue != 1 {
			t.Errorf("MinValue = %v, expected 1", stats.MinValue)
		}
		if stats.MaxValue != 5 {
			t.Errorf("MaxValue = %v, expected 5", stats.MaxValue)
		}
	})

	t.Run("string column", func(t *testing.T) {
		stats := collectColumnStatistics(rows, "name", "VARCHAR")
		if stats.DistinctCount != 3 {
			t.Errorf("DistinctCount = %v, expected 3", stats.DistinctCount)
		}
		if stats.NullCount != 1 {
			t.Errorf("NullCount = %v, expected 1", stats.NullCount)
		}
		if stats.NullFraction != 0.2 {
			t.Errorf("NullFraction = %v, expected 0.2", stats.NullFraction)
		}
		// Average width: "Alice"(5) + "Bob"(3) + "Charlie"(7) + "Alice"(5) = 20 / 4 = 5
		if stats.AvgWidth != 5.0 {
			t.Errorf("AvgWidth = %v, expected 5.0", stats.AvgWidth)
		}
	})

	t.Run("column with duplicates", func(t *testing.T) {
		stats := collectColumnStatistics(rows, "age", "INT")
		if stats.DistinctCount != 3 {
			t.Errorf("DistinctCount = %v, expected 3", stats.DistinctCount)
		}
		if stats.MinValue != 25 {
			t.Errorf("MinValue = %v, expected 25", stats.MinValue)
		}
		if stats.MaxValue != 35 {
			t.Errorf("MaxValue = %v, expected 35", stats.MaxValue)
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		emptyRows := []domain.Row{}
		stats := collectColumnStatistics(emptyRows, "id", "INT")
		if stats.DistinctCount != 0 {
			t.Errorf("DistinctCount = %v, expected 0", stats.DistinctCount)
		}
		if stats.MinValue != nil {
			t.Errorf("MinValue = %v, expected nil", stats.MinValue)
		}
	})
}

func TestCollectStatistics(t *testing.T) {
	t.Run("requires actual data source", func(t *testing.T) {
		// This test documents the behavior but doesn't execute it
		// since CollectStatistics requires an actual DataSource implementation
		// The function signature is:
		// func CollectStatistics(dataSource domain.DataSource, tableName string) (*TableStatistics, error)
	})
}

func TestColumnStatistics(t *testing.T) {
	colStats := &ColumnStatistics{
		Name:          "test_col",
		DataType:      "INT",
		DistinctCount: 1000,
		NullCount:     100,
		MinValue:      0,
		MaxValue:      1000,
		NullFraction:  0.1,
		AvgWidth:      4.5,
	}

	if colStats.Name != "test_col" {
		t.Errorf("Name = %v, expected test_col", colStats.Name)
	}
	if colStats.DataType != "INT" {
		t.Errorf("DataType = %v, expected INT", colStats.DataType)
	}
	if colStats.DistinctCount != 1000 {
		t.Errorf("DistinctCount = %v, expected 1000", colStats.DistinctCount)
	}
}

func TestTableStatistics(t *testing.T) {
	stats := &TableStatistics{
		Name:     "test_table",
		RowCount: 10000,
		ColumnStats: map[string]*ColumnStatistics{
			"id": {
				Name:          "id",
				DataType:      "INT",
				DistinctCount: 10000,
			},
		},
	}

	if stats.Name != "test_table" {
		t.Errorf("Name = %v, expected test_table", stats.Name)
	}
	if stats.RowCount != 10000 {
		t.Errorf("RowCount = %v, expected 10000", stats.RowCount)
	}
	if len(stats.ColumnStats) != 1 {
		t.Errorf("ColumnStats length = %v, expected 1", len(stats.ColumnStats))
	}
}
