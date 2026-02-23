package optimizer

import (
	"context"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestJoinTypeString(t *testing.T) {
	tests := []struct {
		joinType JoinType
		expected string
	}{
		{InnerJoin, "INNER JOIN"},
		{LeftOuterJoin, "LEFT OUTER JOIN"},
		{RightOuterJoin, "RIGHT OUTER JOIN"},
		{FullOuterJoin, "FULL OUTER JOIN"},
		{CrossJoin, "CROSS JOIN"},
		{SemiJoin, "SEMI JOIN"},
		{AntiSemiJoin, "ANTI SEMI JOIN"},
		{HashJoin, "HASH JOIN"},
		{JoinType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.joinType.String()
			if result != tt.expected {
				t.Errorf("JoinType(%d).String() = %s, expected %s", tt.joinType, result, tt.expected)
			}
		})
	}
}

func TestAggregationTypeString(t *testing.T) {
	tests := []struct {
		aggType  AggregationType
		expected string
	}{
		{Count, "COUNT"},
		{Sum, "SUM"},
		{Avg, "AVG"},
		{Max, "MAX"},
		{Min, "MIN"},
		{AggregationType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.aggType.String()
			if result != tt.expected {
				t.Errorf("AggregationType(%d).String() = %s, expected %s", tt.aggType, result, tt.expected)
			}
		})
	}
}

func TestAggregationAlgorithmString(t *testing.T) {
	tests := []struct {
		algorithm AggregationAlgorithm
		expected  string
	}{
		{HashAggAlgorithm, "HASH_AGG"},
		{StreamAggAlgorithm, "STREAM_AGG"},
		{MPP1PhaseAggAlgorithm, "MPP_1PHASE_AGG"},
		{MPP2PhaseAggAlgorithm, "MPP_2PHASE_AGG"},
		{AggregationAlgorithm(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.algorithm.String()
			if result != tt.expected {
				t.Errorf("AggregationAlgorithm(%d).String() = %s, expected %s", tt.algorithm, result, tt.expected)
			}
		})
	}
}

func TestNewDefaultCostModel(t *testing.T) {
	costModel := NewDefaultCostModel()

	if costModel == nil {
		t.Fatal("NewDefaultCostModel() returned nil")
	}

	if costModel.CPUFactor != 0.01 {
		t.Errorf("CPUFactor = %v, expected 0.01", costModel.CPUFactor)
	}

	if costModel.IoFactor != 0.1 {
		t.Errorf("IoFactor = %v, expected 0.1", costModel.IoFactor)
	}

	if costModel.MemoryFactor != 0.001 {
		t.Errorf("MemoryFactor = %v, expected 0.001", costModel.MemoryFactor)
	}
}

func TestDefaultCostModelScanCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name      string
		tableName string
		rowCount  int64
		expected  float64
	}{
		{"zero rows", "test", 0, 0},
		{"100 rows", "test", 100, 11.0},
		{"1000 rows", "test", 1000, 110.0},
		{"large table", "test", 1000000, 110000.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := costModel.ScanCost(tt.tableName, tt.rowCount)
			if result != tt.expected {
				t.Errorf("ScanCost(%s, %d) = %v, expected %v", tt.tableName, tt.rowCount, result, tt.expected)
			}
		})
	}
}

func TestDefaultCostModelFilterCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name        string
		inputRows   int64
		selectivity float64
		expectedMin float64
	}{
		{"zero rows", 0, 0.5, 0},
		{"high selectivity", 1000, 0.9, 900},
		{"low selectivity", 1000, 0.1, 100},
		{"zero selectivity", 1000, 0.0, 0},
		{"full selectivity", 1000, 1.0, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := costModel.FilterCost(tt.inputRows, tt.selectivity)
			if result < tt.expectedMin {
				t.Errorf("FilterCost(%d, %v) = %v, expected >= %v", tt.inputRows, tt.selectivity, result, tt.expectedMin)
			}
		})
	}
}

func TestDefaultCostModelJoinCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name      string
		leftRows  int64
		rightRows int64
		joinType  JoinType
	}{
		{"small join", 10, 10, InnerJoin},
		{"left large", 1000, 10, LeftOuterJoin},
		{"right large", 10, 1000, RightOuterJoin},
		{"large join", 10000, 10000, InnerJoin},
		{"hash join", 1000, 1000, HashJoin},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := costModel.JoinCost(tt.leftRows, tt.rightRows, tt.joinType)
			if result <= 0 {
				t.Errorf("JoinCost(%d, %d, %v) = %v, expected > 0", tt.leftRows, tt.rightRows, tt.joinType, result)
			}
		})
	}
}

func TestDefaultCostModelAggregateCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name        string
		inputRows   int64
		groupByCols int
	}{
		{"no grouping", 1000, 0},
		{"single column", 1000, 1},
		{"multiple columns", 1000, 5},
		{"many columns", 10000, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := costModel.AggregateCost(tt.inputRows, tt.groupByCols)
			if result <= 0 && tt.inputRows > 0 {
				t.Errorf("AggregateCost(%d, %d) = %v, expected > 0", tt.inputRows, tt.groupByCols, result)
			}
		})
	}
}

func TestDefaultCostModelProjectCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name      string
		inputRows int64
		projCols  int
		expected  float64
	}{
		{"zero rows", 0, 5, 0},
		{"one column", 1000, 1, 10.0},
		{"five columns", 1000, 5, 50.0},
		{"many columns", 1000, 20, 200.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := costModel.ProjectCost(tt.inputRows, tt.projCols)
			if result != tt.expected {
				t.Errorf("ProjectCost(%d, %d) = %v, expected %v", tt.inputRows, tt.projCols, result, tt.expected)
			}
		})
	}
}

func TestRuleSetApply(t *testing.T) {
	ctx := context.Background()

	t.Run("empty rule set", func(t *testing.T) {
		rs := RuleSet{}
		plan := &LogicalDataSource{TableName: "test"}

		result, err := rs.Apply(ctx, plan, &OptimizationContext{})
		if err != nil {
			t.Errorf("Apply() error = %v", err)
		}
		if result != plan {
			t.Error("Apply() should return same plan for empty rule set")
		}
	})

	t.Run("nil rule set", func(t *testing.T) {
		plan := &LogicalDataSource{TableName: "test"}

		// This test ensures the Apply method handles edge cases
		rs := RuleSet(nil)
		_, err := rs.Apply(ctx, plan, &OptimizationContext{})
		if err != nil {
			t.Errorf("Apply() error = %v", err)
		}
	})
}

func TestOptimizerHints(t *testing.T) {
	hints := &OptimizerHints{}

	// Test JOIN hints
	t.Run("empty hints", func(t *testing.T) {
		if hints.HashJoinTables == nil {
			hints.HashJoinTables = make([]string, 0)
		}
		if hints.ForceIndex == nil {
			hints.ForceIndex = make(map[string][]string)
		}
		// Should not panic
		_ = len(hints.HashJoinTables)
		_ = len(hints.ForceIndex)
	})

	t.Run("global hints", func(t *testing.T) {
		hints.QBName = "qb1"
		hints.MaxExecutionTime = time.Minute
		hints.MemoryQuota = 1024 * 1024
		hints.ReadConsistentReplica = false
		hints.ResourceGroup = "default"

		if hints.QBName != "qb1" {
			t.Error("QBName not set correctly")
		}
		if hints.MaxExecutionTime != time.Minute {
			t.Error("MaxExecutionTime not set correctly")
		}
		if hints.MemoryQuota != 1024*1024 {
			t.Error("MemoryQuota not set correctly")
		}
	})
}

func TestIndexTypes(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
	}{
		{"BTREE", IndexTypeBTree},
		{"FULLTEXT", IndexTypeFullText},
		{"SPATIAL", IndexTypeSpatial},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.indexType == "" {
				t.Error("Index type constant is empty")
			}
		})
	}
}

func TestAggregationItem(t *testing.T) {
	expr := &parser.Expression{
		Type:   parser.ExprTypeColumn,
		Column: "col1",
	}

	agg := &AggregationItem{
		Type:     Sum,
		Expr:     expr,
		Alias:    "total",
		Distinct: false,
	}

	if agg.Type != Sum {
		t.Errorf("Type = %v, expected Sum", agg.Type)
	}
	if agg.Alias != "total" {
		t.Errorf("Alias = %v, expected total", agg.Alias)
	}
	if agg.Distinct {
		t.Error("Distinct should be false")
	}
}

func TestJoinCondition(t *testing.T) {
	leftExpr := &parser.Expression{
		Type:   parser.ExprTypeColumn,
		Column: "col1",
	}

	rightExpr := &parser.Expression{
		Type:   parser.ExprTypeColumn,
		Column: "col2",
	}

	cond := &JoinCondition{
		Left:     leftExpr,
		Right:    rightExpr,
		Operator: "=",
	}

	if cond.Left != leftExpr {
		t.Error("Left expression not set correctly")
	}
	if cond.Right != rightExpr {
		t.Error("Right expression not set correctly")
	}
	if cond.Operator != "=" {
		t.Error("Operator not set correctly")
	}
}

func TestLimitInfo(t *testing.T) {
	limit := &LimitInfo{
		Limit:  10,
		Offset: 5,
	}

	if limit.Limit != 10 {
		t.Errorf("Limit = %v, expected 10", limit.Limit)
	}
	if limit.Offset != 5 {
		t.Errorf("Offset = %v, expected 5", limit.Offset)
	}
}

func TestOrderByItem(t *testing.T) {
	orderBy := &OrderByItem{
		Column:    "col1",
		Direction: "ASC",
	}

	if orderBy.Column != "col1" {
		t.Errorf("Column = %v, expected col1", orderBy.Column)
	}
	if orderBy.Direction != "ASC" {
		t.Errorf("Direction = %v, expected ASC", orderBy.Direction)
	}
}

func TestStatistics(t *testing.T) {
	stats := &Statistics{
		RowCount:   1000,
		UniqueKeys: 500,
		NullCount:  100,
	}

	if stats.RowCount != 1000 {
		t.Errorf("RowCount = %v, expected 1000", stats.RowCount)
	}
	if stats.UniqueKeys != 500 {
		t.Errorf("UniqueKeys = %v, expected 500", stats.UniqueKeys)
	}
	if stats.NullCount != 100 {
		t.Errorf("NullCount = %v, expected 100", stats.NullCount)
	}
}

func TestOptimizationContext(t *testing.T) {
	ctx := &OptimizationContext{
		TableInfo: make(map[string]*domain.TableInfo),
		Stats:     make(map[string]*Statistics),
		CostModel: NewDefaultCostModel(),
		Hints:     &OptimizerHints{},
	}

	if ctx.TableInfo == nil {
		t.Error("TableInfo map not initialized")
	}
	if ctx.Stats == nil {
		t.Error("Stats map not initialized")
	}
	if ctx.CostModel == nil {
		t.Error("CostModel not initialized")
	}
	if ctx.Hints == nil {
		t.Error("Hints not initialized")
	}
}

func TestColumnInfo(t *testing.T) {
	col := &ColumnInfo{
		Name:     "id",
		Type:     "INT",
		Nullable: false,
	}

	if col.Name != "id" {
		t.Errorf("Name = %v, expected id", col.Name)
	}
	if col.Type != "INT" {
		t.Errorf("Type = %v, expected INT", col.Type)
	}
	if col.Nullable {
		t.Error("Nullable should be false")
	}
}

func TestHypotheticalIndex(t *testing.T) {
	stats := &HypotheticalIndexStats{
		NDV:           1000,
		Selectivity:   0.1,
		EstimatedSize: 1024000,
		NullFraction:  0.05,
		Correlation:   0.8,
	}

	index := &HypotheticalIndex{
		ID:        "idx1",
		TableName: "test_table",
		Columns:   []string{"col1", "col2"},
		IsUnique:  true,
		IsPrimary: false,
		Stats:     stats,
		CreatedAt: time.Now(),
	}

	if index.ID != "idx1" {
		t.Errorf("ID = %v, expected idx1", index.ID)
	}
	if index.TableName != "test_table" {
		t.Errorf("TableName = %v, expected test_table", index.TableName)
	}
	if len(index.Columns) != 2 {
		t.Errorf("Columns length = %v, expected 2", len(index.Columns))
	}
	if index.Stats == nil {
		t.Error("Stats should not be nil")
	}
}

func TestIndexRecommendation(t *testing.T) {
	rec := &IndexRecommendation{
		TableName:        "test_table",
		Columns:          []string{"col1", "col2"},
		EstimatedBenefit: 0.8,
		EstimatedCost:    0.5,
		Reason:           "Improves query performance",
		CreateStatement:  "CREATE INDEX idx ON test_table(col1, col2)",
		RecommendationID: "rec1",
	}

	if rec.TableName != "test_table" {
		t.Errorf("TableName = %v, expected test_table", rec.TableName)
	}
	if rec.EstimatedBenefit != 0.8 {
		t.Errorf("EstimatedBenefit = %v, expected 0.8", rec.EstimatedBenefit)
	}
	if rec.RecommendationID != "rec1" {
		t.Errorf("RecommendationID = %v, expected rec1", rec.RecommendationID)
	}
}

func TestIndexCandidate(t *testing.T) {
	candidate := &IndexCandidate{
		TableName: "test_table",
		Columns:   []string{"col1"},
		Priority:  4,
		Source:    "WHERE",
		Unique:    false,
		IndexType: IndexTypeBTree,
	}

	if candidate.TableName != "test_table" {
		t.Errorf("TableName = %v, expected test_table", candidate.TableName)
	}
	if candidate.Priority != 4 {
		t.Errorf("Priority = %v, expected 4", candidate.Priority)
	}
	if candidate.Source != "WHERE" {
		t.Errorf("Source = %v, expected WHERE", candidate.Source)
	}
}

func TestFullTextIndexCandidate(t *testing.T) {
	candidate := &FullTextIndexCandidate{
		TableName: "articles",
		Columns:   []string{"title", "content"},
		MinLength: 4,
		StopWords: []string{"the", "a", "an"},
	}

	if candidate.TableName != "articles" {
		t.Errorf("TableName = %v, expected articles", candidate.TableName)
	}
	if candidate.MinLength != 4 {
		t.Errorf("MinLength = %v, expected 4", candidate.MinLength)
	}
}

func TestSpatialIndexCandidate(t *testing.T) {
	candidate := &SpatialIndexCandidate{
		TableName:    "locations",
		ColumnName:   "geometry",
		IndexSubType: "POINT",
	}

	if candidate.TableName != "locations" {
		t.Errorf("TableName = %v, expected locations", candidate.TableName)
	}
	if candidate.ColumnName != "geometry" {
		t.Errorf("ColumnName = %v, expected geometry", candidate.ColumnName)
	}
}
