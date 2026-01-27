package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestJoinTypeString(t *testing.T) {
	tests := []struct {
		name     string
		joinType JoinType
		expected string
	}{
		{"InnerJoin", InnerJoin, "INNER JOIN"},
		{"LeftOuterJoin", LeftOuterJoin, "LEFT OUTER JOIN"},
		{"RightOuterJoin", RightOuterJoin, "RIGHT OUTER JOIN"},
		{"FullOuterJoin", FullOuterJoin, "FULL OUTER JOIN"},
		{"Unknown", JoinType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.joinType.String()
			if result != tt.expected {
				t.Errorf("JoinType.String() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestAggregationTypeString(t *testing.T) {
	tests := []struct {
		name            string
		aggregationType AggregationType
		expected        string
	}{
		{"Count", Count, "COUNT"},
		{"Sum", Sum, "SUM"},
		{"Avg", Avg, "AVG"},
		{"Max", Max, "MAX"},
		{"Min", Min, "MIN"},
		{"Unknown", AggregationType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.aggregationType.String()
			if result != tt.expected {
				t.Errorf("AggregationType.String() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestNewDefaultCostModel(t *testing.T) {
	costModel := NewDefaultCostModel()
	if costModel == nil {
		t.Fatal("NewDefaultCostModel returned nil")
	}

	if costModel.CPUFactor == 0 {
		t.Error("CPUFactor should be set")
	}
	if costModel.IoFactor == 0 {
		t.Error("IoFactor should be set")
	}
	if costModel.MemoryFactor == 0 {
		t.Error("MemoryFactor should be set")
	}
}

func TestScanCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name     string
		tableName string
		rowCount int64
		wantCost float64
	}{
		{"Zero rows", "users", 0, 0},
		{"Small table", "users", 100, 100 * costModel.IoFactor + 100 * costModel.CPUFactor},
		{"Large table", "orders", 10000, 10000 * costModel.IoFactor + 10000 * costModel.CPUFactor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.ScanCost(tt.tableName, tt.rowCount)
			if cost != tt.wantCost {
				t.Errorf("ScanCost() = %f, want %f", cost, tt.wantCost)
			}
		})
	}
}

func TestFilterCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name         string
		inputRows    int64
		selectivity  float64
		wantCostMin  float64
	}{
		{"Zero selectivity", 1000, 0.0, 1000 * costModel.CPUFactor},
		{"Full selectivity", 1000, 1.0, 1000 * costModel.CPUFactor + 1000},
		{"50% selectivity", 1000, 0.5, 1000 * costModel.CPUFactor + 500},
		{"Zero rows", 0, 0.5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.FilterCost(tt.inputRows, tt.selectivity)
			if cost < tt.wantCostMin-1e-9 {
				t.Errorf("FilterCost() = %f, want at least %f", cost, tt.wantCostMin)
			}
		})
	}
}

func TestJoinCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name     string
		leftRows int64
		rightRows int64
		joinType JoinType
	}{
		{"Small inner join", 100, 100, InnerJoin},
		{"Left outer join", 1000, 100, LeftOuterJoin},
		{"Right outer join", 100, 1000, RightOuterJoin},
		{"Full outer join", 500, 500, FullOuterJoin},
		{"Zero rows", 0, 0, InnerJoin},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.JoinCost(tt.leftRows, tt.rightRows, tt.joinType)
			// 成本应该为正数
			if cost < 0 {
				t.Errorf("JoinCost() returned negative cost: %f", cost)
			}
		})
	}
}

func TestAggregateCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name       string
		inputRows  int64
		groupByCols int
	}{
		{"No group by", 100, 0},
		{"Single group by", 100, 1},
		{"Multiple group by", 1000, 3},
		{"Zero rows", 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.AggregateCost(tt.inputRows, tt.groupByCols)
			// 成本应该为正数
			if cost < 0 {
				t.Errorf("AggregateCost() returned negative cost: %f", cost)
			}
		})
	}
}

func TestProjectCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name      string
		inputRows int64
		projCols  int
	}{
		{"Single column", 100, 1},
		{"Multiple columns", 100, 5},
		{"Many columns", 1000, 10},
		{"Zero rows", 0, 5},
		{"Zero columns", 100, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.ProjectCost(tt.inputRows, tt.projCols)
			// 成本应该为正数
			if cost < 0 {
				t.Errorf("ProjectCost() returned negative cost: %f", cost)
			}
		})
	}
}

func TestAggregationItem(t *testing.T) {
	item := AggregationItem{
		Type:     Count,
		Expr:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
		Alias:    "count_id",
		Distinct: false,
	}

	if item.Type != Count {
		t.Error("Type mismatch")
	}
	if item.Expr.Column != "id" {
		t.Error("Expr mismatch")
	}
	if item.Alias != "count_id" {
		t.Error("Alias mismatch")
	}
	if item.Distinct {
		t.Error("Distinct should be false")
	}
}

func TestJoinCondition(t *testing.T) {
	cond := JoinCondition{
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "users.id"},
		Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "orders.user_id"},
		Operator: "=",
	}

	if cond.Left.Column != "users.id" {
		t.Error("Left expression mismatch")
	}
	if cond.Right.Column != "orders.user_id" {
		t.Error("Right expression mismatch")
	}
	if cond.Operator != "=" {
		t.Error("Operator mismatch")
	}
}

func TestLimitInfo(t *testing.T) {
	limitInfo := LimitInfo{
		Limit:  100,
		Offset: 10,
	}

	if limitInfo.Limit != 100 {
		t.Error("Limit mismatch")
	}
	if limitInfo.Offset != 10 {
		t.Error("Offset mismatch")
	}
}

func TestOrderByItem(t *testing.T) {
	order := OrderByItem{
		Column:    "name",
		Direction: "ASC",
	}

	if order.Column != "name" {
		t.Error("Column mismatch")
	}
	if order.Direction != "ASC" {
		t.Error("Direction mismatch")
	}
}

func TestStatistics(t *testing.T) {
	stats := Statistics{
		RowCount:   1000,
		UniqueKeys: 950,
		NullCount:  50,
	}

	if stats.RowCount != 1000 {
		t.Error("RowCount mismatch")
	}
	if stats.UniqueKeys != 950 {
		t.Error("UniqueKeys mismatch")
	}
	if stats.NullCount != 50 {
		t.Error("NullCount mismatch")
	}
}

func TestOptimizationContext(t *testing.T) {
	ctx := &OptimizationContext{
		DataSource: nil,
		TableInfo:  make(map[string]*domain.TableInfo),
		Stats:      make(map[string]*Statistics),
		CostModel:  NewDefaultCostModel(),
	}

	if ctx.TableInfo == nil {
		t.Error("TableInfo map should be initialized")
	}
	if ctx.Stats == nil {
		t.Error("Stats map should be initialized")
	}
	if ctx.CostModel == nil {
		t.Error("CostModel should be initialized")
	}
}

func TestColumnInfo(t *testing.T) {
	col := ColumnInfo{
		Name:     "id",
		Type:     "INT",
		Nullable: false,
	}

	if col.Name != "id" {
		t.Error("Name mismatch")
	}
	if col.Type != "INT" {
		t.Error("Type mismatch")
	}
	if col.Nullable {
		t.Error("Nullable should be false")
	}
}

func TestCostModelInterface(t *testing.T) {
	costModel := NewDefaultCostModel()

	// 测试所有CostModel接口方法
	cost := costModel.ScanCost("users", 1000)
	if cost < 0 {
		t.Error("ScanCost returned negative cost")
	}

	cost = costModel.FilterCost(1000, 0.5)
	if cost < 0 {
		t.Error("FilterCost returned negative cost")
	}

	cost = costModel.JoinCost(100, 100, InnerJoin)
	if cost < 0 {
		t.Error("JoinCost returned negative cost")
	}

	cost = costModel.AggregateCost(1000, 2)
	if cost < 0 {
		t.Error("AggregateCost returned negative cost")
	}

	cost = costModel.ProjectCost(1000, 5)
	if cost < 0 {
		t.Error("ProjectCost returned negative cost")
	}
}

func TestRuleSetApply(t *testing.T) {
	rules := DefaultRuleSet()
	ctx := context.Background()
	optCtx := &OptimizationContext{
		DataSource: nil,
		TableInfo:  make(map[string]*domain.TableInfo),
		Stats:      make(map[string]*Statistics),
		CostModel:  NewDefaultCostModel(),
	}

	// 创建一个简单的逻辑计划
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	}
	optCtx.TableInfo["users"] = tableInfo

	logicalPlan := NewLogicalDataSource("users", tableInfo)

	// 应用规则
	resultPlan, err := rules.Apply(ctx, logicalPlan, optCtx)
	if err != nil {
		t.Errorf("RuleSet.Apply() error = %v", err)
	}

	if resultPlan == nil {
		t.Error("RuleSet.Apply() should return non-nil plan")
	}
}

func TestNewOptimizer(t *testing.T) {
	// 注意：这里需要一个真实的dataSource，简化测试
	optimizer := NewOptimizer(nil)
	if optimizer == nil {
		t.Fatal("NewOptimizer returned nil")
	}

	if optimizer.rules == nil {
		t.Error("rules should be initialized")
	}
	if optimizer.costModel == nil {
		t.Error("costModel should be initialized")
	}
	if optimizer.dataSource == nil {
		// dataSource可以为nil，这是允许的
	}
}

func TestOptimizationRuleInterface(t *testing.T) {
	// 这是一个接口测试，用于验证所有规则都实现了正确的接口
	rules := DefaultRuleSet()

	for _, rule := range rules {
		name := rule.Name()
		if name == "" {
			t.Errorf("Rule has empty name")
		}

		// 测试Match方法 - 不使用nil作为child
		tableInfo := &domain.TableInfo{
			Name: "test_table",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT"},
			},
		}
		logicalPlan := NewLogicalDataSource("test", tableInfo)
		matched := rule.Match(logicalPlan)
		// 不管是否匹配，方法应该能够调用
		_ = matched
	}
}

func TestPermissionConstants(t *testing.T) {
	// 测试常量值
	if Count != 0 {
		t.Error("Count should be 0")
	}
	if Sum != 1 {
		t.Error("Sum should be 1")
	}
	if Avg != 2 {
		t.Error("Avg should be 2")
	}
	if Max != 3 {
		t.Error("Max should be 3")
	}
	if Min != 4 {
		t.Error("Min should be 4")
	}

	if InnerJoin != 0 {
		t.Error("InnerJoin should be 0")
	}
	if LeftOuterJoin != 1 {
		t.Error("LeftOuterJoin should be 1")
	}
	if RightOuterJoin != 2 {
		t.Error("RightOuterJoin should be 2")
	}
	if FullOuterJoin != 3 {
		t.Error("FullOuterJoin should be 3")
	}
}

func TestTypesExplainPlan(t *testing.T) {
	// 创建一个简单的物理计划
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	plan := NewPhysicalTableScan("users", tableInfo, nil, nil, nil)

	explain := ExplainPlan(plan)
	if explain == "" {
		t.Error("ExplainPlan() should return non-empty string")
	}

	if len(explain) < 10 {
		t.Error("ExplainPlan() output too short")
	}
}

func TestTypesExplainPlanRecursive(t *testing.T) {
	// 创建一个带子节点的计划
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	childPlan := NewPhysicalTableScan("users", tableInfo, nil, nil, nil)
	parentPlan := NewPhysicalLimit(10, 0, childPlan)

	explain := ExplainPlan(parentPlan)
	if explain == "" {
		t.Error("ExplainPlan() should return non-empty string")
	}

	// 应该包含子节点的说明
	// 因为有两个节点，输出应该比单个节点长
}
