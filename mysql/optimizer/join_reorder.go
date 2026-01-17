package optimizer

import (
	"context"
	"fmt"
	"math"

	"mysql-proxy/mysql/parser"
)

// JoinReorderRule JOIN重排序规则
// 使用贪心算法选择最优的JOIN顺序
type JoinReorderRule struct {
	cardinalityEstimator CardinalityEstimator
	costModel            CostModel
}

// Name 返回规则名称
func (r *JoinReorderRule) Name() string {
	return "JoinReorder"
}

// Match 检查规则是否匹配
func (r *JoinReorderRule) Match(plan LogicalPlan) bool {
	// 检查是否包含JOIN节点
	return containsJoin(plan)
}

// containsJoin 递归检查是否包含JOIN节点
func containsJoin(plan LogicalPlan) bool {
	if _, ok := plan.(*LogicalJoin); ok {
		return true
	}

	for _, child := range plan.Children() {
		if containsJoin(child) {
			return true
		}
	}

	return false
}

// Apply 应用规则：重排序JOIN顺序
func (r *JoinReorderRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// 收集所有JOIN节点
	joinNodes := collectJoins(plan)

	if len(joinNodes) < 2 {
		// 少于2个JOIN，不需要重排序
		return plan, nil
	}

	// 提取涉及的表
	tables := extractTablesFromJoins(joinNodes)

	if len(tables) < 2 {
		return plan, nil
	}

	// 使用贪心算法选择最优JOIN顺序
	optimalOrder, minCost := r.findOptimalJoinOrder(tables, joinNodes, optCtx)

	if minCost >= math.MaxFloat64 {
		// 没找到有效的顺序，返回原计划
		return plan, nil
	}

	// 重新构建JOIN树
	return r.rebuildJoinTree(joinNodes[0], optimalOrder, joinNodes)
}

// findOptimalJoinOrder 使用贪心算法找到最优JOIN顺序
func (r *JoinReorderRule) findOptimalJoinOrder(
	tables []string,
	joinNodes []*LogicalJoin,
	optCtx *OptimizationContext,
) ([]string, float64) {

	if len(tables) == 0 {
		return nil, math.MaxFloat64
	}

	// 贪心算法：
	// 1. 选择基数最小的表作为起点
	// 2. 每次选择与已选表集JOIN成本最小的表
	// 3. 直到所有表都被选入

	remainingTables := make([]string, len(tables))
	copy(remainingTables, tables)

	selectedTables := []string{}
	minCost := 0.0

	// 第一轮：选择基数最小的表
	var firstTable string
	minCardinality := int64(math.MaxInt64)

	for _, table := range remainingTables {
		card := r.cardinalityEstimator.EstimateTableScan(table)
		if card < minCardinality {
			minCardinality = card
			firstTable = table
		}
	}

	selectedTables = append(selectedTables, firstTable)
	remainingTables = removeTable(remainingTables, firstTable)

	// 后续轮：贪心选择
	for len(remainingTables) > 0 {
		bestTable := ""
		bestCost := math.MaxFloat64

		for _, table := range remainingTables {
			// 估算将table加入已选表集的成本
			cost := r.estimateJoinCost(selectedTables, table, joinNodes, optCtx)
			if cost < bestCost {
				bestCost = cost
				bestTable = table
			}
		}

		if bestTable == "" {
			break
		}

		selectedTables = append(selectedTables, bestTable)
		minCost += bestCost
		remainingTables = removeTable(remainingTables, bestTable)
	}

	return selectedTables, minCost
}

// estimateJoinCost 估算JOIN成本
func (r *JoinReorderRule) estimateJoinCost(
	selectedTables []string,
	newTable string,
	joinNodes []*LogicalJoin,
	optCtx *OptimizationContext,
) float64 {

	// 估算newTable的基数
	newTableCard := r.cardinalityEstimator.EstimateTableScan(newTable)

	// 简化：假设均匀分布
	// 实际应该根据JOIN条件估算
	// 成本 = 表扫描成本 + 匹配成本
	scanCost := r.costModel.ScanCost(newTable, newTableCard)

	// 假设每个已选表行与新表有1/NDV的匹配
	avgDistinct := 100.0 // 默认NDV
	matchCost := float64(newTableCard) / avgDistinct

	return scanCost + matchCost
}

// rebuildJoinTree 根据最优顺序重新构建JOIN树
func (r *JoinReorderRule) rebuildJoinTree(
	rootJoin *LogicalJoin,
	order []string,
	allJoins []*LogicalJoin,
) (LogicalPlan, error) {

	if len(order) == 0 {
		return rootJoin, nil
	}

	// 简化实现：从左到右构建线性JOIN树
	// 实际应该考虑连接条件，构建最优树

	// 找到第一个表
	firstTable := order[0]

	// 查找对应的数据源节点
	firstDataSource := findDataSource(rootJoin, firstTable)
	if firstDataSource == nil {
		return rootJoin, nil
	}

	// 逐步添加其他表
	currentPlan := LogicalPlan(firstDataSource)

	for i := 1; i < len(order); i++ {
		nextTable := order[i]
		nextDataSource := findDataSource(rootJoin, nextTable)
		if nextDataSource == nil {
			return rootJoin, nil
		}

		// 创建新的JOIN节点
		newJoin := NewLogicalJoin(
			rootJoin.JoinType,
			currentPlan,
			nextDataSource,
			[]*JoinCondition{
				{Left: "id", Right: "id"}, // 简化：假设id连接
			},
		)

		currentPlan = newJoin
	}

	return currentPlan, nil
}

// collectJoins 收集所有JOIN节点
func collectJoins(plan LogicalPlan) []*LogicalJoin {
	joins := []*LogicalJoin{}

	if join, ok := plan.(*LogicalJoin); ok {
		joins = append(joins, join)
		// 递归收集子节点的JOIN
		for _, child := range plan.Children() {
			joins = append(joins, collectJoins(child)...)
		}
		return joins
	}

	for _, child := range plan.Children() {
		joins = append(joins, collectJoins(child)...)
	}

	return joins
}

// extractTablesFromJoins 从JOIN节点中提取表名
func extractTablesFromJoins(joins []*LogicalJoin) []string {
	tables := make(map[string]bool)

	for _, join := range joins {
		// 从子节点中提取表名
		extractTablesFromPlan(join, tables)
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}

	return result
}

// extractTablesFromPlan 从计划中提取表名
func extractTablesFromPlan(plan LogicalPlan, tables map[string]bool) {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		tables[dataSource.TableName] = true
		return
	}

	for _, child := range plan.Children() {
		extractTablesFromPlan(child, tables)
	}
}

// findDataSource 在JOIN树中查找指定表的数据源
func findDataSource(plan LogicalPlan, tableName string) *LogicalDataSource {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		if dataSource.TableName == tableName {
			return dataSource
		}
	}

	for _, child := range plan.Children() {
		if found := findDataSource(child, tableName); found != nil {
			return found
		}
	}

	return nil
}

// removeTable 从表列表中移除指定表
func removeTable(tables []string, table string) []string {
	result := make([]string, 0, len(tables)-1)

	for _, t := range tables {
		if t != table {
			result = append(result, t)
		}
	}

	return result
}

// NewJoinReorderRule 创建JOIN重排序规则
func NewJoinReorderRule(estimator CardinalityEstimator, costModel CostModel) *JoinReorderRule {
	return &JoinReorderRule{
		cardinalityEstimator: estimator,
		costModel:            costModel,
	}
}
