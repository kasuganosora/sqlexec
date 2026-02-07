package performance

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
)

// PerformanceOptimizer 性能优化器
type PerformanceOptimizer struct {
	indexManager  *IndexManager
	batchExecutor *BatchExecutor
}

// NewPerformanceOptimizer 创建性能优化器
func NewPerformanceOptimizer() *PerformanceOptimizer {
	return &PerformanceOptimizer{
		indexManager: NewIndexManager(),
	}
}

// OptimizeQuery 优化查询
func (po *PerformanceOptimizer) OptimizeQuery(ctx context.Context, plan optimizer.LogicalPlan, optCtx *optimizer.OptimizationContext) (optimizer.LogicalPlan, error) {
	// 1. 索引选择优化
	plan = po.optimizeIndexSelection(plan, optCtx)

	// 2. JOIN 重排序优化
	plan = po.optimizeJoinOrder(plan, optCtx)

	// 3. 谓词下推优化
	plan = po.optimizePredicatePushdown(plan, optCtx)

	return plan, nil
}

// optimizeIndexSelection 优化索引选择
// 遍历逻辑计划，为每个数据源选择最优的索引，减少全表扫描
func (po *PerformanceOptimizer) optimizeIndexSelection(plan optimizer.LogicalPlan, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	switch p := plan.(type) {
	case *optimizer.LogicalDataSource:
		return po.optimizeDataSourceIndexSelection(p, optCtx)
	case *optimizer.LogicalSelection:
		// 递归处理子节点
		child := po.optimizeIndexSelection(p.Children()[0], optCtx)
		if child != p.Children()[0] {
			p.SetChildren(child)
		}
		return p
	case *optimizer.LogicalJoin:
		// 递归处理左右子节点
		left := po.optimizeIndexSelection(p.Children()[0], optCtx)
		right := po.optimizeIndexSelection(p.Children()[1], optCtx)
		if left != p.Children()[0] || right != p.Children()[1] {
			p.SetChildren(left, right)
		}
		return p
	default:
		// 其他类型，递归处理子节点
		for i, child := range plan.Children() {
			newChild := po.optimizeIndexSelection(child, optCtx)
			if newChild != child {
				children := plan.Children()
				children[i] = newChild
				plan.SetChildren(children...)
			}
		}
		return plan
	}
}

// optimizeDataSourceIndexSelection 优化数据源的索引选择
func (po *PerformanceOptimizer) optimizeDataSourceIndexSelection(dataSource *optimizer.LogicalDataSource, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	// 1. 收集所有过滤条件中的列名
	filterColumns := po.extractFilterColumns(dataSource)

	if len(filterColumns) == 0 {
		// 没有过滤条件，无法使用索引
		return dataSource
	}

	// 2. 查找匹配的索引
	bestIndex := po.findBestIndex(dataSource.TableName, filterColumns)

	if bestIndex != nil {
		fmt.Printf("  [INDEX SELECT] 选择索引 %s 用于表 %s\n", bestIndex.Name, dataSource.TableName)
		// 标记使用索引（在 LogicalDataSource 中添加索引信息）
		// 注意：这里简化实现，实际应该修改 LogicalDataSource 结构
		// dataSource.SelectedIndex = bestIndex
	}

	return dataSource
}

// extractFilterColumns 从数据源中提取过滤条件涉及的列
func (po *PerformanceOptimizer) extractFilterColumns(dataSource *optimizer.LogicalDataSource) []string {
	// 从下推的谓词条件中提取列
	predicates := dataSource.GetPushedDownPredicates()
	if predicates == nil {
		return nil
	}

	columns := make([]string, 0)
	seen := make(map[string]bool)

	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		// 提取列名 - 遍历表达式树
		po.extractColumnsFromExpression(pred, &columns, seen)
	}

	return columns
}

// extractColumnsFromExpression 递归提取表达式中的列名
func (po *PerformanceOptimizer) extractColumnsFromExpression(expr interface{}, columns *[]string, seen map[string]bool) {
	// 这里简化实现，因为 parser.Expression 类型在另一个包中
	// 在实际使用中，应该通过反射或者接口方法来访问
	// 当前返回空列表，避免类型依赖
}

// findBestIndex 查找最佳索引
func (po *PerformanceOptimizer) findBestIndex(tableName string, columns []string) *Index {
	indices := po.indexManager.GetIndices(tableName)
	if len(indices) == 0 {
		return nil
	}

	// 评估每个索引
	var bestIndex *Index
	bestScore := 0.0

	for _, index := range indices {
		score := po.calculateIndexScore(index, columns)
		if score > bestScore {
			bestScore = score
			bestIndex = index
		}
	}

	return bestIndex
}

// calculateIndexScore 计算索引评分
func (po *PerformanceOptimizer) calculateIndexScore(index *Index, columns []string) float64 {
	score := 0.0

	// 1. 前缀匹配评分（越靠前的列匹配分数越高）
	for i, col := range columns {
		if i < len(index.Columns) && index.Columns[i] == col {
			// 第一个匹配列得分最高
			score += float64(len(columns)-i) * 2.0
		}
	}

	// 2. 基数评分（基数越高，区分度越好）
	score += float64(index.Cardinality) / 10000.0

	// 3. 主键索引加分
	if index.Primary {
		score += 100.0
	}

	// 4. 唯一索引加分
	if index.Unique {
		score += 50.0
	}

	return score
}

// optimizeJoinOrder 优化JOIN顺序
// 使用贪心算法或动态规划算法重新排序JOIN节点，减少中间结果大小
func (po *PerformanceOptimizer) optimizeJoinOrder(plan optimizer.LogicalPlan, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	// 1. 识别JOIN树并收集涉及的表
	_, tables := po.collectJoinNodes(plan)

	if len(tables) < 2 {
		// 单表或无JOIN，无需重排序
		return plan
	}

	fmt.Printf("  [JOIN REORDER] 检测到 %d 个表需要JOIN\n", len(tables))

	// 2. 选择优化策略
	if len(tables) <= 5 {
		// 小表数量：使用贪心算法
		return po.greedyJoinReorder(plan, tables, optCtx)
	} else {
		// 大表数量：使用简化的贪心算法
		return po.simpleJoinReorder(plan, tables, optCtx)
	}
}

// collectJoinNodes 收集所有JOIN节点和表名
func (po *PerformanceOptimizer) collectJoinNodes(plan optimizer.LogicalPlan) ([]optimizer.LogicalPlan, []string) {
	joinNodes := []optimizer.LogicalPlan{}
	tables := map[string]bool{}

	po.collectJoinsRecursive(plan, &joinNodes, tables)

	tableList := make([]string, 0, len(tables))
	for table := range tables {
		tableList = append(tableList, table)
	}

	return joinNodes, tableList
}

// collectJoinsRecursive 递归收集JOIN节点
func (po *PerformanceOptimizer) collectJoinsRecursive(plan optimizer.LogicalPlan, joinNodes *[]optimizer.LogicalPlan, tables map[string]bool) {
	if plan == nil {
		return
	}

	// 检查是否是JOIN节点
	if join, ok := plan.(*optimizer.LogicalJoin); ok {
		*joinNodes = append(*joinNodes, join)
		// 递归处理子节点
		po.collectJoinsRecursive(join.Children()[0], joinNodes, tables)
		po.collectJoinsRecursive(join.Children()[1], joinNodes, tables)
		return
	}

	// 检查是否是数据源节点
	if dataSource, ok := plan.(*optimizer.LogicalDataSource); ok {
		tables[dataSource.TableName] = true
		return
	}

	// 其他节点，递归处理子节点
	for _, child := range plan.Children() {
		po.collectJoinsRecursive(child, joinNodes, tables)
	}
}

// greedyJoinReorder 贪心算法JOIN重排序
func (po *PerformanceOptimizer) greedyJoinReorder(plan optimizer.LogicalPlan, tables []string, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	// 贪心策略：始终选择使当前结果最小的表
	remainingTables := make([]string, len(tables))
	copy(remainingTables, tables)

	order := []string{}

	for len(remainingTables) > 0 {
		// 找到最优的下一个表
		bestTable := ""
		bestCost := 0.0

		for _, table := range remainingTables {
			// 计算将table加入当前顺序的成本
			cost := po.calculateJoinCost(order, table, optCtx)
			if cost < bestCost {
				bestCost = cost
				bestTable = table
			}
		}

		if bestTable == "" {
			break
		}

		order = append(order, bestTable)

		// 从剩余表中移除
		for i, t := range remainingTables {
			if t == bestTable {
				remainingTables = append(remainingTables[:i], remainingTables[i+1:]...)
				break
			}
		}
	}

	fmt.Printf("  [JOIN REORDER] 贪心顺序: %v\n", order)

	// 根据新顺序构建JOIN计划
	return po.rebuildJoinPlan(order, plan)
}

// simpleJoinReorder 简化JOIN重排序（用于大表场景）
func (po *PerformanceOptimizer) simpleJoinReorder(plan optimizer.LogicalPlan, tables []string, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	// 简化策略：按表大小排序，从小到大JOIN
	sortedTables := po.sortTablesBySize(tables, optCtx)

	fmt.Printf("  [JOIN REORDER] 大小顺序: %v\n", sortedTables)

	return po.rebuildJoinPlan(sortedTables, plan)
}

// calculateJoinCost 计算JOIN成本
func (po *PerformanceOptimizer) calculateJoinCost(currentOrder []string, newTable string, optCtx *optimizer.OptimizationContext) float64 {
	if len(currentOrder) == 0 {
		// 第一个表，只有扫描成本
		return po.estimateScanCost(newTable, optCtx)
	}

	// 计算与最后一个表的JOIN成本
	lastTable := currentOrder[len(currentOrder)-1]

	// 估算基数
	leftCard := po.estimateCardinality(lastTable, optCtx)
	rightCard := po.estimateCardinality(newTable, optCtx)

	// JOIN成本 = build + probe
	joinCost := leftCard + rightCard*0.1 // 假设10%的选择性

	return joinCost
}

// estimateScanCost 估算扫描成本
func (po *PerformanceOptimizer) estimateScanCost(table string, optCtx *optimizer.OptimizationContext) float64 {
	if optCtx == nil {
		return 1000.0 // 默认成本
	}

	if stats, ok := optCtx.Stats[table]; ok {
		return float64(stats.RowCount)
	}

	return 1000.0 // 默认估计
}

// estimateCardinality 估算表的基数
func (po *PerformanceOptimizer) estimateCardinality(table string, optCtx *optimizer.OptimizationContext) float64 {
	if optCtx == nil {
		return 1000.0
	}

	if stats, ok := optCtx.Stats[table]; ok {
		return float64(stats.RowCount)
	}

	return 1000.0
}

// sortTablesBySize 按表大小排序
func (po *PerformanceOptimizer) sortTablesBySize(tables []string, optCtx *optimizer.OptimizationContext) []string {
	type tableCost struct {
		name string
		cost float64
	}

	tableCosts := make([]tableCost, len(tables))
	for i, table := range tables {
		tableCosts[i] = tableCost{
			name: table,
			cost: po.estimateCardinality(table, optCtx),
		}
	}

	// 简单冒泡排序
	for i := 0; i < len(tableCosts); i++ {
		for j := i + 1; j < len(tableCosts); j++ {
			if tableCosts[i].cost > tableCosts[j].cost {
				tableCosts[i], tableCosts[j] = tableCosts[j], tableCosts[i]
			}
		}
	}

	result := make([]string, len(tables))
	for i, tc := range tableCosts {
		result[i] = tc.name
	}

	return result
}

// rebuildJoinPlan 重新构建JOIN计划
// 注意：这是一个框架实现，实际需要根据具体的JOIN节点类型构建
func (po *PerformanceOptimizer) rebuildJoinPlan(order []string, originalPlan optimizer.LogicalPlan) optimizer.LogicalPlan {
	// 简化实现：返回原计划
	// 完整实现需要：
	// 1. 为每个表创建数据源节点
	// 2. 按顺序构建JOIN节点
	// 3. 复制JOIN条件和连接类型
	fmt.Println("  [JOIN REORDER] 框架实现：返回原计划")
	return originalPlan
}

// optimizePredicatePushdown 优化谓词下推
// 将过滤条件尽可能下推到数据源，减少中间结果大小
func (po *PerformanceOptimizer) optimizePredicatePushdown(plan optimizer.LogicalPlan, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	switch p := plan.(type) {
	case *optimizer.LogicalSelection:
		return po.pushDownSelection(p, optCtx)
	case *optimizer.LogicalJoin:
		// 谓词下推到JOIN的两边
		return po.pushDownJoinPredicates(p, optCtx)
	case *optimizer.LogicalProjection:
		// 先处理子节点，再处理投影
		child := po.optimizePredicatePushdown(p.Children()[0], optCtx)
		if child != p.Children()[0] {
			p.SetChildren(child)
		}
		return p
	case *optimizer.LogicalAggregate:
		// 聚合前可以下推HAVING之外的过滤条件
		child := po.optimizePredicatePushdown(p.Children()[0], optCtx)
		if child != p.Children()[0] {
			p.SetChildren(child)
		}
		return p
	default:
		// 其他节点，递归处理子节点
		for i, child := range plan.Children() {
			newChild := po.optimizePredicatePushdown(child, optCtx)
			if newChild != child {
				children := plan.Children()
				children[i] = newChild
				plan.SetChildren(children...)
			}
		}
		return plan
	}
}

// pushDownSelection 推下Selection节点的过滤条件
func (po *PerformanceOptimizer) pushDownSelection(selection *optimizer.LogicalSelection, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	conditions := selection.GetConditions()
	child := selection.Children()[0]

	// 递归处理子节点
	child = po.optimizePredicatePushdown(child, optCtx)

	// 检查是否可以推到数据源
	if dataSource, ok := child.(*optimizer.LogicalDataSource); ok {
		// 推下到数据源
		dataSource.PushDownPredicates(conditions)
		fmt.Printf("  [PREDICATE PUSH] 将 %d 个条件下推到数据源 %s\n", len(conditions), dataSource.TableName)
		return dataSource
	}

	// 检查是否可以推到JOIN
	if join, ok := child.(*optimizer.LogicalJoin); ok {
		// 尝试将条件推到JOIN的左右两边
		// 注意：这里简化处理，实际需要分割条件
		fmt.Printf("  [PREDICATE PUSH] 检查JOIN谓词下推机会\n")
		join.SetChildren(join.Children()[0], join.Children()[1])
		selection.SetChildren(join)
		return selection
	}

	// 无法进一步下推，保留当前Selection
	selection.SetChildren(child)
	return selection
}

// pushDownJoinPredicates 下推JOIN上的谓词
func (po *PerformanceOptimizer) pushDownJoinPredicates(join *optimizer.LogicalJoin, optCtx *optimizer.OptimizationContext) optimizer.LogicalPlan {
	// 递归处理左右子节点
	leftChild := po.optimizePredicatePushdown(join.Children()[0], optCtx)
	rightChild := po.optimizePredicatePushdown(join.Children()[1], optCtx)
	join.SetChildren(leftChild, rightChild)
	return join
}
