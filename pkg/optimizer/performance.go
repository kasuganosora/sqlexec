package optimizer

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Index 索引定义
type Index struct {
	Name       string
	TableName  string
	Columns    []string
	Unique     bool
	Primary    bool
	Cardinality int64 // 基数（唯一值数量）
}

// IndexManager 索引管理器
type IndexManager struct {
	mu      sync.RWMutex
	indices map[string][]*Index // table_name -> indices
	stats   map[string]*IndexStats // index_name -> stats
}

// IndexStats 索引统计信息
type IndexStats struct {
	Name         string
	HitCount     int64
	MissCount    int64
	AvgAccessTime time.Duration
	LastAccessed time.Time
}

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indices: make(map[string][]*Index),
		stats:   make(map[string]*IndexStats),
	}
}

// AddIndex 添加索引
func (im *IndexManager) AddIndex(index *Index) {
	im.mu.Lock()
	defer im.mu.Unlock()

	im.indices[index.TableName] = append(im.indices[index.TableName], index)
	im.stats[index.Name] = &IndexStats{
		Name:         index.Name,
		LastAccessed: time.Now(),
	}
}

// GetIndices 获取表的所有索引
func (im *IndexManager) GetIndices(tableName string) []*Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if indices, ok := im.indices[tableName]; ok {
		result := make([]*Index, len(indices))
		copy(result, indices)
		return result
	}
	return nil
}

// FindBestIndex 查找最佳索引
func (im *IndexManager) FindBestIndex(tableName string, columns []string) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	indices, ok := im.indices[tableName]
	if !ok {
		return nil
	}

	// 寻找列数匹配且基数最高的索引
	var bestIndex *Index
	maxCardinality := int64(0)

	for _, index := range indices {
		if len(index.Columns) >= len(columns) {
			// 检查前几列是否匹配
			match := true
			for i, col := range columns {
				if i >= len(index.Columns) || index.Columns[i] != col {
					match = false
					break
				}
			}

			if match && index.Cardinality > maxCardinality {
				bestIndex = index
				maxCardinality = index.Cardinality
			}
		}
	}

	return bestIndex
}

// RecordIndexAccess 记录索引访问
func (im *IndexManager) RecordIndexAccess(indexName string, duration time.Duration) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if stats, ok := im.stats[indexName]; ok {
		stats.HitCount++
		stats.LastAccessed = time.Now()

		// 更新平均访问时间
		if stats.AvgAccessTime == 0 {
			stats.AvgAccessTime = duration
		} else {
			stats.AvgAccessTime = (stats.AvgAccessTime*time.Duration(stats.HitCount) + duration) / time.Duration(stats.HitCount+1)
		}
	}
}

// GetIndexStats 获取索引统计
func (im *IndexManager) GetIndexStats(indexName string) *IndexStats {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if stats, ok := im.stats[indexName]; ok {
		// 返回副本
		return &IndexStats{
			Name:         stats.Name,
			HitCount:     stats.HitCount,
			MissCount:    stats.MissCount,
			AvgAccessTime: stats.AvgAccessTime,
			LastAccessed: stats.LastAccessed,
		}
	}
	return nil
}

// BatchExecutor 批量执行器
type BatchExecutor struct {
	batchSize     int
	flushInterval time.Duration
	batch         []interface{}
	timer         *time.Timer
	mu            sync.Mutex
	flushFunc      func([]interface{}) error
}

// NewBatchExecutor 创建批量执行器
func NewBatchExecutor(batchSize int, flushInterval time.Duration, flushFunc func([]interface{}) error) *BatchExecutor {
	be := &BatchExecutor{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		flushFunc:     flushFunc,
	}
	be.timer = time.AfterFunc(flushInterval, func() { be.Flush() })
	return be
}

// Add 添加到批次
func (be *BatchExecutor) Add(item interface{}) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	be.batch = append(be.batch, item)

	if len(be.batch) >= be.batchSize {
		return be.flushLocked()
	}

	return nil
}

// flushLocked 刷新批次（调用者必须持有锁）
func (be *BatchExecutor) flushLocked() error {
	if len(be.batch) == 0 {
		be.timer.Reset(be.flushInterval)
		return nil
	}

	items := be.batch
	be.batch = make([]interface{}, 0, be.batchSize)

	err := be.flushFunc(items)
	if err != nil {
		return err
	}

	be.timer.Reset(be.flushInterval)
	return nil
}

// Flush 手动刷新
func (be *BatchExecutor) Flush() error {
	be.mu.Lock()
	defer be.mu.Unlock()
	return be.flushLocked()
}

// Close 关闭批量执行器
func (be *BatchExecutor) Close() error {
	be.timer.Stop()
	return be.Flush()
}

// PriorityQueue 优先队列（用于JOIN重排序等优化）
type PriorityQueue []*PlanNode

// PlanPlan 计划节点
type PlanNode struct {
	Plan     LogicalPlan
	Cost     float64
	Priority int
	Index    int
}

// Len 实现 heap.Interface
func (pq PriorityQueue) Len() int { return len(pq) }

// Less 实现 heap.Interface
func (pq PriorityQueue) Less(i, j int) bool {
	// 优先级高的在前（成本低的优先）
	if pq[i].Priority == pq[j].Priority {
		return pq[i].Cost < pq[j].Cost
	}
	return pq[i].Priority > pq[j].Priority
}

// Swap 实现 heap.Interface
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 实现 heap.Interface
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	node := x.(*PlanNode)
	node.Index = n
	*pq = append(*pq, node)
}

// Pop 实现 heap.Interface
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	node := old[n-1]
	old[n-1] = nil
	node.Index = -1
	*pq = old[0 : n-1]
	return node
}

// NewPriorityQueue 创建优先队列
func NewPriorityQueue() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &pq
}

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
func (po *PerformanceOptimizer) OptimizeQuery(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
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
func (po *PerformanceOptimizer) optimizeIndexSelection(plan LogicalPlan, optCtx *OptimizationContext) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalDataSource:
		return po.optimizeDataSourceIndexSelection(p, optCtx)
	case *LogicalSelection:
		// 递归处理子节点
		child := po.optimizeIndexSelection(p.Children()[0], optCtx)
		if child != p.Children()[0] {
			p.SetChildren(child)
		}
		return p
	case *LogicalJoin:
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
func (po *PerformanceOptimizer) optimizeDataSourceIndexSelection(dataSource *LogicalDataSource, optCtx *OptimizationContext) LogicalPlan {
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
func (po *PerformanceOptimizer) extractFilterColumns(dataSource *LogicalDataSource) []string {
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
func (po *PerformanceOptimizer) optimizeJoinOrder(plan LogicalPlan, optCtx *OptimizationContext) LogicalPlan {
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
func (po *PerformanceOptimizer) collectJoinNodes(plan LogicalPlan) ([]LogicalPlan, []string) {
	joinNodes := []LogicalPlan{}
	tables := map[string]bool{}

	po.collectJoinsRecursive(plan, &joinNodes, tables)

	tableList := make([]string, 0, len(tables))
	for table := range tables {
		tableList = append(tableList, table)
	}

	return joinNodes, tableList
}

// collectJoinsRecursive 递归收集JOIN节点
func (po *PerformanceOptimizer) collectJoinsRecursive(plan LogicalPlan, joinNodes *[]LogicalPlan, tables map[string]bool) {
	if plan == nil {
		return
	}

	// 检查是否是JOIN节点
	if join, ok := plan.(*LogicalJoin); ok {
		*joinNodes = append(*joinNodes, join)
		// 递归处理子节点
		po.collectJoinsRecursive(join.Children()[0], joinNodes, tables)
		po.collectJoinsRecursive(join.Children()[1], joinNodes, tables)
		return
	}

	// 检查是否是数据源节点
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		tables[dataSource.TableName] = true
		return
	}

	// 其他节点，递归处理子节点
	for _, child := range plan.Children() {
		po.collectJoinsRecursive(child, joinNodes, tables)
	}
}

// greedyJoinReorder 贪心算法JOIN重排序
func (po *PerformanceOptimizer) greedyJoinReorder(plan LogicalPlan, tables []string, optCtx *OptimizationContext) LogicalPlan {
	// 贪心策略：始终选择使当前结果最小的表
	remainingTables := make([]string, len(tables))
	copy(remainingTables, tables)

	order := []string{}

	for len(remainingTables) > 0 {
		// 找到最优的下一个表
		bestTable := ""
		bestCost := float64(math.MaxFloat64)

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
func (po *PerformanceOptimizer) simpleJoinReorder(plan LogicalPlan, tables []string, optCtx *OptimizationContext) LogicalPlan {
	// 简化策略：按表大小排序，从小到大JOIN
	sortedTables := po.sortTablesBySize(tables, optCtx)

	fmt.Printf("  [JOIN REORDER] 大小顺序: %v\n", sortedTables)

	return po.rebuildJoinPlan(sortedTables, plan)
}

// calculateJoinCost 计算JOIN成本
func (po *PerformanceOptimizer) calculateJoinCost(currentOrder []string, newTable string, optCtx *OptimizationContext) float64 {
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
func (po *PerformanceOptimizer) estimateScanCost(table string, optCtx *OptimizationContext) float64 {
	if optCtx == nil {
		return 1000.0 // 默认成本
	}

	if stats, ok := optCtx.Stats[table]; ok {
		return float64(stats.RowCount)
	}

	return 1000.0 // 默认估计
}

// estimateCardinality 估算表的基数
func (po *PerformanceOptimizer) estimateCardinality(table string, optCtx *OptimizationContext) float64 {
	if optCtx == nil {
		return 1000.0
	}

	if stats, ok := optCtx.Stats[table]; ok {
		return float64(stats.RowCount)
	}

	return 1000.0
}

// sortTablesBySize 按表大小排序
func (po *PerformanceOptimizer) sortTablesBySize(tables []string, optCtx *OptimizationContext) []string {
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
func (po *PerformanceOptimizer) rebuildJoinPlan(order []string, originalPlan LogicalPlan) LogicalPlan {
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
func (po *PerformanceOptimizer) optimizePredicatePushdown(plan LogicalPlan, optCtx *OptimizationContext) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalSelection:
		return po.pushDownSelection(p, optCtx)
	case *LogicalJoin:
		// 谓词下推到JOIN的两边
		return po.pushDownJoinPredicates(p, optCtx)
	case *LogicalProjection:
		// 先处理子节点，再处理投影
		child := po.optimizePredicatePushdown(p.Children()[0], optCtx)
		if child != p.Children()[0] {
			p.SetChildren(child)
		}
		return p
	case *LogicalAggregate:
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
func (po *PerformanceOptimizer) pushDownSelection(selection *LogicalSelection, optCtx *OptimizationContext) LogicalPlan {
	conditions := selection.GetConditions()
	child := selection.Children()[0]

	// 递归处理子节点
	child = po.optimizePredicatePushdown(child, optCtx)

	// 检查是否可以推到数据源
	if dataSource, ok := child.(*LogicalDataSource); ok {
		// 推下到数据源
		dataSource.PushDownPredicates(conditions)
		fmt.Printf("  [PREDICATE PUSH] 将 %d 个条件下推到数据源 %s\n", len(conditions), dataSource.TableName)
		return dataSource
	}

	// 检查是否可以推到JOIN
	if join, ok := child.(*LogicalJoin); ok {
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
func (po *PerformanceOptimizer) pushDownJoinPredicates(join *LogicalJoin, optCtx *OptimizationContext) LogicalPlan {
	// 递归处理左右子节点
	leftChild := po.optimizePredicatePushdown(join.Children()[0], optCtx)
	rightChild := po.optimizePredicatePushdown(join.Children()[1], optCtx)

	join.SetChildren(leftChild, rightChild)
	return join
}

// EstimateSelectivity 估计过滤条件的选择性
// 基于统计信息计算过滤条件的选择性，提高优化决策的准确性
func (po *PerformanceOptimizer) EstimateSelectivity(filter Filter, stats *Statistics) float64 {
	if stats == nil {
		// 没有统计信息，使用默认选择性
		return po.getDefaultSelectivity(filter)
	}

	// 根据不同的操作符类型计算选择性
	switch filter.Operator {
	case "=":
		return po.estimateEqualitySelectivity(filter, stats)
	case ">", ">=":
		return po.estimateRangeSelectivity(filter, stats, 0.5)
	case "<", "<=":
		return po.estimateRangeSelectivity(filter, stats, 0.5)
	case "!=", "<>":
		// 不等于：1 - 选择性
		return 1.0 - po.estimateEqualitySelectivity(filter, stats)
	case "LIKE":
		// LIKE 操作符，通常选择性较低
		return po.estimateLikeSelectivity(filter)
	case "IN":
		// IN 操作符，选择性取决于值的数量
		return po.estimateInSelectivity(filter)
	default:
		// 未知操作符，使用默认值
		return po.getDefaultSelectivity(filter)
	}
}

// estimateEqualitySelectivity 估算等值条件的选择性
func (po *PerformanceOptimizer) estimateEqualitySelectivity(filter Filter, stats *Statistics) float64 {
	// 选择性 = 1 / 唯一值数量
	if stats.UniqueKeys > 0 {
		return 1.0 / float64(stats.UniqueKeys)
	}

	// 没有唯一值统计，假设10%的选择性
	return 0.1
}

// estimateRangeSelectivity 估算范围条件的选择性
func (po *PerformanceOptimizer) estimateRangeSelectivity(filter Filter, stats *Statistics, defaultFraction float64) float64 {
	if stats == nil || stats.RowCount == 0 {
		return defaultFraction
	}

	// 基于值的分布估算范围选择性
	// 这里简化：对于 >, >=, <, <= 等操作符，假设50%的选择性
	// 完整实现需要直方图统计信息

	// 检查是否有NULL值
	nullFraction := 0.0
	if stats.RowCount > 0 {
		nullFraction = float64(stats.NullCount) / float64(stats.RowCount)
	}

	// 排除NULL值后的选择性
	return defaultFraction * (1.0 - nullFraction)
}

// estimateLikeSelectivity 估算LIKE条件的选择性
func (po *PerformanceOptimizer) estimateLikeSelectivity(filter Filter) float64 {
	if filter.Value == nil {
		return 0.1
	}

	valueStr := fmt.Sprintf("%v", filter.Value)

	// 前缀匹配（如 'abc%'）选择性较高
	if len(valueStr) > 0 && valueStr[len(valueStr)-1] == '%' {
		return 0.3
	}

	// 后缀匹配（如 '%abc'）选择性较低
	if len(valueStr) > 0 && valueStr[0] == '%' {
		return 0.05
	}

	// 包含匹配（如 '%abc%'）选择性中等
	if len(valueStr) > 2 && valueStr[0] == '%' && valueStr[len(valueStr)-1] == '%' {
		return 0.1
	}

	// 无通配符，等价于等值条件
	return 0.1
}

// estimateInSelectivity 估算IN条件的选择性
func (po *PerformanceOptimizer) estimateInSelectivity(filter Filter) float64 {
	// IN (a, b, c) 的选择性 = 不同值的数量 / 表总行数
	// 简化：根据值的数量估算

	// 尝试从值中提取数量
	if values, ok := filter.Value.([]interface{}); ok {
		numValues := len(values)
		if numValues > 0 {
			// 每个值的平均选择性，但不超过1
			selectivity := float64(numValues) * 0.1
			if selectivity > 1.0 {
				return 1.0
			}
			return selectivity
		}
	}

	// 默认假设IN中有3个值
	return 0.3
}

// getDefaultSelectivity 获取默认选择性
func (po *PerformanceOptimizer) getDefaultSelectivity(filter Filter) float64 {
	// 根据操作符类型返回默认选择性
	switch filter.Operator {
	case "=":
		return 0.1 // 等值条件
	case ">", ">=", "<", "<=":
		return 0.3 // 范围条件
	case "!=", "<>":
		return 0.9 // 不等于
	case "LIKE":
		return 0.1 // LIKE
	default:
		return 0.5 // 未知操作符
	}
}

// EstimateJoinSelectivity 估算JOIN的选择性
func (po *PerformanceOptimizer) EstimateJoinSelectivity(leftTable, rightTable string, optCtx *OptimizationContext) float64 {
	// 估算两个表JOIN后的行数
	leftRows := po.getTableRows(leftTable, optCtx)
	rightRows := po.getTableRows(rightTable, optCtx)

	// 笛卡尔积大小
	cartesianSize := float64(leftRows * rightRows)

	// 假设等值连接的选择性为10%
	selectivity := 0.1

	// 计算预期结果行数
	estimatedRows := cartesianSize * selectivity

	// 计算选择性（结果行数 / 笛卡尔积）
	if cartesianSize > 0 {
		return estimatedRows / cartesianSize
	}

	return 0.1
}

// getTableRows 获取表的行数
func (po *PerformanceOptimizer) getTableRows(table string, optCtx *OptimizationContext) int64 {
	if optCtx == nil || optCtx.Stats == nil {
		return 1000 // 默认估计
	}

	if stats, ok := optCtx.Stats[table]; ok {
		return stats.RowCount
	}

	return 1000 // 默认估计
}

// Filter 过滤条件（简化版）
type Filter struct {
	Column   string
	Operator string
	Value    interface{}
}

// OptimizeScan 优化扫描操作
func (po *PerformanceOptimizer) OptimizeScan(tableName string, filters []Filter, optCtx *OptimizationContext) *ScanOptimization {
	optimization := &ScanOptimization{
		UseIndex:      false,
		IndexName:     "",
		PushDown:      true,
		EstimatedRows: 10000,
	}

	// 检查是否有可用的索引
	for _, filter := range filters {
		if index := po.indexManager.FindBestIndex(tableName, []string{filter.Column}); index != nil {
			optimization.UseIndex = true
			optimization.IndexName = index.Name
			optimization.EstimatedRows = index.Cardinality / 10 // 假设索引选择性为10%
			break
		}
	}

	return optimization
}

// ScanOptimization 扫描优化建议
type ScanOptimization struct {
	UseIndex      bool
	IndexName     string
	PushDown      bool
	EstimatedRows int64
}

// Explain 解释优化建议
func (so *ScanOptimization) Explain() string {
	if so.UseIndex {
		return fmt.Sprintf("建议使用索引 %s，预计扫描 %d 行", so.IndexName, so.EstimatedRows)
	}
	if so.PushDown {
		return fmt.Sprintf("建议将过滤条件下推，预计扫描 %d 行", so.EstimatedRows)
	}
	return "全表扫描"
}

// MemoryPool 内存池（用于重用对象减少GC压力）
type MemoryPool struct {
	pools map[string]interface{}
	mu    sync.RWMutex
}

// NewMemoryPool 创建内存池
func NewMemoryPool() *MemoryPool {
	return &MemoryPool{
		pools: make(map[string]interface{}),
	}
}

// GetPool 获取指定类型的池
func (mp *MemoryPool) GetPool(key string) interface{} {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.pools[key]
}

// SetPool 设置指定类型的池
func (mp *MemoryPool) SetPool(key string, pool interface{}) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.pools[key] = pool
}
