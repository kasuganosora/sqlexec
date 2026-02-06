package join

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalPlan 逻辑计划接口（避免循环依赖）
type LogicalPlan interface {
	Children() []LogicalPlan
	Explain() string
}

// JoinType JOIN类型
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
)

// CostModel 成本模型接口
type CostModel interface {
	ScanCost(tableName string, rowCount int64, useIndex bool) float64
	JoinCost(left, right LogicalPlan, joinType JoinType, conditions []*parser.Expression) float64
}

// CardinalityEstimator 基数估算器接口
type CardinalityEstimator interface {
	EstimateTableScan(tableName string) int64
}

// DPJoinReorder DP算法JOIN重排序器
// 使用动态规划算法寻找最优JOIN顺序
type DPJoinReorder struct {
	costModel   CostModel
	estimator   CardinalityEstimator
	maxTables    int // 最大表数量限制
	cache        *ReorderCache
}

// ReorderCache 重排序缓存
type ReorderCache struct {
	mu    sync.RWMutex
	cache  map[string]*ReorderResult
}

// ReorderResult 重排序结果
type ReorderResult struct {
	Order        []string
	Cost         float64
	JoinTreeType string // "left-deep", "right-deep", "bushy"
	Plan         LogicalPlan
}

// NewDPJoinReorder 创建DP JOIN重排序器
func NewDPJoinReorder(costModel CostModel, estimator CardinalityEstimator, maxTables int) *DPJoinReorder {
	return &DPJoinReorder{
		costModel:   costModel,
		estimator:   estimator,
		maxTables:    maxTables,
		cache:        NewReorderCache(1000), // 最多缓存1000个结果
	}
}

// Reorder JOIN重排序（入口函数）
func (dpr *DPJoinReorder) Reorder(ctx context.Context, plan LogicalPlan) (LogicalPlan, error) {
	// 收集所有JOIN节点和涉及的表
	joinNodes, tables := dpr.collectJoinNodes(plan)
	if len(tables) < 2 {
		return plan, nil // 不需要重排序
	}

	if len(tables) > dpr.maxTables {
		// 超过限制，使用贪心算法
		plan := dpr.greedyReorder(tables, joinNodes)
		return plan, nil
	}

	fmt.Printf("  [JOIN REORDER] Reordering %d tables\n", len(tables))

	// 使用DP算法找最优顺序
	result := dpr.dpSearch(tables, joinNodes)

	if result.Plan != nil {
		fmt.Printf("  [JOIN REORDER] Found optimal order: %v, cost=%.2f\n", result.Order, result.Cost)
		return result.Plan, nil
	}

	// DP未找到解，使用贪心算法
	plan = dpr.greedyReorder(tables, joinNodes)
	return plan, nil
}

// collectJoinNodes 收集所有JOIN节点和表名
func (dpr *DPJoinReorder) collectJoinNodes(plan LogicalPlan) ([]LogicalPlan, []string) {
	joinNodes := []LogicalPlan{}
	tables := map[string]bool{}

	dpr.collectJoinsRecursive(plan, &joinNodes, tables)

	tableList := make([]string, 0, len(tables))
	for table := range tables {
		tableList = append(tableList, table)
	}

	return joinNodes, tableList
}

// collectJoinsRecursive 递归收集JOIN节点
func (dpr *DPJoinReorder) collectJoinsRecursive(plan LogicalPlan, joinNodes *[]LogicalPlan, tables map[string]bool) {
	// 检查是否是JOIN节点（简化：检查节点类型或名称）
	if plan != nil && len(plan.Children()) == 2 {
		// 假设这是JOIN节点
		*joinNodes = append(*joinNodes, plan)
		
		// 递归处理子节点
		for _, child := range plan.Children() {
			dpr.collectJoinsRecursive(child, joinNodes, tables)
		}
		return
	}

	// 非JOIN节点，尝试提取表名
	// 简化：假设所有非JOIN节点都是数据源
	if plan != nil {
		// 这里简化处理，实际应该通过接口获取表名
		tables["unknown_table"] = true
	}

	// 递归处理子节点
	if plan != nil {
		for _, child := range plan.Children() {
			dpr.collectJoinsRecursive(child, joinNodes, tables)
		}
	}
}

// dpSearch 使用动态规划搜索最优JOIN顺序
func (dpr *DPJoinReorder) dpSearch(tables []string, joinNodes []LogicalPlan) *ReorderResult {
	// 检查缓存
	cacheKey := dpr.generateCacheKey(tables)
	if cached := dpr.cache.Get(cacheKey); cached != nil {
		fmt.Printf("  [JOIN REORDER] Cache hit for key: %s\n", cacheKey)
		return cached
	}

	n := len(tables)
	if n == 0 {
		return &ReorderResult{
			Order: []string{},
			Cost:  0,
		}
	}

	// DP状态：dp[S][i] 表示使用表集合S的最优顺序
	dp := make(map[string]*DPState)

	// 初始化：单个表
	for _, table := range tables {
		key := table
		cost := dpr.estimateSingleTableCost(table)
		dp[key] = &DPState{
			Order: []string{table},
			Cost:  cost,
			Left:  "",
			Right: "",
		}
	}

	// 递归求解更大的表集合
	bestResult := dpr.solveDP(tables, dp, joinNodes)

	// 如果 DP 未找到解，返回一个默认结果
	if bestResult == nil {
		bestResult = &ReorderResult{
			Order:        tables,
			Cost:         float64(len(tables)) * 1000,
			JoinTreeType: "left-deep",
			Plan:         dpr.buildPlanFromOrder(tables, joinNodes),
		}
	}

	// 缓存结果
	dpr.cache.Set(cacheKey, bestResult)

	return bestResult
}

// solveDP 递归求解DP问题
func (dpr *DPJoinReorder) solveDP(tables []string, dp map[string]*DPState, joinNodes []LogicalPlan) *ReorderResult {
	n := len(tables)
	if n == 1 {
		// 单个表，直接返回
		key := tables[0]
		if state, exists := dp[key]; exists {
			return &ReorderResult{
				Order: state.Order,
				Cost:  state.Cost,
				Plan:  dpr.buildPlanFromOrder(state.Order, joinNodes),
			}
		}
		return nil
	}

	// 枚举所有表分割：S = A ∪ B, A ∩ B = ∅
	bestCost := math.MaxFloat64
	var bestOrder []string

	for i := 0; i < n; i++ {
		// 生成所有2^n - 2个非空真子集（简化实现）
		for mask := 1; mask < (1 << uint(n)); mask++ {
			if mask == 0 || mask == (1<<uint(n))-1 {
				continue // 跳过空集和全集
			}

			leftSet := []string{}
			rightSet := []string{}
			
			for j := 0; j < n; j++ {
				if (mask & (1 << uint(j))) != 0 {
					leftSet = append(leftSet, tables[j])
				} else {
					rightSet = append(rightSet, tables[j])
				}
			}

			if len(leftSet) == 0 || len(rightSet) == 0 {
				continue
			}

			// 递归求解左右子集
			leftKey := dpr.generateCacheKey(leftSet)
			rightKey := dpr.generateCacheKey(rightSet)

			leftState := dp[leftKey]
			rightState := dp[rightKey]

			if leftState == nil || rightState == nil {
				continue
			}

			// 计算JOIN成本
			joinCost := dpr.estimateJoinCost(leftSet, rightSet, joinNodes)

			// 总成本 = 左边成本 + 右边成本 + JOIN成本
			totalCost := leftState.Cost + rightState.Cost + joinCost

			if totalCost < bestCost {
				bestCost = totalCost
				bestOrder = append(leftState.Order, rightState.Order...)
			}
		}
	}

	if len(bestOrder) > 0 {
		return &ReorderResult{
			Order:        bestOrder,
			Cost:         bestCost,
			JoinTreeType: "bushy",
			Plan:         dpr.buildPlanFromOrder(bestOrder, joinNodes),
		}
	}

	return nil
}

// greedyReorder 贪心算法重排序（回退方案）
func (dpr *DPJoinReorder) greedyReorder(tables []string, joinNodes []LogicalPlan) LogicalPlan {
	if len(tables) == 0 {
		return nil
	}

	remainingTables := make([]string, len(tables))
	copy(remainingTables, tables)

	remainingCosts := make(map[string]float64)
	for _, table := range remainingTables {
		remainingCosts[table] = dpr.estimateSingleTableCost(table)
	}

	order := []string{}
	totalCost := 0.0

	for len(remainingTables) > 0 {
		// 选择最优表加入顺序
		bestTable := ""
		bestJoinCost := math.MaxFloat64

		// 尝试每个剩余表
		for _, table := range remainingTables {
			// 计算将table加入当前顺序的成本
			joinCost := dpr.estimateGreedyJoinCost(order, table, remainingCosts[table], joinNodes)
			
			if joinCost < bestJoinCost {
				bestJoinCost = joinCost
				bestTable = table
			}
		}

		if bestTable == "" {
			break
		}

		// 添加到顺序
		order = append(order, bestTable)
		totalCost += bestJoinCost + remainingCosts[bestTable]
		
		// 从剩余表中移除
		for i, t := range remainingTables {
			if t == bestTable {
				remainingTables = append(remainingTables[:i], remainingTables[i+1:]...)
				break
			}
		}
	}

	fmt.Printf("  [JOIN REORDER] Greedy order: %v, cost=%.2f\n", order, totalCost)

	return dpr.buildPlanFromOrder(order, joinNodes)
}

// estimateSingleTableCost 估算单个表的扫描成本
func (dpr *DPJoinReorder) estimateSingleTableCost(table string) float64 {
	return dpr.costModel.ScanCost(table, 10000, false) // 简化：使用默认行数
}

// estimateJoinCost 估算JOIN成本
func (dpr *DPJoinReorder) estimateJoinCost(leftSet, rightSet []string, joinNodes []LogicalPlan) float64 {
	if len(leftSet) == 0 || len(rightSet) == 0 {
		return 0
	}

	// 简化：假设等值连接，使用平均基数
	leftCard := 0.0
	rightCard := 0.0

	for _, table := range leftSet {
		leftCard += float64(dpr.estimator.EstimateTableScan(table))
	}

	for _, table := range rightSet {
		rightCard += float64(dpr.estimator.EstimateTableScan(table))
	}

	// 估算JOIN选择性（简化：假设10%）
	selectivity := 0.1
	joinCost := leftCard * rightCard * selectivity

	return joinCost
}

// estimateGreedyJoinCost 估算贪心JOIN成本
func (dpr *DPJoinReorder) estimateGreedyJoinCost(currentOrder []string, newTable string, tableCost float64, joinNodes []LogicalPlan) float64 {
	if len(currentOrder) == 0 {
		return tableCost
	}

	lastTable := currentOrder[len(currentOrder)-1]
	
	// 估算连接lastTable和newTable的成本
	leftCard := float64(dpr.estimator.EstimateTableScan(lastTable))
	rightCard := float64(dpr.estimator.EstimateTableScan(newTable))
	
	// 简化的JOIN成本估算
	joinCost := leftCard * rightCard * 0.1

	return joinCost
}

// buildPlanFromOrder 根据表顺序构建JOIN树
func (dpr *DPJoinReorder) buildPlanFromOrder(order []string, joinNodes []LogicalPlan) LogicalPlan {
	if len(order) == 0 {
		return nil  // 空order返回nil
	}

	if len(order) == 1 {
		// 单个表，返回数据源
		return dpr.buildDataSource(order[0])
	}

	// 多个表：构建左深树
	left := dpr.buildDataSource(order[0])
	for i := 1; i < len(order); i++ {
		right := dpr.buildDataSource(order[i])
		left = dpr.buildJoinNode(left, right)
	}

	return left
}

// buildJoinNode 构建JOIN节点
func (dpr *DPJoinReorder) buildJoinNode(left, right LogicalPlan) LogicalPlan {
	return &mockLogicalPlan{
		left:  left,
		right: right,
	}
}

// buildDataSource 构建数据源节点
func (dpr *DPJoinReorder) buildDataSource(tableName string) LogicalPlan {
	// 简化实现：创建mock数据源
	return &mockLogicalPlan{
		tableName: tableName,
	}
}

// generateCacheKey 生成缓存键
func (dpr *DPJoinReorder) generateCacheKey(tables []string) string {
	if len(tables) == 0 {
		return ""
	}

	key := ""
	for _, table := range tables {
		key += table + "|"
	}
	return key
}

// DPState DP状态
type DPState struct {
	Order []string
	Cost  float64
	Left  string
	Right string
}

// NewReorderCache 创建重排序缓存
func NewReorderCache(size int) *ReorderCache {
	return &ReorderCache{
		cache: make(map[string]*ReorderResult),
	}
}

// Get 获取缓存的排序结果
func (rc *ReorderCache) Get(key string) *ReorderResult {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.cache[key]
}

// Set 设置缓存的排序结果
func (rc *ReorderCache) Set(key string, result *ReorderResult) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cache[key] = result
}

// Clear 清空缓存
func (rc *ReorderCache) Clear() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cache = make(map[string]*ReorderResult)
}

// Explain 解释重排序结果
func (dpr *DPJoinReorder) Explain(result *ReorderResult) string {
	if result == nil {
		return "No reorder result"
	}

	return fmt.Sprintf(
		"=== JOIN Reorder Result ===\n"+
			"Order: %v\n"+
			"Cost: %.2f\n"+
			"Tree Type: %s\n",
		result.Order, result.Cost, result.JoinTreeType,
	)
}

// mockLogicalPlan Mock逻辑计划（用于测试）
type mockLogicalPlan struct {
	tableName string
	left      LogicalPlan
	right     LogicalPlan
	children  []LogicalPlan
}

func (m *mockLogicalPlan) Children() []LogicalPlan {
	if m.left != nil && m.right != nil {
		return []LogicalPlan{m.left, m.right}
	}
	return m.children
}

func (m *mockLogicalPlan) Explain() string {
	if m.tableName != "" {
		return "DataSource(" + m.tableName + ")"
	}
	if m.left != nil && m.right != nil {
		return "Join(" + m.left.Explain() + ", " + m.right.Explain() + ")"
	}
	return "Mock Plan"
}
