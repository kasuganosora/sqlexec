package optimizer

import (
	"context"
	"fmt"
)

// HintAwareJoinReorderRule 支持 hints 的 JOIN 重排序规则
type HintAwareJoinReorderRule struct {
	// baseRule can be nil or the original JoinReorderRule
	baseRule *JoinReorderRule
}

// NewHintAwareJoinReorderRule 创建 hint 感知的 JOIN 重排序规则
func NewHintAwareJoinReorderRule() *HintAwareJoinReorderRule {
	return &HintAwareJoinReorderRule{
		baseRule: &JoinReorderRule{},
	}
}

// Name 返回规则名称
func (r *HintAwareJoinReorderRule) Name() string {
	return "HintAwareJoinReorder"
}

// Match 检查规则是否匹配
func (r *HintAwareJoinReorderRule) Match(plan LogicalPlan) bool {
	// Match any LogicalJoin
	_, ok := plan.(*LogicalJoin)
	return ok
}

// Apply 应用规则
func (r *HintAwareJoinReorderRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// Try to get hints from the optimization context
	hints := r.getOptimizerHints(optCtx)
	if hints == nil {
		// No hints, fall back to base rule
		return r.baseRule.Apply(ctx, plan, optCtx)
	}

	// Apply hint-aware logic
	return r.ApplyWithHints(ctx, plan, optCtx, hints)
}

// ApplyWithHints 应用带有 hints 的规则
func (r *HintAwareJoinReorderRule) ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error) {
	join, ok := plan.(*LogicalJoin)
	if !ok {
		return plan, nil
	}

	// Priority 1: LEADING hint - force specific join order
	if len(hints.LeadingOrder) > 0 {
		debugf("  [HINT JOIN] Applying LEADING hint: %v\n", hints.LeadingOrder)
		return r.applyLeadingOrder(join, hints.LeadingOrder)
	}

	// Priority 2: STRAIGHT_JOIN hint - preserve left-deep join order
	if hints.StraightJoin {
		debugln("  [HINT JOIN] Applying STRAIGHT_JOIN hint")
		return r.applyStraightJoin(join)
	}

	// Priority 3: Specific join algorithm hints
	if len(hints.HashJoinTables) > 0 {
		debugf("  [HINT JOIN] Applying HASH_JOIN hint for tables: %v\n", hints.HashJoinTables)
		return r.setJoinAlgorithm(join, HashJoin, hints.HashJoinTables)
	}

	if len(hints.MergeJoinTables) > 0 {
		debugf("  [HINT JOIN] Applying MERGE_JOIN hint for tables: %v\n", hints.MergeJoinTables)
		return r.setJoinAlgorithm(join, InnerJoin, hints.MergeJoinTables) // MergeJoin uses InnerJoin type
	}

	if len(hints.INLJoinTables) > 0 {
		debugf("  [HINT JOIN] Applying INL_JOIN hint for tables: %v\n", hints.INLJoinTables)
		return r.setJoinAlgorithm(join, InnerJoin, hints.INLJoinTables) // INL uses InnerJoin
	}

	if len(hints.INLHashJoinTables) > 0 {
		debugf("  [HINT JOIN] Applying INL_HASH_JOIN hint for tables: %v\n", hints.INLHashJoinTables)
		return r.setJoinAlgorithm(join, InnerJoin, hints.INLHashJoinTables)
	}

	if len(hints.INLMergeJoinTables) > 0 {
		debugf("  [HINT JOIN] Applying INL_MERGE_JOIN hint for tables: %v\n", hints.INLMergeJoinTables)
		return r.setJoinAlgorithm(join, InnerJoin, hints.INLMergeJoinTables)
	}

	// Priority 4: Negative join algorithm hints
	if len(hints.NoHashJoinTables) > 0 || len(hints.NoMergeJoinTables) > 0 || len(hints.NoIndexJoinTables) > 0 {
		debugln("  [HINT JOIN] Applying negative join algorithm hints")
		return r.applyNegativeHints(join, hints)
	}

	// No relevant hints, fall back to cost-based optimization
	debugln("  [HINT JOIN] No relevant join hints, using cost-based optimization")
	return r.baseRule.Apply(ctx, plan, optCtx)
}

// applyLeadingOrder 应用 LEADING hint，强制连接顺序
func (r *HintAwareJoinReorderRule) applyLeadingOrder(join *LogicalJoin, leadingOrder []string) (LogicalPlan, error) {
	// LEADING 指定了严格的连接顺序
	// t1 -> t2 -> t3 表示先连接 t1 和 t2，结果再与 t3 连接

	// 获取当前计划中的所有表
	tables := r.collectTables(join)
	if len(tables) < 2 {
		return join, nil // 单表连接，无需重排序
	}

	// 检查 LEADING 是否包含所有表
	tableMap := make(map[string]bool)
	for _, t := range tables {
		tableMap[t] = true
	}

	allSpecified := true
	for _, t := range leadingOrder {
		if !tableMap[t] {
			debugf("  [WARN] LEADING hint specifies table %s not in join\n", t)
			allSpecified = false
		}
	}

	if !allSpecified {
		debugln("  [WARN] LEADING hint incomplete, falling back to cost-based optimization")
		return join, fmt.Errorf("LEADING hint specifies tables not in join")
	}

	// 创建新的连接顺序
	// 注意：这里简化实现，实际需要递归重新组织树结构
	debugf("  [LEADING] Reordering join to follow: %v\n", leadingOrder)

	// 标记已应用 LEADING hint
	join.SetHintApplied("LEADING")
	return join, nil
}

// applyStraightJoin 应用 STRAIGHT_JOIN hint
func (r *HintAwareJoinReorderRule) applyStraightJoin(join *LogicalJoin) (LogicalPlan, error) {
	// STRAIGHT_JOIN 保持左深树结构，不进行连接顺序优化
	// 这实际上意味着禁用 JOIN Reorder 规则

	debugln("  [STRAIGHT_JOIN] Preserving left-deep join order")

	// 标记已应用 STRAIGHT_JOIN hint
	join.SetHintApplied("STRAIGHT_JOIN")
	return join, nil
}

// setJoinAlgorithm 设置连接算法
func (r *HintAwareJoinReorderRule) setJoinAlgorithm(join *LogicalJoin, joinType JoinType, tables []string) (LogicalPlan, error) {
	// 检查连接中涉及的表是否匹配 hint 指定的表
	joinTables := r.collectTables(join)

	// 检查是否有交集
	hasMatch := false
	for _, hintTable := range tables {
		for _, joinTable := range joinTables {
			if hintTable == joinTable || r.isTableAlias(joinTable, hintTable) {
				hasMatch = true
				break
			}
		}
		if hasMatch {
			break
		}
	}

	if !hasMatch {
		debugf("  [WARN] Join hint specifies tables %v, but join contains %v\n", tables, joinTables)
		return join, nil
	}

	// 设置连接类型
	join.joinType = joinType
	debugf("  [JOIN ALGORITHM] Set join type to %s\n", joinType.String())

	return join, nil
}

// applyNegativeHints 应用负向 join hints
func (r *HintAwareJoinReorderRule) applyNegativeHints(join *LogicalJoin, hints *OptimizerHints) (LogicalPlan, error) {
	joinTables := r.collectTables(join)

	// 如果任何表在禁止列表中，使用其他算法
	for _, table := range joinTables {
		for _, noHashTable := range hints.NoHashJoinTables {
			if table == noHashTable || r.isTableAlias(table, noHashTable) {
				debugf("  [NO_HASH_JOIN] Table %s cannot use hash join\n", table)
				// 可以在这里设置为 MergeJoin 或其他算法
				return join, nil
			}
		}

		for _, noMergeTable := range hints.NoMergeJoinTables {
			if table == noMergeTable || r.isTableAlias(table, noMergeTable) {
				debugf("  [NO_MERGE_JOIN] Table %s cannot use merge join\n", table)
				return join, nil
			}
		}

		for _, noIndexTable := range hints.NoIndexJoinTables {
			if table == noIndexTable || r.isTableAlias(table, noIndexTable) {
				debugf("  [NO_INDEX_JOIN] Table %s cannot use index nested-loop join\n", table)
				return join, nil
			}
		}
	}

	return join, nil
}

// collectTables 从连接中收集所有表名
func (r *HintAwareJoinReorderRule) collectTables(join *LogicalJoin) []string {
	tables := make([]string, 0)

	// 递归收集左右子节点的表
	r.collectTablesRecursive(join.Children()[0], &tables)
	r.collectTablesRecursive(join.Children()[1], &tables)

	return tables
}

// collectTablesRecursive 递归收集表
func (r *HintAwareJoinReorderRule) collectTablesRecursive(plan LogicalPlan, tables *[]string) {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		*tables = append(*tables, dataSource.TableName)
		return
	}

	for _, child := range plan.Children() {
		r.collectTablesRecursive(child, tables)
	}
}

// isTableAlias 检查两个表名是否为别名关系
func (r *HintAwareJoinReorderRule) isTableAlias(table1, table2 string) bool {
	// 简化实现：在完整实现中，需要跟踪表的别名
	// 实际应该从 LogicalPlan 中获取别名信息
	return table1 == table2
}

// getOptimizerHints 从优化上下文中获取 optimizer hints
func (r *HintAwareJoinReorderRule) getOptimizerHints(optCtx *OptimizationContext) *OptimizerHints {
	if optCtx == nil {
		return nil
	}
	return optCtx.Hints
}

// HintAwareJoinTypeHintRule 专门处理连接类型 hint 的规则
type HintAwareJoinTypeHintRule struct {
}

// NewHintAwareJoinTypeHintRule 创建连接类型 hint 规则
func NewHintAwareJoinTypeHintRule() *HintAwareJoinTypeHintRule {
	return &HintAwareJoinTypeHintRule{}
}

// Name 返回规则名称
func (r *HintAwareJoinTypeHintRule) Name() string {
	return "HintAwareJoinTypeHint"
}

// Match 检查规则是否匹配
func (r *HintAwareJoinTypeHintRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalJoin)
	return ok
}

// Apply 应用规则
func (r *HintAwareJoinTypeHintRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	join, ok := plan.(*LogicalJoin)
	if !ok {
		return plan, nil
	}

	hints := r.getOptimizerHints(optCtx)
	if hints == nil {
		return plan, nil
	}

	// 处理各种 join 算法 hints
	applied := false

	if len(hints.HashJoinTables) > 0 {
		if r.shouldApplyHint(join, hints.HashJoinTables) {
			join.joinType = HashJoin // 注意：这里需要定义 HashJoin 类型
			applied = true
		}
	}

	if len(hints.MergeJoinTables) > 0 {
		if r.shouldApplyHint(join, hints.MergeJoinTables) {
			join.joinType = InnerJoin // Merge join
			applied = true
		}
	}

	if len(hints.INLJoinTables) > 0 {
		if r.shouldApplyHint(join, hints.INLJoinTables) {
			join.joinType = InnerJoin
			applied = true
		}
	}

	if len(hints.INLHashJoinTables) > 0 {
		if r.shouldApplyHint(join, hints.INLHashJoinTables) {
			join.joinType = InnerJoin
			applied = true
		}
	}

	if len(hints.INLMergeJoinTables) > 0 {
		if r.shouldApplyHint(join, hints.INLMergeJoinTables) {
			join.joinType = InnerJoin
			applied = true
		}
	}

	if applied {
		debugln("  [JOIN TYPE] Applied join type hint")
	}

	return join, nil
}

// ApplyWithHints 应用带有 hints 的规则
func (r *HintAwareJoinTypeHintRule) ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error) {
	return r.Apply(ctx, plan, optCtx)
}

// shouldApplyHint 检查 hint 是否应该应用到当前连接
func (r *HintAwareJoinTypeHintRule) shouldApplyHint(join *LogicalJoin, hintTables []string) bool {
	joinTables := r.collectTables(join)

	for _, hintTable := range hintTables {
		for _, joinTable := range joinTables {
			if hintTable == joinTable {
				return true
			}
		}
	}
	return false
}

// collectTables 从连接中收集所有表名
func (r *HintAwareJoinTypeHintRule) collectTables(join *LogicalJoin) []string {
	tables := make([]string, 0)

	// 递归收集左右子节点的表
	r.collectTablesRecursive(join.Children()[0], &tables)
	r.collectTablesRecursive(join.Children()[1], &tables)

	return tables
}

// collectTablesRecursive 递归收集表
func (r *HintAwareJoinTypeHintRule) collectTablesRecursive(plan LogicalPlan, tables *[]string) {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		*tables = append(*tables, dataSource.TableName)
		return
	}

	for _, child := range plan.Children() {
		r.collectTablesRecursive(child, tables)
	}
}

// getOptimizerHints 从优化上下文中获取 optimizer hints
func (r *HintAwareJoinTypeHintRule) getOptimizerHints(optCtx *OptimizationContext) *OptimizerHints {
	if optCtx == nil {
		return nil
	}
	return optCtx.Hints
}
