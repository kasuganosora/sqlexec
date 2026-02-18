package optimizer

import (
	"context"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// OrderByHintRule ORDER BY Hints优化规则
// 支持 ORDER_INDEX 和 NO_ORDER_INDEX hints
type OrderByHintRule struct {
	estimator CardinalityEstimator
}

// NewOrderByHintRule 创建ORDER BY Hints规则
func NewOrderByHintRule(estimator CardinalityEstimator) *OrderByHintRule {
	return &OrderByHintRule{
		estimator: estimator,
	}
}

// Name 规则名称
func (r *OrderByHintRule) Name() string {
	return "OrderByHint"
}

// Match 检查规则是否匹配
func (r *OrderByHintRule) Match(plan LogicalPlan) bool {
	// 匹配任何包含LogicalSort的计划
	_, ok := plan.(*LogicalSort)
	return ok
}

// Apply 应用规则
func (r *OrderByHintRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	sortPlan, ok := plan.(*LogicalSort)
	if !ok {
		return plan, nil
	}

	// 检查是否有hints
	if optCtx.Hints == nil {
		return plan, nil
	}

	// 获取排序项
	orderByItems := sortPlan.GetOrderBy()
	if len(orderByItems) == 0 {
		return plan, nil
	}

	debugln("  [ORDER BY HINT] Processing ORDER BY hints")

	// 处理每个排序项
	for _, item := range orderByItems {
		// 从表达式中提取列名
		columnName := r.extractColumnName(item.Expr)

		// 提取表名（简化：假设格式为 table.column 或 column）
		tableName, _ := r.parseColumnName(columnName)

		// 检查 ORDER_INDEX hint
		if indexName, ok := optCtx.Hints.OrderIndex[tableName]; ok {
			debugf("  [ORDER BY HINT] Applying ORDER_INDEX: table=%s, index=%s\n", tableName, indexName)
			r.applyOrderIndexHint(sortPlan, tableName, columnName, indexName)
			continue
		}

		// 检查 NO_ORDER_INDEX hint
		if _, ok := optCtx.Hints.NoOrderIndex[tableName]; ok {
			debugf("  [ORDER BY HINT] Applying NO_ORDER_INDEX: table=%s\n", tableName)
			r.applyNoOrderIndexHint(sortPlan, tableName, columnName)
		}
	}

	return sortPlan, nil
}

// extractColumnName 从表达式中提取列名
func (r *OrderByHintRule) extractColumnName(expr parser.Expression) string {
	// 如果是列引用
	if expr.Type == parser.ExprTypeColumn && expr.Column != "" {
		return expr.Column
	}

	// 简化：返回表达式字符串
	return expressionToString(&expr)
}

// parseColumnName 解析列名，返回表名和列名
func (r *OrderByHintRule) parseColumnName(fullName string) (string, string) {
	parts := strings.Split(fullName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", fullName // 没有表名前缀
}

// applyOrderIndexHint 应用ORDER_INDEX hint
func (r *OrderByHintRule) applyOrderIndexHint(sortPlan *LogicalSort, tableName, columnName, indexName string) {
	// 在LogicalSort中添加hint信息
	// 实际实现需要在物理计划中应用这个hint

	// 设置一个标记，表示应该使用指定索引进行排序
	// 这里简化处理，实际应该在物理计划中应用

	debugf("  [ORDER BY HINT] Will use index %s for ordering %s.%s\n", indexName, tableName, columnName)

	// 可以在LogicalSort中添加字段来存储这个hint
	// 例如：
	// sortPlan.ForceIndex = indexName
}

// applyNoOrderIndexHint 应用NO_ORDER_INDEX hint
func (r *OrderByHintRule) applyNoOrderIndexHint(sortPlan *LogicalSort, tableName, columnName string) {
	// 标记不使用索引排序
	// 实际实现应该在物理计划中避免使用索引排序

	debugf("  [ORDER BY HINT] Will NOT use index for ordering %s.%s\n", tableName, columnName)

	// 可以在LogicalSort中添加字段
	// 例如：
	// sortPlan.DisableIndexSort = true
}

// EstimateSortCost 估算排序成本（考虑hints）
func (r *OrderByHintRule) EstimateSortCost(plan *LogicalSort, optCtx *OptimizationContext) float64 {
	baseCost := float64(10000) // 默认成本

	// 获取排序项
	orderByItems := plan.GetOrderBy()

	// 如果有ORDER_INDEX hint，使用索引排序（低成本）
	for _, item := range orderByItems {
		columnName := r.extractColumnName(item.Expr)
		tableName, _ := r.parseColumnName(columnName)
		if optCtx.Hints != nil {
			if _, ok := optCtx.Hints.OrderIndex[tableName]; ok {
				// 使用索引排序，成本降低
				return baseCost * 0.1
			}
		}
	}

	// 否则，使用全表扫描+排序（高成本）
	return baseCost
}

// GetRecommendedSortMethod 根据hints获取推荐的排序方法
func (r *OrderByHintRule) GetRecommendedSortMethod(plan *LogicalSort, optCtx *OptimizationContext) string {
	orderByItems := plan.GetOrderBy()

	for _, item := range orderByItems {
		columnName := r.extractColumnName(item.Expr)
		tableName, _ := r.parseColumnName(columnName)
		if optCtx.Hints != nil {
			if _, ok := optCtx.Hints.OrderIndex[tableName]; ok {
				return "INDEX_SORT"
			}
			if _, ok := optCtx.Hints.NoOrderIndex[tableName]; ok {
				return "EXTERNAL_SORT"
			}
		}
	}

	// 默认：根据数据量选择
	// 实际实现应该评估数据量
	return "EXTERNAL_SORT"
}

// Explain 解释ORDER BY Hints规则
func (r *OrderByHintRule) Explain() string {
	return "OrderByHintRule - Optimizes ORDER BY using index hints"
}

// EnhancedLogicalSort 增强的LogicalSort（支持hints）
type EnhancedLogicalSort struct {
	*LogicalSort
	ForceIndex      string  // 强制使用的索引
	DisableIndex    bool    // 禁止使用索引排序
	SortMethod      string  // 排序方法（INDEX_SORT, EXTERNAL_SORT, QUICK_SORT）
	MemoryLimit     int64   // 内存限制（字节）
}

// GetOrderByItems 获取排序项
func (r *OrderByHintRule) GetOrderByItems(plan *LogicalSort) []OrderByItem {
	// 转换逻辑排序项到物理排序项
	orderByItems := plan.GetOrderBy()
	items := make([]OrderByItem, len(orderByItems))

	for i, item := range orderByItems {
		items[i] = OrderByItem{
			Column:    r.extractColumnName(item.Expr),
			Direction: item.Direction,
		}
	}

	return items
}

// ApplyOrderByHintsToPhysicalPlan 将ORDER BY hints应用到物理计划
func ApplyOrderByHintsToPhysicalPlan(plan *LogicalSort, hints *OptimizerHints, optCtx *OptimizationContext) *EnhancedLogicalSort {
	enhancedSort := &EnhancedLogicalSort{
		LogicalSort: plan,
		SortMethod:  "EXTERNAL_SORT", // 默认
		MemoryLimit: 64 * 1024 * 1024,  // 默认64MB
	}

	if hints == nil {
		return enhancedSort
	}

	orderByItems := plan.GetOrderBy()
	for _, item := range orderByItems {
		columnName := expressionToString(&item.Expr)
		tableName, _ := parseColumnName(columnName)

		// 检查ORDER_INDEX hint
		if indexName, ok := hints.OrderIndex[tableName]; ok {
			enhancedSort.ForceIndex = indexName
			enhancedSort.SortMethod = "INDEX_SORT"
			debugf("  [ORDER BY HINT] Applied ORDER_INDEX %s to table %s\n", indexName, tableName)
		}

		// 检查NO_ORDER_INDEX hint
		if _, ok := hints.NoOrderIndex[tableName]; ok {
			enhancedSort.DisableIndex = true
			enhancedSort.SortMethod = "EXTERNAL_SORT"
			debugf("  [ORDER BY HINT] Applied NO_ORDER_INDEX to table %s\n", tableName)
		}
	}

	return enhancedSort
}

// parseColumnName 解析列名的辅助函数
func parseColumnName(fullName string) (string, string) {
	parts := strings.Split(fullName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", fullName
}

