package optimizer

import (
	"context"
	"fmt"
)

// HintAwareIndexRule 支持 hints 的索引使用规则
type HintAwareIndexRule struct{}

// NewHintAwareIndexRule 创建 hint 感知的索引规则
func NewHintAwareIndexRule() *HintAwareIndexRule {
	return &HintAwareIndexRule{}
}

// Name 返回规则名称
func (r *HintAwareIndexRule) Name() string {
	return "HintAwareIndex"
}

// Match 检查规则是否匹配
func (r *HintAwareIndexRule) Match(plan LogicalPlan) bool {
	// 匹配 LogicalDataSource 和 LogicalSelection
	_, okDataSource := plan.(*LogicalDataSource)
	_, okSelection := plan.(*LogicalSelection)
	return okDataSource || okSelection
}

// Apply 应用规则
func (r *HintAwareIndexRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	if optCtx == nil || optCtx.Hints == nil {
		return plan, nil
	}

	hints := optCtx.Hints
	return r.ApplyWithHints(ctx, plan, optCtx, hints)
}

// ApplyWithHints 应用带有 hints 的规则
func (r *HintAwareIndexRule) ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error) {
	// 优先处理 DataSource（如果 DataSource 是 Selection 的子节点）
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return r.applyIndexHintsToDataSource(dataSource, hints)
	}

	// 处理 Selection
	if selection, ok := plan.(*LogicalSelection); ok {
		// 检查子节点是否是 DataSource
		if len(selection.Children()) > 0 {
			if childDataSource, ok := selection.Children()[0].(*LogicalDataSource); ok {
				// 对 DataSource 应用 hints
				modifiedDataSource, err := r.applyIndexHintsToDataSource(childDataSource, hints)
				if err != nil {
					return nil, err
				}
				if modifiedDataSource != childDataSource {
					// 更新子节点
					selection.SetChildren(modifiedDataSource)
				}
			}
		}
	}

	return plan, nil
}

// applyIndexHintsToDataSource 对 DataSource 应用索引 hints
func (r *HintAwareIndexRule) applyIndexHintsToDataSource(dataSource *LogicalDataSource, hints *OptimizerHints) (LogicalPlan, error) {
	tableName := dataSource.TableName

	// Priority 1: FORCE_INDEX - 强制使用指定索引
	if indexList, ok := hints.ForceIndex[tableName]; ok && len(indexList) > 0 {
		fmt.Printf("  [HINT INDEX] FORCE_INDEX for table %s: %v\n", tableName, indexList)
		dataSource.ForceUseIndex(indexList[0]) // 强制使用第一个索引
		dataSource.SetHintApplied("FORCE_INDEX")
		return dataSource, nil
	}

	// Priority 2: USE_INDEX - 优先使用指定索引
	if indexList, ok := hints.UseIndex[tableName]; ok && len(indexList) > 0 {
		fmt.Printf("  [HINT INDEX] USE_INDEX for table %s: %v\n", tableName, indexList)
		dataSource.PreferIndex(indexList[0]) // 优先使用第一个索引
		dataSource.SetHintApplied("USE_INDEX")
		return dataSource, nil
	}

	// Priority 3: IGNORE_INDEX - 忽略指定索引
	if indexList, ok := hints.IgnoreIndex[tableName]; ok && len(indexList) > 0 {
		fmt.Printf("  [HINT INDEX] IGNORE_INDEX for table %s: %v\n", tableName, indexList)
		for _, idx := range indexList {
			dataSource.IgnoreIndex(idx)
		}
		dataSource.SetHintApplied("IGNORE_INDEX")
		return dataSource, nil
	}

	// Priority 4: ORDER_INDEX - 强制排序索引
	if orderIndex, ok := hints.OrderIndex[tableName]; ok && orderIndex != "" {
		fmt.Printf("  [HINT INDEX] ORDER_INDEX for table %s: %s\n", tableName, orderIndex)
		dataSource.SetOrderIndex(orderIndex)
		dataSource.SetHintApplied("ORDER_INDEX")
		return dataSource, nil
	}

	// Priority 5: NO_ORDER_INDEX - 忽略排序索引
	if noOrderIndex, ok := hints.NoOrderIndex[tableName]; ok && noOrderIndex != "" {
		fmt.Printf("  [HINT INDEX] NO_ORDER_INDEX for table %s: %s\n", tableName, noOrderIndex)
		dataSource.IgnoreOrderIndex(noOrderIndex)
		dataSource.SetHintApplied("NO_ORDER_INDEX")
		return dataSource, nil
	}

	return dataSource, nil
}
