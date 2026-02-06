package optimizer

import (
	"context"
	"fmt"
)

// HintAwareSubqueryRule 支持 hints 的子查询规则
type HintAwareSubqueryRule struct {
	// 可以包含对现有规则的引用
	semiJoinRewriteRule *SemiJoinRewriteRule
	decorrelateRule     *DecorrelateRule
}

// NewHintAwareSubqueryRule 创建 hint 感知的子查询规则
func NewHintAwareSubqueryRule(estimator CardinalityEstimator) *HintAwareSubqueryRule {
	return &HintAwareSubqueryRule{
		semiJoinRewriteRule: &SemiJoinRewriteRule{},
		decorrelateRule:     NewDecorrelateRule(estimator),
	}
}

// Name 返回规则名称
func (r *HintAwareSubqueryRule) Name() string {
	return "HintAwareSubquery"
}

// Match 检查规则是否匹配
func (r *HintAwareSubqueryRule) Match(plan LogicalPlan) bool {
	// 匹配 LogicalApply (子查询)
	_, ok := plan.(*LogicalApply)
	return ok
}

// Apply 应用规则
func (r *HintAwareSubqueryRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	if optCtx == nil || optCtx.Hints == nil {
		// 无 hints，使用默认逻辑
		return r.applyDefaultRules(ctx, plan, optCtx)
	}

	hints := optCtx.Hints
	return r.ApplyWithHints(ctx, plan, optCtx, hints)
}

// ApplyWithHints 应用带有 hints 的规则
func (r *HintAwareSubqueryRule) ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error) {
	apply, ok := plan.(*LogicalApply)
	if !ok {
		return plan, nil
	}

	// Priority 1: SEMI_JOIN_REWRITE hint - 启用 Semi Join 改写
	if hints.SemiJoinRewrite {
		fmt.Printf("  [HINT SUBQUERY] SEMI_JOIN_REWRITE hint applied\n")
		return r.semiJoinRewriteRule.Apply(ctx, apply, optCtx)
	}

	// Priority 2: NO_DECORRELATE hint - 禁用子查询去关联
	if hints.NoDecorrelate {
		fmt.Printf("  [HINT SUBQUERY] NO_DECORRELATE hint applied - skipping decorrelation\n")
		// 返回原始 plan，不进行去关联
		return apply, nil
	}

	// Priority 3: USE_TOJA hint - 使用 TOJA (Try to Join with Aggregation)
	if hints.UseTOJA {
		fmt.Printf("  [HINT SUBQUERY] USE_TOJA hint applied\n")
		// TOJA 是一种优化技术，这里简化实现
		return r.applyTOJA(ctx, apply, optCtx)
	}

	// No relevant hints, use default rules
	return r.applyDefaultRules(ctx, plan, optCtx)
}

// applyDefaultRules 应用默认的子查询优化规则
func (r *HintAwareSubqueryRule) applyDefaultRules(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// 默认情况下应用 Semi Join 改写
	if apply, ok := plan.(*LogicalApply); ok {
		// 尝试 Semi Join 改写
		newPlan, err := r.semiJoinRewriteRule.Apply(ctx, apply, optCtx)
		if err == nil && newPlan != nil {
			return newPlan, nil
		}
		// 如果 Semi Join 改写失败，继续使用原始 plan
	}

	// 如果有 decorrelateRule，应用它
	if r.decorrelateRule != nil {
		return r.decorrelateRule.Apply(ctx, plan, optCtx)
	}

	return plan, nil
}

// applyTOJA 应用 TOJA (Try to Join with Aggregation)
func (r *HintAwareSubqueryRule) applyTOJA(ctx context.Context, apply *LogicalApply, optCtx *OptimizationContext) (LogicalPlan, error) {
	// TOJA 是一种用于优化 EXISTS/IN 子查询的技术
	// 它将子查询转换为 JOIN，并结合聚合来处理语义

	// 简化实现：检查子查询是否可以应用 TOJA
	// 1. 子查询必须是简单的 SELECT
	// 2. 不应该有 GROUP BY
	// 3. 不应该有 HAVING
	// 4. 应该有合适的连接条件

	// 这里只是框架实现，实际 TOJA 需要更复杂的逻辑
	fmt.Printf("  [TOJA] Attempting TOJA transformation\n")

	// 标记已应用 TOJA hint
	// 注意：LogicalApply 可能没有 SetHintApplied 方法，这里简化处理
	return apply, nil
}
