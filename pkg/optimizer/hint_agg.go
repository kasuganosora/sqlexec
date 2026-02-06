package optimizer

import (
	"context"
	"fmt"
)

// HintAwareAggRule 支持 hints 的聚合规则
type HintAwareAggRule struct{}

// NewHintAwareAggRule 创建 hint 感知的聚合规则
func NewHintAwareAggRule() *HintAwareAggRule {
	return &HintAwareAggRule{}
}

// Name 返回规则名称
func (r *HintAwareAggRule) Name() string {
	return "HintAwareAgg"
}

// Match 检查规则是否匹配
func (r *HintAwareAggRule) Match(plan LogicalPlan) bool {
	// 匹配 LogicalAggregate
	_, ok := plan.(*LogicalAggregate)
	return ok
}

// Apply 应用规则
func (r *HintAwareAggRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	if optCtx == nil || optCtx.Hints == nil {
		return plan, nil
	}

	hints := optCtx.Hints
	return r.ApplyWithHints(ctx, plan, optCtx, hints)
}

// ApplyWithHints 应用带有 hints 的规则
func (r *HintAwareAggRule) ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error) {
	agg, ok := plan.(*LogicalAggregate)
	if !ok {
		return plan, nil
	}

	// Priority 1: HASH_AGG - 哈希聚合
	if hints.HashAgg {
		fmt.Printf("  [HINT AGG] HASH_AGG hint applied\n")
		agg.SetAlgorithm(HashAggAlgorithm)
		agg.SetHintApplied("HASH_AGG")
		return agg, nil
	}

	// Priority 2: STREAM_AGG - 流式聚合
	if hints.StreamAgg {
		fmt.Printf("  [HINT AGG] STREAM_AGG hint applied\n")
		agg.SetAlgorithm(StreamAggAlgorithm)
		agg.SetHintApplied("STREAM_AGG")
		return agg, nil
	}

	// Priority 3: MPP_1PHASE_AGG - MPP 单阶段聚合
	if hints.MPP1PhaseAgg {
		fmt.Printf("  [HINT AGG] MPP_1PHASE_AGG hint applied\n")
		agg.SetAlgorithm(MPP1PhaseAggAlgorithm)
		agg.SetHintApplied("MPP_1PHASE_AGG")
		return agg, nil
	}

	// Priority 4: MPP_2PHASE_AGG - MPP 两阶段聚合
	if hints.MPP2PhaseAgg {
		fmt.Printf("  [HINT AGG] MPP_2PHASE_AGG hint applied\n")
		agg.SetAlgorithm(MPP2PhaseAggAlgorithm)
		agg.SetHintApplied("MPP_2PHASE_AGG")
		return agg, nil
	}

	// No relevant hints, use default algorithm
	return agg, nil
}
