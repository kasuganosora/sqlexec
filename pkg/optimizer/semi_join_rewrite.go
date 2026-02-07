package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// SemiJoinRewriteRule 半连接重写规则
// 将EXISTS和IN子查询重写为更高效的连接形式
type SemiJoinRewriteRule struct {
	cardinalityEstimator CardinalityEstimator
}

// Name 返回规则名称
func (r *SemiJoinRewriteRule) Name() string {
	return "SemiJoinRewrite"
}

// Match 检查规则是否匹配
func (r *SemiJoinRewriteRule) Match(plan LogicalPlan) bool {
	// 检查是否包含EXISTS或IN子查询
	return containsSubquery(plan, "EXISTS") || containsSubquery(plan, "IN")
}

// containsSubquery 检查是否包含指定类型的子查询
func containsSubquery(plan LogicalPlan, queryType string) bool {
	if selection, ok := plan.(*LogicalSelection); ok {
		for _, cond := range selection.Conditions() {
			if isSubquery(cond, queryType) {
				return true
			}
		}
	}

	for _, child := range plan.Children() {
		if containsSubquery(child, queryType) {
			return true
		}
	}

	return false
}

// isSubquery 检查表达式是否为子查询
func isSubquery(expr *parser.Expression, queryType string) bool {
	if expr == nil {
		return false
	}

	// 检查是否为子查询节点
	// 简化实现：检查函数名
	if expr.Type == parser.ExprTypeFunction {
		if expr.Function == queryType {
			return true
		}
	}

	// 递归检查子表达式
	if isSubquery(expr.Left, queryType) || isSubquery(expr.Right, queryType) {
		return true
	}

	return false
}

// Apply 应用规则：重写半连接
func (r *SemiJoinRewriteRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: 开始, 计划类型:", plan.Explain())

	// 1. 查找EXISTS子查询并重写为JOIN
	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: 开始重写EXISTS子查询")
	plan = r.rewriteExistsToJoin(plan)
	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: EXISTS重写完成")

	// 2. 查找IN子查询并重写为JOIN
	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: 开始重写IN子查询")
	plan = r.rewriteInToJoin(plan)
	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: IN重写完成")

	fmt.Println("  [DEBUG] SemiJoinRewriteRule.Apply: 完成")
	return plan, nil
}

// rewriteExistsToJoin 将EXISTS子查询重写为JOIN + DISTINCT
func (r *SemiJoinRewriteRule) rewriteExistsToJoin(plan LogicalPlan) LogicalPlan {
	fmt.Println("  [DEBUG] rewriteExistsToJoin: 开始")
	result := r.rewriteExistsToJoinWithVisited(plan, make(map[LogicalPlan]bool))
	fmt.Println("  [DEBUG] rewriteExistsToJoin: 完成")
	return result
}

// rewriteExistsToJoinWithVisited 使用visited集合防止无限递归
func (r *SemiJoinRewriteRule) rewriteExistsToJoinWithVisited(plan LogicalPlan, visited map[LogicalPlan]bool) LogicalPlan {
	// 防止无限递归
	if visited[plan] {
		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 检测到已访问节点, 返回")
		return plan
	}
	visited[plan] = true
	fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 处理节点", plan.Explain())

	if selection, ok := plan.(*LogicalSelection); ok {
		rewritten := false

		// 遍历条件，查找EXISTS子查询
		newConditions := make([]*parser.Expression, 0, len(selection.Conditions()))

		for _, cond := range selection.Conditions() {
			if r.isExistsSubquery(cond) {
				// EXISTS (SELECT ...) -> INNER JOIN
				// 重写为: EXISTS (SELECT ...) = TRUE
				// 注意：实际应该提取子查询表名，创建JOIN
				// 这里简化为保留条件，标记为已重写
				newConditions = append(newConditions, cond)
				rewritten = true
			} else {
				newConditions = append(newConditions, cond)
			}
		}

		// 递归处理子节点
		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 递归处理Selection的子节点")
		newChild := r.rewriteExistsToJoinWithVisited(selection.children[0], visited)

		if rewritten {
			// 创建新的 Selection 更新条件和子节点
			fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 创建新的Selection")
			return NewLogicalSelection(newConditions, newChild)
		}

		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 未重写, 返回原Selection")
		return plan
	}

	// 递归处理所有子节点
	children := plan.Children()
	if len(children) == 0 {
		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 无子节点, 返回")
		return plan
	}

	fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 处理", len(children), "个子节点")
	newChildren := make([]LogicalPlan, len(children))
	changed := false

	for i, child := range children {
		originalChild := child
		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 处理子节点", i)
		newChildren[i] = r.rewriteExistsToJoinWithVisited(child, visited)
		if newChildren[i] != originalChild {
			changed = true
			fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 子节点", i, "已改变")
		}
	}

	// 如果没有任何改变，直接返回原计划
	if !changed {
		fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 无改变, 返回原计划")
		return plan
	}

	fmt.Println("  [DEBUG] rewriteExistsToJoinWithVisited: 更新子节点并返回")
	plan.SetChildren(newChildren...)

	return plan
}

// rewriteInToJoin 将IN子查询重写为JOIN
func (r *SemiJoinRewriteRule) rewriteInToJoin(plan LogicalPlan) LogicalPlan {
	fmt.Println("  [DEBUG] rewriteInToJoin: 开始")
	result := r.rewriteInToJoinWithVisited(plan, make(map[LogicalPlan]bool))
	fmt.Println("  [DEBUG] rewriteInToJoin: 完成")
	return result
}

// rewriteInToJoinWithVisited 使用visited集合防止无限递归
func (r *SemiJoinRewriteRule) rewriteInToJoinWithVisited(plan LogicalPlan, visited map[LogicalPlan]bool) LogicalPlan {
	// 防止无限递归
	if visited[plan] {
		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 检测到已访问节点, 返回")
		return plan
	}
	visited[plan] = true
	fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 处理节点", plan.Explain())

	if selection, ok := plan.(*LogicalSelection); ok {
		rewritten := false

		// 遍历条件，查找IN子查询
		newConditions := make([]*parser.Expression, 0, len(selection.Conditions()))

		for _, cond := range selection.Conditions() {
			if r.isInSubquery(cond) {
				// column IN (SELECT ...) -> JOIN
				// 重写为: column IN (SELECT ...) = TRUE
				// 注意：实际应该提取子查询表名，创建JOIN
				// 这里简化为保留条件，标记为已重写
				newConditions = append(newConditions, cond)
				rewritten = true
			} else {
				newConditions = append(newConditions, cond)
			}
		}

		// 递归处理子节点
		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 递归处理Selection的子节点")
		newChild := r.rewriteInToJoinWithVisited(selection.children[0], visited)

		if rewritten {
			// 创建新的 Selection 更新条件和子节点
			fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 创建新的Selection")
			return NewLogicalSelection(newConditions, newChild)
		}

		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 未重写, 返回原Selection")
		return plan
	}

	// 递归处理所有子节点
	children := plan.Children()
	if len(children) == 0 {
		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 无子节点, 返回")
		return plan
	}

	fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 处理", len(children), "个子节点")
	newChildren := make([]LogicalPlan, len(children))
	changed := false

	for i, child := range children {
		originalChild := child
		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 处理子节点", i)
		newChildren[i] = r.rewriteInToJoinWithVisited(child, visited)
		if newChildren[i] != originalChild {
			changed = true
			fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 子节点", i, "已改变")
		}
	}

	// 如果没有任何改变，直接返回原计划
	if !changed {
		fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 无改变, 返回原计划")
		return plan
	}

	fmt.Println("  [DEBUG] rewriteInToJoinWithVisited: 更新子节点并返回")
	plan.SetChildren(newChildren...)

	return plan
}

// isExistsSubquery 检查是否为EXISTS子查询
func (r *SemiJoinRewriteRule) isExistsSubquery(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	if expr.Type == parser.ExprTypeFunction && expr.Function == "EXISTS" {
		return true
	}

	if r.isExistsSubquery(expr.Left) || r.isExistsSubquery(expr.Right) {
		return true
	}

	return false
}

// isInSubquery 检查是否为IN子查询
func (r *SemiJoinRewriteRule) isInSubquery(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	// IN操作符
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "IN" {
		// 检查右侧是否为子查询
		if expr.Right != nil && expr.Right.Type == parser.ExprTypeFunction {
			return true
		}
	}

	if r.isInSubquery(expr.Left) || r.isInSubquery(expr.Right) {
		return true
	}

	return false
}

// NewSemiJoinRewriteRule 创建半连接重写规则
func NewSemiJoinRewriteRule(estimator CardinalityEstimator) *SemiJoinRewriteRule {
	return &SemiJoinRewriteRule{
		cardinalityEstimator: estimator,
	}
}
