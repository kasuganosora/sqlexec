package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// ORToUnionRule OR转UNION规则
// 将包含OR条件的Selection转换为UNION
type ORToUnionRule struct{}

// NewORToUnionRule 创建OR转UNION规则
func NewORToUnionRule() *ORToUnionRule {
	return &ORToUnionRule{}
}

// Name 返回规则名称
func (r *ORToUnionRule) Name() string {
	return "ORToUnion"
}

// Match 检查规则是否匹配
func (r *ORToUnionRule) Match(plan LogicalPlan) bool {
	selection, ok := plan.(*LogicalSelection)
	if !ok {
		return false
	}

	// 检查是否包含OR条件
	conditions := selection.Conditions()
	return r.hasORCondition(conditions)
}

// Apply 应用规则
func (r *ORToUnionRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	selection, ok := plan.(*LogicalSelection)
	if !ok {
		return plan, nil
	}

	// 转换OR为UNION
	unionPlan := r.convertORToUnion(selection)
	if unionPlan != nil {
		fmt.Println("  [OR2UNION] Converted OR to UNION")
		return unionPlan, nil
	}

	return plan, nil
}

// hasORCondition 检查条件列表中是否包含OR条件
func (r *ORToUnionRule) hasORCondition(conditions []*parser.Expression) bool {
	for _, cond := range conditions {
		if r.isORExpression(cond) {
			return true
		}
	}
	return false
}

// convertORToUnion 将Selection中的OR条件转换为UNION
func (r *ORToUnionRule) convertORToUnion(selection *LogicalSelection) LogicalPlan {
	conditions := selection.Conditions()
	if len(conditions) == 0 {
		return nil
	}

	child := selection.Children()[0]
	if child == nil {
		return nil
	}

	// 提取OR条件并分解为独立分支
	orBranches := r.extractORBranches(conditions)
	if len(orBranches) == 0 {
		return nil
	}

	// 为每个OR分支创建独立的查询计划
	unionChildren := make([]LogicalPlan, 0, len(orBranches))
	for _, branch := range orBranches {
		// 为每个分支创建Selection
		branchSelection := NewLogicalSelection(branch, child)
		unionChildren = append(unionChildren, branchSelection)
	}

	// 创建UNION节点
	return NewLogicalUnion(unionChildren)
}

// extractORBranches 从条件中提取OR分支
// 将复杂的OR表达式分解为简单的独立条件
func (r *ORToUnionRule) extractORBranches(conditions []*parser.Expression) [][]*parser.Expression {
	branches := make([][]*parser.Expression, 0)

	for _, cond := range conditions {
		orExprs := r.extractORExpressions(cond)
		if len(orExprs) > 0 {
			// 为每个OR表达式创建独立分支
			for _, orExpr := range orExprs {
				branches = append(branches, []*parser.Expression{orExpr})
			}
		} else {
			// 非OR条件，添加到所有现有分支
			if len(branches) == 0 {
				branches = append(branches, []*parser.Expression{cond})
			} else {
				for i := range branches {
					branches[i] = append(branches[i], cond)
				}
			}
		}
	}

	return branches
}

// extractORExpressions 从表达式中提取所有顶层OR表达式
func (r *ORToUnionRule) extractORExpressions(expr *parser.Expression) []*parser.Expression {
	var result []*parser.Expression

	// 如果表达式本身是OR
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "or" {
		// 提取左右子表达式
		leftExprs := r.extractORExpressions(expr.Left)
		rightExprs := r.extractORExpressions(expr.Right)

		// 合并结果
		result = append(result, leftExprs...)
		result = append(result, rightExprs...)
		return result
	}

	// 不是OR表达式，直接返回
	return []*parser.Expression{expr}
}

// isORExpression 判断是否是OR表达式
func (r *ORToUnionRule) isORExpression(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	// 检查顶层运算符是否是OR
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "or" {
		return true
	}

	// 递归检查子表达式
	if r.isORExpression(expr.Left) || r.isORExpression(expr.Right) {
		return true
	}

	return false
}

// Explain 解释规则应用
func (r *ORToUnionRule) Explain(result LogicalPlan) string {
	return fmt.Sprintf(
		"ORToUnion: Applied, result type: %T",
		result,
	)
}
