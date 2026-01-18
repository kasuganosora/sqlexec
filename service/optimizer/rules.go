package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/service/parser"
)

// PredicatePushDownRule 谓词下推规则
// �?Selection 节点尽可能下推到 DataSource
type PredicatePushDownRule struct{}

// Name 返回规则名称
func (r *PredicatePushDownRule) Name() string {
	return "PredicatePushDown"
}

// Match 检查规则是否匹�?
func (r *PredicatePushDownRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalSelection)
	return ok
}

// Apply 应用规则
func (r *PredicatePushDownRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	selection, ok := plan.(*LogicalSelection)
	if !ok {
		return plan, nil
	}

	if len(selection.children) == 0 {
		return plan, nil
	}

	child := selection.children[0]

	// 如果子节点是 DataSource，将谓词标记到DataSource上（下推成功�?
	if dataSource, ok := child.(*LogicalDataSource); ok {
		// 将Selection的条件标记到DataSource，表示可以在扫描时过�?
		dataSource.PushDownPredicates(selection.Conditions())
		// 返回child，消除Selection节点（条件已下推到DataSource�?
		return child, nil
	}

	// 如果子节点是 Selection，合并条�?
	if childSelection, ok := child.(*LogicalSelection); ok {
		// 合并条件列表
		mergedConditions := append(selection.Conditions(), childSelection.Conditions()...)
		return NewLogicalSelection(mergedConditions, childSelection.Children()[0]), nil
	}

	// 尝试下推到其他节�?
	// 简化实现：不下�?
	return plan, nil
}

// ColumnPruningRule 列裁剪规�?
// 移除不需要的�?
type ColumnPruningRule struct{}

// Name 返回规则名称
func (r *ColumnPruningRule) Name() string {
	return "ColumnPruning"
}

// Match 检查规则是否匹�?
func (r *ColumnPruningRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalProjection)
	return ok
}

// Apply 应用规则
func (r *ColumnPruningRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	projection, ok := plan.(*LogicalProjection)
	if !ok {
		return plan, nil
	}

	if len(projection.children) == 0 {
		return plan, nil
	}

	child := projection.children[0]

	// 收集子节点需要的�?
	requiredCols := make(map[string]bool)
	for _, expr := range projection.Exprs {
		collectRequiredColumns(expr, requiredCols)
	}
	
	// 打印需要的�?
	keys := make([]string, 0, len(requiredCols))
	for k := range requiredCols {
		keys = append(keys, k)
	}
	fmt.Printf("  [DEBUG] ColumnPruningRule.Apply: 需要的�? %v\n", keys)

	// 如果子节点是 DataSource，调整输出列
	if dataSource, ok := child.(*LogicalDataSource); ok {
		// 筛选出需要的�?
		newColumns := []ColumnInfo{}
		for _, col := range dataSource.Columns {
			if requiredCols[col.Name] {
				newColumns = append(newColumns, col)
			}
		}
		// 如果有变化，创建新的 DataSource
		if len(newColumns) < len(dataSource.Columns) {
			fmt.Printf("  [DEBUG] ColumnPruningRule.Apply: 原列�? %d, 裁剪�? %d\n", len(dataSource.Columns), len(newColumns))
			// 保存下推的谓词和Limit信息
			predicates := dataSource.GetPushedDownPredicates()
			limitInfo := dataSource.GetPushedDownLimit()
			
			newDataSource := NewLogicalDataSource(dataSource.TableName, dataSource.TableInfo)
			newDataSource.Columns = newColumns
			newDataSource.PushDownPredicates(predicates)
			if limitInfo != nil {
				newDataSource.PushDownLimit(limitInfo.Limit, limitInfo.Offset)
			}
			projection.children[0] = newDataSource
			fmt.Printf("  [DEBUG] ColumnPruningRule.Apply: 列裁剪完成\n")
		}
	}

	return plan, nil
}

// collectRequiredColumns 收集表达式需要的�?
func collectRequiredColumns(expr *parser.Expression, cols map[string]bool) {
	if expr == nil {
		return
	}

	if expr.Type == parser.ExprTypeColumn && expr.Column != "" {
		cols[expr.Column] = true
	}

	// 递归处理子表达式
	collectRequiredColumns(expr.Left, cols)
	collectRequiredColumns(expr.Right, cols)
}

// ProjectionEliminationRule 投影消除规则
// 移除不必要的投影节点
type ProjectionEliminationRule struct{}

// Name 返回规则名称
func (r *ProjectionEliminationRule) Name() string {
	return "ProjectionElimination"
}

// Match 检查规则是否匹�?
func (r *ProjectionEliminationRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalProjection)
	return ok
}

// Apply 应用规则
func (r *ProjectionEliminationRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	projection, ok := plan.(*LogicalProjection)
	if !ok {
		return plan, nil
	}

	if len(projection.children) == 0 {
		return plan, nil
	}

	child := projection.children[0]

	// 如果投影只是简单地传递所有列，可以消�?
	childSchema := child.Schema()
	if len(projection.Exprs) == len(childSchema) {
		allPassThrough := true
		for i, expr := range projection.Exprs {
			if expr.Type != parser.ExprTypeColumn || expr.Column != childSchema[i].Name {
				allPassThrough = false
				break
			}
		}
		if allPassThrough {
			return child, nil
		}
	}

	return plan, nil
}

// LimitPushDownRule Limit 下推规则
// �?Limit 尽可能下�?
type LimitPushDownRule struct{}

// Name 返回规则名称
func (r *LimitPushDownRule) Name() string {
	return "LimitPushDown"
}

// Match 检查规则是否匹�?
func (r *LimitPushDownRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalLimit)
	return ok
}

// Apply 应用规则
func (r *LimitPushDownRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	limit, ok := plan.(*LogicalLimit)
	if !ok {
		return plan, nil
	}

	if len(limit.children) == 0 {
		return plan, nil
	}

	child := limit.children[0]

	// 如果子节点是 DataSource，下推到DataSource
	if dataSource, ok := child.(*LogicalDataSource); ok {
		// 标记Limit到DataSource
		dataSource.PushDownLimit(limit.Limit(), limit.Offset())
		// 返回child，消除Limit节点（已下推�?
		return child, nil
	}

	// 如果子节点是 Selection，可以下推到Selection的子节点
	if selection, ok := child.(*LogicalSelection); ok {
		// 创建新的 Selection，其子节点是新的 Limit
		newLimit := NewLogicalLimit(limit.Limit(), limit.Offset(), selection.Children()[0])
		return NewLogicalSelection(selection.Conditions(), newLimit), nil
	}

	return plan, nil
}

// ConstantFoldingRule 常量折叠规则
// 计算常量表达�?
type ConstantFoldingRule struct{}

// Name 返回规则名称
func (r *ConstantFoldingRule) Name() string {
	return "ConstantFolding"
}

// Match 检查规则是否匹�?
func (r *ConstantFoldingRule) Match(plan LogicalPlan) bool {
	// 简化实现：总是匹配
	return true
}

// Apply 应用规则
func (r *ConstantFoldingRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	evaluator := NewExpressionEvaluatorWithoutAPI()
	return r.foldConstants(plan, evaluator)
}

// foldConstants 递归折叠常量表达�?
func (r *ConstantFoldingRule) foldConstants(plan LogicalPlan, evaluator *ExpressionEvaluator) (LogicalPlan, error) {
	// 先处理子节点
	children := plan.Children()
	for i, child := range children {
		newChild, err := r.foldConstants(child, evaluator)
		if err != nil {
			return nil, err
		}
		if newChild != child {
			plan.SetChildren(children...)
			children[i] = newChild
		}
	}

	// 根据不同的算子类型进行常量折�?
	switch p := plan.(type) {
	case *LogicalSelection:
		return r.foldSelectionConstants(p, evaluator)
	case *LogicalProjection:
		return r.foldProjectionConstants(p, evaluator)
	case *LogicalJoin:
		return r.foldJoinConstants(p, evaluator)
	}

	return plan, nil
}

// foldSelectionConstants 折叠 Selection 中的常量
func (r *ConstantFoldingRule) foldSelectionConstants(selection *LogicalSelection, evaluator *ExpressionEvaluator) (LogicalPlan, error) {
	newConditions := []*parser.Expression{}
	changed := false

	for _, cond := range selection.Conditions() {
		folded, isConst, err := r.tryFoldExpression(cond, evaluator)
		if err != nil {
			return nil, err
		}

		if isConst {
			// 常量表达�?
			if folded == nil {
				// 条件永远为假，可以丢弃整�?Selection
				return selection, nil
			}
			// 检查是否为 false 布尔�?
			if folded.Value != nil {
				if boolVal, ok := folded.Value.(bool); ok && !boolVal {
					// 条件永远为假，可以丢弃整�?Selection
					return selection, nil
				}
			}
			// 条件永远为真，可以移除这个条�?
			changed = true
		} else {
			newConditions = append(newConditions, folded)
		}
	}

	if changed {
		if len(newConditions) == 0 {
			// 所有条件都被移除，返回子节�?
			if len(selection.children) > 0 {
				return selection.children[0], nil
			}
		} else {
			// 创建新的 Selection 更新条件
			return NewLogicalSelection(newConditions, selection.children[0]), nil
		}
	}

	return selection, nil
}

// foldProjectionConstants 折叠 Projection 中的常量
func (r *ConstantFoldingRule) foldProjectionConstants(projection *LogicalProjection, evaluator *ExpressionEvaluator) (LogicalPlan, error) {
	newExprs := []*parser.Expression{}
	changed := false

	for _, expr := range projection.Exprs {
		folded, isConst, err := r.tryFoldExpression(expr, evaluator)
		if err != nil {
			return nil, err
		}
		if isConst {
			newExprs = append(newExprs, folded)
			changed = true
		} else {
			newExprs = append(newExprs, expr)
		}
	}

	if changed {
		projection.Exprs = newExprs
	}

	return projection, nil
}

// foldJoinConstants 折叠 Join 中的常量
func (r *ConstantFoldingRule) foldJoinConstants(join *LogicalJoin, evaluator *ExpressionEvaluator) (LogicalPlan, error) {
	// 简化：不处�?
	return join, nil
}

// tryFoldExpression 尝试折叠表达�?
// 返回: (新表达式, 是否是常�? 错误)
func (r *ConstantFoldingRule) tryFoldExpression(expr *parser.Expression, evaluator *ExpressionEvaluator) (*parser.Expression, bool, error) {
	if expr == nil {
		return nil, false, nil
	}

	// 如果是字面量，已经是常量
	if expr.Type == parser.ExprTypeValue {
		return expr, true, nil
	}

	// 如果是列引用，不能折�?
	if expr.Type == parser.ExprTypeColumn {
		return expr, false, nil
	}

	// 如果是运算符，尝试计�?
	if expr.Type == parser.ExprTypeOperator && expr.Operator != "" {
		// 一元运算符
		if expr.Right == nil && expr.Left != nil {
			leftFolded, leftIsConst, err := r.tryFoldExpression(expr.Left, evaluator)
			if err != nil {
				return nil, false, err
			}
			if leftIsConst {
				// 尝试计算
				result, err := evaluator.Evaluate(&parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: expr.Operator,
					Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: leftFolded},
				}, parser.Row{})
				if err == nil {
					return &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: result,
					}, true, nil
				}
			}
			expr.Left = leftFolded
			return expr, false, nil
		}

		// 二元运算�?
		if expr.Left != nil && expr.Right != nil {
			leftFolded, leftIsConst, err := r.tryFoldExpression(expr.Left, evaluator)
			if err != nil {
				return nil, false, err
			}
			rightFolded, rightIsConst, err := r.tryFoldExpression(expr.Right, evaluator)
			if err != nil {
				return nil, false, err
			}

			if leftIsConst && rightIsConst {
				// 两个操作数都是常量，可以计算
				result, err := evaluator.Evaluate(&parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: expr.Operator,
					Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: leftFolded},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: rightFolded},
				}, parser.Row{})
				if err == nil {
					return &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: result,
					}, true, nil
				}
			}

			expr.Left = leftFolded
			expr.Right = rightFolded
			if leftFolded != expr.Left || rightFolded != expr.Right {
				return expr, false, nil
			}
		}
	}

	return expr, false, nil
}

// DefaultRuleSet 返回默认规则�?
func DefaultRuleSet() RuleSet {
	rules := RuleSet{
		&PredicatePushDownRule{},
		&ColumnPruningRule{},
		&ProjectionEliminationRule{},
		&LimitPushDownRule{},
		&ConstantFoldingRule{},
		&JoinReorderRule{},
		&JoinEliminationRule{},
		&SemiJoinRewriteRule{},
	}
	fmt.Println("  [DEBUG] DefaultRuleSet: 创建规则�? 数量:", len(rules))
	for i, r := range rules {
		fmt.Printf("  [DEBUG]   规则%d: %s\n", i, r.Name())
	}
	return rules
}

// RuleExecutor 规则执行�?
type RuleExecutor struct {
	rules RuleSet
}

// NewRuleExecutor 创建规则执行�?
func NewRuleExecutor(rules RuleSet) *RuleExecutor {
	return &RuleExecutor{
		rules: rules,
	}
}

// Execute 执行所有规�?
func (re *RuleExecutor) Execute(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	fmt.Println("  [DEBUG] RuleExecutor: 开始执行规�? 规则数量:", len(re.rules))
	current := plan
	maxIterations := 10 // 防止无限循环
	iterations := 0

	for iterations < maxIterations {
		fmt.Println("  [DEBUG] RuleExecutor: 迭代", iterations+1)
		changed := false

		for _, rule := range re.rules {
			if rule.Match(current) {
				fmt.Println("  [DEBUG] RuleExecutor: 规则", rule.Name(), "匹配")
				newPlan, err := rule.Apply(ctx, current, optCtx)
				if err != nil {
					return nil, fmt.Errorf("rule %s failed: %w", rule.Name(), err)
				}
				if newPlan != current {
					current = newPlan
					changed = true
					fmt.Println("  [DEBUG] RuleExecutor: 规则", rule.Name(), "已应�?)
				}
			}

			// 递归应用到子节点
			children := current.Children()
			fmt.Println("  [DEBUG] RuleExecutor: 处理子节�? 子节点数:", len(children))
			for i, child := range children {
				fmt.Println("  [DEBUG] RuleExecutor: 递归处理子节�?, i, "类型:", child.Explain())
				newChild, err := re.Execute(ctx, child, optCtx)
				if err != nil {
					return nil, err
				}
				if newChild != child {
					children = current.Children()
					children[i] = newChild
					current.SetChildren(children...)
					changed = true
					fmt.Println("  [DEBUG] RuleExecutor: 子节�?, i, "已更�?)
				}
			}
		}

		if !changed {
			fmt.Println("  [DEBUG] RuleExecutor: 没有变化，退�?)
			break
		}

		iterations++
	}

	fmt.Println("  [DEBUG] RuleExecutor: 执行完成, 总迭代次�?", iterations)
	return current, nil
}
