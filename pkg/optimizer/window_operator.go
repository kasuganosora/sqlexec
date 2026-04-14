package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// WindowOperator 窗口函数执行算子
type WindowOperator struct {
	// 子算子
	child PhysicalPlan

	// 窗口函数定义
	windowFuncs []*WindowFunctionDef

	// 表达式求值器
	evaluator *ExpressionEvaluator
}

// WindowFunctionDef 窗口函数定义
type WindowFunctionDef struct {
	Expr      *parser.WindowExpression
	ResultCol string // 结果列名
}

// NewWindowOperator 创建窗口函数算子
func NewWindowOperator(child PhysicalPlan, windowFuncs []*parser.WindowExpression) *WindowOperator {
	funcDefs := make([]*WindowFunctionDef, len(windowFuncs))
	for i, wf := range windowFuncs {
		funcDefs[i] = &WindowFunctionDef{
			Expr:      wf,
			ResultCol: fmt.Sprintf("window_%d", i),
		}
	}

	return &WindowOperator{
		child:       child,
		windowFuncs: funcDefs,
		evaluator:   NewExpressionEvaluatorWithoutAPI(),
	}
}

// Execute 执行窗口函数
// DEPRECATED: 执行逻辑已迁移到 pkg/executor 包，此方法保留仅为兼容性
func (op *WindowOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("WindowOperator.Execute is deprecated. Please use pkg/executor instead")
}
