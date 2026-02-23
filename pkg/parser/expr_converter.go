package parser

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ExprConverter AST 表达式转换器
// 用于将 TiDB AST 表达式转换为内部表达式格式
type ExprConverter struct{}

// NewExprConverter 创建表达式转换器
func NewExprConverter() *ExprConverter {
	return &ExprConverter{}
}

// Convert 转换 AST 表达式到内部表达式
// 第二阶段基础版本：保留字符串表达式，扩展复杂表达式的解析能力
func (c *ExprConverter) Convert(astExpr ast.ExprNode) (string, error) {
	if astExpr == nil {
		return "", fmt.Errorf("expression is nil")
	}

	// 简化实现：直接返回表达式的文本表示
	// 为将来的 TiDB Expression 集成预留扩展点
	return c.convertToString(astExpr), nil
}

// ConvertColumnRef 转换列引用表达式
func (c *ExprConverter) ConvertColumnRef(colRef *ast.ColumnNameExpr) (string, error) {
	if colRef == nil || colRef.Name == nil {
		return "", fmt.Errorf("column reference is nil")
	}
	return colRef.Name.Name.String(), nil
}

// ConvertFunctionCall 转换函数调用表达式
func (c *ExprConverter) ConvertFunctionCall(funcCall *ast.FuncCallExpr) (string, error) {
	if funcCall == nil {
		return "", fmt.Errorf("function call is nil")
	}
	return funcCall.FnName.String(), nil
}

// ConvertBinaryOp 转换二元运算表达式
func (c *ExprConverter) ConvertBinaryOp(op *ast.BinaryOperationExpr) (string, error) {
	if op == nil {
		return "", fmt.Errorf("binary operation is nil")
	}
	// 返回运算符的字符串表示
	return op.Op.String(), nil
}

// convertToString 将 AST 表达式转换为字符串
func (c *ExprConverter) convertToString(expr ast.ExprNode) string {
	// 直接使用 Text() 方法获取字符串表示
	// 这个方法已经在第一阶段中使用
	return expr.Text()
}

// ValidateExpression 验证表达式语法
func (c *ExprConverter) ValidateExpression(expr ast.ExprNode) error {
	if expr == nil {
		return fmt.Errorf("expression cannot be nil")
	}

	// 基础验证：检查是否为有效的表达式节点
	switch expr.(type) {
	case *ast.BinaryOperationExpr:
		return nil
	case *ast.UnaryOperationExpr:
		return nil
	case *ast.FuncCallExpr:
		return nil
	case *ast.ColumnNameExpr:
		return nil
	case ast.ValueExpr:
		// 有效的表达式类型
		return nil
	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// ExtractDependencies 从表达式中提取依赖的列名
// 这个方法已经在 adapter.go 中实现，这里提供统一的接口
func (c *ExprConverter) ExtractDependencies(expr ast.ExprNode) []string {
	deps := make([]string, 0)

	var traverse func(ast.ExprNode)
	traverse = func(node ast.ExprNode) {
		if node == nil {
			return
		}

		switch n := node.(type) {
		case *ast.ColumnNameExpr:
			if n.Name != nil {
				deps = append(deps, n.Name.Name.String())
			}
		case *ast.BinaryOperationExpr:
			traverse(n.L)
			traverse(n.R)
		case *ast.UnaryOperationExpr:
			traverse(n.V)
		case *ast.FuncCallExpr:
			for _, arg := range n.Args {
				traverse(arg)
			}
		case *ast.ParenthesesExpr:
			traverse(n.Expr)
		case *ast.PatternLikeOrIlikeExpr:
			traverse(n.Expr)
			traverse(n.Pattern)
		case *ast.BetweenExpr:
			traverse(n.Expr)
			traverse(n.Left)
			traverse(n.Right)
		case *ast.CaseExpr:
			for _, when := range n.WhenClauses {
				traverse(when.Expr)
				traverse(when.Result)
			}
			traverse(n.ElseClause)
		}
	}

	traverse(expr)

	// 去重
	uniqueDeps := make(map[string]bool)
	result := make([]string, 0)
	for _, dep := range deps {
		if !uniqueDeps[dep] {
			uniqueDeps[dep] = true
			result = append(result, dep)
		}
	}

	return result
}
