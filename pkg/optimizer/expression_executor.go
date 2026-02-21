package optimizer

import (
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ExpressionExecutor 表达式执行器
// 负责处理无 FROM 子句查询中的表达式求值
type ExpressionExecutor struct {
	currentDB     string
	functionAPI   interface{} // 避免循环依赖，实际类型为 *builtin.FunctionAPI
	exprEvaluator *ExpressionEvaluator
	sessionVars   map[string]string // 会话级系统变量覆盖
}

// NewExpressionExecutor 创建表达式执行器
func NewExpressionExecutor(currentDB string, functionAPI interface{}, exprEvaluator *ExpressionEvaluator) *ExpressionExecutor {
	return &ExpressionExecutor{
		currentDB:     currentDB,
		functionAPI:   functionAPI,
		exprEvaluator: exprEvaluator,
	}
}

// SetSessionVars sets session-level variable overrides for SELECT @@variable queries
func (e *ExpressionExecutor) SetSessionVars(vars map[string]string) {
	e.sessionVars = vars
}

// SetCurrentDB 设置当前数据库
func (e *ExpressionExecutor) SetCurrentDB(dbName string) {
	e.currentDB = dbName
}

// HandleNoFromQuery 处理没有 FROM 子句的查询（如 SELECT DATABASE(), SELECT NOW()）
func (e *ExpressionExecutor) HandleNoFromQuery(stmt *parser.SelectStatement) (*Result, error) {
	debugln("  [DEBUG] handleNoFromQuery: 开始处理")
	debugf("  [DEBUG] handleNoFromQuery: currentDB = %q\n", e.currentDB)

	// 构建空 row（用于表达式求值）
	row := make(parser.Row)

	// 处理多个列
	columns := make([]domain.ColumnInfo, 0, len(stmt.Columns))
	rowData := make(map[string]interface{})
	colIdx := 0

	for _, col := range stmt.Columns {
		if col.Expr == nil {
			return nil, fmt.Errorf("column expression is nil")
		}

		// 确定列名
		colName := col.Alias
		if colName == "" {
			colName = col.Name
		}
		if colName == "" {
			// 如果没有别名和名称，根据表达式生成列名
			colName = e.generateColumnName(col.Expr)
		}

		// 特殊处理函数调用：如果有名称但没有括号，添加括号
		if col.Expr.Type == parser.ExprTypeFunction && colName != "" && !strings.HasSuffix(colName, "()") {
			colName = colName + "()"
		}

		// 特殊处理：如果生成的列名是 NULL，尝试从表达式中提取更多信息
		if colName == "NULL" {
			// 对于无法确定列名的情况，使用默认名称
			colName = fmt.Sprintf("expr_%d", colIdx)
		}

		debugf("  [DEBUG] handleNoFromQuery: 处理列 %s, 表达式类型=%s\n", colName, col.Expr.Type)

		// 计算表达式值
		value, err := e.evaluateNoFromExpression(col.Expr, row)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression for column %s: %w", colName, err)
		}

		// 确定列类型
		colType := e.inferType(value)

		columns = append(columns, domain.ColumnInfo{Name: colName, Type: colType})
		rowData[colName] = value
		colIdx++
	}

	return &Result{
		Columns: columns,
		Rows:    []map[string]interface{}{rowData},
		Total:   1,
	}, nil
}

// Result 查询结果（简化版，避免循环依赖）
type Result struct {
	Columns []domain.ColumnInfo
	Rows    []map[string]interface{}
	Total   int64
}

// generateColumnName 根据表达式生成列名
func (e *ExpressionExecutor) generateColumnName(expr *parser.Expression) string {
	// 处理 nil 表达式
	if expr == nil {
		return ""
	}

	// 特殊处理：如果是系统变量，使用 Column 字段
	if expr.Column != "" && (strings.HasPrefix(expr.Column, "@@") || strings.HasPrefix(expr.Column, "@")) {
		return expr.Column
	}

	switch expr.Type {
	case parser.ExprTypeValue:
		// 常量值：使用值的字符串表示
		if expr.Value != nil {
			return fmt.Sprintf("%v", expr.Value)
		}
		// Value 为 nil 的情况，可能是系统变量或 NULL
		return "NULL"

	case parser.ExprTypeFunction:
		// 函数调用：使用函数名()
		if expr.Function != "" {
			return expr.Function + "()"
		}
		return "function"

	case parser.ExprTypeOperator:
		// 运算符表达式：递归生成操作数和运算符
		return e.generateOperatorColumnName(expr)

	case parser.ExprTypeColumn:
		// 列引用：使用列名
		if expr.Column != "" {
			return expr.Column
		}
		return "column"

	default:
		return "expr"
	}
}

// generateOperatorColumnName 为运算符表达式生成列名
func (e *ExpressionExecutor) generateOperatorColumnName(expr *parser.Expression) string {
	if expr.Operator == "" {
		return "expr"
	}

	// 将解析器的运算符名称转换为SQL符号
	opSymbol := e.operatorToSQL(expr.Operator)

	if expr.Left != nil {
		leftName := e.generateColumnName(expr.Left)
		if expr.Right != nil {
			rightName := e.generateColumnName(expr.Right)
			// 二元运算符
			return leftName + opSymbol + rightName
		}
		// 一元运算符
		return opSymbol + leftName
	}

	if expr.Right != nil {
		rightName := e.generateColumnName(expr.Right)
		return opSymbol + rightName
	}

	return "expr"
}

// operatorToSQL 将解析器的运算符名称转换为SQL符号
func (e *ExpressionExecutor) operatorToSQL(op string) string {
	switch strings.ToLower(op) {
	case "plus":
		return "+"
	case "minus":
		return "-"
	case "mul":
		return "*"
	case "div":
		return "/"
	case "eq":
		return "="
	case "neq":
		return "!="
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	case "and":
		return " AND "
	case "or":
		return " OR "
	case "not":
		return "NOT "
	case "like":
		return " LIKE "
	default:
		return op
	}
}

// evaluateNoFromExpression 评估无 FROM 子句的表达式
func (e *ExpressionExecutor) evaluateNoFromExpression(expr *parser.Expression, row parser.Row) (interface{}, error) {
	// 特殊处理：如果 Column 字段包含系统变量（即使 Type 不是 COLUMN）
	if expr.Column != "" && (strings.HasPrefix(expr.Column, "@@") || strings.HasPrefix(expr.Column, "@")) {
		return e.evaluateVariable(expr.Column)
	}

	switch expr.Type {
	case parser.ExprTypeValue:
		// 常量值（包括 NULL 字面量）
		debugf("  [DEBUG] evaluateNoFromExpression: 常量值=%v\n", expr.Value)
		return expr.Value, nil

	case parser.ExprTypeColumn:
		// 变量引用（系统变量、会话变量）
		if expr.Column != "" {
			return e.evaluateVariable(expr.Column)
		}
		return nil, fmt.Errorf("column reference without column name")

	case parser.ExprTypeFunction:
		// 函数调用
		return e.evaluateFunctionExpression(expr, row)

	case parser.ExprTypeOperator:
		// 运算符表达式
		ctx := NewSimpleExpressionContext(row)
		return e.exprEvaluator.Evaluate(expr, ctx)

	default:
		return nil, fmt.Errorf("unsupported expression type: %s", expr.Type)
	}
}

// evaluateVariable 评估变量（系统变量或会话变量）
func (e *ExpressionExecutor) evaluateVariable(colName string) (interface{}, error) {
	varName := strings.ToUpper(strings.TrimSpace(colName))

	debugf("  [DEBUG] evaluateVariable: 变量名=%s\n", varName)

	// 处理系统变量（@@variable）
	if strings.HasPrefix(varName, "@@") {
		return e.evaluateSystemVariable(varName)
	}

	// 处理会话变量（@variable）
	if strings.HasPrefix(varName, "@") && !strings.HasPrefix(varName, "@@") {
		return e.evaluateSessionVariable(varName)
	}

	return nil, fmt.Errorf("unsupported variable: %s", colName)
}

// evaluateSystemVariable 评估系统变量
func (e *ExpressionExecutor) evaluateSystemVariable(varName string) (interface{}, error) {
	// 移除 @@ 前缀
	name := strings.TrimPrefix(varName, "@@")

	// 移除作用域前缀（@@global., @@session., @@local.）
	name = strings.TrimPrefix(name, "GLOBAL.")
	name = strings.TrimPrefix(name, "SESSION.")
	name = strings.TrimPrefix(name, "LOCAL.")

	debugf("  [DEBUG] evaluateSystemVariable: 系统变量=%s\n", name)

	nameLower := strings.ToLower(name)

	// Check session-level overrides first (from SET statements)
	if e.sessionVars != nil {
		if val, ok := e.sessionVars[nameLower]; ok {
			return val, nil
		}
	}

	// Fall back to shared system variable definitions (single source of truth)
	for _, v := range information_schema.GetSystemVariableDefs() {
		if v.Name == nameLower {
			return v.Value, nil
		}
	}

	return nil, fmt.Errorf("unknown system variable: %s", name)
}

// evaluateSessionVariable 评估会话变量
func (e *ExpressionExecutor) evaluateSessionVariable(varName string) (interface{}, error) {
	// 移除 @ 前缀
	name := strings.TrimPrefix(varName, "@")

	debugf("  [DEBUG] evaluateSessionVariable: 会话变量=%s\n", name)

	// 当前实现中，我们无法访问 session 对象
	// 这是一个限制，需要在未来改进架构
	return nil, fmt.Errorf("session variables not yet supported in no-FROM queries: %s", name)
}

// evaluateFunctionExpression 评估函数表达式
func (e *ExpressionExecutor) evaluateFunctionExpression(expr *parser.Expression, row parser.Row) (interface{}, error) {
	funcName := strings.ToUpper(expr.Function)

	debugf("  [DEBUG] evaluateFunctionExpression: 函数名=%s\n", funcName)

	// 特殊处理 DATABASE() 函数（因为它需要当前数据库上下文）
	if funcName == "DATABASE" {
		return e.currentDB, nil
	}

	// 对于其他函数，使用 ExpressionEvaluator
	ctx := NewSimpleExpressionContext(row)
	return e.exprEvaluator.Evaluate(expr, ctx)
}

// inferType 推断值的类型
func (e *ExpressionExecutor) inferType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32, int64, uint64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "bool"
	case string:
		return "string"
	case time.Time:
		return "datetime"
	default:
		return "string"
	}
}
