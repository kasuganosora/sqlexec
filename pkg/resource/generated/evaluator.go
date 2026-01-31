package generated

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// GeneratedColumnEvaluator 生成列求值器
type GeneratedColumnEvaluator struct {
	functionAPI *builtin.FunctionAPI
	exprCache   *ExpressionCache
}

// NewGeneratedColumnEvaluator 创建生成列求值器
func NewGeneratedColumnEvaluator() *GeneratedColumnEvaluator {
	return &GeneratedColumnEvaluator{
		functionAPI: builtin.NewFunctionAPI(),
		exprCache:   NewExpressionCache(),
	}
}

// NewGeneratedColumnEvaluatorWithCache 使用指定缓存创建求值器
func NewGeneratedColumnEvaluatorWithCache(cache *ExpressionCache) *GeneratedColumnEvaluator {
	return &GeneratedColumnEvaluator{
		functionAPI: builtin.NewFunctionAPI(),
		exprCache:   cache,
	}
}

// Evaluate 评估单个生成列表达式
func (e *GeneratedColumnEvaluator) Evaluate(
	expr string,
	row domain.Row,
	schema *domain.TableInfo,
) (interface{}, error) {
	result, err := e.evaluateExpression(expr, row)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// EvaluateAll 递归计算所有生成列
func (e *GeneratedColumnEvaluator) EvaluateAll(
	row domain.Row,
	schema *domain.TableInfo,
) (domain.Row, error) {
	order, err := e.GetEvaluationOrder(schema)
	if err != nil {
		return nil, err
	}

	result := make(domain.Row)
	for k, v := range row {
		result[k] = v
	}

	for _, colName := range order {
		colInfo := e.getColumnInfo(colName, schema)
		if colInfo == nil || !colInfo.IsGenerated {
			continue
		}

		if colInfo.GeneratedType == "VIRTUAL" {
			continue
		}

		val, evalErr := e.Evaluate(colInfo.GeneratedExpr, result, schema)
		if evalErr != nil {
			result[colName] = nil
			continue
		}

		castedVal, castErr := CastToType(val, colInfo.Type)
		if castErr != nil {
			result[colName] = nil
			continue
		} else {
			result[colName] = castedVal
		}
	}

	return result, nil
}

// GetEvaluationOrder 获取生成列计算顺序（拓扑排序）
func (e *GeneratedColumnEvaluator) GetEvaluationOrder(
	schema *domain.TableInfo,
) ([]string, error) {
	validator := &GeneratedColumnValidator{}
	graph := validator.BuildDependencyGraph(schema)

	inDegree := make(map[string]int)
	visited := make(map[string]bool)
	order := make([]string, 0)

	for _, col := range schema.Columns {
		colName := col.Name
		inDegree[colName] = 0
	}

	for from, deps := range graph {
		inDegree[from] = len(deps)
	}

	for {
		found := false
		for colName := range inDegree {
			if inDegree[colName] == 0 && !visited[colName] {
				visited[colName] = true
				order = append(order, colName)
				found = true

				for other, deps := range graph {
					for _, dep := range deps {
						if dep == colName {
							inDegree[other]--
						}
					}
				}
			}
		}

		if !found {
			break
		}
	}

	if len(order) != len(schema.Columns) {
		return nil, fmt.Errorf("circular dependency detected in generated columns")
	}

	return order, nil
}

// GetExpressionCache 获取表达式缓存
func (e *GeneratedColumnEvaluator) GetExpressionCache() *ExpressionCache {
	return e.exprCache
}

// GetFunctionAPI 获取函数API
func (e *GeneratedColumnEvaluator) GetFunctionAPI() *builtin.FunctionAPI {
	return e.functionAPI
}

// GetColumnInfo 获取列信息
func (e *GeneratedColumnEvaluator) getColumnInfo(colName string, schema *domain.TableInfo) *domain.ColumnInfo {
	for _, col := range schema.Columns {
		if col.Name == colName {
			return &col
		}
	}
	return nil
}

// evaluateExpression 表达式求值（主入口）
func (e *GeneratedColumnEvaluator) evaluateExpression(expr string, row domain.Row) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}

	// 1. 处理完整的括号表达式（以(开头以)结尾）
	if expr[0] == '(' {
		parenDepth := 0
		matchIndex := -1
		for i, ch := range expr {
			if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
				if parenDepth == 0 {
					matchIndex = i
					break
				}
			}
		}
		
		if matchIndex == len(expr)-1 {
			return e.evaluateParentheses(expr, row)
		}
	}

	// 2. 处理比较运算（优先级最低）
	if idx := e.findOperator(expr, "="); idx >= 0 && !strings.Contains(expr, "!=") &&
		!strings.Contains(expr, "<=") && !strings.Contains(expr, ">=") {
		return e.evaluateComparison(expr, "=", idx, row)
	}
	if idx := e.findOperator(expr, "!="); idx >= 0 {
		return e.evaluateComparison(expr, "!=", idx, row)
	}
	if idx := e.findOperator(expr, "<="); idx >= 0 {
		return e.evaluateComparison(expr, "<=", idx, row)
	}
	if idx := e.findOperator(expr, ">="); idx >= 0 {
		return e.evaluateComparison(expr, ">=", idx, row)
	}
	if idx := e.findOperator(expr, "<"); idx >= 0 {
		return e.evaluateComparison(expr, "<", idx, row)
	}
	if idx := e.findOperator(expr, ">"); idx >= 0 {
		return e.evaluateComparison(expr, ">", idx, row)
	}

	// 3. 处理加减运算（优先级低于乘除）
	// 找到最右边的加减运算符
	if idx := e.findOperator(expr, "+-"); idx >= 0 {
		op := string(expr[idx])
		left := strings.TrimSpace(expr[:idx])
		right := strings.TrimSpace(expr[idx+1:])

		leftVal, err := e.evaluateExpression(left, row)
		if err != nil {
			return nil, err
		}
		rightVal, err := e.evaluateExpression(right, row)
		if err != nil {
			return nil, err
		}

		return e.performBinaryOp(leftVal, rightVal, op)
	}

	// 4. 处理乘除模运算（优先级高）
	if idx := e.findOperator(expr, "*/%"); idx >= 0 {
		op := string(expr[idx])
		left := strings.TrimSpace(expr[:idx])
		right := strings.TrimSpace(expr[idx+1:])

		leftVal, err := e.evaluateExpression(left, row)
		if err != nil {
			return nil, err
		}
		rightVal, err := e.evaluateExpression(right, row)
		if err != nil {
			return nil, err
		}

		return e.performBinaryOp(leftVal, rightVal, op)
	}

	// 5. 处理函数调用
	if idx := strings.Index(expr, "("); idx > 0 {
		return e.evaluateFunctionCall(expr, row)
	}

	// 6. 尝试解析为数字字面量
	if val, err := strconv.ParseFloat(expr, 64); err == nil {
		return val, nil
	}

	// 7. 尝试解析为布尔字面量
	if expr == "true" || expr == "TRUE" {
		return true, nil
	}
	if expr == "false" || expr == "FALSE" {
		return false, nil
	}

	// 8. 尝试作为列引用获取
	if val, ok := row[expr]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("unsupported expression: %s", expr)
}

// evaluateParentheses 求值括号表达式
func (e *GeneratedColumnEvaluator) evaluateParentheses(expr string, row domain.Row) (interface{}, error) {
	if !e.isBalancedParentheses(expr) {
		return nil, fmt.Errorf("unbalanced parentheses: %s", expr)
	}

	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	return e.evaluateExpression(inner, row)
}

// evaluateComparison 求值比较表达式
func (e *GeneratedColumnEvaluator) evaluateComparison(expr string, op string, opIdx int, row domain.Row) (interface{}, error) {
	var left, right string

	if op == "!=" || op == "<=" || op == ">=" {
		left = strings.TrimSpace(expr[:opIdx])
		right = strings.TrimSpace(expr[opIdx+2:])
	} else {
		left = strings.TrimSpace(expr[:opIdx])
		right = strings.TrimSpace(expr[opIdx+1:])
	}

	leftVal, err := e.evaluateExpression(left, row)
	if err != nil {
		return nil, err
	}
	rightVal, err := e.evaluateExpression(right, row)
	if err != nil {
		return nil, err
	}

	return e.performBinaryOp(leftVal, rightVal, op)
}

// evaluateFunctionCall 求值函数调用
func (e *GeneratedColumnEvaluator) evaluateFunctionCall(expr string, row domain.Row) (interface{}, error) {
	idx := strings.Index(expr, "(")
	if idx <= 0 {
		return nil, fmt.Errorf("invalid function call: %s", expr)
	}

	funcName := strings.TrimSpace(expr[:idx])
	paramsStr := strings.TrimSpace(expr[idx+1 : len(expr)-1])

	var params []interface{}
	if paramsStr != "" {
		paramExprs := e.splitByComma(paramsStr)
		for _, paramExpr := range paramExprs {
			val, err := e.evaluateExpression(paramExpr, row)
			if err != nil {
				return nil, err
			}
			params = append(params, val)
		}
	}

	function, err := e.functionAPI.GetFunction(funcName)
	if err != nil {
		return nil, fmt.Errorf("function %s error: %w", funcName, err)
	}

	if function.Handler != nil {
		return function.Handler(params)
	} else if function.AggregateHandler != nil {
		return nil, fmt.Errorf("aggregate function %s cannot be used in scalar context", funcName)
	}

	return nil, fmt.Errorf("function %s has no handler", funcName)
}

// performBinaryOp 执行二元运算
func (e *GeneratedColumnEvaluator) performBinaryOp(left, right interface{}, op string) (interface{}, error) {
	if left == nil || right == nil {
		switch op {
		case "=", "!=", "<", "<=", ">", ">=":
			return nil, nil
		default:
			return nil, nil
		}
	}

	leftVal := e.toFloat64(left)
	rightVal := e.toFloat64(right)

	switch op {
	case "+":
		return leftVal + rightVal, nil
	case "-":
		return leftVal - rightVal, nil
	case "*":
		return leftVal * rightVal, nil
	case "/":
		if rightVal == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return leftVal / rightVal, nil
	case "%":
		if rightVal == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return float64(int(leftVal) % int(rightVal)), nil
	case "=":
		return leftVal == rightVal, nil
	case "!=":
		return leftVal != rightVal, nil
	case "<":
		return leftVal < rightVal, nil
	case "<=":
		return leftVal <= rightVal, nil
	case ">":
		return leftVal > rightVal, nil
	case ">=":
		return leftVal >= rightVal, nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", op)
	}
}

// toFloat64 将值转换为float64
func (e *GeneratedColumnEvaluator) toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return 0.0
	default:
		if fv, ok := val.(float64); ok {
			return fv
		}
		return 0.0
	}
}

// findOperator 查找不在括号内的运算符（从左到右）
func (e *GeneratedColumnEvaluator) findOperator(expr, operators string) int {
	parenDepth := 0

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '(' {
			parenDepth++
		} else if ch == ')' {
			parenDepth--
		} else if parenDepth == 0 && strings.ContainsRune(operators, rune(ch)) {
			return i
		}
	}

	return -1
}

// isBalancedParentheses 检查括号是否平衡
func (e *GeneratedColumnEvaluator) isBalancedParentheses(expr string) bool {
	count := 0
	for _, ch := range expr {
		if ch == '(' {
			count++
		} else if ch == ')' {
			count--
			if count < 0 {
				return false
			}
		}
	}
	return count == 0
}

// splitByComma 按逗号分割字符串（忽略括号内的逗号）
func (e *GeneratedColumnEvaluator) splitByComma(expr string) []string {
	result := []string{}
	current := ""
	parenDepth := 0

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '(' {
			parenDepth++
			current += string(ch)
		} else if ch == ')' {
			parenDepth--
			current += string(ch)
		} else if ch == ',' && parenDepth == 0 {
			result = append(result, strings.TrimSpace(current))
			current = ""
		} else {
			current += string(ch)
		}
	}

	if current != "" {
		result = append(result, strings.TrimSpace(current))
	}

	return result
}
