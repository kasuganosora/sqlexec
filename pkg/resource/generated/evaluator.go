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
// 第二阶段支持：完整的 SQL 表达式，包括 CASE WHEN、子查询、复杂函数等
// 第一阶段MVP支持：算术、比较、逻辑运算和基本函数
func (e *GeneratedColumnEvaluator) Evaluate(
	expr string,
	row domain.Row,
	schema *domain.TableInfo,
) (interface{}, error) {
	// 第二阶段：完整表达式支持（使用字符串解析，待集成 TiDB Expression）
	// 当前保持第一阶段逻辑，后续可替换为 TiDB Expression 包
	// 支持的表达式：
	// 1. 算术运算：+, -, *, /, %
	// 2. 比较运算：=, !=, >, <, >=, <=
	// 3. 逻辑运算：AND, OR, NOT
	// 4. 字符串操作：CONCAT
	// 5. 基本函数：UPPER, LOWER, SUBSTRING, TRIM
	// 6. 内置函数（通过 builtin.FunctionAPI）
	// 7. 用户自定义函数（UDF）

	// 表达式解析并计算
	result, err := e.evaluateSimpleExpression(expr, row)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// EvaluateAll 递归计算所有生成列（级联更新）
func (e *GeneratedColumnEvaluator) EvaluateAll(
	row domain.Row,
	schema *domain.TableInfo,
) (domain.Row, error) {
	// 获取生成列计算顺序（拓扑排序）
	order, err := e.GetEvaluationOrder(schema)
	if err != nil {
		// 计算失败时返回错误
		return nil, err
	}

	// 按顺序计算生成列
	result := make(domain.Row)
	for k, v := range row {
		result[k] = v
	}

	for _, colName := range order {
		colInfo := e.getColumnInfo(colName, schema)
		if colInfo == nil || !colInfo.IsGenerated {
			continue
		}

		val, evalErr := e.Evaluate(colInfo.GeneratedExpr, result, schema)
		if evalErr != nil {
			// 计算失败时设为 NULL，继续下一个
			result[colName] = nil
			continue
		}

		// 类型转换
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

	// 拓扑排序
	inDegree := make(map[string]int)
	visited := make(map[string]bool)
	order := make([]string, 0)

	// 初始化入度
	for _, col := range schema.Columns {
		colName := col.Name
		inDegree[colName] = 0
	}

	// 计算入度
	for from, deps := range graph {
		inDegree[from] = len(deps)
	}

	// Kahn 算法进行拓扑排序
	queue := make([]string, 0)
	for _, col := range schema.Columns {
		colName := col.Name
		if inDegree[colName] == 0 {
			queue = append(queue, colName)
			visited[colName] = true
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		// 只添加生成列到结果中
		colInfo := e.getColumnInfo(node, schema)
		if colInfo != nil && colInfo.IsGenerated {
			order = append(order, node)
		}

		// 减少依赖此节点的其他节点的入度
		for otherName, deps := range graph {
			for i, dep := range deps {
				if dep == node {
					deps = append(deps[:i], deps[i+1:]...)
					graph[otherName] = deps
					inDegree[otherName]--
					break
				}
			}
		}

		// 将入度为0的节点加入队列
		for _, col := range schema.Columns {
			colName := col.Name
			if !visited[colName] && inDegree[colName] == 0 {
				queue = append(queue, colName)
				visited[colName] = true
			}
		}
	}

	// 检查是否存在循环依赖
	if len(order) != countGeneratedColumns(schema) {
		return nil, fmt.Errorf("cyclic dependency detected in generated columns")
	}

	return order, nil
}

// evaluateSimpleExpression 评估简单表达式
// 简化实现，处理基本的算术和比较运算
func (e *GeneratedColumnEvaluator) evaluateSimpleExpression(expr string, row domain.Row) (interface{}, error) {
	// 移除空格
	expr = strings.TrimSpace(expr)

	// 处理简单的列引用
	if val, ok := row[expr]; ok {
		return val, nil
	}

	// 尝试解析为数字字面量
	if isNumericLiteral(expr) {
		return parseNumericLiteral(expr)
	}

	// 尝试解析为布尔字面量
	if expr == "true" || expr == "TRUE" {
		return true, nil
	}
	if expr == "false" || expr == "FALSE" {
		return false, nil
	}

	// 处理比较运算（优先级高，先处理）
	if strings.Contains(expr, "=") && !strings.Contains(expr, "!=") && !strings.Contains(expr, "<=") && !strings.Contains(expr, ">=") {
		return e.evaluateBinaryOp(expr, row, "=")
	}
	if strings.Contains(expr, "!=") {
		return e.evaluateBinaryOp(expr, row, "!=")
	}
	if strings.Contains(expr, "<=") {
		return e.evaluateBinaryOp(expr, row, "<=")
	}
	if strings.Contains(expr, ">=") {
		return e.evaluateBinaryOp(expr, row, ">=")
	}
	if strings.Contains(expr, "<") && !strings.Contains(expr, "<=") {
		return e.evaluateBinaryOp(expr, row, "<")
	}
	if strings.Contains(expr, ">") && !strings.Contains(expr, ">=") {
		return e.evaluateBinaryOp(expr, row, ">")
	}

	// 处理乘除模运算（优先级高于加减）
	if strings.Contains(expr, "/") {
		return e.evaluateBinaryOp(expr, row, "/")
	}
	if strings.Contains(expr, "%") {
		return e.evaluateBinaryOp(expr, row, "%")
	}
	if strings.Contains(expr, "*") {
		return e.evaluateBinaryOp(expr, row, "*")
	}

	// 处理加减运算（优先级较低）
	if strings.Contains(expr, "+") {
		return e.evaluateBinaryOp(expr, row, "+")
	}
	if strings.Contains(expr, "-") {
		return e.evaluateBinaryOp(expr, row, "-")
	}

	// 处理函数调用
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return e.evaluateFunctionCall(expr, row)
	}

	// 如果包含冒号且不是函数调用，可能是列引用（如 price:10），直接尝试获取
	if strings.Contains(expr, ":") && !strings.Contains(expr, "(") {
		if val, ok := row[expr]; ok {
			return val, nil
		}
	}

	return nil, fmt.Errorf("unsupported expression: %s", expr)
}

// isNumericLiteral 检查是否为数字字面量
func isNumericLiteral(expr string) bool {
	expr = strings.TrimSpace(expr)
	if len(expr) == 0 {
		return false
	}
	// 检查是否为整数或浮点数格式
	for i, c := range expr {
		if i == 0 && c == '-' {
			continue // 允许负号开头
		}
		if (c < '0' || c > '9') && c != '.' {
			return false
		}
	}
	return true
}

// parseNumericLiteral 解析数字字面量
func parseNumericLiteral(expr string) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	// 尝试解析为浮点数
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, nil
	}
	// 尝试解析为整数
	if i, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return int64(i), nil
	}
	// 如果失败，返回错误
	return nil, fmt.Errorf("invalid numeric literal: %s", expr)
}

// evaluateBinaryOp 评估二元运算
func (e *GeneratedColumnEvaluator) evaluateBinaryOp(expr string, row domain.Row, op string) (interface{}, error) {
	// 简化实现：仅处理最简单的形式
	// 实际应该使用完整的表达式解析器

	// 分割表达式
	parts := e.splitByOperator(expr, op)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid binary operation: %s", expr)
	}

	// 评估左操作数
	left, err := e.evaluateSimpleExpression(parts[0], row)
	if err != nil {
		return nil, err
	}

	// 评估右操作数
	right, err := e.evaluateSimpleExpression(parts[1], row)
	if err != nil {
		return nil, err
	}

	// 执行运算
	return e.performBinaryOp(left, right, op)
}

// evaluateFunctionCall 评估函数调用
func (e *GeneratedColumnEvaluator) evaluateFunctionCall(expr string, row domain.Row) (interface{}, error) {
	// 提取函数名和参数
	parts := strings.SplitN(expr, "(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid function call: %s", expr)
	}

	funcName := strings.TrimSpace(strings.ToUpper(parts[0]))
	argsStr := strings.TrimSuffix(parts[1], ")")

	// 解析参数
	args, err := e.parseArguments(argsStr, row)
	if err != nil {
		return nil, err
	}

	// 调用内置函数
	funcMeta, err := e.functionAPI.GetFunction(funcName)
	if err != nil {
		return nil, fmt.Errorf("function %s not found", funcName)
	}

	// 调用函数处理器
	if funcMeta.Handler != nil {
		return funcMeta.Handler(args)
	}

	return nil, fmt.Errorf("function %s has no handler", funcName)
}

// parseArguments 解析函数参数
func (e *GeneratedColumnEvaluator) parseArguments(argsStr string, row domain.Row) ([]interface{}, error) {
	argsStr = strings.TrimSpace(argsStr)
	if argsStr == "" {
		return []interface{}{}, nil
	}

	// 简化实现：按逗号分割（不考虑嵌套括号）
	parts := strings.Split(argsStr, ",")
	args := make([]interface{}, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		val, err := e.evaluateSimpleExpression(part, row)
		if err != nil {
			return nil, err
		}
		args = append(args, val)
	}

	return args, nil
}

// performBinaryOp 执行二元运算
func (e *GeneratedColumnEvaluator) performBinaryOp(left, right interface{}, op string) (interface{}, error) {
	// NULL 传播：如果任一操作数为 NULL，结果为 NULL
	if left == nil || right == nil {
		return nil, nil
	}

	// 类型转换
	leftFloat, leftOk := toFloat64(left)
	rightFloat, rightOk := toFloat64(right)

	// 算术运算
	if leftOk && rightOk {
		switch op {
		case "+":
			return leftFloat + rightFloat, nil
		case "-":
			return leftFloat - rightFloat, nil
		case "*":
			return leftFloat * rightFloat, nil
		case "/":
			if rightFloat == 0 {
				return nil, nil // 除零返回NULL
			}
			return leftFloat / rightFloat, nil
		case "%":
			if rightFloat == 0 {
				return nil, nil // 模零返回NULL
			}
			return float64(int64(leftFloat) % int64(rightFloat)), nil
		}
	}

	// 比较运算
	switch op {
	case "=":
		return compareValues(left, right) == 0, nil
	case "!=":
		return compareValues(left, right) != 0, nil
	case "<":
		return compareValues(left, right) < 0, nil
	case "<=":
		return compareValues(left, right) <= 0, nil
	case ">":
		return compareValues(left, right) > 0, nil
	case ">=":
		return compareValues(left, right) >= 0, nil
	}

	return nil, fmt.Errorf("unsupported operator: %s", op)
}

// splitByOperator 按运算符分割表达式
func (e *GeneratedColumnEvaluator) splitByOperator(expr, op string) []string {
	// 简化实现：按第一个出现的运算符分割
	// 避免重复匹配（例如：<= 被错误地分割为 < 和 =）
	opIndex := strings.Index(expr, op)
	if opIndex == -1 {
		return []string{expr}
	}

	left := strings.TrimSpace(expr[:opIndex])
	right := strings.TrimSpace(expr[opIndex+len(op):])

	return []string{left, right}
}

// getColumnInfo 获取列信息
func (e *GeneratedColumnEvaluator) getColumnInfo(name string, schema *domain.TableInfo) *domain.ColumnInfo {
	for i, col := range schema.Columns {
		if col.Name == name {
			return &schema.Columns[i]
		}
	}
	return nil
}

// toFloat64 转换为 float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// compareValues 比较两个值
func compareValues(left, right interface{}) int {
	leftFloat, leftOk := toFloat64(left)
	rightFloat, rightOk := toFloat64(right)

	if leftOk && rightOk {
		if leftFloat < rightFloat {
			return -1
		} else if leftFloat > rightFloat {
			return 1
		}
		return 0
	}

	// 字符串比较
	leftStr, leftOkStr := left.(string)
	rightStr, rightOkStr := right.(string)

	if leftOkStr && rightOkStr {
		if leftStr < rightStr {
			return -1
		} else if leftStr > rightStr {
			return 1
		}
		return 0
	}

	// 处理 nil
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	return 0
}
