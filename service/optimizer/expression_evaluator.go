package optimizer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/service/builtin"
	"github.com/kasuganosora/sqlexec/service/parser"
)

// ExpressionEvaluator 表达式求值器
type ExpressionEvaluator struct {
	functionAPI *builtin.FunctionAPI
}

// NewExpressionEvaluator 创建表达式求值器
func NewExpressionEvaluator(fnAPI *builtin.FunctionAPI) *ExpressionEvaluator {
	return &ExpressionEvaluator{
		functionAPI: fnAPI,
	}
}

// NewExpressionEvaluatorWithoutAPI 创建不依赖函数API的表达式求值器
// 用于不需要调用函数的场景（如常量折叠�?
func NewExpressionEvaluatorWithoutAPI() *ExpressionEvaluator {
	return &ExpressionEvaluator{
		functionAPI: nil,
	}
}

// Evaluate 计算表达式的�?
func (e *ExpressionEvaluator) Evaluate(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr.Type {
	case parser.ExprTypeColumn:
		// 列引�?
		if val, exists := row[expr.Column]; exists {
			return val, nil
		}
		return nil, fmt.Errorf("column not found: %s", expr.Column)

	case parser.ExprTypeValue:
		// 字面量�?
		return expr.Value, nil

	case parser.ExprTypeOperator:
		// 运算符表达式
		return e.evaluateOperator(expr, row)

	case parser.ExprTypeFunction:
		// 函数调用
		return e.evaluateFunction(expr, row)

	default:
		return nil, fmt.Errorf("unsupported expression type: %d", expr.Type)
	}
}

// evaluateOperator 计算运算符表达式
func (e *ExpressionEvaluator) evaluateOperator(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr.Operator == "" {
		return nil, fmt.Errorf("operator is empty")
	}

	// 处理逻辑运算�?
	if strings.EqualFold(expr.Operator, "and") || strings.EqualFold(expr.Operator, "or") {
		return e.evaluateLogicalOp(expr, row)
	}

	// 处理一元运算符
	if expr.Right == nil {
		return e.evaluateUnaryOp(expr, row)
	}

	// 处理二元运算�?
	if expr.Left == nil || expr.Right == nil {
		return nil, fmt.Errorf("invalid operator expression: missing operand")
	}

	left, err := e.Evaluate(expr.Left, row)
	if err != nil {
		return nil, fmt.Errorf("left operand evaluation failed: %w", err)
	}

	right, err := e.Evaluate(expr.Right, row)
	if err != nil {
		return nil, fmt.Errorf("right operand evaluation failed: %w", err)
	}

	// 根据运算符类型计�?
	switch strings.ToLower(expr.Operator) {
	case "=":
		return e.compareValues(left, right) == 0, nil
	case "!=", "<>":
		return e.compareValues(left, right) != 0, nil
	case ">":
		return e.compareValues(left, right) > 0, nil
	case ">=":
		return e.compareValues(left, right) >= 0, nil
	case "<":
		return e.compareValues(left, right) < 0, nil
	case "<=":
		return e.compareValues(left, right) <= 0, nil
	case "+":
		return e.addValues(left, right)
	case "-":
		return e.subValues(left, right)
	case "*":
		return e.mulValues(left, right)
	case "/":
		return e.divValues(left, right)
	case "like":
		return e.likeValues(left, right), nil
	case "not like":
		return !e.likeValues(left, right), nil
	case "in":
		return e.inValues(left, right), nil
	case "not in":
		return !e.inValues(left, right), nil
	case "between":
		if vals, ok := right.([]interface{}); ok && len(vals) == 2 {
			return e.betweenValues(left, vals[0], vals[1]), nil
		}
		return false, nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", expr.Operator)
	}
}

// evaluateLogicalOp 计算逻辑运算�?
func (e *ExpressionEvaluator) evaluateLogicalOp(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, fmt.Errorf("invalid logical operator expression")
	}

	left, err := e.Evaluate(expr.Left, row)
	if err != nil {
		return nil, err
	}

	// 短路求�?
	if strings.EqualFold(expr.Operator, "and") {
		if !e.isTrue(left) {
			return false, nil
		}
		right, err := e.Evaluate(expr.Right, row)
		if err != nil {
			return nil, err
		}
		return e.isTrue(right), nil
	}

	// OR
	if e.isTrue(left) {
		return true, nil
	}
	right, err := e.Evaluate(expr.Right, row)
	if err != nil {
		return nil, err
	}
	return e.isTrue(right), nil
}

// evaluateUnaryOp 计算一元运算符
func (e *ExpressionEvaluator) evaluateUnaryOp(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr.Left == nil {
		return nil, fmt.Errorf("invalid unary operator expression: missing operand")
	}

	operand, err := e.Evaluate(expr.Left, row)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(expr.Operator) {
	case "-":
		// 负号
		if num, ok := toFloat64(operand); ok {
			return -num, nil
		}
		return nil, fmt.Errorf("cannot apply unary minus to non-numeric value")
	case "+":
		// 正号
		return operand, nil
	case "not":
		// 逻辑�?
		return !e.isTrue(operand), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", expr.Operator)
	}
}

// evaluateFunction 计算函数调用（支持自定义函数�?
func (e *ExpressionEvaluator) evaluateFunction(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr.Function == "" {
		return nil, fmt.Errorf("function name is empty")
	}

	// 转换为小写以支持大小写不敏感的函数名
	funcName := strings.ToLower(expr.Function)

	// 优先从FunctionAPI获取函数（包括内置和用户函数�?
	if e.functionAPI == nil {
		return nil, fmt.Errorf("function API not initialized")
	}

	info, err := e.functionAPI.GetFunction(funcName)
	if err != nil {
		return nil, fmt.Errorf("function not found: %s", expr.Function)
	}

	// 计算参数（带类型检查）
	args := make([]interface{}, 0, len(expr.Args))
	for i, argExpr := range expr.Args {
		argValue, err := e.Evaluate(&argExpr, row)
		if err != nil {
			return nil, fmt.Errorf("argument %d evaluation failed for function %s: %w", i, expr.Function, err)
		}

		// 类型检查和自动转换
		convertedValue, err := e.convertToExpectedType(argValue, info.Parameters, i)
		if err != nil {
			return nil, fmt.Errorf("argument %d type conversion failed for function %s: %w", i, expr.Function, err)
		}
		args = append(args, convertedValue)
	}

	// 调用函数处理函数
	result, err := info.Handler(args)
	if err != nil {
		return nil, fmt.Errorf("function %s execution failed: %w", expr.Function, err)
	}

	return result, nil
}

// convertToExpectedType 将值转换为期望的类�?
func (e *ExpressionEvaluator) convertToExpectedType(value interface{}, params []builtin.FunctionParam, argIndex int) (interface{}, error) {
	if argIndex >= len(params) {
		return value, nil // 参数数量不匹配，返回原�?
	}

	expectedType := params[argIndex].Type

	// 如果期望类型为空或值为nil，直接返�?
	if expectedType == "" || value == nil {
		return value, nil
	}

	// 类型转换映射
	switch expectedType {
	case "int", "integer":
		return e.toInt(value)
	case "bigint", "long":
		return e.toInt64(value)
	case "decimal", "numeric", "number":
		return e.toFloat64(value)
	case "varchar", "char", "text", "string":
		return e.toString(value)
	default:
		return value, nil // 未知类型，返回原�?
	}
}

// exists 检查值是否存在（替代 ! 运算符）
func (e *ExpressionEvaluator) exists(v interface{}) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case string:
		return len(v.(string)) > 0
	case []interface{}:
		return len(v.([]interface{})) > 0
	case map[string]interface{}:
		return len(v.(map[string]interface{})) > 0
	default:
		return true
	}
}

// toInt 转换为int
func (e *ExpressionEvaluator) toInt(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot convert nil to int")
	}
	switch val := v.(type) {
	case int:
		return val, nil
	case int8:
		return int(val), nil
	case int16:
		return int(val), nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case float32:
		return int(float64(val)), nil
	case float64:
		return int(val), nil
	case string:
		// 尝试解析字符�?
		var result int
		_, err := fmt.Sscanf(val, "%d", &result)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to int: %v", val, err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to int", reflect.TypeOf(v))
	}
}

// toInt64 转换为int64
func (e *ExpressionEvaluator) toInt64(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot convert nil to int64")
	}
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case string:
		// 尝试解析字符�?
		var result int64
		_, err := fmt.Sscanf(val, "%d", &result)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to int64: %v", val, err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to int64", reflect.TypeOf(v))
	}
}

// toFloat64 转换为float64
func (e *ExpressionEvaluator) toFloat64(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot convert nil to float64")
	}
	switch val := v.(type) {
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	case string:
		result, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return result, nil
		}
		// 尝试解析整数
		var intResult int64
		_, intErr := fmt.Sscanf(val, "%d", &intResult)
		if intErr == nil {
			return float64(intResult), nil
		}
		return nil, err
	default:
		return nil, fmt.Errorf("cannot convert %T to float64", reflect.TypeOf(v))
	}
}

// toString 转换为string
func (e *ExpressionEvaluator) toString(v interface{}) (interface{}, error) {
	if v == nil {
		return "", nil
	}
	switch val := v.(type) {
	case string:
		return val, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%v", val), nil
	case bool:
		return fmt.Sprintf("%t", val), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// compareValues 比较两个�?
// 返回 -1: a < b, 0: a == b, 1: a > b
func (e *ExpressionEvaluator) compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 尝试数值比�?
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// 字符串比�?
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// addValues 加法运算
func (e *ExpressionEvaluator) addValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum + bNum, nil
	}

	// 字符串连�?
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr + bStr, nil
}

// subValues 减法运算
func (e *ExpressionEvaluator) subValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum - bNum, nil
	}
	return nil, fmt.Errorf("cannot subtract non-numeric values")
}

// mulValues 乘法运算
func (e *ExpressionEvaluator) mulValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum * bNum, nil
	}
	return nil, fmt.Errorf("cannot multiply non-numeric values")
}

// divValues 除法运算
func (e *ExpressionEvaluator) divValues(a, b interface{}) (interface{}, error) {
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if !aOk || !bOk {
		return nil, fmt.Errorf("cannot divide non-numeric values")
	}
	if bNum == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return aNum / bNum, nil
}

// likeValues LIKE 模式匹配
func (e *ExpressionEvaluator) likeValues(value, pattern interface{}) bool {
	valStr := fmt.Sprintf("%v", value)
	patStr := fmt.Sprintf("%v", pattern)

	// 简单的LIKE实现：支�?% �?_ 通配�?
	patternRegex := strings.ReplaceAll(patStr, "%", ".*")
	patternRegex = strings.ReplaceAll(patternRegex, "_", ".")
	patternRegex = "^" + patternRegex + "$"

	// 注意：完整的实现应该使用正则表达式包
	// 这里简化为使用strings.Contains和通配符匹�?
	if !strings.Contains(patStr, "%") && !strings.Contains(patStr, "_") {
		return valStr == patStr
	}

	// 简化实现：只检查是否包�?
	if strings.HasPrefix(patStr, "%") && strings.HasSuffix(patStr, "%") {
		subPat := strings.Trim(patStr, "%")
		return strings.Contains(valStr, subPat)
	}
	if strings.HasPrefix(patStr, "%") {
		subPat := strings.TrimPrefix(patStr, "%")
		return strings.HasSuffix(valStr, subPat)
	}
	if strings.HasSuffix(patStr, "%") {
		subPat := strings.TrimSuffix(patStr, "%")
		return strings.HasPrefix(valStr, subPat)
	}

	return false
}

// inValues IN 操作
func (e *ExpressionEvaluator) inValues(value, values interface{}) bool {
	valList, ok := values.([]interface{})
	if !ok {
		return false
	}

	for _, v := range valList {
		if e.compareValues(value, v) == 0 {
			return true
		}
	}
	return false
}

// betweenValues BETWEEN 操作
func (e *ExpressionEvaluator) betweenValues(value, min, max interface{}) bool {
	return e.compareValues(value, min) >= 0 && e.compareValues(value, max) <= 0
}

// isTrue 判断值是否为�?
func (e *ExpressionEvaluator) isTrue(value interface{}) bool {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(v).Int() != 0
	case uint, uint8, uint16, uint32, uint64:
		return reflect.ValueOf(v).Uint() != 0
	case float32, float64:
		return reflect.ValueOf(v).Float() != 0
	case string:
		return v != ""
	default:
		return true
	}
}
