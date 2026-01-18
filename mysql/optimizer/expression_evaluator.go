package optimizer

import (
	"fmt"
	"reflect"
	"strings"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// ExpressionEvaluator 表达式求值器
type ExpressionEvaluator struct{}

// NewExpressionEvaluator 创建表达式求值器
func NewExpressionEvaluator() *ExpressionEvaluator {
	return &ExpressionEvaluator{}
}

// Evaluate 计算表达式的值
func (e *ExpressionEvaluator) Evaluate(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr.Type {
	case parser.ExprTypeColumn:
		// 列引用
		if val, exists := row[expr.Column]; exists {
			return val, nil
		}
		return nil, fmt.Errorf("column not found: %s", expr.Column)

	case parser.ExprTypeValue:
		// 字面量值
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

	// 处理逻辑运算符
	if strings.EqualFold(expr.Operator, "and") || strings.EqualFold(expr.Operator, "or") {
		return e.evaluateLogicalOp(expr, row)
	}

	// 处理一元运算符
	if expr.Right == nil {
		return e.evaluateUnaryOp(expr, row)
	}

	// 处理二元运算符
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

	// 根据运算符类型计算
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

// evaluateLogicalOp 计算逻辑运算符
func (e *ExpressionEvaluator) evaluateLogicalOp(expr *parser.Expression, row parser.Row) (interface{}, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, fmt.Errorf("invalid logical operator expression")
	}

	left, err := e.Evaluate(expr.Left, row)
	if err != nil {
		return nil, err
	}

	// 短路求值
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
		// 逻辑非
		return !e.isTrue(operand), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", expr.Operator)
	}
}

// evaluateFunction 计算函数调用
func (e *ExpressionEvaluator) evaluateFunction(expr *parser.Expression, row resource.Row) (interface{}, error) {
	if expr.Function == "" {
		return nil, fmt.Errorf("function name is empty")
	}

	// TODO: 实现内置函数
	// 例如: COUNT, SUM, AVG, MAX, MIN, UPPER, LOWER, SUBSTRING 等
	return nil, fmt.Errorf("function evaluation not implemented: %s", expr.Function)
}

// compareValues 比较两个值
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

	// 尝试数值比较
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

	// 字符串比较
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

	// 字符串连接
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

	// 简单的LIKE实现：支持 % 和 _ 通配符
	patternRegex := strings.ReplaceAll(patStr, "%", ".*")
	patternRegex = strings.ReplaceAll(patternRegex, "_", ".")
	patternRegex = "^" + patternRegex + "$"

	// 注意：完整的实现应该使用正则表达式包
	// 这里简化为使用strings.Contains和通配符匹配
	if !strings.Contains(patStr, "%") && !strings.Contains(patStr, "_") {
		return valStr == patStr
	}

	// 简化实现：只检查是否包含
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

// isTrue 判断值是否为真
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
