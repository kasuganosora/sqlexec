package optimizer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/executor"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/utils"
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
// 用于不需要调用函数的场景（如常量折叠）
func NewExpressionEvaluatorWithoutAPI() *ExpressionEvaluator {
	return &ExpressionEvaluator{
		functionAPI: nil,
	}
}

// Evaluate 实现 executor.ExpressionEvaluator 接口
func (e *ExpressionEvaluator) Evaluate(expr interface{}, ctx executor.ExpressionContext) (interface{}, error) {
	parserExpr, ok := expr.(*parser.Expression)
	if !ok {
		return nil, fmt.Errorf("expression must be *parser.Expression")
	}

	// 将 ExpressionContext 转换为 parser.Row
	row := make(parser.Row)
	if ctx != nil {
		// 从 context 获取值 - 这里简化处理
		// 实际实现可能需要遍历所有可能的列名
	}

	return e.evaluateInternal(parserExpr, row)
}

// evaluateInternal 内部计算方法
func (e *ExpressionEvaluator) evaluateInternal(expr *parser.Expression, row parser.Row) (any, error) {
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
		return nil, fmt.Errorf("unsupported expression type: %v", expr.Type)
	}
}

// EvaluateBoolean 评估表达式并返回布尔值（实现 executor.ExpressionEvaluator 接口）
func (e *ExpressionEvaluator) EvaluateBoolean(expr interface{}, ctx executor.ExpressionContext) (bool, error) {
	parserExpr, ok := expr.(*parser.Expression)
	if !ok {
		return false, fmt.Errorf("expression must be *parser.Expression")
	}

	// 将 ExpressionContext 转换为 parser.Row
	row := make(parser.Row)
	if ctx != nil {
		// 尝试从 context 获取值
		// 这里简化处理，实际可能需要更多逻辑
	}

	result, err := e.evaluateInternal(parserExpr, row)
	if err != nil {
		return false, err
	}

	return e.isTrue(result), nil
}

// Validate 验证表达式（实现 executor.ExpressionEvaluator 接口）
func (e *ExpressionEvaluator) Validate(expr interface{}) error {
	parserExpr, ok := expr.(*parser.Expression)
	if !ok {
		return fmt.Errorf("expression must be *parser.Expression")
	}

	return e.validateExpression(parserExpr)
}

// validateExpression 递归验证表达式
func (e *ExpressionEvaluator) validateExpression(expr *parser.Expression) error {
	if expr == nil {
		return nil
	}

	switch expr.Type {
	case parser.ExprTypeColumn:
		if expr.Column == "" {
			return fmt.Errorf("column expression without column name")
		}
		return nil

	case parser.ExprTypeValue:
		// 常量值总是有效的
		return nil

	case parser.ExprTypeOperator:
		if expr.Operator == "" {
			return fmt.Errorf("operator expression without operator")
		}
		if expr.Left != nil {
			if err := e.validateExpression(expr.Left); err != nil {
				return fmt.Errorf("left operand: %w", err)
			}
		}
		if expr.Right != nil {
			if err := e.validateExpression(expr.Right); err != nil {
				return fmt.Errorf("right operand: %w", err)
			}
		}
		return nil

	case parser.ExprTypeFunction:
		if expr.Function == "" {
			return fmt.Errorf("function expression without function name")
		}
		for i, arg := range expr.Args {
			if err := e.validateExpression(&arg); err != nil {
				return fmt.Errorf("argument %d: %w", i, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown expression type: %v", expr.Type)
	}
}

// evaluateOperator 计算运算符表达式
func (e *ExpressionEvaluator) evaluateOperator(expr *parser.Expression, row parser.Row) (any, error) {
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

	left, err := e.evaluateInternal(expr.Left, row)
	if err != nil {
		return nil, fmt.Errorf("left operand evaluation failed: %w", err)
	}

	right, err := e.evaluateInternal(expr.Right, row)
	if err != nil {
		return nil, fmt.Errorf("right operand evaluation failed: %w", err)
	}

	// 根据运算符类型计算
	switch strings.ToLower(expr.Operator) {
	case "=":
		return utils.CompareValuesForSort(left, right) == 0, nil
	case "!=", "<>":
		return utils.CompareValuesForSort(left, right) != 0, nil
	case ">":
		return utils.CompareValuesForSort(left, right) > 0, nil
	case ">=":
		return utils.CompareValuesForSort(left, right) >= 0, nil
	case "<":
		return utils.CompareValuesForSort(left, right) < 0, nil
	case "<=":
		return utils.CompareValuesForSort(left, right) <= 0, nil
	case "+", "plus":
		return e.addValues(left, right)
	case "-", "minus":
		return e.subValues(left, right)
	case "*", "mul":
		return e.mulValues(left, right)
	case "/", "div":
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
func (e *ExpressionEvaluator) evaluateLogicalOp(expr *parser.Expression, row parser.Row) (any, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, fmt.Errorf("invalid logical operator expression")
	}

	left, err := e.evaluateInternal(expr.Left, row)
	if err != nil {
		return nil, err
	}

	// 短路求值
	if strings.EqualFold(expr.Operator, "and") {
		if !e.isTrue(left) {
			return false, nil
		}
		right, err := e.evaluateInternal(expr.Right, row)
		if err != nil {
			return nil, err
		}
		return e.isTrue(right), nil
	}

	// OR
	if e.isTrue(left) {
		return true, nil
	}
	right, err := e.evaluateInternal(expr.Right, row)
	if err != nil {
		return nil, err
	}
	return e.isTrue(right), nil
}

// evaluateUnaryOp 计算一元运算符
func (e *ExpressionEvaluator) evaluateUnaryOp(expr *parser.Expression, row parser.Row) (any, error) {
	if expr.Left == nil {
		return nil, fmt.Errorf("invalid unary operator expression: missing operand")
	}

	operand, err := e.evaluateInternal(expr.Left, row)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(expr.Operator) {
	case "-":
		// 负号
		if num, err := utils.ToFloat64(operand); err == nil {
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

// evaluateFunction 计算函数调用（支持自定义函数）
func (e *ExpressionEvaluator) evaluateFunction(expr *parser.Expression, row parser.Row) (any, error) {
	if expr.Function == "" {
		return nil, fmt.Errorf("function name is empty")
	}

	// 转换为小写以支持大小写不敏感的函数名
	funcName := strings.ToLower(expr.Function)

	// 优先从FunctionAPI获取函数（包括内置和用户函数）
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
		argValue, err := e.evaluateInternal(&argExpr, row)
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

// convertToExpectedType 将值转换为期望的类型
func (e *ExpressionEvaluator) convertToExpectedType(value any, params []builtin.FunctionParam, argIndex int) (any, error) {
	if argIndex >= len(params) {
		return value, nil // 参数数量不匹配，返回原值
	}

	expectedType := params[argIndex].Type

	// 如果期望类型为空或值为nil，直接返回
	if expectedType == "" || value == nil {
		return value, nil
	}

	// 类型转换映射
	switch expectedType {
	case "int", "integer":
		result, err := utils.ToInt(value)
		return result, err
	case "bigint", "long":
		return utils.ToInt64(value)
	case "decimal", "numeric", "number":
		return utils.ToFloat64(value)
	case "varchar", "char", "text", "string":
		return utils.ToString(value), nil
	default:
		return value, nil // 未知类型，返回原值
	}
}

// exists 检查值是否存在（替代 ! 运算符）
func (e *ExpressionEvaluator) exists(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case string:
		return len(v.(string)) > 0
	case []any:
		return len(v.([]any)) > 0
	case map[string]any:
		return len(v.(map[string]any)) > 0
	default:
		return true
	}
}

// addValues 加法运算
func (e *ExpressionEvaluator) addValues(a, b any) (any, error) {
	aNum, aErr := utils.ToFloat64(a)
	bNum, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum + bNum, nil
	}

	// 字符串连接
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr + bStr, nil
}

// subValues 减法运算
func (e *ExpressionEvaluator) subValues(a, b any) (any, error) {
	aNum, aErr := utils.ToFloat64(a)
	bNum, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum - bNum, nil
	}
	return nil, fmt.Errorf("cannot subtract non-numeric values")
}

// mulValues 乘法运算
func (e *ExpressionEvaluator) mulValues(a, b any) (any, error) {
	aNum, aErr := utils.ToFloat64(a)
	bNum, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return aNum * bNum, nil
	}
	return nil, fmt.Errorf("cannot multiply non-numeric values")
}

// divValues 除法运算
func (e *ExpressionEvaluator) divValues(a, b any) (any, error) {
	aNum, aErr := utils.ToFloat64(a)
	bNum, bErr := utils.ToFloat64(b)
	if aErr != nil || bErr != nil {
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
func (e *ExpressionEvaluator) inValues(value, values any) bool {
	valList, ok := values.([]any)
	if !ok {
		return false
	}

	for _, v := range valList {
		if utils.CompareValuesForSort(value, v) == 0 {
			return true
		}
	}
	return false
}

// betweenValues BETWEEN 操作
func (e *ExpressionEvaluator) betweenValues(value, min, max any) bool {
	return utils.CompareValuesForSort(value, min) >= 0 && utils.CompareValuesForSort(value, max) <= 0
}

// isTrue 判断值是否为真
func (e *ExpressionEvaluator) isTrue(value any) bool {
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

// ExtractCorrelatedColumns extracts correlated columns from an expression
// Returns columns that reference tables outside of current subquery scope
func ExtractCorrelatedColumns(expr *parser.Expression, outerSchema []ColumnInfo) []CorrelatedColumn {
	if expr == nil {
		return nil
	}

	correlated := []CorrelatedColumn{}

	// Check if this expression references an outer column
	if expr.Type == parser.ExprTypeColumn {
		for _, col := range outerSchema {
			if expr.Column == col.Name {
				correlated = append(correlated, CorrelatedColumn{
					Table:      "",
					Column:     col.Name,
					OuterLevel: 1,
				})
				break
			}
		}
	}

	// Recursively check left and right sub-expressions
	correlated = append(correlated, ExtractCorrelatedColumns(expr.Left, outerSchema)...)
	correlated = append(correlated, ExtractCorrelatedColumns(expr.Right, outerSchema)...)

	// Check function arguments
	for _, arg := range expr.Args {
		correlated = append(correlated, ExtractCorrelatedColumns(&arg, outerSchema)...)
	}

	return correlated
}

// ReplaceCorrelatedColumns replaces correlated columns with join references
// Mapping: correlated_column_name -> new_column_name
func ReplaceCorrelatedColumns(expr *parser.Expression, mapping map[string]string) *parser.Expression {
	if expr == nil {
		return nil
	}

	newExpr := *expr
	newExpr.Left = ReplaceCorrelatedColumns(expr.Left, mapping)
	newExpr.Right = ReplaceCorrelatedColumns(expr.Right, mapping)

	// Replace column references
	if expr.Type == parser.ExprTypeColumn {
		if newCol, ok := mapping[expr.Column]; ok {
			newExpr.Column = newCol
		}
	}

	// Replace function arguments
	if len(expr.Args) > 0 {
		newArgs := make([]parser.Expression, 0, len(expr.Args))
		for i, arg := range expr.Args {
			replacedArg := ReplaceCorrelatedColumns(&arg, mapping)
			newArgs[i] = *replacedArg
		}
		newExpr.Args = newArgs
	}

	return &newExpr
}

// Decorrelate decorrelate an expression by replacing correlated columns with join references
// Returns decorrelated expression and mapping used
func Decorrelate(expr *parser.Expression, outerSchema []ColumnInfo) (*parser.Expression, map[string]string) {
	if expr == nil {
		return nil, nil
	}

	correlatedCols := ExtractCorrelatedColumns(expr, outerSchema)
	if len(correlatedCols) == 0 {
		// No correlation, return as-is
		return expr, nil
	}

	// Create mapping: correlated_column -> join_column
	mapping := make(map[string]string)
	for _, col := range correlatedCols {
		mapping[col.Column] = "join_" + col.Column
	}

	// Replace correlated columns
	decorrelated := ReplaceCorrelatedColumns(expr, mapping)
	return decorrelated, mapping
}

// SimpleExpressionContext 是一个简单的 ExpressionContext 实现
type SimpleExpressionContext struct {
	row       parser.Row
	variables map[string]interface{}
	functions map[string]interface{}
}

// NewSimpleExpressionContext 创建简单的表达式上下文
func NewSimpleExpressionContext(row parser.Row) *SimpleExpressionContext {
	return &SimpleExpressionContext{
		row:       row,
		variables: make(map[string]interface{}),
		functions: make(map[string]interface{}),
	}
}

// GetRowValue 获取行值
func (ctx *SimpleExpressionContext) GetRowValue(colName string) (interface{}, bool) {
	if ctx.row == nil {
		return nil, false
	}
	val, ok := ctx.row[colName]
	return val, ok
}

// GetVariable 获取变量值
func (ctx *SimpleExpressionContext) GetVariable(varName string) (interface{}, bool) {
	val, ok := ctx.variables[varName]
	return val, ok
}

// GetFunction 获取函数
func (ctx *SimpleExpressionContext) GetFunction(funcName string) (interface{}, bool) {
	fn, ok := ctx.functions[funcName]
	return fn, ok
}

// GetCurrentTime 获取当前时间
func (ctx *SimpleExpressionContext) GetCurrentTime() interface{} {
	return nil // 简化实现
}

// SetVariable 设置变量
func (ctx *SimpleExpressionContext) SetVariable(name string, value interface{}) {
	ctx.variables[name] = value
}

// Ensure ExpressionEvaluator implements executor.ExpressionEvaluator
var _ executor.ExpressionEvaluator = (*ExpressionEvaluator)(nil)

// Ensure SimpleExpressionContext implements executor.ExpressionContext
var _ executor.ExpressionContext = (*SimpleExpressionContext)(nil)
