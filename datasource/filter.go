package datasource

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// Filter 过滤器
type Filter struct {
	functionManager  *FunctionManager
	subqueryExecutor *SubqueryExecutor
}

// NewFilter 创建过滤器
func NewFilter(functionManager *FunctionManager, subqueryExecutor *SubqueryExecutor) *Filter {
	return &Filter{
		functionManager:  functionManager,
		subqueryExecutor: subqueryExecutor,
	}
}

// MatchConditions 检查行是否匹配条件
func (f *Filter) MatchConditions(row Row, conditions []Condition, tableConfig *TableConfig) bool {
	for _, cond := range conditions {
		// 字段名不敏感处理
		var value interface{}
		for k, v := range row {
			if strings.EqualFold(k, cond.Field) {
				value = v
				break
			}
		}
		// 检查是否是函数调用
		if strings.Contains(cond.Field, "(") && strings.Contains(cond.Field, ")") {
			// 解析函数名和参数
			funcName := strings.Split(cond.Field, "(")[0]
			argsStr := strings.TrimSuffix(strings.Split(cond.Field, "(")[1], ")")
			args := strings.Split(argsStr, ",")

			// 获取函数
			fn, ok := f.functionManager.GetFunction(funcName)
			if !ok {
				return false
			}

			// 准备参数
			funcArgs := make([]interface{}, len(args))
			for i, arg := range args {
				arg = strings.TrimSpace(arg)
				if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
					// 字符串参数
					funcArgs[i] = strings.Trim(arg, "'")
				} else if v, err := strconv.ParseInt(arg, 10, 64); err == nil {
					// 整数参数
					funcArgs[i] = v
				} else if v, err := strconv.ParseFloat(arg, 64); err == nil {
					// 浮点数参数
					funcArgs[i] = v
				} else {
					// 其他参数（如字段名）
					funcArgs[i] = row[arg]
				}
			}

			// 调用函数
			result, err := fn.Call(funcArgs...)
			if err != nil {
				return false
			}

			// 比较结果
			if !f.compareValues(result, cond.Operator, cond.Value) {
				return false
			}
		} else if cond.Subquery != nil {
			// 处理子查询
			params := make(map[string]interface{})
			for k, v := range row {
				params[k] = v
			}
			subqueryResults, err := f.subqueryExecutor.ExecuteSubquery(context.Background(), cond.Subquery, params)
			if err != nil {
				return false
			}
			if !f.compareSubqueryResults(value, cond.Operator, subqueryResults) {
				return false
			}
		} else {
			if !f.compareValues(value, cond.Operator, cond.Value) {
				return false
			}
		}
	}
	return true
}

// compareValues 比较值
func (f *Filter) compareValues(value interface{}, operator string, target interface{}) bool {
	switch operator {
	case "=":
		return f.equal(value, target)
	case "!=":
		return !f.equal(value, target)
	case ">":
		return f.greaterThan(value, target)
	case ">=":
		return f.greaterThanOrEqual(value, target)
	case "<":
		return f.lessThan(value, target)
	case "<=":
		return f.lessThanOrEqual(value, target)
	case "LIKE":
		return f.like(value, target)
	case "IN":
		return f.in(value, target)
	default:
		return false
	}
}

// compareSubqueryResults 比较子查询结果
func (f *Filter) compareSubqueryResults(value interface{}, operator string, results [][]interface{}) bool {
	switch operator {
	case "EXISTS":
		return len(results) > 0
	case "IN":
		if value == nil {
			return false
		}
		for _, row := range results {
			if len(row) > 0 {
				val1 := toFloat64(value)
				val2 := toFloat64(row[0])
				if val1 == val2 {
					return true
				}
			}
		}
		return false
	case "NOT IN":
		if value == nil {
			return true
		}
		for _, row := range results {
			if len(row) > 0 {
				val1 := toFloat64(value)
				val2 := toFloat64(row[0])
				if val1 == val2 {
					return false
				}
			}
		}
		return true
	case "=":
		if len(results) == 1 && len(results[0]) == 1 {
			return f.equal(value, results[0][0])
		}
		return false
	case "!=":
		if len(results) == 1 && len(results[0]) == 1 {
			return !f.equal(value, results[0][0])
		}
		return true
	case ">":
		if len(results) == 1 && len(results[0]) == 1 {
			return f.greaterThan(value, results[0][0])
		}
		return false
	case ">=":
		if len(results) == 1 && len(results[0]) == 1 {
			return f.greaterThanOrEqual(value, results[0][0])
		}
		return false
	case "<":
		if len(results) == 1 && len(results[0]) == 1 {
			return f.lessThan(value, results[0][0])
		}
		return false
	case "<=":
		if len(results) == 1 && len(results[0]) == 1 {
			return f.lessThanOrEqual(value, results[0][0])
		}
		return false
	default:
		return false
	}
}

// equal 比较相等
func (f *Filter) equal(a, b interface{}) bool {
	switch v1 := a.(type) {
	case string:
		if v2, ok := b.(string); ok {
			return v1 == v2
		}
	case int64:
		if v2, ok := b.(int64); ok {
			return v1 == v2
		}
	case float64:
		if v2, ok := b.(float64); ok {
			return v1 == v2
		}
	case bool:
		if v2, ok := b.(bool); ok {
			return v1 == v2
		}
	case time.Time:
		if v2, ok := b.(time.Time); ok {
			return v1.Equal(v2)
		}
	}
	return false
}

// greaterThan 比较大于
func (f *Filter) greaterThan(a, b interface{}) bool {
	switch v1 := a.(type) {
	case int64:
		if v2, ok := b.(int64); ok {
			return v1 > v2
		}
	case float64:
		if v2, ok := b.(float64); ok {
			return v1 > v2
		}
	case time.Time:
		if v2, ok := b.(time.Time); ok {
			return v1.After(v2)
		}
	}
	return false
}

// lessThan 比较小于
func (f *Filter) lessThan(a, b interface{}) bool {
	switch v1 := a.(type) {
	case int64:
		if v2, ok := b.(int64); ok {
			return v1 < v2
		}
	case float64:
		if v2, ok := b.(float64); ok {
			return v1 < v2
		}
	case time.Time:
		if v2, ok := b.(time.Time); ok {
			return v1.Before(v2)
		}
	}
	return false
}

// greaterThanOrEqual 比较大于等于
func (f *Filter) greaterThanOrEqual(a, b interface{}) bool {
	return f.equal(a, b) || f.greaterThan(a, b)
}

// lessThanOrEqual 比较小于等于
func (f *Filter) lessThanOrEqual(a, b interface{}) bool {
	return f.equal(a, b) || f.lessThan(a, b)
}

// like 模糊匹配
func (f *Filter) like(value interface{}, pattern interface{}) bool {
	if str, ok := value.(string); ok {
		if pat, ok := pattern.(string); ok {
			// 将SQL LIKE模式转换为正则表达式
			regex := strings.ReplaceAll(pat, "%", ".*")
			regex = strings.ReplaceAll(regex, "_", ".")
			return strings.Contains(str, pat)
		}
	}
	return false
}

// in 包含检查
func (f *Filter) in(value interface{}, list interface{}) bool {
	if values, ok := list.([]interface{}); ok {
		for _, v := range values {
			if f.equal(value, v) {
				return true
			}
		}
	}
	return false
}
