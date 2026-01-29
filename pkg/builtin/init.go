package builtin

import "fmt"

// InitBuiltinFunctions 初始化所有内置函数
func InitBuiltinFunctions() {
	// 初始化聚合函数
	InitAggregateFunctions()
	
	// 其他函数（数学、字符串、日期）已在各自的init()函数中自动注册
	// 只需确保包被导入即可
}

// GetAllCategories 获取所有函数类别
func GetAllCategories() []string {
	return []string{
		"math",
		"string",
		"date",
		"json",
		"aggregate",
	}
}

// GetFunctionCount 获取函数总数
func GetFunctionCount() int {
	return len(globalRegistry.functions)
}

// GetFunctionCountByCategory 按类别获取函数数量
func GetFunctionCountByCategory(category string) int {
	return len(globalRegistry.ListByCategory(category))
}

// ============ 公共辅助函数 ============

// ToFloat64 转换为float64
func ToFloat64(arg interface{}) (float64, error) {
	switch v := arg.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", arg)
	}
}

// ToInt64 转换为int64
func ToInt64(arg interface{}) (int64, error) {
	switch v := arg.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", arg)
	}
}

// ToString 转换为string
func ToString(arg interface{}) string {
	switch v := arg.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", arg)
	}
}
