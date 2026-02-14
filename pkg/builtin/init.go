package builtin

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

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
		"financial",
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

// ============ 公共辅助函数 (using utils package) ============

// ToFloat64 转换为float64
func ToFloat64(arg interface{}) (float64, error) {
	return utils.ToFloat64(arg)
}

// ToInt64 转换为int64
func ToInt64(arg interface{}) (int64, error) {
	return utils.ToInt64(arg)
}

// ToString 转换为string
func ToString(arg interface{}) string {
	return utils.ToString(arg)
}
