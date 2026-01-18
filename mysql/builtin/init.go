package builtin

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
