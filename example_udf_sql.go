package main

import (
	"fmt"
	"log"
	"strings"

	"mysql-proxy/mysql/builtin"
)

// 这个示例展示如何在SQL中使用自定义函数（UDF）
func main() {
	fmt.Println("========== SQL UDF使用示例 ==========\n")

	// 创建函数API
	api := builtin.NewFunctionAPI()

	// 初始化内置函数
	builtin.InitBuiltinFunctions()

	// 示例1: 商业计算函数
	example1_BusinessFunctions(api)

	// 示例2: 字符串处理函数
	example2_StringFunctions(api)

	// 示例3: 数据验证函数
	example3_ValidationFunctions(api)

	// 示例4: 模拟SQL查询
	example4_SimulateSQLQuery(api)

	// 示例5: 性能优化UDF
	example5_OptimizedUDF(api)

	fmt.Println("========== 所有示例完成 ==========")
}

// ============ 示例1: 商业计算函数 ============
func example1_BusinessFunctions(api *builtin.FunctionAPI) {
	fmt.Println("=== 示例1: 商业计算函数 ===\n")

	// 创建折扣计算UDF
	discountUDF := builtin.NewUDFBuilder("apply_discount").
		WithParameter("price", "number", false).
		WithParameter("discount_rate", "number", false).
		WithReturnType("number").
		WithBody(`{{.price}} - {{.price}} * {{.discount_rate}}`).
		WithLanguage("SQL").
		WithDescription("计算折扣后价格").
		WithAuthor("Finance Team").
		Build()

	if err := api.RegisterUDF(discountUDF); err != nil {
		log.Fatal(err)
	}

	// 使用示例
	fn, _ := api.GetFunction("apply_discount")
	
	testCases := []struct {
		price float64
		rate float64
	}{
		{100.0, 0.1},  // 10% 折扣
		{250.0, 0.2},  // 20% 折扣
		{99.99, 0.15}, // 15% 折扣
	}

	fmt.Println("折扣计算示例:")
	for _, tc := range testCases {
		result, _ := fn.Handler([]interface{}{tc.price, tc.rate})
		fmt.Printf("  原价: $%.2f, 折扣: %.0f%% -> $%.2f\n", 
			tc.price, tc.rate*100, result)
	}
	fmt.Println()
}

// ============ 示例2: 字符串处理函数 ============
func example2_StringFunctions(api *builtin.FunctionAPI) {
	fmt.Println("=== 示例2: 字符串处理函数 ===\n")

	// 创建用户名格式化UDF
	formatUsernameUDF := builtin.NewUDFBuilder("format_username").
		WithParameter("first_name", "string", false).
		WithParameter("last_name", "string", false).
		WithReturnType("string").
		WithBody(`{{.first_name}}.{{.last_name}}`).
		WithLanguage("SQL").
		WithDescription("生成用户名格式").
		Build()

	if err := api.RegisterUDF(formatUsernameUDF); err != nil {
		log.Fatal(err)
	}

	// 使用示例
	fn, _ := api.GetFunction("format_username")
	
	users := []struct {
		first string
		last string
	}{
		{"John", "Smith"},
		{"Jane", "Doe"},
		{"Alice", "Johnson"},
	}

	fmt.Println("用户名格式化:")
	for _, user := range users {
		result, _ := fn.Handler([]interface{}{user.first, user.last})
		fmt.Printf("  %s %s -> %s\n", user.first, user.last, result)
	}
	fmt.Println()
}

// ============ 示例3: 数据验证函数 ============
func example3_ValidationFunctions(api *builtin.FunctionAPI) {
	fmt.Println("=== 示例3: 数据验证函数 ===\n")

	// 创建年龄验证UDF
	validateAgeUDF := builtin.NewUDFBuilder("is_adult").
		WithParameter("age", "number", false).
		WithReturnType("string").
		WithBody(`{{if ge .age 18}}Adult{{else}}Minor{{end}}`).
		WithLanguage("SQL").
		WithDescription("验证是否成年").
		Build()

	if err := api.RegisterUDF(validateAgeUDF); err != nil {
		log.Fatal(err)
	}

	// 使用示例
	fn, _ := api.GetFunction("is_adult")
	
	ages := []int{16, 18, 21, 65}

	fmt.Println("年龄验证:")
	for _, age := range ages {
		result, _ := fn.Handler([]interface{}{age})
		fmt.Printf("  Age %d -> %s\n", age, result)
	}
	fmt.Println()
}

// ============ 示例4: 模拟SQL查询 ============
func example4_SimulateSQLQuery(api *builtin.FunctionAPI) {
	fmt.Println("=== 示例4: 模拟SQL查询 ===\n")

	// 注册更多UDF
	discountUDF := builtin.NewUDFBuilder("discount").
		WithParameter("price", "number", false).
		WithReturnType("string").
		WithBody(`{{.price}}`).
		Build()
	api.RegisterUDF(discountUDF)

	// 模拟SQL查询结果
	fmt.Println("模拟SQL查询:")
	fmt.Println("SELECT id, name, apply_discount(price, 0.1) AS discounted_price")
	fmt.Println("FROM products")
	fmt.Println("WHERE price > 50;")
	fmt.Println()

	fmt.Println("结果:")
	fmt.Println("+----+-------+------------------+")
	fmt.Println("| id | name  | discounted_price |")
	fmt.Println("+----+-------+------------------+")

	// 模拟查询处理
	products := []struct {
		id    int
		name  string
		price float64
	}{
		{1, "Laptop", 999.99},
		{2, "Mouse", 29.99},
		{3, "Monitor", 299.99},
		{4, "Keyboard", 79.99},
	}

	fn, _ := api.GetFunction("discount")
	for _, p := range products {
		if p.price > 50 {
			result, _ := fn.Handler([]interface{}{p.price})
			fmt.Printf("| %2d | %-5s | %16.2f |\n", p.id, p.name, result)
		}
	}
	fmt.Println("+----+-------+------------------+")
	fmt.Println()
}

// ============ 示例5: 性能优化UDF ============
func example5_OptimizedUDF(api *builtin.FunctionAPI) {
	fmt.Println("=== 示例5: 性能优化UDF ===\n")

	// 创建确定性的UDF（可被缓存）
	hashUDF := builtin.NewUDFBuilder("simple_hash").
		WithParameter("value", "string", false).
		WithReturnType("string").
		WithBody(`{{.value}}_hash`).
		WithLanguage("SQL").
		WithDeterminism(true).
		WithDescription("简单哈希函数").
		Build()

	if err := api.RegisterUDF(hashUDF); err != nil {
		log.Fatal(err)
	}

	// 性能测试
	fn, _ := api.GetFunction("simple_hash")
	
	fmt.Println("UDF确定性验证:")
	
	testValues := []string{"test1", "test2", "test1"}
	
	for _, val := range testValues {
		result, _ := fn.Handler([]interface{}{val})
		fmt.Printf("  simple_hash('%s') = %s\n", val, result)
	}

	// 检查UDF元数据
	udf, _ := api.GetUDF("simple_hash")
	fmt.Printf("\nUDF '%s' 元数据:\n", udf.Metadata.Name)
	fmt.Printf("  确定性: %v\n", udf.Metadata.Determinism)
	fmt.Printf("  描述: %s\n", udf.Metadata.Description)
	fmt.Printf("  参数数: %d\n", len(udf.Metadata.Parameters))
	fmt.Printf("  返回类型: %s\n", udf.Metadata.ReturnType)
	fmt.Printf("  语言: %s\n", udf.Metadata.Language)
	fmt.Println()
}

// ============ 辅助函数 ============

// printUDFInfo 打印UDF信息
func printUDFInfo(api *builtin.FunctionAPI, name string) {
	if udf, err := api.GetUDF(name); err == nil {
		fmt.Printf("  函数名: %s\n", udf.Metadata.Name)
		fmt.Printf("  描述: %s\n", udf.Metadata.Description)
		fmt.Printf("  参数: ")
		for i, param := range udf.Metadata.Parameters {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s:%s", param.Name, param.Type)
		}
		fmt.Printf("\n  返回类型: %s\n", udf.Metadata.ReturnType)
		fmt.Printf("  确定性: %v\n", udf.Metadata.Determinism)
		fmt.Printf("  语言: %s\n", udf.Metadata.Language)
		fmt.Println()
	}
}

// printDivider 打印分割线
func printDivider(char string, length int) {
	fmt.Println(strings.Repeat(char, length))
}
