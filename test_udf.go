package main

import (
	"fmt"
	"log"

	"mysql-proxy/mysql/builtin"
)

func main() {
	fmt.Println("========== 用户自定义函数（UDF）测试 ==========\n")

	// 创建函数API
	api := builtin.NewFunctionAPI()

	// 测试1: 创建简单的SQL表达式UDF
	test1_SimpleExpression(api)

	// 测试2: 创建算术表达式UDF
	test2_ArithmeticExpression(api)

	// 测试3: 创建函数调用UDF
	test3_FunctionCall(api)

	// 测试4: 使用构建器创建UDF
	test4_BuilderPattern(api)

	// 测试5: 模板表达式UDF
	test5_TemplateExpression(api)

	// 测试6: 列出和查询UDF
	test6_ListAndQuery(api)

	// 测试7: 注销UDF
	test7_Unregister(api)

	fmt.Println("\n========== 所有UDF测试完成 ==========")
}

// ============ 测试1: 简单表达式 ============
func test1_SimpleExpression(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试1: 简单表达式UDF ===")

	// 创建一个简单的参数引用UDF
	udf := builtin.NewUDFBuilder("get_x").
		WithParameter("x", "number", false).
		WithReturnType("number").
		WithBody("@x").
		WithDescription("获取参数x的值").
		Build()

	if err := api.RegisterUDF(udf); err != nil {
		log.Fatal(err)
	}

	// 调用UDF
	fn, _ := api.GetFunction("get_x")
	result, err := fn.Handler([]interface{}{42})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ get_x(42) = %v\n\n", result)
}

// ============ 测试2: 算术表达式 ============
func test2_ArithmeticExpression(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试2: 算术表达式UDF ===")

	// 创建一个简单的加法UDF
	udf := builtin.NewUDFBuilder("add_numbers").
		WithParameter("a", "number", false).
		WithParameter("b", "number", false).
		WithReturnType("number").
		WithBody("@a + @b").
		WithDescription("加法运算").
		Build()

	if err := api.RegisterUDF(udf); err != nil {
		log.Fatal(err)
	}

	// 调用UDF
	fn, _ := api.GetFunction("add_numbers")
	result, err := fn.Handler([]interface{}{10, 20})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ add_numbers(10, 20) = %v\n\n", result)
}

// ============ 测试3: 函数调用 ============
func test3_FunctionCall(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试3: 函数调用UDF ===")

	// 创建一个调用内置函数的UDF
	udf := builtin.NewUDFBuilder("double_abs").
		WithParameter("x", "number", false).
		WithReturnType("number").
		WithBody("abs(@x) * 2").
		WithDescription("计算绝对值的两倍").
		Build()

	if err := api.RegisterUDF(udf); err != nil {
		log.Fatal(err)
	}

	// 调用UDF
	fn, _ := api.GetFunction("double_abs")
	result, err := fn.Handler([]interface{}{-5})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ double_abs(-5) = %v\n\n", result)
}

// ============ 测试4: 构建器模式 ============
func test4_BuilderPattern(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试4: 构建器模式UDF ===")

	// 使用构建器创建复杂的UDF
	udf := builtin.NewUDFBuilder("greet_user").
		WithParameter("name", "string", false).
		WithParameter("title", "string", true).
		WithReturnType("string").
		WithBody("concat(@title, ' ', @name)").
		WithLanguage("SQL").
		WithDeterminism(true).
		WithDescription("生成问候语").
		WithAuthor("System").
		Build()

	if err := api.RegisterUDF(udf); err != nil {
		log.Fatal(err)
	}

	// 调用UDF
	fn, _ := api.GetFunction("greet_user")
	result, err := fn.Handler([]interface{}{"Alice", "Hello"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ greet_user('Alice', 'Hello') = %v\n\n", result)
}

// ============ 测试5: 模板表达式 ============
func test5_TemplateExpression(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试5: 模板表达式UDF ===")

	// 创建一个模板表达式UDF
	udf := builtin.NewUDFBuilder("format_price").
		WithParameter("price", "number", false).
		WithParameter("currency", "string", false).
		WithReturnType("string").
		WithBody(`{{.price}} {{.currency}}`).
		WithLanguage("SQL").
		WithDescription("格式化价格").
		Build()

	if err := api.RegisterUDF(udf); err != nil {
		log.Fatal(err)
	}

	// 调用UDF
	fn, _ := api.GetFunction("format_price")
	result, err := fn.Handler([]interface{}{99.99, "USD"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ format_price(99.99, 'USD') = %v\n\n", result)
}

// ============ 测试6: 列出和查询 ============
func test6_ListAndQuery(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试6: 列出和查询UDF ===")

	// 列出所有UDF
	udfs := api.ListUDFs()
	fmt.Printf("注册的UDF数量: %d\n", len(udfs))

	// 统计
	fmt.Printf("UDF统计:\n")
	for _, udf := range udfs {
		fmt.Printf("  - %s: %s\n", udf.Metadata.Name, udf.Metadata.Description)
	}

	// 检查是否存在
	exists := api.UDFExists("add_numbers")
	fmt.Printf("add_numbers 存在: %v\n", exists)

	// 获取UDF元数据
	udf, err := api.GetUDF("add_numbers")
	if err == nil {
		fmt.Printf("add_numbers 元数据:\n")
		fmt.Printf("  参数: %d\n", len(udf.Metadata.Parameters))
		fmt.Printf("  返回类型: %s\n", udf.Metadata.ReturnType)
		fmt.Printf("  创建时间: %v\n", udf.Metadata.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	fmt.Println()
}

// ============ 测试7: 注销UDF ============
func test7_Unregister(api *builtin.FunctionAPI) {
	fmt.Println("=== 测试7: 注销UDF ===")

	// 列出当前UDF
	count := api.CountUDFs()
	fmt.Printf("当前UDF数量: %d\n", count)

	// 注销一个UDF
	err := api.UnregisterUDF("get_x")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("已注销 get_x\n")

	// 再次统计
	newCount := api.CountUDFs()
	fmt.Printf("注销后UDF数量: %d\n", newCount)

	// 检查是否还存在
	exists := api.UDFExists("get_x")
	fmt.Printf("get_x 是否存在: %v\n\n", exists)

	// 清除所有UDF
	api.ClearUDFs()
	fmt.Printf("已清除所有UDF\n")
	fmt.Printf("最终UDF数量: %d\n", api.CountUDFs())
}
