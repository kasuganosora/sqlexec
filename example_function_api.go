package main

import (
	"fmt"
	"log"

	"mysql-proxy/mysql/builtin"
)

// ============ 示例1: 使用函数API注册自定义函数 ============

func example1_RegisterCustomFunctions() {
	fmt.Println("\n=== 示例1: 注册自定义函数 ===")

	// 创建函数API
	api := builtin.NewFunctionAPI()

	// 注册一个简单的自定义函数
	err := builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
		"square", "Square", "计算平方", "number",
		func(args []interface{}) (interface{}, error) {
			if len(args) < 1 {
				return nil, fmt.Errorf("square requires at least 1 argument")
			}
			val, err := builtin.ToFloat64(args[0])
			if err != nil {
				return nil, err
			}
			return val * val, nil
		},
		1,
	)
	if err != nil {
		log.Fatal(err)
	}

	// 注册一个可变参数函数
	err = builtin.RegisterVariadicScalar(api, builtin.CategoryString,
		"join", "Join", "连接多个字符串", "string",
		func(args []interface{}) (interface{}, error) {
			result := ""
			for i, arg := range args {
				if i > 0 {
					result += " "
				}
				result += fmt.Sprintf("%v", arg)
			}
			return result, nil
		},
		1,
	)
	if err != nil {
		log.Fatal(err)
	}

	// 测试自定义函数
	squareFn, ok := api.GetFunction("square")
	if ok {
		result, err := squareFn.Handler([]interface{}{5.0})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("square(5) = %v\n", result)
	}

	joinFn, ok := api.GetFunction("join")
	if ok {
		result, err := joinFn.Handler([]interface{}{"Hello", "World", "!"})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("join('Hello', 'World', '!') = %v\n", result)
	}
}

// ============ 示例2: 使用构建器创建复杂函数 ============

func example2_UseBuilder() {
	fmt.Println("\n=== 示例2: 使用构建器创建函数 ===")

	api := builtin.NewFunctionAPI()

	// 使用构建器创建一个复杂的自定义函数
	err := builtin.MathFunctionBuilder("distance", "Distance", "计算两点之间的距离").
		WithDescription("计算二维平面上两点之间的欧几里得距离").
		WithParameter("x1", "number", "第一个点的X坐标", true).
		WithParameter("y1", "number", "第一个点的Y坐标", true).
		WithParameter("x2", "number", "第二个点的X坐标", true).
		WithParameter("y2", "number", "第二个点的Y坐标", true).
		WithExample("SELECT distance(0, 0, 3, 4) FROM dual").
		WithExample("SELECT distance(1, 1, 4, 5) FROM dual").
		WithTags([]string{"math", "geometry", "distance"}).
		WithHandler(func(args []interface{}) (interface{}, error) {
			x1, _ := builtin.ToFloat64(args[0])
			y1, _ := builtin.ToFloat64(args[1])
			x2, _ := builtin.ToFloat64(args[2])
			y2, _ := builtin.ToFloat64(args[3])

			dx := x2 - x1
			dy := y2 - y1
			return builtin.Sqrt(dx*dx + dy*dy), nil
		}).
		Register(api)

	if err != nil {
		log.Fatal(err)
	}

	// 测试距离函数
	distanceFn, ok := api.GetFunction("distance")
	if ok {
		result, err := distanceFn.Handler([]interface{}{0.0, 0.0, 3.0, 4.0})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("distance(0, 0, 3, 4) = %v\n", result)
	}
}

// ============ 示例3: 函数查询和搜索 ============

func example3_QueryFunctions() {
	fmt.Println("\n=== 示例3: 查询和搜索函数 ===")

	api := builtin.NewFunctionAPI()

	// 注册一些测试函数
	builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
		"test1", "Test1", "测试函数1", "number",
		func(args []interface{}) (interface{}, error) { return args[0], nil },
		1,
	)

	builtin.RegisterSimpleScalar(api, builtin.CategoryString,
		"test2", "Test2", "测试函数2", "string",
		func(args []interface{}) (interface{}, error) { return args[0], nil },
		1,
	)

	// 列出所有数学函数
	fmt.Printf("\n数学函数 (%d个):\n", api.CountFunctionsByCategory(builtin.CategoryMath))
	mathFuncs := api.ListFunctionsByCategory(builtin.CategoryMath)
	for _, fn := range mathFuncs {
		fmt.Printf("  - %s: %s\n", fn.DisplayName, fn.Description)
	}

	// 列出所有字符串函数
	fmt.Printf("\n字符串函数 (%d个):\n", api.CountFunctionsByCategory(builtin.CategoryString))
	stringFuncs := api.ListFunctionsByCategory(builtin.CategoryString)
	for _, fn := range stringFuncs {
		fmt.Printf("  - %s: %s\n", fn.DisplayName, fn.Description)
	}

	// 搜索函数
	fmt.Println("\n搜索包含 'test' 的函数:")
	searchResults := api.SearchFunctions("test")
	for _, fn := range searchResults {
		fmt.Printf("  - %s (%s): %s\n", fn.Name, fn.DisplayName, fn.Description)
	}

	// 按类型列出
	fmt.Println("\n标量函数:")
	scalarFuncs := api.ListFunctionsByType(builtin.FunctionTypeScalar)
	for _, fn := range scalarFuncs {
		fmt.Printf("  - %s\n", fn.Name)
	}
}

// ============ 示例4: 用户函数管理 ============

func example4_UserFunctions() {
	fmt.Println("\n=== 示例4: 用户函数管理 ===")

	api := builtin.NewFunctionAPI()

	// 注册用户函数
	err := builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
		"user_func1", "UserFunc1", "用户自定义函数1", "number",
		func(args []interface{}) (interface{}, error) { return args[0], nil },
		1,
	)
	if err != nil {
		log.Fatal(err)
	}

	// 列出用户函数
	userFuncs := api.GetRegistry().ListUserFunctions()
	fmt.Printf("用户函数 (%d个):\n", len(userFuncs))
	for _, fn := range userFuncs {
		fmt.Printf("  - %s: %s\n", fn.Name, fn.Description)
	}

	// 清除用户函数
	api.ClearUserFunctions()
	fmt.Println("已清除所有用户函数")
	fmt.Printf("用户函数数: %d\n", len(api.GetRegistry().ListUserFunctions()))
}

// ============ 示例5: 别名管理 ============

func example5_AliasManagement() {
	fmt.Println("\n=== 示例5: 别名管理 ===")

	api := builtin.NewFunctionAPI()

	// 注意：别名功能需要在实际函数存在时使用
	// 这里我们演示如何添加和删除别名
	fmt.Println("注意：别名管理示例")

	// 列出所有别名（初始为空）
	aliases := api.GetFunctionAliases()
	fmt.Printf("当前别名数: %d\n", len(aliases))
}

// ============ 示例6: 生成文档 ============

func example6_GenerateDocumentation() {
	fmt.Println("\n=== 示例6: 生成文档 ===")

	api := builtin.NewFunctionAPI()

	// 注册一些测试函数
	builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
		"demo_func", "DemoFunc", "演示函数", "number",
		func(args []interface{}) (interface{}, error) { return args[0], nil },
		1,
	)

	// 生成Markdown文档
	docs := api.GenerateDocumentation()
	fmt.Println("生成的文档（前500字符）：")
	if len(docs) > 500 {
		fmt.Println(docs[:500] + "...")
	} else {
		fmt.Println(docs)
	}

	// 生成JSON文档
	jsonDoc, err := api.GenerateJSON()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n生成的JSON文档（前200字符）：")
	if len(jsonDoc) > 200 {
		fmt.Println(jsonDoc[:200] + "...")
	} else {
		fmt.Println(jsonDoc)
	}
}

// ============ 示例7: 完整应用集成示例 ============

func example7_ApplicationIntegration() {
	fmt.Println("\n=== 示例7: 应用程序集成 ===")

	// 创建查询引擎的函数API
	api := builtin.NewFunctionAPI()

	// 注册应用特定的函数
	err := api.RegisterScalarFunction("app_hash", "AppHash", "应用哈希函数",
		func(args []interface{}) (interface{}, error) {
			// 模拟哈希计算
			input := fmt.Sprintf("%v", args[0])
			hash := 0
			for _, c := range input {
				hash = hash*31 + int(c)
			}
			return hash, nil
		},
		builtin.WithCategory(builtin.CategoryString),
		builtin.WithReturnType("integer"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// 注册另一个应用函数
	err = api.RegisterScalarFunction("app_format", "AppFormat", "应用格式化函数",
		func(args []interface{}) (interface{}, error) {
			return fmt.Sprintf("[APP] %v", args[0]), nil
		},
		builtin.WithCategory(builtin.CategoryString),
		builtin.WithReturnType("string"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// 使用应用函数
	fmt.Println("\n应用特定函数:")
	
	hashFn, _ := api.GetFunction("app_hash")
	result, _ := hashFn.Handler([]interface{}{"test"})
	fmt.Printf("  app_hash('test') = %v\n", result)

	formatFn, _ := api.GetFunction("app_format")
	result, _ = formatFn.Handler([]interface{}{"Hello"})
	fmt.Printf("  app_format('Hello') = %v\n", result)

	// 列出所有注册的函数
	fmt.Printf("\n引擎中的函数总数: %d\n", api.CountFunctions())
}

// ============ 主函数 ============

func main() {
	fmt.Println("========== 函数注册系统使用示例 ==========")

	// 运行所有示例
	example1_RegisterCustomFunctions()
	example2_UseBuilder()
	example3_QueryFunctions()
	example4_UserFunctions()
	example5_AliasManagement()
	example6_GenerateDocumentation()
	example7_ApplicationIntegration()

	fmt.Println("\n========== 所有示例运行完成 ==========")
}
