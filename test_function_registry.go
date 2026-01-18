package main

import (
	"fmt"
	"log"

	"mysql-proxy/mysql/builtin"
)

func main() {
	fmt.Println("=== 函数注册系统测试 ===\n")

	// 创建函数API
	api := builtin.NewFunctionAPI()

	// 示例1: 注册简单函数
	registerSimpleFunctions(api)

	// 示例2: 查询函数
	queryFunctions(api)

	// 示例3: 搜索函数
	searchFunctions(api)

	// 示例4: 用户函数管理
	manageUserFunctions(api)

	// 示例5: 生成文档
	generateDocs(api)

	fmt.Println("\n=== 所有测试完成 ===")
}

func registerSimpleFunctions(api *builtin.FunctionAPI) {
	fmt.Println("【1. 注册自定义函数】")

	// 注册平方函数
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
	fmt.Println("  ✅ 注册 square 函数")

	// 注册字符串拼接函数
	err = builtin.RegisterSimpleScalar(api, builtin.CategoryString,
		"str_join", "StrJoin", "连接字符串", "string",
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
		2,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  ✅ 注册 str_join 函数")

	// 测试函数
	squareFn, ok := api.GetFunction("square")
	if ok {
		result, err := squareFn.Handler([]interface{}{5.0})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  ✅ square(5) = %v\n", result)
	}
}

func queryFunctions(api *builtin.FunctionAPI) {
	fmt.Println("\n【2. 查询函数】")

	// 列出所有函数
	allFuncs := api.ListFunctions()
	fmt.Printf("  总函数数: %d\n", len(allFuncs))

	// 按类别列出
	categories := []builtin.FunctionCategory{
		builtin.CategoryMath,
		builtin.CategoryString,
		builtin.CategoryDate,
		builtin.CategoryAggregate,
	}

	for _, cat := range categories {
		count := api.CountFunctionsByCategory(cat)
		if count > 0 {
			fmt.Printf("  %s: %d 个函数\n", string(cat), count)
		}
	}
}

func searchFunctions(api *builtin.FunctionAPI) {
	fmt.Println("\n【3. 搜索函数】")

	// 搜索包含'square'的函数
	results := api.SearchFunctions("square")
	fmt.Printf("  搜索'square'找到 %d 个函数\n", len(results))
	for _, fn := range results {
		fmt.Printf("    - %s: %s\n", fn.DisplayName, fn.Description)
	}
}

func manageUserFunctions(api *builtin.FunctionAPI) {
	fmt.Println("\n【4. 用户函数管理】")

	// 注册用户函数
	err := api.RegisterScalarFunction("user_hello", "UserHello", "用户问候函数",
		func(args []interface{}) (interface{}, error) {
			return fmt.Sprintf("Hello, %v!", args[0]), nil
		},
		builtin.WithCategory(builtin.CategoryString),
		builtin.WithReturnType("string"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("  ✅ 注册 user_hello 函数")

	// 列出用户函数
	userFuncs := api.GetRegistry().ListUserFunctions()
	fmt.Printf("  用户函数数: %d\n", len(userFuncs))
	for _, fn := range userFuncs {
		fmt.Printf("    - %s: %s\n", fn.Name, fn.Description)
	}

	// 清除用户函数
	api.ClearUserFunctions()
	fmt.Println("  ✅ 清除所有用户函数")
	fmt.Printf("  用户函数数: %d\n", len(api.GetRegistry().ListUserFunctions()))
}

func generateDocs(api *builtin.FunctionAPI) {
	fmt.Println("\n【5. 生成文档】")

	// 生成Markdown文档
	docs := api.GenerateDocumentation()
	lines := countLines(docs)
	fmt.Printf("  Markdown文档: %d 行\n", lines)

	// 生成JSON文档
	jsonDoc, err := api.GenerateJSON()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  JSON文档: %d 字符\n", len(jsonDoc))
}

func countLines(s string) int {
	count := 0
	for _, c := range s {
		if c == '\n' {
			count++
		}
	}
	return count
}
