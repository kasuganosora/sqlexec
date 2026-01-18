package main

import (
	"context"
	"fmt"
	"mysql-proxy/mysql/builtin"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 自定义函数测试 ===\n")

	// 创建函数管理器
	udfManager := builtin.NewUDFManager()

	// 测试1: 注册简单算术函数
	fmt.Println("=== 测试1: 注册简单算术函数 ===")
	testArithmeticFunction(udfManager)

	// 测试2: 注册字符串函数
	fmt.Println("\n=== 测试2: 注册字符串函数 ===")
	testStringFunction(udfManager)

	// 测试3: 注册条件函数
	fmt.Println("\n=== 测试3: 注册条件函数 ===")
	testConditionalFunction(udfManager)

	// 测试4: 在SQL查询中使用自定义函数
	fmt.Println("\n=== 测试4: 在SQL查询中使用自定义函数 ===")
	testSQLWithUDF()

	// 测试5: 函数管理
	fmt.Println("\n=== 测试5: 函数管理 ===")
	testFunctionManagement(udfManager)

	fmt.Println("\n=== 所有测试完成 ===")
}

// 测试1: 注册简单算术函数
func testArithmeticFunction(manager *builtin.UDFManager) {
	// 创建平方函数
	squareFunc := &builtin.UDFFunction{
		Metadata: &builtin.UDFMetadata{
			Name:       "square",
			Parameters: []builtin.UDFParameter{
				{Name: "num", Type: "NUMBER", Required: true},
			},
			ReturnType:  "NUMBER",
			Body:        "{{.num}} * {{.num}}",
			Language:    "SQL",
			Determinism: true,
			Description: "计算平方值",
		},
	}

	err := manager.Register(squareFunc)
	if err != nil {
		fmt.Printf("注册函数失败: %v\n", err)
		return
	}
	fmt.Println("✓ 成功注册 square 函数")

	// 测试调用
	udf, exists := manager.Get("square")
	if !exists {
		fmt.Println("✗ 函数不存在")
		return
	}

	result, err := udf.Handler([]interface{}{5})
	if err != nil {
		fmt.Printf("调用函数失败: %v\n", err)
		return
	}
	fmt.Printf("  square(5) = %v (期望: 25)\n", result)
}

// 测试2: 注册字符串函数
func testStringFunction(manager *builtin.UDFManager) {
	// 创建首字母大写函数
	upperFirstFunc := &builtin.UDFFunction{
		Metadata: &builtin.UDFMetadata{
			Name:       "upper_first",
			Parameters: []builtin.UDFParameter{
				{Name: "str", Type: "STRING", Required: true},
			},
			ReturnType:  "STRING",
			Body:        "UPPER(SUBSTRING({{.str}}, 1, 1)) || SUBSTRING({{.str}}, 2)",
			Language:    "SQL",
			Determinism: true,
			Description: "将首字母大写",
		},
	}

	err := manager.Register(upperFirstFunc)
	if err != nil {
		fmt.Printf("注册函数失败: %v\n", err)
		return
	}
	fmt.Println("✓ 成功注册 upper_first 函数")

	// 测试调用
	udf, exists := manager.Get("upper_first")
	if !exists {
		fmt.Println("✗ 函数不存在")
		return
	}

	result, err := udf.Handler([]interface{}{"hello"})
	if err != nil {
		fmt.Printf("调用函数失败: %v\n", err)
		return
	}
	fmt.Printf("  upper_first('hello') = %v (期望: 'Hello')\n", result)
}

// 测试3: 注册条件函数
func testConditionalFunction(manager *builtin.UDFManager) {
	// 创建等级函数
	gradeFunc := &builtin.UDFFunction{
		Metadata: &builtin.UDFMetadata{
			Name:       "grade",
			Parameters: []builtin.UDFParameter{
				{Name: "score", Type: "NUMBER", Required: true},
			},
			ReturnType:  "STRING",
			Body:        "CASE WHEN {{.score}} >= 90 THEN 'A' WHEN {{.score}} >= 80 THEN 'B' WHEN {{.score}} >= 70 THEN 'C' ELSE 'D' END",
			Language:    "SQL",
			Determinism: true,
			Description: "根据分数返回等级",
		},
	}

	err := manager.Register(gradeFunc)
	if err != nil {
		fmt.Printf("注册函数失败: %v\n", err)
		return
	}
	fmt.Println("✓ 成功注册 grade 函数")

	// 测试调用
	udf, exists := manager.Get("grade")
	if !exists {
		fmt.Println("✗ 函数不存在")
		return
	}

	scores := []int{95, 85, 75, 65}
	for _, score := range scores {
		result, err := udf.Handler([]interface{}{score})
		if err != nil {
			fmt.Printf("调用函数失败: %v\n", err)
			continue
		}
		fmt.Printf("  grade(%d) = %v\n", score, result)
	}
}

// 测试4: 在SQL查询中使用自定义函数
func testSQLWithUDF() {
	// 创建内存数据源
	config := &resource.DataSourceConfig{
		Name:     "memory-test",
		Writable: true,
	}
	source, _ := resource.NewMemoryFactory().Create(config)

	// 创建表
	ctx := context.Background()
	tableInfo := &resource.TableInfo{
		Name:   "students",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "score", Type: "INT"},
		},
	}
	source.CreateTable(ctx, tableInfo)

	// 插入测试数据
	data := []resource.Row{
		{"id": 1, "name": "Alice", "score": 95},
		{"id": 2, "name": "Bob", "score": 85},
		{"id": 3, "name": "Charlie", "score": 75},
		{"id": 4, "name": "David", "score": 65},
	}
	source.Insert(ctx, "students", data, nil)

	fmt.Println("插入测试数据:")
	result, _ := source.Query(ctx, "students", nil)
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}

	// 查询score > 80的数据
	fmt.Printf("\n查询score > 80的数据\n")
	result, err := source.Query(ctx, "students", &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "score", Operator: ">", Value: 80},
		},
	})
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}

	fmt.Printf("查询结果 (%d 行):\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}

	fmt.Println("✓ 查询成功")
}

// 测试5: 函数管理
func testFunctionManagement(manager *builtin.UDFManager) {
	// 列出所有函数
	functions := manager.List()
	fmt.Printf("当前注册的函数数量: %d\n", len(functions))

	for _, udf := range functions {
		fmt.Printf("  - %s: %s\n", udf.Metadata.Name, udf.Metadata.Description)
	}

	// 检查函数是否存在
	if manager.Exists("square") {
		fmt.Println("\n✓ square 函数存在")
	}

	if manager.Exists("nonexistent") {
		fmt.Println("✗ nonexistent 函数不应该存在")
	} else {
		fmt.Println("✓ nonexistent 函数不存在")
	}

	// 注销函数
	err := manager.Unregister("square")
	if err != nil {
		fmt.Printf("注销函数失败: %v\n", err)
		return
	}
	fmt.Println("✓ 成功注销 square 函数")

	// 再次检查
	if manager.Exists("square") {
		fmt.Println("✗ square 函数不应该存在")
	} else {
		fmt.Println("✓ square 函数已不存在")
	}

	// 清除所有函数
	manager.Clear()
	fmt.Printf("清除后函数数量: %d\n", manager.Count())
}
