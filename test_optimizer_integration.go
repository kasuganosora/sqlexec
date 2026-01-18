package main

import (
	"context"
	"fmt"
	"log"
	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== Optimizer 集成测试 ===\n")

	// 1. 创建内存数据源
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	// 内存数据源默认可写，不需要额外设置

	// 2. 创建测试表
	createTable(dataSource)

	// 3. 插入测试数据
	insertTestData(dataSource)

	// 4. 创建优化执行器（启用优化器）
	executor := optimizer.NewOptimizedExecutor(dataSource, true)

	// 5. 测试优化查询
	testOptimizedQuery(executor)

	// 6. 测试禁用优化器的查询
	testUnoptimizedQuery(executor)

	fmt.Println("\n=== 所有测试完成 ===")
}

// createTable 创建测试表
func createTable(ds resource.DataSource) {
	// 确保数据源已连接
	if !ds.IsConnected() {
		if err := ds.Connect(context.Background()); err != nil {
			log.Fatalf("连接数据源失败: %v", err)
		}
	}

	tableInfo := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "city", Type: "string", Nullable: true},
		},
	}

	err := ds.CreateTable(context.Background(), tableInfo)
	if err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	fmt.Println("✅ 创建表 users 成功")
}

// insertTestData 插入测试数据
func insertTestData(ds resource.DataSource) {
	rows := []resource.Row{
		{"id": int64(1), "name": "Alice", "age": int64(25), "city": "Beijing"},
		{"id": int64(2), "name": "Bob", "age": int64(30), "city": "Shanghai"},
		{"id": int64(3), "name": "Charlie", "age": int64(35), "city": "Guangzhou"},
		{"id": int64(4), "name": "David", "age": int64(28), "city": "Shenzhen"},
		{"id": int64(5), "name": "Eve", "age": int64(32), "city": "Beijing"},
	}

	_, err := ds.Insert(context.Background(), "users", rows, &resource.InsertOptions{Replace: false})
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	fmt.Printf("✅ 插入 %d 行数据\n", len(rows))
}

// testOptimizedQuery 测试优化查询
func testOptimizedQuery(executor *optimizer.OptimizedExecutor) {
	fmt.Println("\n【测试1】使用优化器执行查询")

	adapter := parser.NewSQLAdapter()

	// 测试1: 基本查询
	fmt.Println("\n--- 测试1.1: 基本查询 ---")
	sql := "SELECT * FROM users"
	result, err := parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))

	// 测试2: WHERE 条件查询（谓词下推）
	fmt.Println("\n--- 测试1.2: WHERE 条件查询 ---")
	sql = "SELECT * FROM users WHERE age > 30"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("   %v\n", row)
	}

	// 测试3: 列裁剪查询
	fmt.Println("\n--- 测试1.3: 列裁剪查询 ---")
	sql = "SELECT name, age FROM users"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行，列数: %d\n", len(result.Rows), len(result.Columns))
	if len(result.Rows) > 0 {
		fmt.Printf("   列名: %v\n", getColumnNames(result.Columns))
	}

	// 测试4: ORDER BY 查询
	fmt.Println("\n--- 测试1.4: ORDER BY 查询 ---")
	sql = "SELECT * FROM users ORDER BY age DESC"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
	if len(result.Rows) > 0 {
		fmt.Printf("   第一行年龄: %v\n", result.Rows[0]["age"])
	}

	// 测试5: LIMIT 查询（Limit 下推）
	fmt.Println("\n--- 测试1.5: LIMIT 查询 ---")
	sql = "SELECT * FROM users LIMIT 2"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行（应限制为2）\n", len(result.Rows))

	// 测试6: 复合查询
	fmt.Println("\n--- 测试1.6: 复合查询 ---")
	sql = "SELECT name, city FROM users WHERE age >= 25 AND age <= 32 ORDER BY name LIMIT 3"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("   %v\n", row)
	}
}

// testUnoptimizedQuery 测试禁用优化器的查询
func testUnoptimizedQuery(executor *optimizer.OptimizedExecutor) {
	fmt.Println("\n【测试2】禁用优化器执行查询")

	// 禁用优化器
	executor.SetUseOptimizer(false)

	adapter := parser.NewSQLAdapter()

	// 测试: 基本查询
	fmt.Println("\n--- 测试2.1: 基本查询（无优化） ---")
	sql := "SELECT * FROM users"
	result, err := parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))

	// 测试: WHERE 条件查询
	fmt.Println("\n--- 测试2.2: WHERE 条件查询（无优化） ---")
	sql = "SELECT * FROM users WHERE city = 'Beijing'"
	result, err = parseAndExecute(executor, adapter, sql)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("   %v\n", row)
	}

	// 重新启用优化器
	executor.SetUseOptimizer(true)
}

// parseAndExecute 解析并执行 SQL
func parseAndExecute(executor *optimizer.OptimizedExecutor, adapter *parser.SQLAdapter, sql string) (*resource.QueryResult, error) {
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("解析失败: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("解析失败: %s", parseResult.Error)
	}

	if parseResult.Statement.Type != parser.SQLTypeSelect {
		return nil, fmt.Errorf("仅支持 SELECT 查询")
	}

	return executor.ExecuteSelect(context.Background(), parseResult.Statement.Select)
}

// getColumnNames 获取列名列表
func getColumnNames(columns []resource.ColumnInfo) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}
