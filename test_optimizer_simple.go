package main

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 简单优化器测试 ===\n")

	// 1. 创建内存数据源
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		fmt.Printf("创建数据源失败: %v\n", err)
		return
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			fmt.Printf("连接数据源失败: %v\n", err)
			return
		}
	}

	// 2. 创建测试表
	tableInfo := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "city", Type: "string", Nullable: true},
		},
	}

	err = dataSource.CreateTable(context.Background(), tableInfo)
	if err != nil {
		fmt.Printf("创建表失败: %v\n", err)
		return
	}
	fmt.Println("✅ 创建表 users 成功")

	// 3. 插入测试数据
	rows := []resource.Row{
		{"id": int64(1), "name": "Alice", "age": int64(25), "city": "Beijing"},
		{"id": int64(2), "name": "Bob", "age": int64(30), "city": "Shanghai"},
		{"id": int64(3), "name": "Charlie", "age": int64(35), "city": "Guangzhou"},
		{"id": int64(4), "name": "David", "age": int64(28), "city": "Shenzhen"},
		{"id": int64(5), "name": "Eve", "age": int64(32), "city": "Beijing"},
	}

	_, err = dataSource.Insert(context.Background(), "users", rows, &resource.InsertOptions{Replace: false})
	if err != nil {
		fmt.Printf("插入数据失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 插入 %d 行数据\n\n", len(rows))

	// 4. 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(dataSource, true)

	// 5. 测试基本查询
	fmt.Println("--- 测试: 基本查询 ---")
	testSQL := "SELECT * FROM users"
	result, err := executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n\n", len(result.Rows))
	}

	// 6. 测试WHERE条件
	fmt.Println("--- 测试: WHERE age > 30 ---")
	testSQL = "SELECT * FROM users WHERE age > 30"
	result, err = executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("   %v\n", row)
		}
		fmt.Println()
	}

	// 7. 测试LIMIT
	fmt.Println("--- 测试: LIMIT 2 ---")
	testSQL = "SELECT * FROM users LIMIT 2"
	result, err = executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n\n", len(result.Rows))
	}

	// 8. 测试禁用优化器
	fmt.Println("--- 测试: 禁用优化器 ---")
	executor.SetUseOptimizer(false)
	testSQL = "SELECT name, city FROM users WHERE city = 'Beijing'"
	result, err = executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("   %v\n", row)
		}
	}

	fmt.Println("\n=== 测试完成 ===")
}

func executeSQL(executor *optimizer.OptimizedExecutor, sql string) (*resource.QueryResult, error) {
	adapter := parser.NewSQLAdapter()
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
