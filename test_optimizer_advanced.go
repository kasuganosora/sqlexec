package main

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 优化器高级功能测试 ===\n")

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

	ctx := context.Background()

	// 2. 创建表
	tableInfo := &resource.TableInfo{
		Name: "products",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "price", Type: "float64", Nullable: true},
			{Name: "category", Type: "string", Nullable: true},
			{Name: "stock", Type: "int64", Nullable: true},
		},
	}

	err = dataSource.CreateTable(ctx, tableInfo)
	if err != nil {
		fmt.Printf("创建表失败: %v\n", err)
		return
	}
	fmt.Println("✅ 创建表 products 成功")

	// 3. 插入测试数据
	rows := []resource.Row{
		{"id": int64(1), "name": "Product A", "price": 100.0, "category": "Electronics", "stock": int64(50)},
		{"id": int64(2), "name": "Product B", "price": 200.0, "category": "Electronics", "stock": int64(30)},
		{"id": int64(3), "name": "Product C", "price": 50.0, "category": "Books", "stock": int64(100)},
		{"id": int64(4), "name": "Product D", "price": 150.0, "category": "Electronics", "stock": int64(20)},
		{"id": int64(5), "name": "Product E", "price": 75.0, "category": "Books", "stock": int64(80)},
	}

	_, err = dataSource.Insert(ctx, "products", rows, &resource.InsertOptions{Replace: false})
	if err != nil {
		fmt.Printf("插入数据失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 插入 %d 行数据\n\n", len(rows))

	// 4. 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(dataSource, true)

	// 测试1: 列裁剪 - 只查询name和price
	fmt.Println("\n--- 测试1: 列裁剪 (SELECT name, price FROM products) ---")
	testSQL := "SELECT name, price FROM products"
	result, err := executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
		fmt.Printf("   列数: %d (应该只有name和price)\n", len(result.Columns))
		for _, row := range result.Rows {
			fmt.Printf("   %v\n", row)
		}
	}

	// 测试2: 列裁剪 + WHERE
	fmt.Println("\n--- 测试2: 列裁剪 + WHERE (SELECT name, price FROM products WHERE price > 100) ---")
	testSQL = "SELECT name, price FROM products WHERE price > 100"
	result, err = executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("   %v\n", row)
		}
	}

	// 测试3: 列裁剪 + LIMIT
	fmt.Println("\n--- 测试3: 列裁剪 + LIMIT (SELECT name, price FROM products LIMIT 2) ---")
	testSQL = "SELECT name, price FROM products LIMIT 2"
	result, err = executeSQL(executor, testSQL)
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n\n", err)
	} else {
		fmt.Printf("✅ 查询成功，返回 %d 行\n", len(result.Rows))
		for _, row := range result.Rows {
			fmt.Printf("   %v\n", row)
		}
	}

	// 测试4: 谓词下推 + 列裁剪
	fmt.Println("\n--- 测试4: 谓词下推 + 列裁剪 (SELECT name FROM products WHERE category = 'Electronics') ---")
	testSQL = "SELECT name FROM products WHERE category = 'Electronics'"
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
	// 1. 解析SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("解析SQL失败: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("解析失败: %s", parseResult.Error)
	}

	if parseResult.Statement.Type != parser.SQLTypeSelect {
		return nil, fmt.Errorf("仅支持 SELECT 查询")
	}

	// 2. 执行查询
	result, err := executor.ExecuteSelect(context.Background(), parseResult.Statement.Select)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %w", err)
	}

	return result, nil
}
