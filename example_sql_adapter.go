package main

import (
	"context"
	"fmt"
	"log"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== SQL 解析适配器演示 ===\n")

	// 创建内存数据源
	dsConfig := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "test",
	}
	dataSource, err := resource.CreateDataSource(dsConfig)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	// 连接数据源
	ctx := context.Background()
	if err := dataSource.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer dataSource.Close(ctx)

	// 创建查询构建器
	builder := parser.NewQueryBuilder(dataSource)

	// 测试 DDL 操作
	fmt.Println("1. 测试 CREATE TABLE")
	testCreateTable(ctx, builder)

	// 测试 INSERT 操作
	fmt.Println("\n2. 测试 INSERT")
	testInsert(ctx, builder)

	// 测试 SELECT 操作
	fmt.Println("\n3. 测试 SELECT")
	testSelect(ctx, builder)

	// 测试 UPDATE 操作
	fmt.Println("\n4. 测试 UPDATE")
	testUpdate(ctx, builder)

	// 测试 DELETE 操作
	fmt.Println("\n5. 测试 DELETE")
	testDelete(ctx, builder)

	fmt.Println("\n=== 演示完成 ===")
}

func testCreateTable(ctx context.Context, builder *parser.QueryBuilder) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), age INT, status VARCHAR(50))"
	result, err := builder.BuildAndExecute(ctx, sql)
	if err != nil {
		fmt.Printf("  ✗ 失败: %v\n", err)
		return
	}
	fmt.Printf("  ✓ 表创建成功\n")
	_ = result
}

func testInsert(ctx context.Context, builder *parser.QueryBuilder) {
	testCases := []string{
		"INSERT INTO users (name, age, status) VALUES ('Alice', 25, 'active')",
		"INSERT INTO users (name, age, status) VALUES ('Bob', 30, 'inactive')",
		"INSERT INTO users (name, age, status) VALUES ('Charlie', 35, 'active')",
	}

	for _, sql := range testCases {
		result, err := builder.BuildAndExecute(ctx, sql)
		if err != nil {
			fmt.Printf("  ✗ 失败: %v\n", err)
			continue
		}
		fmt.Printf("  ✓ 插入成功 (影响行数: %d)\n", result.Total)
	}
}

func testSelect(ctx context.Context, builder *parser.QueryBuilder) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "查询所有用户",
			sql:  "SELECT * FROM users",
		},
		{
			name: "带条件查询",
			sql:  "SELECT * FROM users WHERE age > 25",
		},
		{
			name: "排序查询",
			sql:  "SELECT * FROM users ORDER BY age DESC",
		},
		{
			name: "限制查询",
			sql:  "SELECT * FROM users LIMIT 2",
		},
	}

	for _, tc := range testCases {
		result, err := builder.BuildAndExecute(ctx, tc.sql)
		if err != nil {
			fmt.Printf("  ✗ %s 失败: %v\n", tc.name, err)
			continue
		}

		fmt.Printf("  ✓ %s (返回 %d 行)\n", tc.name, len(result.Rows))

		// 打印结果
		for _, row := range result.Rows {
			fmt.Printf("    - %v\n", row)
		}
	}
}

func testUpdate(ctx context.Context, builder *parser.QueryBuilder) {
	sql := "UPDATE users SET status = 'active' WHERE age > 25"
	result, err := builder.BuildAndExecute(ctx, sql)
	if err != nil {
		fmt.Printf("  ✗ 失败: %v\n", err)
		return
	}
	fmt.Printf("  ✓ 更新成功 (影响行数: %d)\n", result.Total)
}

func testDelete(ctx context.Context, builder *parser.QueryBuilder) {
	sql := "DELETE FROM users WHERE age < 30"
	result, err := builder.BuildAndExecute(ctx, sql)
	if err != nil {
		fmt.Printf("  ✗ 失败: %v\n", err)
		return
	}
	fmt.Printf("  ✓ 删除成功 (影响行数: %d)\n", result.Total)
}
