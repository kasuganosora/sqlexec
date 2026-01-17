package main

import (
	"context"
	"fmt"
	"log"

	"github.com/pingcap/tidb/pkg/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	ctx := context.Background()

	fmt.Println("=== TiDB Parser 简单集成演示 ===\n")

	// 演示 1: 使用 TiDB Parser 解析 SQL
	demoTiDBParser()

	// 演示 2: 将解析结果应用到数据源
	demoParserWithDataSource(ctx)

	fmt.Println("\n=== 演示完成 ===")
}

// demoTiDBParser 演示 TiDB Parser
func demoTiDBParser() {
	fmt.Println("--- 演示 1: TiDB Parser ---")

	// 创建解析器
	p := parser.New()

	// 解析 SQL 语句
	sql := "SELECT id, name, email FROM users WHERE age >= 25"
	stmtNodes, warns, err := p.Parse(sql, "", "")
	if err != nil {
		log.Fatalf("解析SQL失败: %v", err)
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("解析到 %d 条语句\n", len(stmtNodes))
	fmt.Printf("警告数量: %d\n", len(warns))

	if len(warns) > 0 {
		for _, w := range warns {
			fmt.Printf("警告: %s\n", w.Error())
		}
	}

	// 打印解析结果
	for i, stmt := range stmtNodes {
		fmt.Printf("\n语句 %d: %s\n", i+1, stmt.Text())
	}

	// 解析更多类型的 SQL
	sqls := []string{
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET age = 26 WHERE id = 1",
		"DELETE FROM users WHERE age < 18",
		"CREATE TABLE products (id INT, name VARCHAR(255))",
		"DROP TABLE old_table",
	}

	fmt.Println("\n--- 解析多种 SQL 语句 ---")
	for _, sql := range sqls {
		stmtNodes, _, err := p.Parse(sql, "", "")
		if err != nil {
			fmt.Printf("✗ 解析失败: %s\n", sql)
			continue
		}
		if len(stmtNodes) > 0 {
			fmt.Printf("✓ 解析成功: %s (类型: %s)\n", sql[:30]+"...", stmtNodes[0].Text())
		}
	}

	// 测试复杂的 JOIN 查询
	fmt.Println("\n--- 解析 JOIN 查询 ---")
	joinSQL := "SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age >= 25"
	stmtNodes, _, err = p.Parse(joinSQL, "", "")
	if err != nil {
		log.Fatalf("解析JOIN失败: %v", err)
	}
	if len(stmtNodes) > 0 {
		fmt.Printf("✓ JOIN查询解析成功: %s\n", stmtNodes[0].Text())
	}

	// 测试子查询
	fmt.Println("\n--- 解析子查询 ---")
	subquerySQL := "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 1000)"
	stmtNodes, _, err = p.Parse(subquerySQL, "", "")
	if err != nil {
		log.Fatalf("解析子查询失败: %v", err)
	}
	if len(stmtNodes) > 0 {
		fmt.Printf("✓ 子查询解析成功: %s\n", stmtNodes[0].Text())
	}

	fmt.Println()
}

// demoParserWithDataSource 演示解析器与数据源的配合使用
func demoParserWithDataSource(ctx context.Context) {
	fmt.Println("--- 演示 2: 解析器与数据源配合 ---")

	// 创建数据源
	config := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "test_db",
	}

	ds, err := resource.CreateDataSource(config)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	if err := ds.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer ds.Close(ctx)

	// 创建测试表
	tableInfo := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Nullable: false, Primary: true},
			{Name: "name", Type: "varchar", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}
	fmt.Println("✓ 创建表 users")

	// 插入测试数据
	rows := []resource.Row{
		{"name": "Alice", "email": "alice@example.com", "age": 25},
		{"name": "Bob", "email": "bob@example.com", "age": 30},
		{"name": "Charlie", "email": "charlie@example.com", "age": 35},
	}

	inserted, err := ds.Insert(ctx, "users", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}
	fmt.Printf("✓ 插入了 %d 行数据\n", inserted)

	// 使用 TiDB Parser 解析查询
	p := parser.New()

	selectSQL := "SELECT * FROM users WHERE age >= 30"
	stmtNodes, _, err := p.Parse(selectSQL, "", "")
	if err != nil {
		log.Fatalf("解析SQL失败: %v", err)
	}

	fmt.Printf("\n解析SQL: %s\n", selectSQL)
	fmt.Printf("语句类型: %s\n", stmtNodes[0].Text())

	// 根据解析结果执行查询
	// 注意：这里需要手动提取表名和条件
	// 完整的实现需要遍历 AST 树

	// 简化实现：直接使用数据源查询
	result, err := ds.Query(ctx, "users", &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "age", Operator: ">=", Value: 30},
		},
		OrderBy: "name",
		Order:   "ASC",
	})
	if err != nil {
		log.Fatalf("执行查询失败: %v", err)
	}

	fmt.Printf("✓ 查询到 %d 行数据:\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  - ID: %v, Name: %v, Email: %v, Age: %v\n",
			row["id"], row["name"], row["email"], row["age"])
	}

	fmt.Println()
}

// demoTiDBFeatures 演示 TiDB Parser 的特性
func demoTiDBFeatures() {
	fmt.Println("--- TiDB Parser 特性演示 ---")

	p := parser.New()

	// 测试 TiDB 支持的各种 SQL 语法
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "窗口函数",
			sql:  "SELECT name, age, RANK() OVER (ORDER BY age DESC) as rank FROM users",
		},
		{
			name: "CTE (Common Table Expression)",
			sql:  "WITH ranked_users AS (SELECT name, age, RANK() OVER (ORDER BY age) as r FROM users) SELECT * FROM ranked_users WHERE r <= 10",
		},
		{
			name: "CASE 表达式",
			sql:  "SELECT name, CASE WHEN age >= 60 THEN 'senior' WHEN age >= 30 THEN 'middle' ELSE 'junior' END as age_group FROM users",
		},
		{
			name: "UNION",
			sql:  "SELECT name FROM users UNION SELECT name FROM employees",
		},
		{
			name: "GROUP BY + HAVING",
			sql:  "SELECT age, COUNT(*) as cnt FROM users GROUP BY age HAVING COUNT(*) > 1",
		},
	}

	fmt.Println("测试 TiDB Parser 支持的高级特性:\n")
	for _, tc := range testCases {
		stmtNodes, _, err := p.Parse(tc.sql, "", "")
		status := "✗"
		if err == nil && len(stmtNodes) > 0 {
			status = "✓"
		}
		fmt.Printf("%s %s\n", status, tc.name)
		if err != nil {
			fmt.Printf("  错误: %v\n", err)
		}
	}

	fmt.Println("\n注意: 虽然可以解析这些高级特性，但我们的数据源接口需要扩展才能完全支持它们。")
}
