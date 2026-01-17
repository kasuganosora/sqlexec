package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 性能基准测试（最终版）===\n")

	// 创建数据源
	factory := resource.NewMemoryFactory()
	dataSource, err := factory.Create(&resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "benchmark",
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := dataSource.Connect(ctx); err != nil {
		panic(err)
	}
	defer dataSource.Close(ctx)

	// 生成测试数据
	fmt.Println("1. 生成测试数据...")
	generateTestData(ctx, dataSource)

	// 创建查询构建器
	builder := parser.NewQueryBuilder(dataSource)

	// 运行性能测试
	fmt.Println("\n2. 运行性能测试...\n")

	// 测试1: 简单查询
	benchmarkSimpleQuery(ctx, builder)

	// 测试2: 过滤查询
	benchmarkFilterQuery(ctx, builder)

	// 测试3: 排序查询
	benchmarkSortQuery(ctx, builder)

	// 测试4: LIMIT查询
	benchmarkLimitQuery(ctx, builder)

	fmt.Println("\n=== 所有测试完成 ===")
}

func generateTestData(ctx context.Context, dataSource resource.DataSource) {
	// 创建employees表
	employeesTable := &resource.TableInfo{
		Name: "employees",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, Primary: true},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "age", Type: "INTEGER", Nullable: false},
			{Name: "salary", Type: "FLOAT", Nullable: false},
			{Name: "department_id", Type: "INTEGER", Nullable: false},
		},
	}

	if err := dataSource.CreateTable(ctx, employeesTable); err != nil {
		panic(err)
	}

	// 创建departments表
	deptsTable := &resource.TableInfo{
		Name: "departments",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, Primary: true},
			{Name: "name", Type: "VARCHAR", Nullable: false},
			{Name: "budget", Type: "FLOAT", Nullable: false},
		},
	}

	if err := dataSource.CreateTable(ctx, deptsTable); err != nil {
		panic(err)
	}

	// 生成departments数据（5个部门）
	fmt.Printf("   生成部门数据...")
	departments := []string{"IT", "HR", "Finance", "Sales", "Marketing"}
	for i, deptName := range departments {
		row := resource.Row{
			"id":     i + 1,
			"name":   deptName,
			"budget": float64((i + 1) * 100000),
		}
		_, err := dataSource.Insert(ctx, "departments", []resource.Row{row}, nil)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println(" 完成")

	// 生成employees数据（10000行）
	fmt.Printf("   生成10000行员工数据...")
	rand.Seed(time.Now().UnixNano())
	start := time.Now()

	for i := 0; i < 10000; i++ {
		row := resource.Row{
			"id":           i + 1,
			"name":         fmt.Sprintf("Employee_%d", rand.Intn(1000)),
			"age":          rand.Intn(40) + 20, // 20-60岁
			"salary":       float64(rand.Intn(50000) + 30000),
			"department_id": rand.Intn(5) + 1, // 1-5
		}

		_, err := dataSource.Insert(ctx, "employees", []resource.Row{row}, nil)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf(" 完成 (用时: %v)\n", time.Since(start))
}

func benchmarkSimpleQuery(ctx context.Context, builder *parser.QueryBuilder) {
	fmt.Println("--- 测试1: 简单查询 ---")

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "SELECT * FROM employees",
			sql:  "SELECT * FROM employees",
		},
		{
			name: "SELECT id, name, salary FROM employees",
			sql:  "SELECT id, name, salary FROM employees",
		},
	}

	for _, test := range tests {
		iterations := 3
		totalTime := time.Duration(0)

		for i := 0; i < iterations; i++ {
			start := time.Now()
			result, err := builder.BuildAndExecute(ctx, test.sql)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("   %s: 错误 - %v\n", test.name, err)
				break
			}

			totalTime += duration

			if i == 0 {
				fmt.Printf("   %s: %v (返回 %d 行)\n", test.name, duration, result.Total)
			}
		}

		avgTime := totalTime / time.Duration(iterations)
		fmt.Printf("   平均: %v, 吞吐量: %.0f 行/秒\n", avgTime, float64(10000)/avgTime.Seconds())
	}
}

func benchmarkFilterQuery(ctx context.Context, builder *parser.QueryBuilder) {
	fmt.Println("\n--- 测试2: 过滤查询 ---")

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "WHERE age > 40",
			sql:  "SELECT * FROM employees WHERE age > 40",
		},
		{
			name: "WHERE age > 30 AND age < 50",
			sql:  "SELECT * FROM employees WHERE age > 30 AND age < 50",
		},
		{
			name: "WHERE salary >= 40000 AND salary <= 60000",
			sql:  "SELECT * FROM employees WHERE salary >= 40000 AND salary <= 60000",
		},
		{
			name: "WHERE department_id = 1",
			sql:  "SELECT * FROM employees WHERE department_id = 1",
		},
	}

	for _, test := range tests {
		iterations := 3
		totalTime := time.Duration(0)
		var result *resource.QueryResult
		var err error

		for i := 0; i < iterations; i++ {
			start := time.Now()
			result, err = builder.BuildAndExecute(ctx, test.sql)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("   %s: 错误 - %v\n", test.name, err)
				break
			}

			totalTime += duration
		}

		if err != nil {
			continue
		}

		avgTime := totalTime / time.Duration(iterations)
		fmt.Printf("   %s: %v (返回 %d 行)\n", test.name, avgTime, result.Total)
	}
}

func benchmarkSortQuery(ctx context.Context, builder *parser.QueryBuilder) {
	fmt.Println("\n--- 测试3: 排序查询 ---")

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ORDER BY age ASC",
			sql:  "SELECT * FROM employees ORDER BY age ASC",
		},
		{
			name: "ORDER BY salary DESC",
			sql:  "SELECT * FROM employees ORDER BY salary DESC",
		},
		{
			name: "ORDER BY with LIMIT",
			sql:  "SELECT * FROM employees ORDER BY salary DESC LIMIT 100",
		},
	}

	for _, test := range tests {
		iterations := 3
		totalTime := time.Duration(0)
		var result *resource.QueryResult
		var err error

		for i := 0; i < iterations; i++ {
			start := time.Now()
			result, err = builder.BuildAndExecute(ctx, test.sql)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("   %s: 错误 - %v\n", test.name, err)
				break
			}

			totalTime += duration
		}

		if err != nil {
			continue
		}

		avgTime := totalTime / time.Duration(iterations)
		fmt.Printf("   %s: %v (返回 %d 行)\n", test.name, avgTime, result.Total)
	}
}

func benchmarkLimitQuery(ctx context.Context, builder *parser.QueryBuilder) {
	fmt.Println("\n--- 测试4: LIMIT查询 ---")

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "LIMIT 10",
			sql:  "SELECT * FROM employees LIMIT 10",
		},
		{
			name: "LIMIT 100",
			sql:  "SELECT * FROM employees LIMIT 100",
		},
		{
			name: "LIMIT 1000",
			sql:  "SELECT * FROM employees LIMIT 1000",
		},
	}

	for _, test := range tests {
		iterations := 5
		totalTime := time.Duration(0)
		var result *resource.QueryResult
		var err error

		for i := 0; i < iterations; i++ {
			start := time.Now()
			result, err = builder.BuildAndExecute(ctx, test.sql)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("   %s: 错误 - %v\n", test.name, err)
				break
			}

			totalTime += duration
		}

		if err != nil {
			continue
		}

		avgTime := totalTime / time.Duration(iterations)
		fmt.Printf("   %s: %v (返回 %d 行)\n", test.name, avgTime, result.Total)
	}
}
