package main

import (
	"context"
	"fmt"
	"log"

	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 单表 select * 过滤测试 ===\n")

	// 创建内存数据源
	config := &resource.DataSourceConfig{
		Type:     resource.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	}

	ds, err := resource.CreateDataSource(config)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	ctx := context.Background()
	if err := ds.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer ds.Close(ctx)

	// 创建表
	tableInfo := &resource.TableInfo{
		Name:   "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "_hidden", Type: "string", Nullable: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 插入数据
	rows := []resource.Row{
		{
			"id":      1,
			"name":    "Alice",
			"_hidden": "secret",
			resource.TTLField: -1,
		},
	}

	_, err = ds.Insert(ctx, "users", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	// 测试 select *（使用 QueryOptions.SelectAll = true）
	fmt.Println("测试 1: QueryOptions.SelectAll = true")
	result1, err := ds.Query(ctx, "users", &resource.QueryOptions{SelectAll: true})
	if err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("  返回列数: %d\n", len(result1.Columns))
		fmt.Println("  列名:")
		for _, col := range result1.Columns {
			fmt.Printf("    - %s\n", col.Name)
		}
		fmt.Printf("  返回行数: %d\n", len(result1.Rows))
		if len(result1.Rows) > 0 {
			fmt.Println("  第一行数据中的所有键:")
			for k := range result1.Rows[0] {
				fmt.Printf("    - %s\n", k)
			}
			fmt.Println("  第一行数据:")
			for k, v := range result1.Rows[0] {
				fmt.Printf("    %s: %v\n", k, v)
			}
		}
	}

	// 测试 select *（使用 QueryOptions.SelectAll = false，但模拟 JOIN 的行为）
	fmt.Println("\n测试 2: QueryOptions.SelectAll = false（模拟 JOIN）")
	result2, err := ds.Query(ctx, "users", &resource.QueryOptions{SelectAll: false})
	if err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("  返回列数: %d\n", len(result2.Columns))
		fmt.Println("  列名:")
		for _, col := range result2.Columns {
			fmt.Printf("    - %s\n", col.Name)
		}
		fmt.Printf("  返回行数: %d\n", len(result2.Rows))
		if len(result2.Rows) > 0 {
			fmt.Println("  第一行数据中的所有键:")
			for k := range result2.Rows[0] {
				fmt.Printf("    - %s\n", k)
			}
			fmt.Println("  第一行数据:")
			for k, v := range result2.Rows[0] {
				fmt.Printf("    %s: %v\n", k, v)
			}
		}
	}

	fmt.Println("\n测试完成")
}
