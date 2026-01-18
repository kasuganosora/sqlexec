package main

import (
	"context"
	"fmt"
	"log"

	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== 简化测试 ===\n")

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
		{
			"id":      2,
			"name":    "Bob",
			"_hidden": "secret2",
			resource.TTLField: -1,
		},
	}

	_, err = ds.Insert(ctx, "users", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	// 测试 select *
	fmt.Println("测试: SELECT * (SelectAll = true)")
	result, err := ds.Query(ctx, "users", &resource.QueryOptions{SelectAll: true})
	if err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("  返回列数: %d\n", len(result.Columns))
		fmt.Println("  列名:")
		for _, col := range result.Columns {
			fmt.Printf("    - %s\n", col.Name)
		}
		fmt.Printf("  返回行数: %d\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Println("  第一行数据:")
			for k, v := range result.Rows[0] {
				fmt.Printf("    %s: %v\n", k, v)
			}
		}
	}

	fmt.Println("\n测试完成")
}
