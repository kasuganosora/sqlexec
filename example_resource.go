package main

import (
	"context"
	"fmt"
	"log"
	"mysql-proxy/mysql/resource"
)

func main() {
	ctx := context.Background()

	fmt.Println("=== 数据源接口演示 ===\n")

	// 演示 1: 使用内存数据源
	demoMemorySource(ctx)

	// 演示 2: 使用数据源管理器
	demoDataSourceManager(ctx)

	fmt.Println("\n=== 演示完成 ===")
}

// demoMemorySource 演示内存数据源的使用
func demoMemorySource(ctx context.Context) {
	fmt.Println("--- 演示 1: 内存数据源 ---")

	// 创建内存数据源
	config := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "demo_db",
	}

	ds, err := resource.CreateDataSource(config)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	// 连接数据源
	if err := ds.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer ds.Close(ctx)

	// 创建表
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

	// 插入数据
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

	// 查询数据
	result, err := ds.Query(ctx, "users", &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "age", Operator: ">=", Value: 30},
		},
		OrderBy: "age",
		Order:   "ASC",
	})
	if err != nil {
		log.Fatalf("查询数据失败: %v", err)
	}

	fmt.Printf("✓ 查询到 %d 行数据 (age >= 30):\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  - ID: %v, Name: %v, Email: %v, Age: %v\n",
			row["id"], row["name"], row["email"], row["age"])
	}

	// 更新数据
	updates := resource.Row{"age": 31}
	updated, err := ds.Update(ctx, "users",
		[]resource.Filter{{Field: "name", Operator: "=", Value: "Bob"}},
		updates, nil)
	if err != nil {
		log.Fatalf("更新数据失败: %v", err)
	}
	fmt.Printf("✓ 更新了 %d 行数据\n", updated)

	// 删除数据
	deleted, err := ds.Delete(ctx, "users",
		[]resource.Filter{{Field: "age", Operator: "<", Value: 30}},
		nil)
	if err != nil {
		log.Fatalf("删除数据失败: %v", err)
	}
	fmt.Printf("✓ 删除了 %d 行数据\n", deleted)

	// 获取表列表
	tables, err := ds.GetTables(ctx)
	if err != nil {
		log.Fatalf("获取表列表失败: %v", err)
	}
	fmt.Printf("✓ 数据库中的表: %v\n", tables)

	fmt.Println()
}

// demoDataSourceManager 演示数据源管理器的使用
func demoDataSourceManager(ctx context.Context) {
	fmt.Println("--- 演示 2: 数据源管理器 ---")

	// 创建数据源管理器
	manager := resource.NewDataSourceManager()

	// 创建并注册内存数据源
	memoryConfig := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "cache_db",
	}

	if err := manager.CreateAndRegister(ctx, "cache", memoryConfig); err != nil {
		log.Fatalf("注册数据源失败: %v", err)
	}
	fmt.Println("✓ 注册数据源: cache")

	// 创建并注册另一个内存数据源
	memoryConfig2 := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "session_db",
	}

	if err := manager.CreateAndRegister(ctx, "session", memoryConfig2); err != nil {
		log.Fatalf("注册数据源失败: %v", err)
	}
	fmt.Println("✓ 注册数据源: session")

	// 设置默认数据源
	if err := manager.SetDefault("cache"); err != nil {
		log.Fatalf("设置默认数据源失败: %v", err)
	}
	fmt.Println("✓ 设置默认数据源: cache")

	// 列出所有数据源
	sources := manager.List()
	fmt.Printf("✓ 已注册的数据源: %v\n", sources)

	// 获取数据源状态
	status := manager.GetStatus()
	for name, connected := range status {
		fmt.Printf("  - %s: %v\n", name, connected)
	}

	// 获取默认数据源
	defaultDS, err := manager.GetDefault()
	if err != nil {
		log.Fatalf("获取默认数据源失败: %v", err)
	}
	fmt.Printf("✓ 默认数据源类型: %T\n", defaultDS)

	// 在cache数据源中创建表
	tableInfo := &resource.TableInfo{
		Name: "cache_items",
		Columns: []resource.ColumnInfo{
			{Name: "key", Type: "varchar", Nullable: false, Primary: true},
			{Name: "value", Type: "varchar", Nullable: false},
		},
	}

	if err := manager.CreateTable(ctx, "cache", tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}
	fmt.Println("✓ 在cache数据源中创建表: cache_items")

	// 向cache数据源插入数据
	rows := []resource.Row{
		{"key": "user:1", "value": "Alice"},
		{"key": "user:2", "value": "Bob"},
	}

	inserted, err := manager.Insert(ctx, "cache", "cache_items", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}
	fmt.Printf("✓ 向cache数据源插入 %d 行数据\n", inserted)

	// 查询cache数据源的数据
	result, err := manager.Query(ctx, "cache", "cache_items", nil)
	if err != nil {
		log.Fatalf("查询数据失败: %v", err)
	}

	fmt.Printf("✓ 从cache数据源查询到 %d 行数据:\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  - Key: %v, Value: %v\n", row["key"], row["value"])
	}

	// 清理
	if err := manager.CloseAll(ctx); err != nil {
		log.Printf("关闭所有数据源时出错: %v", err)
	}
	fmt.Println("✓ 关闭所有数据源")

	fmt.Println()
}
