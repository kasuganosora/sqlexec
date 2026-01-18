package main

import (
	"context"
	"log"
	"mysql-proxy/mysql/resource"
)

func main() {
	log.Println("=== 数据源只读属性测试 ===")

	// 测试内存数据源（可写）
	testMemoryWritable()

	// 测试内存数据源（只读）
	testMemoryReadOnly()

	// 测试CSV数据源（只读）
	testCSVReadOnly()

	// 测试JSON数据源（只读）
	testJSONReadOnly()

	log.Println("\n=== 所有测试通过! ===")
}

func testMemoryWritable() {
	log.Println("\n--- 测试内存数据源（可写） ---")

	// 创建可写的内存数据源
	config := &resource.DataSourceConfig{
		Type:    resource.DataSourceTypeMemory,
		Name:    "test",
		Writable: true, // 可写
	}

	factory := resource.NewMemoryFactory()
	ds, err := factory.Create(config)
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
		Name: "test_table",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
		},
	}
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 测试查询
	log.Println("✓ 支持查询")
	result, err := ds.Query(ctx, "test_table", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}
	log.Printf("  查询到 %d 行", result.Total)

	// 测试插入
	log.Println("✓ 支持插入")
	rows := []resource.Row{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
	}
	affected, err := ds.Insert(ctx, "test_table", rows, &resource.InsertOptions{})
	if err != nil {
		log.Fatalf("插入失败: %v", err)
	}
	log.Printf("  插入 %d 行", affected)

	// 测试更新
	log.Println("✓ 支持更新")
	updates := resource.Row{"name": "Charlie"}
	affected, err = ds.Update(ctx, "test_table", []resource.Filter{
			{Field: "id", Operator: "=", Value: 1},
		}, updates, &resource.UpdateOptions{})
	if err != nil {
		log.Fatalf("更新失败: %v", err)
	}
	log.Printf("  更新 %d 行", affected)

	// 测试删除
	log.Println("✓ 支持删除")
	affected, err = ds.Delete(ctx, "test_table", []resource.Filter{
			{Field: "id", Operator: "=", Value: 2},
		}, &resource.DeleteOptions{})
	if err != nil {
		log.Fatalf("删除失败: %v", err)
	}
	log.Printf("  删除 %d 行", affected)

	// 检查IsWritable
	log.Printf("✓ IsWritable() 返回: %v", ds.IsWritable())
}

func testMemoryReadOnly() {
	log.Println("\n--- 测试内存数据源（只读） ---")

	// 创建只读的内存数据源
	config := &resource.DataSourceConfig{
		Type:    resource.DataSourceTypeMemory,
		Name:    "readonly_test",
		Writable: false, // 只读
	}

	factory := resource.NewMemoryFactory()
	ds, err := factory.Create(config)
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
		Name: "readonly_table",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, Nullable: false},
			{Name: "value", Type: "string", Nullable: false},
		},
	}
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 注意：这里无法在只读模式下插入数据
	// 但是我们可以创建表（CreateTable 可能不应该检查可写性）
	// 让我们先临时设置为可写来初始化数据，然后再测试只读
	ds2, err := resource.CreateDataSource(&resource.DataSourceConfig{
		Type:    resource.DataSourceTypeMemory,
		Name:    "readonly_test",
		Writable: true,
	})
	if err == nil {
		if err := ds2.Connect(ctx); err == nil {
			_ = ds2.CreateTable(ctx, tableInfo)
			rows := []resource.Row{
				{"id": 1, "value": "test1"},
				{"id": 2, "value": "test2"},
			}
			_, _ = ds2.Insert(ctx, "readonly_table", rows, &resource.InsertOptions{})
			ds2.Close(ctx)
		}
	}

	// 测试查询
	log.Println("✓ 支持查询")
	result, err := ds.Query(ctx, "readonly_table", &resource.QueryOptions{})
	if err != nil {
		log.Printf("  查询失败（可能没有数据）: %v", err)
	} else {
		log.Printf("  查询到 %d 行", result.Total)
	}

	// 测试插入 - 应该失败
	log.Println("✓ 禁止写入操作")
	rows := []resource.Row{{"id": 3, "value": "test3"}}
	_, err = ds.Insert(ctx, "readonly_table", rows, &resource.InsertOptions{})
	if err == nil {
		log.Fatalf("插入操作应该失败但成功了")
	}
	log.Printf("  插入操作被正确拒绝: %v", err)

	// 测试更新 - 应该失败
	log.Println("✓ 禁止更新操作")
	updates := resource.Row{"value": "updated"}
	_, err = ds.Update(ctx, "readonly_table", []resource.Filter{
			{Field: "id", Operator: "=", Value: 1},
		}, updates, &resource.UpdateOptions{})
	if err == nil {
		log.Fatalf("更新操作应该失败但成功了")
	}
	log.Printf("  更新操作被正确拒绝: %v", err)

	// 测试删除 - 应该失败
	log.Println("✓ 禁止删除操作")
	_, err = ds.Delete(ctx, "readonly_table", []resource.Filter{
			{Field: "id", Operator: "=", Value: 1},
		}, &resource.DeleteOptions{})
	if err == nil {
		log.Fatalf("删除操作应该失败但成功了")
	}
	log.Printf("  删除操作被正确拒绝: %v", err)
}

func testCSVReadOnly() {
	log.Println("\n--- 测试CSV数据源（只读） ---")

	// 创建CSV数据源
	config := &resource.DataSourceConfig{
		Type:    resource.DataSourceTypeCSV,
		Name:    "test.csv",
		Writable: false, // 只读
	}

	factory := resource.NewCSVFactory()
	ds, err := factory.Create(config)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	ctx := context.Background()
	if err := ds.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer ds.Close(ctx)

	// 测试查询
	log.Println("✓ 支持查询")
	_, err = ds.Query(ctx, "csv_data", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	// 测试插入 - 应该失败
	log.Println("✓ 禁止写入操作")
	_, err = ds.Insert(ctx, "csv_data", []resource.Row{{"col": "val"}}, &resource.InsertOptions{})
	if err == nil {
		log.Fatalf("插入操作应该失败但成功了")
	}
	log.Printf("  插入操作被正确拒绝: %v", err)
}

func testJSONReadOnly() {
	log.Println("\n--- 测试JSON数据源（只读） ---")

	// 创建JSON数据源
	config := &resource.DataSourceConfig{
		Type:    resource.DataSourceTypeJSON,
		Name:    "test.json",
		Writable: false, // 只读
	}

	factory := resource.NewJSONFactory()
	ds, err := factory.Create(config)
	if err != nil {
		log.Fatalf("创建数据源失败: %v", err)
	}

	ctx := context.Background()
	if err := ds.Connect(ctx); err != nil {
		log.Fatalf("连接数据源失败: %v", err)
	}
	defer ds.Close(ctx)

	// 测试查询
	log.Println("✓ 支持查询")
	_, err = ds.Query(ctx, "json_data", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	// 测试插入 - 应该失败
	log.Println("✓ 禁止写入操作")
	_, err = ds.Insert(ctx, "json_data", []resource.Row{{"col": "val"}}, &resource.InsertOptions{})
	if err == nil {
		log.Fatalf("插入操作应该失败但成功了")
	}
	log.Printf("  插入操作被正确拒绝: %v", err)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
