package main

import (
	"context"
	"fmt"
	"log"

	"mysql-proxy/mysql/resource"
)

func main() {
	ctx := context.Background()

	// 创建内存数据源
	source, err := resource.CreateDataSource(&resource.DataSourceConfig{
		Type:    resource.DataSourceTypeMemory,
		Name:    "test_db",
		Writable: true,
	})
	if err != nil {
		log.Fatalf("Failed to create data source: %v", err)
	}

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer source.Close(ctx)

	fmt.Println("=== 测试1: 自动递增和默认值 ===")

	// 创建带自动递增和默认值的表
	usersTable := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{
				Name:         "id",
				Type:         "int64",
				Primary:      true,
				AutoIncrement: true,
			},
			{
				Name:     "name",
				Type:     "string",
				Nullable: false,
			},
			{
				Name:     "email",
				Type:     "string",
				Nullable: false,
				Unique:   true, // 唯一约束
			},
			{
				Name:    "age",
				Type:    "int",
				Default: "18", // 默认值
			},
			{
				Name:     "status",
				Type:     "string",
				Default:  "active", // 默认值
				Nullable: true,
			},
		},
	}

	if err := source.CreateTable(ctx, usersTable); err != nil {
		log.Fatalf("Failed to create users table: %v", err)
	}

	// 插入数据（测试自动递增和默认值）
	users := []resource.Row{
		{"name": "Alice", "email": "alice@example.com"},          // age和status使用默认值
		{"name": "Bob", "email": "bob@example.com", "age": 25},    // status使用默认值
		{"name": "Charlie", "email": "charlie@example.com", "age": 30, "status": "inactive"},
	}

	inserted, err := source.Insert(ctx, "users", users, nil)
	if err != nil {
		log.Fatalf("Failed to insert users: %v", err)
	}
	fmt.Printf("成功插入 %d 条用户记录\n", inserted)

	// 查询数据验证
	result, err := source.Query(ctx, "users", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query users: %v", err)
	}

	fmt.Println("\n用户数据:")
	for i, row := range result.Rows {
		fmt.Printf("  %d. ID=%v, Name=%v, Email=%v, Age=%v, Status=%v\n",
			i+1, row["id"], row["name"], row["email"], row["age"], row["status"])
	}

	fmt.Println("\n=== 测试2: 唯一约束 ===")

	// 尝试插入重复的email（应该失败）
	duplicateUser := resource.Row{
		"name":  "David",
		"email": "alice@example.com", // 与第一条记录的email重复
		"age":   22,
	}

	_, err = source.Insert(ctx, "users", []resource.Row{duplicateUser}, nil)
	if err != nil {
		fmt.Printf("✓ 唯一约束生效: %v\n", err)
	} else {
		fmt.Println("✗ 唯一约束未生效（应该拒绝重复email）")
	}

	fmt.Println("\n=== 测试3: 外键约束 ===")

	// 创建订单表（带外键）
	ordersTable := &resource.TableInfo{
		Name: "orders",
		Columns: []resource.ColumnInfo{
			{
				Name:         "id",
				Type:         "int64",
				Primary:      true,
				AutoIncrement: true,
			},
			{
				Name: "product_name",
				Type: "string",
			},
			{
				Name: "user_id",
				Type: "int64",
				ForeignKey: &resource.ForeignKeyInfo{
					Table:    "users",
					Column:   "id",
					OnDelete: "RESTRICT",
				},
			},
		},
	}

	if err := source.CreateTable(ctx, ordersTable); err != nil {
		log.Fatalf("Failed to create orders table: %v", err)
	}

	// 插入有效订单（user_id存在）
	orders := []resource.Row{
		{"product_name": "Product A", "user_id": 1},
		{"product_name": "Product B", "user_id": 2},
	}

	inserted, err = source.Insert(ctx, "orders", orders, nil)
	if err != nil {
		log.Fatalf("Failed to insert orders: %v", err)
	}
	fmt.Printf("成功插入 %d 条订单记录\n", inserted)

	// 查询订单
	ordersResult, err := source.Query(ctx, "orders", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query orders: %v", err)
	}

	fmt.Println("\n订单数据:")
	for i, row := range ordersResult.Rows {
		fmt.Printf("  %d. ID=%v, Product=%v, UserID=%v\n",
			i+1, row["id"], row["product_name"], row["user_id"])
	}

	// 尝试插入无效订单（user_id不存在）
	invalidOrder := resource.Row{
		"product_name": "Product C",
		"user_id":      999, // 不存在的用户ID
	}

	_, err = source.Insert(ctx, "orders", []resource.Row{invalidOrder}, nil)
	if err != nil {
		fmt.Printf("✓ 外键约束生效: %v\n", err)
	} else {
		fmt.Println("✗ 外键约束未生效（应该拒绝无效的user_id）")
	}

	fmt.Println("\n=== 测试4: 更新时验证唯一约束 ===")

	// 尝试更新email为已存在的值
	_, updateErr := source.Update(ctx, "users",
		[]resource.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		resource.Row{"email": "bob@example.com"},
		nil)
	if updateErr != nil {
		fmt.Printf("✓ 更新时唯一约束生效: %v\n", updateErr)
	} else {
		fmt.Println("✗ 更新时唯一约束未生效")
	}

	// 正常更新（不违反约束）
	_, updateErr = source.Update(ctx, "users",
		[]resource.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		resource.Row{"age": 20},
		nil)
	if updateErr != nil {
		fmt.Printf("更新失败: %v\n", updateErr)
	} else {
		fmt.Println("✓ 正常更新成功")
	}

	// 查询更新后的数据
	result, err = source.Query(ctx, "users", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query users: %v", err)
	}

	fmt.Println("\n更新后的用户数据:")
	for i, row := range result.Rows {
		fmt.Printf("  %d. ID=%v, Name=%v, Email=%v, Age=%v, Status=%v\n",
			i+1, row["id"], row["name"], row["email"], row["age"], row["status"])
	}

	fmt.Println("\n=== 测试5: 级联删除（RESTRICT策略） ===")

	// 尝试删除有订单关联的用户（应该失败）
	_, deleteErr := source.Delete(ctx, "users",
		[]resource.Filter{{Field: "id", Operator: "=", Value: 1}},
		nil)
	if deleteErr != nil {
		fmt.Printf("✓ RESTRICT策略生效（不能删除有订单关联的用户）: %v\n", deleteErr)
	} else {
		fmt.Println("✗ RESTRICT策略未生效")
	}

	// 删除没有订单关联的用户
	_, deleteErr = source.Delete(ctx, "users",
		[]resource.Filter{{Field: "id", Operator: "=", Value: 3}},
		nil)
	if deleteErr != nil {
		fmt.Printf("删除失败: %v\n", deleteErr)
	} else {
		fmt.Println("✓ 成功删除没有关联的用户")
	}

	// 查询最终状态
	result, err = source.Query(ctx, "users", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query users: %v", err)
	}

	ordersResult, err = source.Query(ctx, "orders", &resource.QueryOptions{})
	if err != nil {
		log.Fatalf("Failed to query orders: %v", err)
	}

	fmt.Printf("\n最终状态: %d 条用户记录, %d 条订单记录\n", len(result.Rows), len(ordersResult.Rows))

	fmt.Println("\n=== 所有约束测试完成 ===")
}
