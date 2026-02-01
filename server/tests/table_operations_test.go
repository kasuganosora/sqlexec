package tests

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestTableOperations 测试表操作（CREATE/DROP/TRUNCATE）
func TestTableOperations(t *testing.T) {
	ctx := context.Background()

	t.Log("=== 开始表操作测试 ===")

	// 创建内存数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("连接数据源失败: %v", err)
	}
	t.Log("✓ 数据源连接成功")

	// 测试1: 创建表
	t.Run("创建表", func(t *testing.T) {
		t.Log("\n=== 测试1: 创建表 ===")

		productsSchema := &domain.TableInfo{
			Name:   "products",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "name", Type: "string", Nullable: false},
				{Name: "price", Type: "float64", Nullable: true},
				{Name: "stock", Type: "int64", Nullable: true},
			},
		}

		err = ds.CreateTable(ctx, productsSchema)
		if err != nil {
			t.Fatalf("创建products表失败: %v", err)
		}
		t.Log("✓ products表创建成功")

		// 验证表存在
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("获取表列表失败: %v", err)
		}

		if len(tables) != 1 || tables[0] != "products" {
			t.Errorf("期望1个表[products]，得到%d个表: %v", len(tables), tables)
		}
		t.Logf("✓ 表列表验证成功: %v", tables)

		// 验证表结构
		tableInfo, err := ds.GetTableInfo(ctx, "products")
		if err != nil {
			t.Fatalf("获取表信息失败: %v", err)
		}

		if tableInfo.Name != "products" {
			t.Errorf("期望表名products，得到%s", tableInfo.Name)
		}
		if len(tableInfo.Columns) != 4 {
			t.Errorf("期望4个列，得到%d个", len(tableInfo.Columns))
		}

		// 检查主键
		hasPrimaryKey := false
		for _, col := range tableInfo.Columns {
			if col.Primary {
				hasPrimaryKey = true
				if col.Name != "id" {
					t.Errorf("期望主键列id，得到%s", col.Name)
				}
			}
		}
		if !hasPrimaryKey {
			t.Error("未找到主键列")
		}
		t.Log("✓ 表结构验证成功")
	})

	// 测试2: 插入数据并查询
	t.Run("插入数据", func(t *testing.T) {
		t.Log("\n=== 测试2: 插入数据 ===")

		productsData := []domain.Row{
			{"id": int64(1), "name": "Product A", "price": float64(99.99), "stock": int64(100)},
			{"id": int64(2), "name": "Product B", "price": float64(199.99), "stock": int64(50)},
			{"id": int64(3), "name": "Product C", "price": float64(299.99), "stock": int64(30)},
		}

		affected, err := ds.Insert(ctx, "products", productsData, &domain.InsertOptions{})
		if err != nil {
			t.Fatalf("插入数据失败: %v", err)
		}
		if affected != int64(3) {
			t.Errorf("期望插入3行，得到%d行", affected)
		}
		t.Log("✓ 插入数据成功: 3行")

		// 查询验证
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("查询数据失败: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("期望3行数据，得到%d行", len(result.Rows))
		}
		t.Logf("✓ 查询验证成功: %d 行", len(result.Rows))
	})

	// 测试3: TRUNCATE TABLE
	t.Run("清空表", func(t *testing.T) {
		t.Log("\n=== 测试3: TRUNCATE TABLE ===")

		err := ds.TruncateTable(ctx, "products")
		if err != nil {
			t.Fatalf("清空表失败: %v", err)
		}
		t.Log("✓ 表清空成功")

		// 验证数据已清空
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("查询数据失败: %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("期望0行数据，得到%d行", len(result.Rows))
		}
		t.Log("✓ 表数据验证为空")

		// 验证表结构仍然存在
		tableInfo, err := ds.GetTableInfo(ctx, "products")
		if err != nil {
			t.Fatalf("获取表信息失败: %v", err)
		}

		if tableInfo.Name != "products" || len(tableInfo.Columns) != 4 {
			t.Error("表结构被破坏")
		}
		t.Log("✓ 表结构保持完整")
	})

	// 测试4: DROP TABLE
	t.Run("删除表", func(t *testing.T) {
		t.Log("\n=== 测试4: DROP TABLE ===")

		err := ds.DropTable(ctx, "products")
		if err != nil {
			t.Fatalf("删除表失败: %v", err)
		}
		t.Log("✓ 表删除成功")

		// 验证表不存在
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("获取表列表失败: %v", err)
		}

		if len(tables) != 0 {
			t.Errorf("期望0个表，得到%d个表: %v", len(tables), tables)
		}
		t.Log("✓ 表列表验证为空")

		// 验证不能查询已删除的表
		_, err = ds.Query(ctx, "products", &domain.QueryOptions{})
		if err == nil {
			t.Error("查询已删除的表应该返回错误")
		}
		t.Logf("✓ 查询已删除表正确返回错误: %v", err)
	})

	// 测试5: 创建重复表
	t.Run("创建重复表", func(t *testing.T) {
		t.Log("\n=== 测试5: 创建重复表 ===")

		duplicateSchema := &domain.TableInfo{
			Name:   "users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "name", Type: "string", Nullable: false},
			},
		}

		// 第一次创建
		err := ds.CreateTable(ctx, duplicateSchema)
		if err != nil {
			t.Fatalf("第一次创建users表失败: %v", err)
		}
		t.Log("✓ 第一次创建users表成功")

		// 第二次创建（应该失败）
		err = ds.CreateTable(ctx, duplicateSchema)
		if err == nil {
			t.Error("创建重复表应该返回错误")
		}
		t.Logf("✓ 创建重复表正确返回错误: %v", err)
	})

	// 测试6: 删除不存在的表
	t.Run("删除不存在的表", func(t *testing.T) {
		t.Log("\n=== 测试6: 删除不存在的表 ===")

		err := ds.DropTable(ctx, "nonexistent_table")
		if err == nil {
			t.Error("删除不存在的表应该返回错误")
		}
		t.Logf("✓ 删除不存在的表正确返回错误: %v", err)
	})

	// 测试7: 清空不存在的表
	t.Run("清空不存在的表", func(t *testing.T) {
		t.Log("\n=== 测试7: 清空不存在的表 ===")

		err := ds.TruncateTable(ctx, "nonexistent_table")
		if err == nil {
			t.Error("清空不存在的表应该返回错误")
		}
		t.Logf("✓ 清空不存在的表正确返回错误: %v", err)
	})

	t.Log("\n=== 表操作测试完成 ===")
}

// TestMultipleTables 测试多表操作
func TestMultipleTables(t *testing.T) {
	ctx := context.Background()

	t.Log("=== 开始多表操作测试 ===")

	// 创建内存数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("连接数据源失败: %v", err)
	}

	// 创建多个表
	tables := []struct {
		name   string
		schema *domain.TableInfo
		data   []domain.Row
	}{
		{
			name: "customers",
			schema: &domain.TableInfo{
				Name:   "customers",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false, Primary: true},
					{Name: "name", Type: "string", Nullable: false},
					{Name: "email", Type: "string", Nullable: true},
				},
			},
			data: []domain.Row{
				{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
				{"id": int64(2), "name": "Bob", "email": "bob@example.com"},
			},
		},
		{
			name: "orders",
			schema: &domain.TableInfo{
				Name:   "orders",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false, Primary: true},
					{Name: "customer_id", Type: "int64", Nullable: false},
					{Name: "amount", Type: "float64", Nullable: false},
				},
			},
			data: []domain.Row{
				{"id": int64(1), "customer_id": int64(1), "amount": float64(99.99)},
				{"id": int64(2), "customer_id": int64(2), "amount": float64(199.99)},
			},
		},
	}

	// 创建所有表
	for _, table := range tables {
		err := ds.CreateTable(ctx, table.schema)
		if err != nil {
			t.Fatalf("创建%s表失败: %v", table.name, err)
		}
		t.Logf("✓ %s表创建成功", table.name)

		// 插入数据
		_, err = ds.Insert(ctx, table.name, table.data, &domain.InsertOptions{})
		if err != nil {
			t.Fatalf("插入%s表数据失败: %v", table.name, err)
		}
		t.Logf("✓ %s表数据插入成功: %d 行", table.name, len(table.data))
	}

	// 验证所有表
	tableList, err := ds.GetTables(ctx)
	if err != nil {
		t.Fatalf("获取表列表失败: %v", err)
	}

	if len(tableList) != 2 {
		t.Errorf("期望2个表，得到%d个", len(tableList))
	}
	t.Logf("✓ 表列表验证成功: %v", tableList)

	// 验证每个表的数据
	for _, table := range tables {
		result, err := ds.Query(ctx, table.name, &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("查询%s表失败: %v", table.name, err)
		}

		if len(result.Rows) != len(table.data) {
			t.Errorf("%s表期望%d行，得到%d行", table.name, len(table.data), len(result.Rows))
		}
		t.Logf("✓ %s表数据验证成功: %d 行", table.name, len(result.Rows))
	}

	t.Log("\n=== 多表操作测试完成 ===")
}
