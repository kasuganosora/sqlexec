package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// ==================== 集成测试 - 绕过协议层 ====================
// 模拟应用内部使用场景：从Session获取DB和Executor

// TestBasicIntegration 基础集成测试
func TestBasicIntegration(t *testing.T) {
	ctx := context.Background()

	t.Log("=== 开始基础集成测试 ===")

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

	// 创建表结构
	usersSchema := &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "status", Type: "int64", Nullable: true},
			{Name: "balance", Type: "int64", Nullable: true},
		},
	}
	err = ds.CreateTable(ctx, usersSchema)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}
	t.Log("✓ users表创建成功")

	// 插入测试数据
	usersData := []domain.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "age": int64(25), "status": int64(1), "balance": int64(1000)},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "age": int64(30), "status": int64(1), "balance": int64(2000)},
		{"id": int64(3), "name": "Charlie", "email": "charlie@example.com", "age": int64(35), "status": int64(0), "balance": int64(1500)},
		{"id": int64(4), "name": "David", "email": "david@example.com", "age": int64(28), "status": int64(1), "balance": int64(3000)},
		{"id": int64(5), "name": "Eve", "email": "eve@example.com", "age": int64(32), "status": int64(0), "balance": int64(500)},
	}
	_, err = ds.Insert(ctx, "users", usersData, &domain.InsertOptions{})
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}
	t.Log("✓ 测试数据插入成功: 5行")

	// 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(ds, true)
	t.Log("✓ 优化执行器创建成功")

	// 创建SQL解析器
	adapter := parser.NewSQLAdapter()

	// 测试1: 基础SELECT查询
	t.Run("测试基础SELECT", func(t *testing.T) {
		t.Log("\n=== 测试1: 基础SELECT查询 ===")

		// 解析SQL
		parseResult, err := adapter.Parse("SELECT * FROM users")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}
		if !parseResult.Success {
			t.Fatalf("SQL解析不成功: %s", parseResult.Error)
		}

		t.Logf("SQL类型: %s", parseResult.Statement.Type)

		// 执行查询
		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		// 验证结果
		if result == nil {
			t.Fatal("查询结果为空")
		}

		t.Logf("✓ 查询执行成功")
		t.Logf("查询结果行数: %d", len(result.Rows))
		t.Logf("列数: %d", len(result.Columns))

		// 打印结果
		for i, row := range result.Rows {
			t.Logf("  行 %d: %+v", i+1, row)
		}

		if len(result.Rows) != 5 {
			t.Errorf("期望5行数据，得到%d行", len(result.Rows))
		}
	})

	// 测试2: 带WHERE条件的查询
	t.Run("测试WHERE条件查询", func(t *testing.T) {
		t.Log("\n=== 测试2: WHERE条件查询 ===")

		// 解析SQL
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE age = 25")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}
		if !parseResult.Success {
			t.Fatalf("SQL解析不成功: %s", parseResult.Error)
		}

		// 执行查询
		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		// 验证结果
		if len(result.Rows) != 1 {
			t.Errorf("期望1行数据，得到%d行", len(result.Rows))
		}

		if result.Rows[0]["name"] != "Alice" {
			t.Errorf("期望name=Alice，得到%s", result.Rows[0]["name"])
		}

		t.Logf("✓ WHERE条件查询成功")
		t.Logf(" 查询结果: %+v", result.Rows[0])
	})

	// 测试3: 带ORDER BY的查询
	t.Run("测试ORDER BY排序", func(t *testing.T) {
		t.Log("\n=== 测试3: ORDER BY排序 ===")

		// 解析SQL
		parseResult, err := adapter.Parse("SELECT * FROM users ORDER BY age ASC")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}
		if !parseResult.Success {
			t.Fatalf("SQL解析不成功: %s", parseResult.Error)
		}

		// 执行查询
		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		// 验证结果
		if len(result.Rows) < 2 {
			t.Fatalf("期望至少2行数据，得到%d行", len(result.Rows))
		}

		// 验证排序（升序）
		firstAge := result.Rows[0]["age"].(int64)
		secondAge := result.Rows[1]["age"].(int64)
		if firstAge > secondAge {
				t.Errorf("排序错误: %d > %d", firstAge, secondAge)
			}

		t.Logf("✓ ORDER BY排序成功")
		for i, row := range result.Rows {
			t.Logf("  行 %d: age=%v", i+1, row["age"])
		}
	})

	// 测试4: 带LIMIT的查询
	t.Run("测试LIMIT分页", func(t *testing.T) {
		t.Log("\n=== 测试4: LIMIT分页 ===")

		// 解析SQL
		parseResult, err := adapter.Parse("SELECT * FROM users LIMIT 2")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}
		if !parseResult.Success {
			t.Fatalf("SQL解析不成功: %s", parseResult.Error)
		}

		// 执行查询
		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

			// 验证结果
		if len(result.Rows) != 2 {
			t.Errorf("期望2行数据，得到%d行", len(result.Rows))
		}

		t.Logf("✓ LIMIT分页成功")
		for i, row := range result.Rows {
			t.Logf(" 行 %d: %+v", i+1, row)
		}
	})

	t.Log("\n=== 基础集成测试完成 ===")
}

// TestComplexQuery 复杂查询测试
func TestComplexQuery(t *testing.T) {
	ctx := context.Background()

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

	// 创建表和插入数据
	usersSchema := &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "status", Type: "int64", Nullable: true},
			{Name: "balance", Type: "int64", Nullable: true},
		},
	}
	err = ds.CreateTable(ctx, usersSchema)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	usersData := []domain.Row{
		{"id": int64(1), "name": "Alice", "email": "alice@example.com", "age": int64(25), "status": int64(1), "balance": int64(1000)},
		{"id": int64(2), "name": "Bob", "email": "bob@example.com", "age": int64(30), "status": int64(1), "balance": int64(2000)},
		{"id": int64(3), "name": "Charlie", "email": "charlie@example.com", "age": int64(35), "status": int64(0), "balance": int64(1500)},
		{"id": int64(4), "name": "David", "email": "david@example.com", "age": int64(28), "status": int64(1), "balance": int64(3000)},
	}
	_, err = ds.Insert(ctx, "users", usersData, &domain.InsertOptions{})
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(ds, true)

	t.Run("组合条件查询", func(t *testing.T) {
		ctx := context.Background()
		adapter := parser.NewSQLAdapter()

		// 解析组合条件SQL
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE age > 25 AND age < 35 AND status = 1")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}
		if !parseResult.Success {
			t.Fatalf("SQL解析不成功: %s", parseResult.Error)
		}

		// 执行查询
		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

			if len(result.Rows) != 2 {
			t.Errorf("期望2行数据，得到%d行", len(result.Rows))
			}

		t.Logf("✓ 组合条件查询成功: %d 行", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  行 %d: %+v", i+1, row)
		}
	})

	t.Log("\n✓ 复杂查询测试完成")
}

// TestCRUDOperations CRUD操作测试
func TestCRUDOperations(t *testing.T) {
	ctx := context.Background()

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

	// 创建表
	usersSchema := &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "balance", Type: "int64", Nullable: true},
		},
	}
	err = ds.CreateTable(ctx, usersSchema)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	// 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(ds, true)
	adapter := parser.NewSQLAdapter()

	t.Run("完整CRUD流程", func(t *testing.T) {
		t.Log("\n=== 开始完整CRUD流程测试 ===")

		// 1. INSERT
		t.Log("\n1. 插入测试数据")
		insertParse, err := adapter.Parse("INSERT INTO users (id, name, email, age, balance) VALUES (100, 'TestUser', 'test@example.com', 30, 5000)")
		if err != nil {
			t.Fatalf("INSERT解析失败: %v", err)
		}

		insertResult, err := executor.ExecuteInsert(ctx, insertParse.Statement.Insert)
		if err != nil {
			t.Fatalf("INSERT失败: %v", err)
		}
		t.Logf("✓ INSERT成功: 影响 %d 行", insertResult.Total)

		// 验证INSERT
		t.Log("\n2. 验证插入")
		selectParse, err := adapter.Parse("SELECT * FROM users WHERE id = 100")
		if err != nil {
			t.Fatalf("SELECT解析失败: %v", err)
		}

		selectResult, err := executor.ExecuteSelect(ctx, selectParse.Statement.Select)
		if err != nil {
			t.Fatalf("SELECT验证失败: %v", err)
		}

		if len(selectResult.Rows) != 1 || selectResult.Rows[0]["id"] != int64(100) {
			t.Errorf("期望id=100，得到%d", selectResult.Rows[0]["id"])
		}
		t.Logf("✓ INSERT验证成功: id=%v, name=%s", selectResult.Rows[0]["id"], selectResult.Rows[0]["name"])

		// 3. UPDATE
		t.Log("\n3. 更新测试数据")
		updateParse, err := adapter.Parse("UPDATE users SET balance = 8000 WHERE id = 100")
		if err != nil {
			t.Fatalf("UPDATE解析失败: %v", err)
		}

		updateResult, err := executor.ExecuteUpdate(ctx, updateParse.Statement.Update)
		if err != nil {
			t.Fatalf("UPDATE失败: %v", err)
		}
		t.Logf("✓ UPDATE成功: 影响 %d 行", updateResult.Total)

		// 验证UPDATE
		t.Log("\n4. 验证更新")
		selectParse, err = adapter.Parse("SELECT * FROM users WHERE id = 100")
		if err != nil {
			t.Fatalf("SELECT解析失败: %v", err)
		}

		selectResult, err = executor.ExecuteSelect(ctx, selectParse.Statement.Select)
		if err != nil {
			t.Fatalf("SELECT验证失败: %v", err)
		}

		if len(selectResult.Rows) != 1 || selectResult.Rows[0]["balance"] != int64(8000) {
			t.Errorf("期望balance=8000，得到%d", selectResult.Rows[0]["balance"])
		}
		t.Logf("✓ UPDATE验证成功: balance=%d", selectResult.Rows[0]["balance"])

		// 5. DELETE
		t.Log("\n5. 删除测试数据")
		deleteParse, err := adapter.Parse("DELETE FROM users WHERE id = 100")
		if err != nil {
			t.Fatalf("DELETE解析失败: %v", err)
		}

		deleteResult, err := executor.ExecuteDelete(ctx, deleteParse.Statement.Delete)
		if err != nil {
			t.Fatalf("DELETE失败: %v", err)
		}
		t.Logf("✓ DELETE成功: 影响 %d 行", deleteResult.Total)

		// 验证DELETE
		t.Log("\n6. 验证删除")
		selectParse, err = adapter.Parse("SELECT * FROM users WHERE id = 100")
		if err != nil {
			t.Fatalf("SELECT解析失败: %v", err)
		}

		selectResult, err = executor.ExecuteSelect(ctx, selectParse.Statement.Select)
		if err != nil {
			t.Fatalf("SELECT验证失败: %v", err)
		}

		if len(selectResult.Rows) != 0 {
			t.Errorf("期望0行数据，得到%d行", len(selectResult.Rows))
		}

		t.Logf("✓ DELETE验证成功: 数据已删除")
	})

	t.Log("\n=== 完整CRUD流程测试通过 ===")

	t.Log("\n========== 集成测试完成 ==========")
}

// TestPerformance 性能测试
func TestPerformance(t *testing.T) {
	ctx := context.Background()

	// 创建数据源和表
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("连接数据源失败: %v", err)
	}

	// 创建表
	usersSchema := &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "email", Type: "string", Nullable: true},
		{Name: "salary", Type: "int64", Nullable: true},
		},
	}

	err = ds.CreateTable(ctx, usersSchema)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	// 插入大量测试数据
	t.Log("插入100,000条测试数据...")
	insertData := make([]domain.Row, 0, 100000)
	for i := 0; i < 100000; i++ {
		insertData = append(insertData, domain.Row{
			"id":      int64(i + 1),
			"name":    fmt.Sprintf("User%d", i+1),
			"email":   fmt.Sprintf("user%d@example.com", i+1),
			"age":     int64(20 + (i % 60)),
			"salary":  int64((i+1) * 1000),
		})
	}
	_, err = ds.Insert(ctx, "users", insertData, &domain.InsertOptions{})
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}
	t.Log("✓ 测试数据插入完成")

	// 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(ds, true)
	adapter := parser.NewSQLAdapter()

	// 测试点查询性能
	t.Run("点查询性能", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE id = 50000")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}

		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		duration := time.Since(start)

		if len(result.Rows) != 1 {
			t.Errorf("期望1行数据，得到%d行", len(result.Rows))
		}

		t.Logf("✓ 点查询性能:")
		t.Logf("  查询耗时: %v", duration)
		t.Logf("  结果行数: %d", len(result.Rows))
	})

	// 测试全表扫描性能
	t.Run("全表扫描性能", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE salary > 50000 ORDER BY salary DESC LIMIT 10")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}

		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		duration := time.Since(start)

		t.Logf("✓ 全表扫描性能:")
		t.Logf("  查询耗时: %v", duration)
		t.Logf("  结果行数: %d", len(result.Rows))
	})
}

// TestIndexUsage 索引使用测试
func TestIndexUsage(t *testing.T) {
	ctx := context.Background()

	// 创建数据源和表
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("连接数据源失败: %v", err)
	}

	// 创建表和索引
	usersSchema := &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
		{Name: "salary", Type: "int64", Nullable: true},
		},
	}

	err = ds.CreateTable(ctx, usersSchema)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	// 插入大量数据
	t.Log("插入10,000条测试数据...")
	insertData := make([]domain.Row, 0, 10000)
	for i := 0; i < 10000; i++ {
		insertData = append(insertData, domain.Row{
			"id":      int64(i + 1),
			"name":    fmt.Sprintf("User%d", i+1),
			"salary":  int64((i+1) * 100),
		})
	}
	_, err = ds.Insert(ctx, "users", insertData, &domain.InsertOptions{})
	if err != nil {
		t.Fatalf("插入数据失败: %v", err)
	}
	t.Log("✓ 测试数据插入完成")

	// 创建优化执行器
	executor := optimizer.NewOptimizedExecutor(ds, true)
	adapter := parser.NewSQLAdapter()

	// 测试索引点查询性能
	t.Run("索引点查询性能", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE id = 5000")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}

		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		duration := time.Since(start)

		t.Logf("✓ 索引点查询性能:")
		t.Logf("  查询耗时: %v", duration)
		t.Logf("  结果行数: %d", len(result.Rows))

			if len(result.Rows) != 1 {
			t.Errorf("期望1行数据，得到%d行", len(result.Rows))
		}
	})

	// 测试索引范围查询性能
	t.Run("索引范围查询性能", func(t *testing.T) {
		ctx := context.Background()

		start := time.Now()
		parseResult, err := adapter.Parse("SELECT * FROM users WHERE salary > 50000 ORDER BY salary ASC LIMIT 10")
		if err != nil {
			t.Fatalf("解析SQL失败: %v", err)
		}

		result, err := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
		if err != nil {
			t.Fatalf("执行查询失败: %v", err)
		}

		duration := time.Since(start)

		t.Logf("✓ 索引范围查询性能:")
		t.Logf("  查询耗时: %v", duration)
		t.Logf("  结果行数: %d", len(result.Rows))
	})
}
