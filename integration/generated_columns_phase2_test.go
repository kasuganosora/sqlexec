package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
)

// TestGeneratedColumnsPhase2V1 第二阶段 VIRTUAL 列功能测试
func TestGeneratedColumnsPhase2V1(t *testing.T) {
	ctx := context.Background()

	t.Log("=== 第二阶段 VIRTUAL 列功能测试 ===")

	// 创建内存数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_virtual",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// 创建包含 VIRTUAL 列的表
	schema := &domain.TableInfo{
		Name:   "products",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false, Primary: true},
			{Name: "name", Type: "VARCHAR(100)", Nullable: false},
			{Name: "price", Type: "DECIMAL(10,2)", Nullable: false},
			{Name: "quantity", Type: "INT", Nullable: false},
			{
				Name:         "total",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "price * quantity",
				GeneratedDepends: []string{"price", "quantity"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)
	t.Log("✓ 创建表成功")

	// 测试插入数据（VIRTUAL 列不应被存储）
	rows := []domain.Row{
		{"id": int64(1), "name": "Apple", "price": 10.5, "quantity": int64(2)},
		{"id": int64(2), "name": "Banana", "price": 5.0, "quantity": int64(3)},
		{"id": int64(3), "name": "Orange", "price": 7.5, "quantity": int64(1)},
	}

	count, err := ds.Insert(ctx, "products", rows, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	t.Log("✓ 插入数据成功")

	// 查询数据并验证 VIRTUAL 列的计算
	queryResult, err := ds.Query(ctx, "products", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, queryResult)
	assert.Len(t, queryResult.Rows, 3)

	// 验证 VIRTUAL 列的计算结果
	assert.Equal(t, 21.0, queryResult.Rows[0]["total"], "Apple: 10.5 * 2 = 21.0")
	assert.Equal(t, 15.0, queryResult.Rows[1]["total"], "Banana: 5.0 * 3 = 15.0")
	assert.Equal(t, 7.5, queryResult.Rows[2]["total"], "Orange: 7.5 * 1 = 7.5")
	t.Log("✓ VIRTUAL 列计算正确")
}

// TestGeneratedColumnsPhase2V2 第二阶段 STORED 和 VIRTUAL 混合测试
func TestGeneratedColumnsPhase2V2(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_mixed",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// 创建包含 STORED 和 VIRTUAL 生成列的表
	schema := &domain.TableInfo{
		Name:   "orders",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false, Primary: true},
			{Name: "item_price", Type: "DECIMAL(10,2)", Nullable: false},
			{Name: "item_qty", Type: "INT", Nullable: false},
			{
				Name:         "item_total",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "STORED",
				GeneratedExpr: "item_price * item_qty",
				GeneratedDepends: []string{"item_price", "item_qty"},
			},
			{
				Name:         "item_total_v",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "item_price * item_qty",
				GeneratedDepends: []string{"item_price", "item_qty"},
			},
			{
				Name:         "double_total",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "item_total_v * 2",
				GeneratedDepends: []string{"item_total_v"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"id": int64(1), "item_price": 10.0, "item_qty": int64(2)},
		{"id": int64(2), "item_price": 5.0, "item_qty": int64(3)},
	}

	_, err = ds.Insert(ctx, "orders", rows, nil)
	assert.NoError(t, err)

	// 查询验证
	queryResult, err := ds.Query(ctx, "orders", &domain.QueryOptions{})
	assert.NoError(t, err)

	// STORED 列应该被存储
	assert.Equal(t, 20.0, queryResult.Rows[0]["item_total"])
	// VIRTUAL 列动态计算
	assert.Equal(t, 20.0, queryResult.Rows[0]["item_total_v"])
	// 依赖其他 VIRTUAL 列的 VIRTUAL 列也应该正确计算
	assert.Equal(t, 40.0, queryResult.Rows[0]["double_total"])
}

// TestGeneratedColumnsPhase2V3 VIRTUAL 列 NULL 传播测试
func TestGeneratedColumnsPhase2V3(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_null",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	schema := &domain.TableInfo{
		Name:   "test_table",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "a", Type: "INT", Nullable: true},
			{Name: "b", Type: "INT", Nullable: true},
			{
				Name:         "sum",
				Type:         "INT",
				Nullable:     true,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "a + b",
				GeneratedDepends: []string{"a", "b"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 插入包含 NULL 的数据
	rows := []domain.Row{
		{"id": int64(1), "a": nil, "b": int64(10)},
		{"id": int64(2), "a": int64(5), "b": nil},
		{"id": int64(3), "a": int64(3), "b": int64(4)},
	}

	_, err = ds.Insert(ctx, "test_table", rows, nil)
	assert.NoError(t, err)

	// 验证 NULL 传播
	queryResult, err := ds.Query(ctx, "test_table", &domain.QueryOptions{})
	assert.NoError(t, err)

	// a 或 b 为 NULL 时，sum 也应为 NULL
	assert.Nil(t, queryResult.Rows[0]["sum"])
	assert.Nil(t, queryResult.Rows[1]["sum"])
	// 两者都不为 NULL 时，正常计算
	assert.Equal(t, int64(7), queryResult.Rows[2]["sum"])
}

// TestGeneratedColumnsPhase2V4 复杂表达式测试
func TestGeneratedColumnsPhase2V4(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_complex",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	schema := &domain.TableInfo{
		Name:   "employees",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "base_salary", Type: "DECIMAL(10,2)", Nullable: false},
			{Name: "bonus", Type: "DECIMAL(10,2)", Nullable: true},
			{
				Name:         "total_salary",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "base_salary + bonus",
				GeneratedDepends: []string{"base_salary", "bonus"},
			},
			{
				Name:         "with_tax",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "total_salary * 0.9",
				GeneratedDepends: []string{"total_salary"},
			},
			{
				Name:         "final_salary",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "with_tax + 100",
				GeneratedDepends: []string{"with_tax"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"id": int64(1), "base_salary": 5000.0, "bonus": 500.0},
	}

	_, err = ds.Insert(ctx, "employees", rows, nil)
	assert.NoError(t, err)

	// 验证复杂表达式计算链
	queryResult, err := ds.Query(ctx, "employees", &domain.QueryOptions{})
	assert.NoError(t, err)

	// total_salary = 5000 + 500 = 5500
	assert.Equal(t, 5500.0, queryResult.Rows[0]["total_salary"])
	// with_tax = 5500 * 0.9 = 4950.0
	assert.Equal(t, 4950.0, queryResult.Rows[0]["with_tax"])
	// final_salary = 4950 + 100 = 5050.0
	assert.Equal(t, 5050.0, queryResult.Rows[0]["final_salary"])
}

// TestGeneratedColumnsPhase2V5 UPDATE 操作级联更新测试
func TestGeneratedColumnsPhase2V5(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_update",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	schema := &domain.TableInfo{
		Name:   "inventory",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "price", Type: "DECIMAL(10,2)", Nullable: false},
			{Name: "qty", Type: "INT", Nullable: false},
			{
				Name:         "subtotal",
				Type:         "DECIMAL(10,2)",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "price * qty",
				GeneratedDepends: []string{"price", "qty"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 初始数据
	initialRow := domain.Row{"id": int64(1), "price": 10.0, "qty": int64(2)}
	_, err = ds.Insert(ctx, "inventory", []domain.Row{initialRow}, nil)
	assert.NoError(t, err)

	// 更新基础列
	updates := domain.Row{"price": 15.0}
	count, err := ds.Update(ctx, "inventory", []domain.Filter{
		{Column: "id", Operator: "=", Value: int64(1)},
	}, updates, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 验证 VIRTUAL 列是否重新计算
	queryResult, err := ds.Query(ctx, "inventory", &domain.QueryOptions{})
	assert.NoError(t, err)

	// price 从 10.0 更新为 15.0，subtotal 应从 20.0 更新为 30.0
	assert.Equal(t, 30.0, queryResult.Rows[0]["subtotal"])
}

// TestGeneratedColumnsPhase2V6 SQL 解析 VIRTUAL 列语法测试
func TestGeneratedColumnsPhase2V6(t *testing.T) {
	adapter := parser.NewSQLAdapter()

	// 测试 VIRTUAL 列 SQL 解析
	sqls := []string{
		"CREATE TABLE t1 (id INT, price DECIMAL, total DECIMAL GENERATED ALWAYS AS (price * 2) VIRTUAL)",
		"CREATE TABLE t2 (id INT, price DECIMAL, total DECIMAL GENERATED ALWAYS AS (price * 2) STORED)",
		"CREATE TABLE t3 (id INT, price DECIMAL, qty INT, subtotal DECIMAL GENERATED ALWAYS AS (price * qty) STORED, total DECIMAL GENERATED ALWAYS AS (subtotal * 1.1) VIRTUAL)",
	}

	for i, sql := range sqls {
		result, err := adapter.Parse(sql)
		assert.NoError(t, err, fmt.Sprintf("SQL %d 解析失败: %v", i, err))
		assert.True(t, result.Success, fmt.Sprintf("SQL %d 解析不成功", i))
		assert.NotNil(t, result.Statement, fmt.Sprintf("SQL %d 语句为空", i))

		createStmt, ok := result.Statement.(*parser.CreateStatement)
		assert.True(t, ok, fmt.Sprintf("SQL %d 不是 CREATE 语句", i))

		// 验证生成列信息
		virtualCount := 0
		storedCount := 0
		for _, col := range createStmt.Columns {
			if col.IsGenerated {
				if col.GeneratedType == "VIRTUAL" {
					virtualCount++
				} else if col.GeneratedType == "STORED" {
					storedCount++
				}
			}
		}

		switch i {
		case 0:
			assert.Equal(t, 1, virtualCount, "t1 应有 1 个 VIRTUAL 列")
			assert.Equal(t, 0, storedCount, "t1 不应有 STORED 列")
		case 1:
			assert.Equal(t, 0, virtualCount, "t2 不应有 VIRTUAL 列")
			assert.Equal(t, 1, storedCount, "t2 应有 1 个 STORED 列")
		case 2:
			assert.Equal(t, 1, virtualCount, "t3 应有 1 个 VIRTUAL 列")
			assert.Equal(t, 1, storedCount, "t3 应有 1 个 STORED 列")
		}

		t.Logf("✓ SQL %d 解析正确: %d VIRTUAL, %d STORED", i, virtualCount, storedCount)
	}
}

// TestGeneratedColumnsPhase2V7 性能测试 - 表达式缓存
func TestGeneratedColumnsPhase2V7(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_perf",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	schema := &domain.TableInfo{
		Name:   "perf_table",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "a", Type: "INT", Nullable: false},
			{Name: "b", Type: "INT", Nullable: false},
			{Name: "c", Type: "INT", Nullable: false},
			{
				Name:         "sum_all",
				Type:         "INT",
				Nullable:     false,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "a + b + c",
				GeneratedDepends: []string{"a", "b", "c"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 批量插入数据测试性能
	rows := make([]domain.Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = domain.Row{
			"id": int64(i + 1),
			"a":  int64(i),
			"b":  int64(i * 2),
			"c":  int64(i * 3),
		}
	}

	_, err = ds.Insert(ctx, "perf_table", rows, nil)
	assert.NoError(t, err)

	// 查询并验证
	queryResult, err := ds.Query(ctx, "perf_table", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.Len(t, queryResult.Rows, 100)

	// 验证计算正确性
	for i, row := range queryResult.Rows {
		expected := int64(i + 1) + int64(i*2) + int64(i*3)
		assert.Equal(t, expected, row["sum_all"], fmt.Sprintf("行 %d 计算错误", i))
	}

	t.Log("✓ 性能测试通过：100 行 VIRTUAL 列计算正确")
}

// TestGeneratedColumnsPhase2V8 错误处理测试
func TestGeneratedColumnsPhase2V8(t *testing.T) {
	ctx := context.Background()
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_error",
		Writable: true,
	})
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	schema := &domain.TableInfo{
		Name:   "error_table",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "dividend", Type: "INT", Nullable: true},
			{Name: "divisor", Type: "INT", Nullable: true},
			{
				Name:         "quotient",
				Type:         "INT",
				Nullable:     true,
				IsGenerated:  true,
				GeneratedType: "VIRTUAL",
				GeneratedExpr: "dividend / divisor",
				GeneratedDepends: []string{"dividend", "divisor"},
			},
		},
	}

	err = ds.CreateTable(ctx, schema)
	assert.NoError(t, err)

	// 测试除零错误
	rows := []domain.Row{
		{"id": int64(1), "dividend": int64(10), "divisor": int64(0)},
	}

	_, err = ds.Insert(ctx, "error_table", rows, nil)
	assert.NoError(t, err)

	// 查询并验证错误处理
	queryResult, err := ds.Query(ctx, "error_table", &domain.QueryOptions{})
	assert.NoError(t, err)

	// 除零应该返回 NULL
	assert.Nil(t, queryResult.Rows[0]["quotient"])
	t.Log("✓ 错误处理正确：除零返回 NULL")

	// 测试正常除法
	normalRow := domain.Row{"id": int64(2), "dividend": int64(10), "divisor": int64(2)}
	_, err = ds.Insert(ctx, "error_table", []domain.Row{normalRow}, nil)
	assert.NoError(t, err)

	queryResult, err = ds.Query(ctx, "error_table", &domain.QueryOptions{})
	assert.NoError(t, err)

	assert.Equal(t, int64(5), queryResult.Rows[1]["quotient"])
}
