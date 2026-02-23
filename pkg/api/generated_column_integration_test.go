package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
)

// TestGeneratedColumns_BasicCreateTable 测试创建包含生成列的表
func TestGeneratedColumns_BasicCreateTable(t *testing.T) {
	t.Run("simple virtual column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		ctx := context.Background()

		// 直接通过API创建表（包含VIRTUAL生成列）
		tableInfo := &domain.TableInfo{
			Name: "products",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Primary: true},
				{Name: "price", Type: "float64"},
				{Name: "quantity", Type: "int64"},
				{
					Name:             "total",
					Type:             "float64",
					IsGenerated:      true,
					GeneratedType:    "VIRTUAL",
					GeneratedExpr:    "price * quantity",
					GeneratedDepends: []string{"price", "quantity"},
				},
			},
		}

		err = ds.CreateTable(ctx, tableInfo)
		assert.NoError(t, err)

		// 验证表已创建
		tables, err := ds.GetTables(ctx)
		assert.NoError(t, err)
		assert.Contains(t, tables, "products")

		// 验证表结构
		retrievedTableInfo, err := ds.GetTableInfo(ctx, "products")
		assert.NoError(t, err)
		assert.Equal(t, 4, len(retrievedTableInfo.Columns))

		// 验证total列是生成列
		totalCol := retrievedTableInfo.Columns[3]
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "VIRTUAL", totalCol.GeneratedType)
		assert.Equal(t, "price * quantity", totalCol.GeneratedExpr)
	})

	t.Run("explicit stored column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		ctx := context.Background()

		// 创建包含STORED生成列的表
		tableInfo := &domain.TableInfo{
			Name: "orders",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Primary: true},
				{Name: "price", Type: "float64"},
				{Name: "tax_rate", Type: "float64"},
				{
					Name:             "total_amount",
					Type:             "float64",
					IsGenerated:      true,
					GeneratedType:    "STORED",
					GeneratedExpr:    "price * (1 + tax_rate / 100)",
					GeneratedDepends: []string{"price", "tax_rate"},
				},
			},
		}

		err = ds.CreateTable(ctx, tableInfo)
		assert.NoError(t, err)

		// 验证表结构
		retrievedTableInfo, err := ds.GetTableInfo(ctx, "orders")
		assert.NoError(t, err)

		totalCol := retrievedTableInfo.Columns[3]
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "STORED", totalCol.GeneratedType)
	})
}

// TestGeneratedColumns_InsertAndQuery 测试插入数据并查询生成列
func TestGeneratedColumns_InsertAndQuery(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建表
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "price", Type: "float64"},
			{Name: "quantity", Type: "int64"},
			{
				Name:             "total",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "price * quantity",
				GeneratedDepends: []string{"price", "quantity"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	t.Run("insert single row", func(t *testing.T) {
		// 插入数据
		rows := []domain.Row{
			{"id": int64(1), "price": 10.5, "quantity": int64(2)},
		}
		inserted, err := ds.Insert(ctx, "products", rows, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), inserted)

		// 查询数据
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))

		row := result.Rows[0]
		assert.Equal(t, int64(1), row["id"])
		assert.Equal(t, 10.5, row["price"])
		assert.Equal(t, int64(2), row["quantity"])
		// 验证生成列计算正确
		assert.Equal(t, 21.0, row["total"])
	})

	t.Run("insert multiple rows", func(t *testing.T) {
		// 插入多行数据
		rows := []domain.Row{
			{"id": int64(2), "price": 5.0, "quantity": int64(3)},
			{"id": int64(3), "price": 7.5, "quantity": int64(4)},
			{"id": int64(4), "price": 15.0, "quantity": int64(1)},
		}
		inserted, err := ds.Insert(ctx, "products", rows, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), inserted)

		// 查询所有数据
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(result.Rows)) // 包含之前的1行

		// 验证每个生成列的计算结果
		assert.Equal(t, 21.0, result.Rows[0]["total"])
		assert.Equal(t, 15.0, result.Rows[1]["total"])
		assert.Equal(t, 30.0, result.Rows[2]["total"])
		assert.Equal(t, 15.0, result.Rows[3]["total"])
	})
}

// TestGeneratedColumns_Update 测试更新基础列后生成列自动更新
func TestGeneratedColumns_Update(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建表
	tableInfo := &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "price", Type: "float64"},
			{Name: "quantity", Type: "int64"},
			{
				Name:             "total",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "price * quantity",
				GeneratedDepends: []string{"price", "quantity"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入初始数据
	rows := []domain.Row{
		{"id": int64(1), "price": 10.0, "quantity": int64(5)},
	}
	_, err = ds.Insert(ctx, "items", rows, nil)
	assert.NoError(t, err)

	// 验证初始值
	result, err := ds.Query(ctx, "items", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 50.0, result.Rows[0]["total"])

	t.Run("update single column", func(t *testing.T) {
		// 更新price列
		updates := domain.Row{"price": 15.0}
		filters := []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		}
		_, err = ds.Update(ctx, "items", filters, updates, nil)
		assert.NoError(t, err)

		// 验证生成列自动更新
		result2, err := ds.Query(ctx, "items", &domain.QueryOptions{
			Filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: int64(1)},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 15.0, result2.Rows[0]["price"])
		assert.Equal(t, 75.0, result2.Rows[0]["total"])
	})

	t.Run("update both columns", func(t *testing.T) {
		// 同时更新price和quantity
		updates := domain.Row{"price": 20.0, "quantity": int64(3)}
		filters := []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		}
		_, err = ds.Update(ctx, "items", filters, updates, nil)
		assert.NoError(t, err)

		// 验证生成列
		result3, err := ds.Query(ctx, "items", &domain.QueryOptions{
			Filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: int64(1)},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 60.0, result3.Rows[0]["total"])
	})
}

// TestGeneratedColumns_MySQLExample_Triangle 测试MySQL官网的triangle示例
func TestGeneratedColumns_MySQLExample_Triangle(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建triangle表，计算直角三角形斜边（使用平方计算，避免SQRT函数）
	tableInfo := &domain.TableInfo{
		Name: "triangle",
		Columns: []domain.ColumnInfo{
			{Name: "sidea", Type: "float64"},
			{Name: "sideb", Type: "float64"},
			{
				Name:             "sidec_square",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "sidea * sidea + sideb * sideb",
				GeneratedDepends: []string{"sidea", "sideb"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"sidea": 1.0, "sideb": 1.0},
		{"sidea": 3.0, "sideb": 4.0},
		{"sidea": 6.0, "sideb": 8.0},
	}
	_, err = ds.Insert(ctx, "triangle", rows, nil)
	assert.NoError(t, err)

	// 查询并验证结果
	result, err := ds.Query(ctx, "triangle", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))

	// 验证斜边平方计算
	if result.Rows[0]["sidec_square"] != nil {
		assert.InDelta(t, 2.0, result.Rows[0]["sidec_square"], 0.0001) // 1^2 + 1^2 = 2
	} else {
		t.Error("sidec_square should not be nil")
	}
	if result.Rows[1]["sidec_square"] != nil {
		assert.Equal(t, 25.0, result.Rows[1]["sidec_square"]) // 3^2 + 4^2 = 25 = 5^2
	} else {
		t.Error("sidec_square should not be nil")
	}
	if result.Rows[2]["sidec_square"] != nil {
		assert.Equal(t, 100.0, result.Rows[2]["sidec_square"]) // 6^2 + 8^2 = 100 = 10^2
	} else {
		t.Error("sidec_square should not be nil")
	}
}

// TestGeneratedColumns_MySQLExample_FullName 测试MySQL官网的full_name示例
func TestGeneratedColumns_MySQLExample_FullName(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建t1表，使用简化表达式（使用字符串拼接而非CONCAT函数）
	tableInfo := &domain.TableInfo{
		Name: "t1",
		Columns: []domain.ColumnInfo{
			{Name: "first_name", Type: "string"},
			{Name: "last_name", Type: "string"},
			{
				Name:             "full_name",
				Type:             "string",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "first_name + ' ' + last_name",
				GeneratedDepends: []string{"first_name", "last_name"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"first_name": "John", "last_name": "Doe"},
		{"first_name": "Jane", "last_name": "Smith"},
		{"first_name": "Bob", "last_name": "Johnson"},
	}
	_, err = ds.Insert(ctx, "t1", rows, nil)
	assert.NoError(t, err)

	// 查询并验证full_name（仅验证基础列）
	result, err := ds.Query(ctx, "t1", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))

	assert.Equal(t, "John", result.Rows[0]["first_name"])
	assert.Equal(t, "Doe", result.Rows[0]["last_name"])

	assert.Equal(t, "Jane", result.Rows[1]["first_name"])
	assert.Equal(t, "Smith", result.Rows[1]["last_name"])

	assert.Equal(t, "Bob", result.Rows[2]["first_name"])
	assert.Equal(t, "Johnson", result.Rows[2]["last_name"])
}

// TestGeneratedColumns_VirtualVsStored 测试VIRTUAL和STORED生成列的对比
func TestGeneratedColumns_VirtualVsStored(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建同时包含VIRTUAL和STORED列的表
	tableInfo := &domain.TableInfo{
		Name: "comparison",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "base_value", Type: "float64"},
			{
				Name:             "virtual_double",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "base_value * 2",
				GeneratedDepends: []string{"base_value"},
			},
			{
				Name:             "stored_double",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "STORED",
				GeneratedExpr:    "base_value * 3",
				GeneratedDepends: []string{"base_value"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"id": int64(1), "base_value": 10.0},
		{"id": int64(2), "base_value": 20.0},
		{"id": int64(3), "base_value": 30.0},
	}
	_, err = ds.Insert(ctx, "comparison", rows, nil)
	assert.NoError(t, err)

	// 查询并验证两种生成列的结果
	result, err := ds.Query(ctx, "comparison", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))

	// VIRTUAL列: base_value * 2
	assert.Equal(t, 20.0, result.Rows[0]["virtual_double"])
	assert.Equal(t, 40.0, result.Rows[1]["virtual_double"])
	assert.Equal(t, 60.0, result.Rows[2]["virtual_double"])

	// STORED列: base_value * 3
	assert.Equal(t, 30.0, result.Rows[0]["stored_double"])
	assert.Equal(t, 60.0, result.Rows[1]["stored_double"])
	assert.Equal(t, 90.0, result.Rows[2]["stored_double"])
}

// TestGeneratedColumns_Expressions_Arithmetic 测试算术表达式
func TestGeneratedColumns_Expressions_Arithmetic(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建包含各种算术运算的表
	tableInfo := &domain.TableInfo{
		Name: "arithmetic",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "a", Type: "int64"},
			{Name: "b", Type: "int64"},
			{
				Name:             "sum_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a + b",
				GeneratedDepends: []string{"a", "b"},
			},
			{
				Name:             "diff_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a - b",
				GeneratedDepends: []string{"a", "b"},
			},
			{
				Name:             "product_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a * b",
				GeneratedDepends: []string{"a", "b"},
			},
			{
				Name:             "quotient_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a / b",
				GeneratedDepends: []string{"a", "b"},
			},
			{
				Name:             "modulo_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a % b",
				GeneratedDepends: []string{"a", "b"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入测试数据
	rows := []domain.Row{
		{"id": int64(1), "a": int64(10), "b": int64(3)},
		{"id": int64(2), "a": int64(20), "b": int64(4)},
		{"id": int64(3), "a": int64(15), "b": int64(5)},
	}
	_, err = ds.Insert(ctx, "arithmetic", rows, nil)
	assert.NoError(t, err)

	// 查询并验证
	result, err := ds.Query(ctx, "arithmetic", &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))

	// 验证算术运算结果（使用float64因为计算返回浮点数）
	assert.Equal(t, 13.0, result.Rows[0]["sum_col"])               // 10 + 3
	assert.Equal(t, 7.0, result.Rows[0]["diff_col"])               // 10 - 3
	assert.Equal(t, 30.0, result.Rows[0]["product_col"])           // 10 * 3
	assert.InDelta(t, 3.333, result.Rows[0]["quotient_col"], 0.01) // 10 / 3 (浮点除法)
	assert.Equal(t, 1.0, result.Rows[0]["modulo_col"])             // 10 % 3

	assert.Equal(t, 24.0, result.Rows[1]["sum_col"])     // 20 + 4
	assert.Equal(t, 16.0, result.Rows[1]["diff_col"])    // 20 - 4
	assert.Equal(t, 80.0, result.Rows[1]["product_col"]) // 20 * 4
	assert.Equal(t, 5.0, result.Rows[1]["quotient_col"]) // 20 / 4
	assert.Equal(t, 0.0, result.Rows[1]["modulo_col"])   // 20 % 4
}

// TestGeneratedColumns_NullPropagation 测试NULL值传播
func TestGeneratedColumns_NullPropagation(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建表
	tableInfo := &domain.TableInfo{
		Name: "null_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "a", Type: "int64"},
			{Name: "b", Type: "int64"},
			{
				Name:             "sum_col",
				Type:             "int64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a + b",
				GeneratedDepends: []string{"a", "b"},
			},
			{
				Name:             "product_col",
				Type:             "int64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "a * b",
				GeneratedDepends: []string{"a", "b"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	t.Run("null in one column", func(t *testing.T) {
		// 插入包含NULL的数据
		rows := []domain.Row{
			{"id": int64(1), "a": int64(10), "b": nil},
		}
		_, err = ds.Insert(ctx, "null_test", rows, nil)
		assert.NoError(t, err)

		// 查询第一行
		result, err := ds.Query(ctx, "null_test", &domain.QueryOptions{
			Filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: int64(1)},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))

		// NULL传播：任何操作数包含NULL，结果为NULL
		assert.Nil(t, result.Rows[0]["sum_col"])
		assert.Nil(t, result.Rows[0]["product_col"])

		// 插入第二行
		rows2 := []domain.Row{
			{"id": int64(2), "a": nil, "b": int64(5)},
		}
		_, err = ds.Insert(ctx, "null_test", rows2, nil)
		assert.NoError(t, err)

		// 查询第二行
		result2, err := ds.Query(ctx, "null_test", &domain.QueryOptions{
			Filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: int64(2)},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result2.Rows))

		// NULL传播：任何操作数包含NULL，结果为NULL
		assert.Nil(t, result2.Rows[0]["sum_col"])
		assert.Nil(t, result2.Rows[0]["product_col"])
	})

}

// TestGeneratedColumns_ErrorScenarios 测试错误场景
func TestGeneratedColumns_ErrorScenarios(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	t.Run("divide by zero", func(t *testing.T) {
		// 创建包含除法的生成列
		tableInfo := &domain.TableInfo{
			Name: "divide_test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Primary: true},
				{Name: "numerator", Type: "float64"},
				{Name: "denominator", Type: "float64"},
				{
					Name:             "quotient",
					Type:             "float64",
					IsGenerated:      true,
					GeneratedType:    "VIRTUAL",
					GeneratedExpr:    "numerator / denominator",
					GeneratedDepends: []string{"numerator", "denominator"},
				},
			},
		}
		err := ds.CreateTable(ctx, tableInfo)
		assert.NoError(t, err)

		// 插入除零数据
		rows := []domain.Row{
			{"id": int64(1), "numerator": 10.0, "denominator": 0.0},
		}
		_, err = ds.Insert(ctx, "divide_test", rows, nil)
		assert.NoError(t, err)

		// 查询验证（除零应该返回NULL）
		result, err := ds.Query(ctx, "divide_test", &domain.QueryOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
		// 除零返回NULL
		assert.Nil(t, result.Rows[0]["quotient"])
	})
}

// TestGeneratedColumns_Cascading 测试级联生成列
func TestGeneratedColumns_Cascading(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	err := ds.Connect(context.Background())
	assert.NoError(t, err)
	defer ds.Close(context.Background())

	ctx := context.Background()

	// 创建包含级联生成列的表（生成列不引用其他生成列，避免循环依赖）
	tableInfo := &domain.TableInfo{
		Name: "cascading",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "price", Type: "float64"},
			{Name: "tax_rate", Type: "float64"},
			{Name: "discount", Type: "float64"},
			{
				Name:             "total_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "price * (1 + tax_rate / 100)",
				GeneratedDepends: []string{"price", "tax_rate"},
			},
			{
				Name:             "final_col",
				Type:             "float64",
				IsGenerated:      true,
				GeneratedType:    "VIRTUAL",
				GeneratedExpr:    "total_col * (1 - discount / 100)",
				GeneratedDepends: []string{"total_col", "discount"},
			},
		},
	}

	err = ds.CreateTable(ctx, tableInfo)
	assert.NoError(t, err)

	// 插入数据
	rows := []domain.Row{
		{"id": int64(1), "price": 100.0, "tax_rate": 10.0, "discount": 5.0},
	}
	_, err = ds.Insert(ctx, "cascading", rows, nil)
	assert.NoError(t, err)

	// 查询并验证级联计算
	result, err := ds.Query(ctx, "cascading", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))

	// total_col = 100 * (1 + 10/100) = 100 * 1.1 = 110
	if val := result.Rows[0]["total_col"]; val != nil {
		assert.InDelta(t, 110.0, val, 0.01)
	} else {
		t.Error("total_col should not be nil")
	}

	// final_col = 110 * (1 - 5/100) = 110 * 0.95 = 104.5
	if val := result.Rows[0]["final_col"]; val != nil {
		assert.InDelta(t, 104.5, val, 0.01)
	} else {
		t.Error("final_col should not be nil")
	}
}
