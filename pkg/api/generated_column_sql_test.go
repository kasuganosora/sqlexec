package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
)

// TestGeneratedColumns_SQLIntegration 测试通过SQL使用生成列的完整流程
func TestGeneratedColumns_SQLIntegration(t *testing.T) {
	t.Run("CREATE TABLE with virtual generated column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 使用SQL创建包含VIRTUAL生成列的表
		result, err := session.Execute(`
			CREATE TABLE products (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				price DECIMAL(10,2),
				quantity INT,
				total_price DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
			)
		`)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(0), result.RowsAffected)

		// 验证表已创建
		tables, err := ds.GetTables(context.Background())
		assert.NoError(t, err)
		assert.Contains(t, tables, "products")

		// 验证表结构
		tableInfo, err := ds.GetTableInfo(context.Background(), "products")
		assert.NoError(t, err)
		assert.Equal(t, 5, len(tableInfo.Columns))

		// 验证total_price是生成列
		totalCol := tableInfo.Columns[4]
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "VIRTUAL", totalCol.GeneratedType)
		assert.Equal(t, "price * quantity", totalCol.GeneratedExpr)
		assert.Contains(t, totalCol.GeneratedDepends, "price")
		assert.Contains(t, totalCol.GeneratedDepends, "quantity")
	})

	t.Run("CREATE TABLE with stored generated column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 使用SQL创建包含STORED生成列的表
		result, err := session.Execute(`
			CREATE TABLE orders (
				id INT PRIMARY KEY,
				price DECIMAL(10,2),
				tax_rate DECIMAL(5,2),
				total_amount DECIMAL(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate / 100)) STORED
			)
		`)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// 验证表结构
		tableInfo, err := ds.GetTableInfo(context.Background(), "orders")
		assert.NoError(t, err)

		totalCol := tableInfo.Columns[3]
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "STORED", totalCol.GeneratedType)
	})

	t.Run("INSERT and SELECT with generated column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 创建表
		_, err = session.Execute(`
			CREATE TABLE items (
				id INT PRIMARY KEY,
				price DECIMAL(10,2),
				quantity INT,
				total DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
			)
		`)
		assert.NoError(t, err)

		// 插入数据
		result, err := session.Execute(`INSERT INTO items (id, price, quantity) VALUES (1, 10.50, 5)`)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected)

		result, err = session.Execute(`INSERT INTO items (id, price, quantity) VALUES (2, 20.00, 3)`)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected)

		// 查询数据，验证生成列的值
		query, err := session.Query(`SELECT id, price, quantity, total FROM items ORDER BY id`)
		assert.NoError(t, err)
		defer query.Close()

		var rows []map[string]interface{}
		for query.Next() {
			row := query.Row()
			rows = append(rows, row)
		}

		assert.Equal(t, 2, len(rows))

		// 验证第一行数据
		row1 := rows[0]
		assert.Equal(t, float64(1), row1["id"])
		assert.Equal(t, 10.50, row1["price"])
		assert.Equal(t, float64(5), row1["quantity"])
		assert.Equal(t, 52.50, row1["total"])

		// 验证第二行数据
		row2 := rows[1]
		assert.Equal(t, float64(2), row2["id"])
		assert.Equal(t, 20.00, row2["price"])
		assert.Equal(t, float64(3), row2["quantity"])
		assert.Equal(t, 60.00, row2["total"])
	})

	t.Run("UPDATE and verify generated column recalculation", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 创建表
		_, err = session.Execute(`
			CREATE TABLE products (
				id INT PRIMARY KEY,
				price DECIMAL(10,2),
				quantity INT,
				total DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
			)
		`)
		assert.NoError(t, err)

		// 插入初始数据
		_, err = session.Execute(`INSERT INTO products (id, price, quantity) VALUES (1, 100.00, 2)`)
		assert.NoError(t, err)

		// 查询初始值
		row, _ := session.QueryOne(`SELECT total FROM products WHERE id = 1`)
		assert.Equal(t, 200.00, row["total"])

		// 更新price
		_, err = session.Execute(`UPDATE products SET price = 150.00 WHERE id = 1`)
		assert.NoError(t, err)

		// 验证生成列重新计算
		row, _ = session.QueryOne(`SELECT total FROM products WHERE id = 1`)
		assert.Equal(t, 300.00, row["total"])

		// 更新quantity
		_, err = session.Execute(`UPDATE products SET quantity = 4 WHERE id = 1`)
		assert.NoError(t, err)

		// 验证生成列再次重新计算
		row, _ = session.QueryOne(`SELECT total FROM products WHERE id = 1`)
		assert.Equal(t, 600.00, row["total"])
	})

	t.Run("multiple generated columns", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 创建包含多个生成列的表
		_, err = session.Execute(`
			CREATE TABLE rectangles (
				id INT PRIMARY KEY,
				width DECIMAL(10,2),
				height DECIMAL(10,2),
				area DECIMAL(10,2) GENERATED ALWAYS AS (width * height) VIRTUAL,
				perimeter DECIMAL(10,2) GENERATED ALWAYS AS (2 * (width + height)) VIRTUAL
			)
		`)
		assert.NoError(t, err)

		// 插入数据
		_, err = session.Execute(`INSERT INTO rectangles (id, width, height) VALUES (1, 5.0, 3.0)`)
		assert.NoError(t, err)

		// 查询并验证两个生成列
		row, _ := session.QueryOne(`SELECT width, height, area, perimeter FROM rectangles WHERE id = 1`)
		assert.Equal(t, 5.0, row["width"])
		assert.Equal(t, 3.0, row["height"])
		assert.Equal(t, 15.0, row["area"])
		assert.Equal(t, 16.0, row["perimeter"])
	})

	t.Run("MySQL triangle example - validation logic", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// MySQL官方三角形验证示例
		_, err = session.Execute(`
			CREATE TABLE triangles (
				id INT PRIMARY KEY,
				sidea DOUBLE,
				sideb DOUBLE,
				sidec DOUBLE,
				triangle_check1 BOOLEAN GENERATED ALWAYS AS (sidea + sideb > sidec) VIRTUAL,
				triangle_check2 BOOLEAN GENERATED ALWAYS AS (sidea + sidec > sideb) VIRTUAL,
				triangle_check3 BOOLEAN GENERATED ALWAYS AS (sideb + sidec > sidea) VIRTUAL
			)
		`)
		assert.NoError(t, err)

		// 插入有效三角形 (3, 4, 5)
		_, err = session.Execute(`INSERT INTO triangles (id, sidea, sideb, sidec) VALUES (1, 3.0, 4.0, 5.0)`)
		assert.NoError(t, err)

		// 验证
		row, _ := session.QueryOne(`
			SELECT triangle_check1, triangle_check2, triangle_check3 
			FROM triangles WHERE id = 1
		`)
		assert.Equal(t, true, row["triangle_check1"])
		assert.Equal(t, true, row["triangle_check2"])
		assert.Equal(t, true, row["triangle_check3"])

		// 插入无效三角形 (1, 1, 10)
		_, err = session.Execute(`INSERT INTO triangles (id, sidea, sideb, sidec) VALUES (2, 1.0, 1.0, 10.0)`)
		assert.NoError(t, err)

		// 验证至少有一个条件为false
		row, _ = session.QueryOne(`
			SELECT triangle_check1, triangle_check2, triangle_check3 
			FROM triangles WHERE id = 2
		`)
		// 1 + 1 > 10 应该为false
		assert.Equal(t, false, row["triangle_check1"])
	})

	t.Run("implicit virtual generated column", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 省略VIRTUAL/STORED，默认为VIRTUAL
		_, err = session.Execute(`
			CREATE TABLE employees (
				id INT PRIMARY KEY,
				base_salary DECIMAL(10,2),
				bonus DECIMAL(10,2),
				total_compensation DECIMAL(10,2) GENERATED ALWAYS AS (base_salary + bonus)
			)
		`)
		assert.NoError(t, err)

		// 验证默认是VIRTUAL
		tableInfo, _ := ds.GetTableInfo(context.Background(), "employees")
		totalCol := tableInfo.Columns[3]
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "VIRTUAL", totalCol.GeneratedType)

		// 插入并查询
		_, err = session.Execute(`INSERT INTO employees (id, base_salary, bonus) VALUES (1, 5000.00, 1000.00)`)
		assert.NoError(t, err)

		row, _ := session.QueryOne(`SELECT total_compensation FROM employees WHERE id = 1`)
		assert.Equal(t, 6000.00, row["total_compensation"])
	})

	t.Run("DELETE with generated columns", func(t *testing.T) {
		ds := memory.NewMVCCDataSource(nil)
		err := ds.Connect(context.Background())
		assert.NoError(t, err)
		defer ds.Close(context.Background())

		db, _ := NewDB(nil)
		_ = db.RegisterDataSource("test", ds)
		_ = db.SetDefaultDataSource("test")

		session := db.Session()
		defer session.Close()

		// 创建表
		_, err = session.Execute(`
			CREATE TABLE test_table (
				id INT PRIMARY KEY,
				value INT,
				doubled INT GENERATED ALWAYS AS (value * 2) VIRTUAL
			)
		`)
		assert.NoError(t, err)

		// 插入多条数据
		_, err = session.Execute(`INSERT INTO test_table (id, value) VALUES (1, 10), (2, 20), (3, 30)`)
		assert.NoError(t, err)

		// 删除一条
		result, err := session.Execute(`DELETE FROM test_table WHERE id = 2`)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result.RowsAffected)

		// 验证剩余数据
		query, _ := session.Query(`SELECT id, value, doubled FROM test_table ORDER BY id`)
		defer query.Close()

		var rows []map[string]interface{}
		for query.Next() {
			rows = append(rows, query.Row())
		}

		assert.Equal(t, 2, len(rows))
		assert.Equal(t, float64(1), rows[0]["id"])
		assert.Equal(t, int64(20), rows[0]["doubled"])
		assert.Equal(t, float64(3), rows[1]["id"])
		assert.Equal(t, int64(60), rows[1]["doubled"])
	})
}
