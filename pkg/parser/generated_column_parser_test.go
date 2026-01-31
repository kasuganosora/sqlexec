package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestParseGeneratedColumn 测试解析生成列的SQL语句
func TestParseGeneratedColumn(t *testing.T) {
	adapter := NewSQLAdapter()

	t.Run("virtual generated column", func(t *testing.T) {
		sql := `CREATE TABLE products (
			id INT PRIMARY KEY,
			price DECIMAL(10,2),
			quantity INT,
			total_price DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
		)`

		result, err := adapter.Parse(sql)
		assert.NoError(t, err)
		assert.True(t, result.Success)

		// 验证解析结果
		assert.NotNil(t, result.Statement)
		assert.Equal(t, SQLTypeCreate, result.Statement.Type)

		createStmt := result.Statement.Create
		assert.Equal(t, "products", createStmt.Name)
		assert.Equal(t, 4, len(createStmt.Columns))

		// 验证生成列
		totalCol := findColumnByName(createStmt.Columns, "total_price")
		assert.NotNil(t, totalCol)
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "VIRTUAL", totalCol.GeneratedType)
		assert.Equal(t, "price * quantity", totalCol.GeneratedExpr)
		assert.Contains(t, totalCol.GeneratedDepends, "price")
		assert.Contains(t, totalCol.GeneratedDepends, "quantity")
	})

	t.Run("stored generated column", func(t *testing.T) {
		sql := `CREATE TABLE orders (
			id INT PRIMARY KEY,
			price DECIMAL(10,2),
			tax_rate DECIMAL(5,2),
			total_with_tax DECIMAL(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate / 100)) STORED
		)`

		result, err := adapter.Parse(sql)
		assert.NoError(t, err)
		assert.True(t, result.Success)

		createStmt := result.Statement.Create
		assert.Equal(t, "orders", createStmt.Name)

		totalCol := findColumnByName(createStmt.Columns, "total_with_tax")
		assert.NotNil(t, totalCol)
		assert.True(t, totalCol.IsGenerated)
		assert.Equal(t, "STORED", totalCol.GeneratedType)
		assert.Equal(t, "price * (1 + tax_rate / 100)", totalCol.GeneratedExpr)
	})

	t.Run("implicit virtual generated column", func(t *testing.T) {
		sql := `CREATE TABLE employees (
			id INT PRIMARY KEY,
			first_name VARCHAR(50),
			last_name VARCHAR(50),
			full_name VARCHAR(100) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))
		)`

		result, err := adapter.Parse(sql)
		assert.NoError(t, err)
		assert.True(t, result.Success)

		createStmt := result.Statement.Create
		fullNameCol := findColumnByName(createStmt.Columns, "full_name")
		assert.NotNil(t, fullNameCol)
		assert.True(t, fullNameCol.IsGenerated)
		assert.Equal(t, "VIRTUAL", fullNameCol.GeneratedType) // 默认是VIRTUAL
		assert.Equal(t, "CONCAT(first_name, ' ', last_name)", fullNameCol.GeneratedExpr)
	})
}

// findColumnByName 根据列名查找列定义
func findColumnByName(columns []ColumnInfo, name string) *ColumnInfo {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}

// TestGeneratedColumnASTStructure 测试生成列的AST结构
func TestGeneratedColumnASTStructure(t *testing.T) {
	p := NewParser()

	sql := `CREATE TABLE test (
		a INT,
		b INT,
		c INT GENERATED ALWAYS AS (a + b) VIRTUAL
	)`

	stmt, err := p.ParseCreateTableStmt(sql)
	assert.NoError(t, err)
	assert.NotNil(t, stmt)

	// 检查AST结构
	assert.Equal(t, "test", stmt.Table.Name.String())
	assert.Equal(t, 3, len(stmt.Cols))

	// 检查生成列的定义
	cCol := stmt.Cols[2]
	assert.Equal(t, "c", cCol.Name.Name.String())

	// 查找 ColumnOptionGenerated
	var generatedOpt *ast.ColumnOption
	for _, opt := range cCol.Options {
		if opt.Tp == ast.ColumnOptionGenerated {
			generatedOpt = opt
			break
		}
	}

	assert.NotNil(t, generatedOpt)
	assert.False(t, generatedOpt.Stored) // VIRTUAL
	assert.NotNil(t, generatedOpt.Expr)
	assert.Equal(t, "a + b", generatedOpt.Expr.Text())
}

// TestGeneratedColumnWithComplexExpression 测试复杂表达式的生成列
func TestGeneratedColumnWithComplexExpression(t *testing.T) {
	adapter := NewSQLAdapter()

	sql := `CREATE TABLE inventory (
		id INT PRIMARY KEY,
		unit_price DECIMAL(10,2),
		quantity INT,
		discount DECIMAL(5,2),
		tax_rate DECIMAL(5,2),
		final_price DECIMAL(12,2) GENERATED ALWAYS AS (
			unit_price * quantity * (1 - discount / 100) * (1 + tax_rate / 100)
		) STORED
	)`

	result, err := adapter.Parse(sql)
	assert.NoError(t, err)
	assert.True(t, result.Success)

	createStmt := result.Statement.Create
	priceCol := findColumnByName(createStmt.Columns, "final_price")
	assert.NotNil(t, priceCol)
	assert.True(t, priceCol.IsGenerated)
	assert.Equal(t, "STORED", priceCol.GeneratedType)

	// 验证依赖的列
	assert.Contains(t, priceCol.GeneratedDepends, "unit_price")
	assert.Contains(t, priceCol.GeneratedDepends, "quantity")
	assert.Contains(t, priceCol.GeneratedDepends, "discount")
	assert.Contains(t, priceCol.GeneratedDepends, "tax_rate")
}
