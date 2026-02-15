package api

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// TestInformationSchema_Schemata 测试 information_schema.schemata 表
func TestInformationSchema_Schemata(t *testing.T) {
	// 创建测试数据源
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试查询 schemata 表
	rows, err := session.QueryAll("SELECT * FROM information_schema.schemata")
	assert.NoError(t, err)
	assert.NotEmpty(t, rows)

	// 验证结果包含注册的数据源
	found := false
	for _, row := range rows {
		if row["schema_name"] == "testdb" {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find 'testdb' in schemata")

	session.Close()
}

// TestInformationSchema_Tables 测试 information_schema.tables 表
func TestInformationSchema_Tables(t *testing.T) {
	// 创建测试数据源
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试查询 tables 表
	rows, err := session.QueryAll("SELECT * FROM information_schema.tables WHERE table_schema = 'testdb'")
	assert.NoError(t, err)
	assert.NotEmpty(t, rows)

	// 验证结果包含表信息
	for _, row := range rows {
		assert.Equal(t, "testdb", row["table_schema"])
		assert.NotEmpty(t, row["table_name"])
	}

	session.Close()
}

// TestInformationSchema_Columns 测试 information_schema.columns 表
func TestInformationSchema_Columns(t *testing.T) {
	// 创建测试数据源
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
		{Name: "email", Type: "varchar(255)", Nullable: true, Unique: true},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试查询 columns 表
	rows, err := session.QueryAll("SELECT * FROM information_schema.columns WHERE table_schema = 'testdb'")
	assert.NoError(t, err)
	assert.NotEmpty(t, rows)

	// 验证结果包含列信息
	foundColumns := make(map[string]bool)
	for _, row := range rows {
		colName := row["column_name"].(string)
		foundColumns[colName] = true
		assert.Equal(t, "testdb", row["table_schema"])
		assert.NotEmpty(t, row["data_type"])
	}

	// 验证所有列都被找到
	assert.True(t, foundColumns["id"], "Should find 'id' column")
	assert.True(t, foundColumns["name"], "Should find 'name' column")
	assert.True(t, foundColumns["email"], "Should find 'email' column")

	session.Close()
}

// TestInformationSchema_TableConstraints 测试 information_schema.table_constraints 表
func TestInformationSchema_TableConstraints(t *testing.T) {
	// 创建测试数据源（带主键和唯一约束）
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "email", Type: "varchar(255)", Nullable: true, Unique: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试查询 table_constraints 表
	rows, err := session.QueryAll("SELECT * FROM information_schema.table_constraints WHERE table_schema = 'testdb'")
	assert.NoError(t, err)
	assert.NotEmpty(t, rows)

	// 验证结果包含约束信息
	for _, row := range rows {
		assert.Equal(t, "testdb", row["table_schema"])
		constraintType := row["constraint_type"].(string)
		assert.Contains(t, []string{"PRIMARY KEY", "UNIQUE"}, constraintType)
	}

	session.Close()
}

// TestInformationSchema_KeyColumnUsage 测试 information_schema.key_column_usage 表
func TestInformationSchema_KeyColumnUsage(t *testing.T) {
	// 创建测试数据源（带主键和唯一约束）
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "email", Type: "varchar(255)", Nullable: true, Unique: true},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试查询 key_column_usage 表
	rows, err := session.QueryAll("SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'testdb'")
	assert.NoError(t, err)
	assert.NotEmpty(t, rows)

	// 验证结果包含键信息
	foundIDKey := false
	for _, row := range rows {
		assert.Equal(t, "testdb", row["table_schema"])
		colName := row["column_name"].(string)
		if colName == "id" && row["constraint_name"].(string) == "PRIMARY" {
			foundIDKey = true
		}
	}
	assert.True(t, foundIDKey, "Should find PRIMARY key for 'id' column")

	session.Close()
}

// TestInformationSchema_SelectAll 测试 SELECT * FROM information_schema.schemata
func TestInformationSchema_SelectAll(t *testing.T) {
	// 创建多个测试数据源
	ds1 := createTestDataSource(t, "db1", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
	})
	ds2 := createTestDataSource(t, "db2", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("db1", ds1)
	assert.NoError(t, err)
	err = db.RegisterDataSource("db2", ds2)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试 SELECT *
	rows, err := session.QueryAll("SELECT * FROM information_schema.schemata")
	assert.NoError(t, err)
	assert.Len(t, rows, 3) // Should have 3 databases (information_schema, db1 and db2) - config only when registry is set

	session.Close()
}

// TestInformationSchema_WhereClause 测试 WHERE 子句
func TestInformationSchema_WhereClause(t *testing.T) {
	// 创建多个测试数据源
	ds1 := createTestDataSource(t, "db1", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
	})
	ds2 := createTestDataSource(t, "db2", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("db1", ds1)
	assert.NoError(t, err)
	err = db.RegisterDataSource("db2", ds2)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试 WHERE 子句
	rows, err := session.QueryAll("SELECT * FROM information_schema.schemata WHERE schema_name = 'db1'")
	assert.NoError(t, err)
	assert.Len(t, rows, 1) // Should only return db1
	assert.Equal(t, "db1", rows[0]["schema_name"])

	session.Close()
}

// TestInformationSchema_ReadOnly 测试 information_schema 只读特性
func TestInformationSchema_ReadOnly(t *testing.T) {
	// 创建测试数据源
	testDS := createTestDataSource(t, "testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
	})

	// 创建 DB 并注册数据源
	db, err := NewDB(&DBConfig{
		CacheEnabled:  false,
		DefaultLogger: NewDefaultLogger(LogInfo),
	})
	assert.NoError(t, err)

	err = db.RegisterDataSource("testdb", testDS)
	assert.NoError(t, err)

	// 创建会话
	session := db.Session()

	// 测试 INSERT 应该失败
	_, err = session.Execute("INSERT INTO information_schema.schemata (schema_name) VALUES ('test')")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")

	// 测试 UPDATE 应该失败
	_, err = session.Execute("UPDATE information_schema.schemata SET schema_name = 'test' WHERE schema_name = 'db1'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")

	// 测试 DELETE 应该失败
	_, err = session.Execute("DELETE FROM information_schema.schemata WHERE schema_name = 'db1'")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")

	session.Close()
}

// createTestDataSource 创建一个简单的测试数据源
func createTestDataSource(t *testing.T, name string, columns []domain.ColumnInfo) domain.DataSource {
	return NewMockDataSourceWithTableInfo(name, columns)
}
