package integration_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// TestE2E_COM_INIT_DB 测试 COM_INIT_DB 命令
func TestE2E_COM_INIT_DB(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试 USE 命令
		_, err := conn.Exec("USE test_db")
		if err != nil {
			return fmt.Errorf("USE failed: %w", err)
		}

		// 验证数据库已切换
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}

		if dbName != "test_db" {
			return fmt.Errorf("expected database 'test_db', got '%s'", dbName)
		}

		// 测试切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 验证数据库已切换
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE information_schema failed: %w", err)
		}

		if dbName != "information_schema" {
			return fmt.Errorf("expected database 'information_schema', got '%s'", dbName)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_SelectDatabase 测试 SELECT DATABASE() 函数
func TestE2E_SelectDatabase(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13307)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 初始数据库应该为空或默认值
		var dbName string
		err := conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}

		// 初始状态，数据库可能为空字符串
		t.Logf("Initial database: %s", dbName)

		// 切换到 test_db
		_, err = conn.Exec("USE test_db")
		if err != nil {
			return fmt.Errorf("USE test_db failed: %w", err)
		}

		// 验证数据库
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE failed: %w", err)
		}

		if dbName != "test_db" {
			return fmt.Errorf("expected 'test_db', got '%s'", dbName)
		}

		// 切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 再次验证数据库
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE information_schema failed: %w", err)
		}

		if dbName != "information_schema" {
			return fmt.Errorf("expected 'information_schema', got '%s'", dbName)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_InformationSchemaQuery 测试 information_schema 虚拟表查询
func TestE2E_InformationSchemaQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13308)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 创建测试表
	err = testServer.CreateTestTable("test_table", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true, AutoIncrement: true, Nullable: false},
		{Name: "name", Type: "string", Nullable: false},
	})
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 查询 schemata 表（使用完整路径避免连接池问题）
		rows, err := conn.Query("SELECT * FROM information_schema.schemata")
		if err != nil {
			return fmt.Errorf("SELECT * FROM information_schema.schemata failed: %w", err)
		}
		defer rows.Close()

		// 验证有数据返回
		hasData := false
		for rows.Next() {
			hasData = true
			break
		}
		if !hasData {
			return fmt.Errorf("schemata table returned no data")
		}

		// 查询 tables 表（使用完整路径避免连接池问题）
		rows2, err := conn.Query("SELECT * FROM information_schema.tables")
		if err != nil {
			return fmt.Errorf("SELECT * FROM information_schema.tables failed: %w", err)
		}
		defer rows2.Close()

		// 验证有数据返回
		hasData2 := false
		for rows2.Next() {
			hasData2 = true
			break
		}
		if err = rows2.Err(); err != nil {
			return fmt.Errorf("rows2.Err(): %w", err)
		}
		if !hasData2 {
			return fmt.Errorf("tables table returned no data")
		}

		// 查询 columns 表（使用完整路径避免连接池问题）
		rows3, err := conn.Query("SELECT * FROM information_schema.columns")
		if err != nil {
			return fmt.Errorf("SELECT * FROM columns failed: %w", err)
		}
		defer rows3.Close()

		// 验证有数据返回
		hasData3 := false
		for rows3.Next() {
			hasData3 = true
			break
		}
		if err = rows3.Err(); err != nil {
			return fmt.Errorf("rows3.Err(): %w", err)
		}
		if !hasData3 {
			return fmt.Errorf("columns table returned no data")
		}

		// 使用完整路径查询
		rows4, err := conn.Query("SELECT * FROM information_schema.schemata")
		if err != nil {
			return fmt.Errorf("SELECT * FROM information_schema.schemata failed: %w", err)
		}
		defer rows4.Close()

		// 验证有数据返回
		hasData4 := false
		for rows4.Next() {
			hasData4 = true
			break
		}
		if err = rows4.Err(); err != nil {
			return fmt.Errorf("rows4.Err(): %w", err)
		}
		if !hasData4 {
			return fmt.Errorf("information_schema.schemata returned no data")
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_ErrorPacket 测试错误包的发送
func TestE2E_ErrorPacket(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13309)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试查询不存在的表
		_, err := conn.Query("SELECT * FROM non_existent_table")
		if err == nil {
			return fmt.Errorf("expected error for non-existent table, got nil")
		}

		// 验证错误消息
		if !containsString(err.Error(), "not found") && !containsString(err.Error(), "1146") {
			return fmt.Errorf("error message doesn't contain expected text: %v", err)
		}

		// 测试查询不存在的列
		_, err = conn.Query("SELECT non_existent_column FROM schemata")
		if err == nil {
			return fmt.Errorf("expected error for non-existent column, got nil")
		}

		// 验证连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should still be active after error: %w", err)
		}

		// 验证可以继续执行查询
		var version string
		err = conn.QueryRow("SELECT @@version_comment").Scan(&version)
		if err != nil {
			return fmt.Errorf("should be able to query after error: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_MultipleDBSwitching 测试多次数据库切换
func TestE2E_MultipleDBSwitching(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13310)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		databases := []string{"db1", "db2", "db3", "information_schema", "db1", "information_schema"}

		for _, db := range databases {
			// 切换数据库
			_, err := conn.Exec(fmt.Sprintf("USE %s", db))
			if err != nil {
				return fmt.Errorf("USE %s failed: %w", db, err)
			}

			// 验证当前数据库
			var dbName string
			err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
			if err != nil {
				return fmt.Errorf("SELECT DATABASE() failed: %w", err)
			}

			if dbName != db {
				return fmt.Errorf("expected '%s', got '%s'", db, dbName)
			}
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_ConnectionRecovery 测试连接在错误后恢复
func TestE2E_ConnectionRecovery(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13311)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 执行一系列操作

		// 1. 正常查询
		var version string
		err = conn.QueryRow("SELECT @@version_comment").Scan(&version)
		if err != nil {
			return fmt.Errorf("initial query failed: %w", err)
		}

		// 2. 切换数据库
		_, err = conn.Exec("USE test_db")
		if err != nil {
			return fmt.Errorf("USE failed: %w", err)
		}

		// 3. 查询当前数据库
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}

		// 4. 执行一个会失败的查询
		_, err = conn.Query("SELECT * FROM non_existent_table")
		if err == nil {
			return fmt.Errorf("expected error for non-existent table")
		}

		// 5. 验证连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should still be active: %w", err)
		}

		// 6. 继续执行正常查询
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after error failed: %w", err)
		}

		if dbName != "test_db" {
			return fmt.Errorf("expected 'test_db', got '%s'", dbName)
		}

		// 7. 切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 8. 查询 information_schema 表
		rows, err := conn.Query("SELECT * FROM schemata LIMIT 1")
		if err != nil {
			return fmt.Errorf("SELECT from schemata failed: %w", err)
		}
		rows.Close()

		// 9. 验证数据库仍然是 information_schema
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("final SELECT DATABASE() failed: %w", err)
		}

		if dbName != "information_schema" {
			return fmt.Errorf("expected 'information_schema', got '%s'", dbName)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_InformationSchemaWithRealData 测试 information_schema 与真实数据
func TestE2E_InformationSchemaWithRealData(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13312)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 创建测试表
	err = testServer.CreateTestTable("users", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true, AutoIncrement: true, Nullable: false},
		{Name: "name", Type: "string", Nullable: false},
		{Name: "age", Type: "int", Nullable: true},
	})

	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// 插入测试数据
	err = testServer.InsertTestData("users", []domain.Row{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
	})

	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 切换到 information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 查询 tables 表，找到 users 表
		rows, err := conn.Query("SELECT table_name, table_schema FROM tables WHERE table_name = 'users'")
		if err != nil {
			return fmt.Errorf("SELECT from tables failed: %w", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			var tableName, tableSchema string
			err = rows.Scan(&tableName, &tableSchema)
			if err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}

			if tableName == "users" {
				found = true
				t.Logf("Found table: %s in schema: %s", tableName, tableSchema)
			}
		}

		if !found {
			return fmt.Errorf("users table not found in information_schema.tables")
		}

		// 查询 columns 表，找到 users 表的列
		rows, err = conn.Query("SELECT column_name, data_type FROM columns WHERE table_name = 'users'")
		if err != nil {
			return fmt.Errorf("SELECT from columns failed: %w", err)
		}
		defer rows.Close()

		columnCount := 0
		for rows.Next() {
			var columnName, dataType string
			err = rows.Scan(&columnName, &dataType)
			if err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			columnCount++
			t.Logf("Found column: %s with type: %s", columnName, dataType)
		}

		if columnCount < 3 {
			return fmt.Errorf("expected at least 3 columns, got %d", columnCount)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_DatabaseContextCache 测试数据库上下文缓存
func TestE2E_DatabaseContextCache(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13313)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 在 db1 中执行 SELECT DATABASE()
		_, err := conn.Exec("USE db1")
		if err != nil {
			return fmt.Errorf("USE db1 failed: %w", err)
		}

		var db1 string
		for i := 0; i < 3; i++ {
			err = conn.QueryRow("SELECT DATABASE()").Scan(&db1)
			if err != nil {
				return fmt.Errorf("SELECT DATABASE() failed in db1: %w", err)
			}
			if db1 != "db1" {
				return fmt.Errorf("iteration %d: expected 'db1', got '%s'", i, db1)
			}
		}

		// 切换到 db2
		_, err = conn.Exec("USE db2")
		if err != nil {
			return fmt.Errorf("USE db2 failed: %w", err)
		}

		// 多次执行 SELECT DATABASE()，确保缓存正确更新
		var db2 string
		for i := 0; i < 3; i++ {
			err = conn.QueryRow("SELECT DATABASE()").Scan(&db2)
			if err != nil {
				return fmt.Errorf("SELECT DATABASE() failed in db2: %w", err)
			}
			if db2 != "db2" {
				return fmt.Errorf("iteration %d: expected 'db2', got '%s'", i, db2)
			}
		}

		// 切换回 db1
		_, err = conn.Exec("USE db1")
		if err != nil {
			return fmt.Errorf("USE db1 again failed: %w", err)
		}

		var db1Again string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&db1Again)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed in db1 again: %w", err)
		}

		if db1Again != "db1" {
			return fmt.Errorf("expected 'db1', got '%s'", db1Again)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_ShowDatabases 测试 SHOW DATABASES 命令
func TestE2E_ShowDatabases(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13314)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 执行 SHOW DATABASES
		rows, err := conn.Query("SHOW DATABASES")
		if err != nil {
			return fmt.Errorf("SHOW DATABASES failed: %w", err)
		}
		defer rows.Close()

		// 验证有数据库返回
		foundDBs := make(map[string]bool)
		for rows.Next() {
			var dbName string
			err = rows.Scan(&dbName)
			if err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			foundDBs[dbName] = true
		}

		// 检查是否有信息架构数据库
		if !foundDBs["information_schema"] {
			return fmt.Errorf("information_schema not found in SHOW DATABASES")
		}

		t.Logf("Found databases: %v", foundDBs)

		return nil
	})

	assert.NoError(t, err)
}

// containsString 检查字符串是否包含子串
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
