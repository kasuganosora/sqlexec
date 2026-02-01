package tests

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// =====================================================
// 场景1：正常使用流程测试
// =====================================================

// TestScenario_DatabaseSwitching 测试数据库切换流程
func TestScenario_DatabaseSwitching(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13320)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 步骤1：查询初始数据库
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}
		t.Logf("初始数据库: %s", dbName)

		// 步骤2：切换到test_db
		_, err = conn.Exec("USE test_db")
		if err != nil {
			return fmt.Errorf("USE test_db failed: %w", err)
		}

		// 步骤3：验证数据库已切换
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE failed: %w", err)
		}
		if dbName != "test_db" {
			return fmt.Errorf("expected 'test_db', got '%s'", dbName)
		}
		t.Logf("成功切换到: %s", dbName)

		// 步骤4：切换到information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 步骤5：验证数据库已切换
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE information_schema failed: %w", err)
		}
		if dbName != "information_schema" {
			return fmt.Errorf("expected 'information_schema', got '%s'", dbName)
		}
		t.Logf("成功切换到: %s", dbName)

		return nil
	})

	assert.NoError(t, err)
}

// TestScenario_InformationSchemaQuery 测试information_schema查询
func TestScenario_InformationSchemaQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13321)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 切换到information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 验证当前数据库
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}
		if dbName != "information_schema" {
			return fmt.Errorf("expected 'information_schema', got '%s'", dbName)
		}

		// 查询schemata表（不带前缀）
		rows, err := conn.Query("SELECT schema_name FROM schemata")
		if err != nil {
			return fmt.Errorf("SELECT FROM schemata failed: %w", err)
		}
		defer rows.Close()

		schemaCount := 0
		for rows.Next() {
			schemaCount++
		}
		if schemaCount == 0 {
			return fmt.Errorf("schemata table returned no data")
		}
		t.Logf("找到 %d 个数据库", schemaCount)

		// 查询tables表（不带前缀）
		rows, err = conn.Query("SELECT table_name FROM tables LIMIT 5")
		if err != nil {
			return fmt.Errorf("SELECT FROM tables failed: %w", err)
		}
		defer rows.Close()

		tableCount := 0
		for rows.Next() {
			tableCount++
		}
		t.Logf("找到 %d 个表", tableCount)

		return nil
	})

	assert.NoError(t, err)
}

// =====================================================
// 场景2：错误处理测试
// =====================================================

// TestScenario_NonExistentTable 测试查询不存在的表
func TestScenario_NonExistentTable(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13322)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 查询不存在的表
		_, err := conn.Query("SELECT * FROM non_existent_table")
		if err == nil {
			return fmt.Errorf("expected error for non-existent table, got nil")
		}

		t.Logf("正确返回错误: %v", err)

		// 验证错误后连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should still be active after error: %w", err)
		}

		// 验证可以正常查询
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("query after error should work: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestScenario_InvalidSQLSyntax 测试无效SQL语法
func TestScenario_InvalidSQLSyntax(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13323)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 无效的SQL语法
		_, err := conn.Query("SELEC * FROM users")
		if err == nil {
			return fmt.Errorf("expected error for invalid SQL, got nil")
		}

		t.Logf("正确识别语法错误: %v", err)

		// 验证连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should be active after syntax error: %w", err)
		}

		// 验证可以执行有效的查询
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("valid query after error should work: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// =====================================================
// 场景3：边界条件测试
// =====================================================

// TestScenario_PingKeepAlive 测试Ping保活
func TestScenario_PingKeepAlive(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13324)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 多次Ping测试连接保活
		for i := 0; i < 10; i++ {
			err := conn.Ping()
			if err != nil {
				return fmt.Errorf("ping %d failed: %w", i+1, err)
			}
			time.Sleep(10 * time.Millisecond)
		}

		t.Log("成功完成10次Ping")
		return nil
	})

	assert.NoError(t, err)
}

// TestScenario_EmptyQuery 测试空查询
func TestScenario_EmptyQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13325)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 执行空查询
		_, err := conn.Query("")
		if err == nil {
			return fmt.Errorf("expected error for empty query, got nil")
		}

		t.Logf("正确返回空查询错误: %v", err)

		// 验证连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should be active after empty query: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// =====================================================
// 场景4：并发和连接池测试
// =====================================================

// TestScenario_ConcurrentQueries 测试并发查询
func TestScenario_ConcurrentQueries(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13326)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 并发执行多个查询
		errChan := make(chan error, 10)

		// 启动10个并发查询
		for i := 0; i < 10; i++ {
			go func(queryID int) {
				var dbName string
				err := conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
				errChan <- err
			}(i)
		}

		// 收集结果
		successCount := 0
		errorCount := 0
		for i := 0; i < 10; i++ {
			err := <-errChan
			if err != nil {
				errorCount++
				t.Logf("并发查询 %d 失败: %v", i+1, err)
			} else {
				successCount++
			}
		}

		if errorCount > 0 {
			return fmt.Errorf("%d out of 10 concurrent queries failed", errorCount)
		}

		t.Logf("成功执行 %d 个并发查询", successCount)
		return nil
	})

	assert.NoError(t, err)
}

// TestScenario_ConnectionPool 测试连接池
func TestScenario_ConnectionPool(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13327)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 设置连接池参数
		conn.SetMaxOpenConns(5)
		conn.SetMaxIdleConns(2)
		conn.SetConnMaxLifetime(5 * time.Minute)

		// 使用多个并发连接
		errChan := make(chan error, 10)
		for i := 0; i < 10; i++ {
			go func(connID int) {
				var dbName string
				err := conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
				errChan <- err
			}(i)
		}

		// 等待所有连接完成
		for i := 0; i < 10; i++ {
			err := <-errChan
			if err != nil {
				return fmt.Errorf("connection pool operation %d failed: %w", i+1, err)
			}
		}

		t.Log("连接池测试通过")
		return nil
	})

	assert.NoError(t, err)
}

// =====================================================
// 场景5：多次数据库切换
// =====================================================

// TestScenario_MultipleDatabaseSwitches 测试多次数据库切换
func TestScenario_MultipleDatabaseSwitches(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13328)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		databases := []string{"db1", "db2", "information_schema", "db1", "information_schema"}

		for i, dbName := range databases {
			// 切换数据库
			_, err := conn.Exec("USE " + dbName)
			if err != nil {
				return fmt.Errorf("USE %s failed (iteration %d): %w", dbName, i+1, err)
			}

			// 验证当前数据库
			var currentDB string
			err = conn.QueryRow("SELECT DATABASE()").Scan(&currentDB)
			if err != nil {
				return fmt.Errorf("SELECT DATABASE() failed after USE %s: %w", dbName, err)
			}

			if currentDB != dbName {
				return fmt.Errorf("expected '%s', got '%s' after %d switches", dbName, currentDB, i+1)
			}

			t.Logf("第 %d 次切换: %s", i+1, dbName)

			// 如果在information_schema中，查询schemata表
			if dbName == "information_schema" {
				rows, err := conn.Query("SELECT schema_name FROM schemata")
				if err != nil {
					return fmt.Errorf("SELECT FROM schemata in information_schema failed: %w", err)
				}
				defer rows.Close()
				
				count := 0
				for rows.Next() {
					count++
				}
				
				if count < 1 {
					return fmt.Errorf("schemata should have at least 1 row in information_schema")
				}
				t.Logf("  - schemata表有 %d 行", count)
			}
		}

		t.Log("成功完成多次数据库切换")
		return nil
	})

	assert.NoError(t, err)
}
