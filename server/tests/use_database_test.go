package tests

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_UseDatabaseAfterConnection 测试连接后 USE 命令是否能正确设置数据库
func TestE2E_UseDatabaseAfterConnection(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 1. 先查询当前数据库（应该是 default）
		var initialDB string
		err := conn.QueryRow("SELECT DATABASE()").Scan(&initialDB)
		if err != nil {
			return fmt.Errorf("initial SELECT DATABASE() failed: %w", err)
		}
		t.Logf("Initial database: %s", initialDB)
		assert.Equal(t, "default", initialDB, "Initial database should be default")

		// 2. 切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 3. 验证当前数据库已切换到 information_schema
		var currentDB string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&currentDB)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE failed: %w", err)
		}
		t.Logf("Current database after USE: %s", currentDB)
		t.Logf("Expected: information_schema, Got: %s", currentDB)

		// BUG: 这里应该返回 information_schema，但实际返回 default
		if currentDB != "information_schema" {
			return fmt.Errorf("DATABASE() returned %q but expected 'information_schema' - BUG REPRODUCED", currentDB)
		}
		t.Logf("✓ DATABASE() correctly returned 'information_schema'")

		// 4. 验证可以查询 information_schema 的表
		rows, err := conn.Query("SHOW TABLES")
		if err != nil {
			return fmt.Errorf("SHOW TABLES failed: %w", err)
		}
		defer rows.Close()

		tableCount := 0
		for rows.Next() {
			tableCount++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating SHOW TABLES failed: %w", err)
		}

		t.Logf("Found %d tables in information_schema", tableCount)
		if tableCount == 0 {
			return fmt.Errorf("no tables found in information_schema - BUG")
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_MultipleDatabaseSwitching 测试多次切换数据库
func TestE2E_MultipleDatabaseSwitching(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		var db1 string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&db1)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}
		t.Logf("After USE information_schema: %s", db1)
		if db1 != "information_schema" {
			return fmt.Errorf("DATABASE() returned %q but expected 'information_schema'", db1)
		}

		// 切换回 `default`（使用反引号）
		_, err = conn.Exec("USE `default`")
		if err != nil {
			return fmt.Errorf("USE `default` failed: %w", err)
		}

		var db2 string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&db2)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}
		t.Logf("After USE `default`: %s", db2)
		if db2 != "default" {
			return fmt.Errorf("DATABASE() returned %q but expected 'default'", db2)
		}

		// 再次切换到 information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed again: %w", err)
		}

		var db3 string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&db3)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}
		t.Logf("After USE information_schema again: %s", db3)
		if db3 != "information_schema" {
			return fmt.Errorf("DATABASE() returned %q but expected 'information_schema'", db3)
		}

		return nil
	})

	assert.NoError(t, err)
}
