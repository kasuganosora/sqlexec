package tests

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_COM_INIT_DB_Command 测试 COM_INIT_DB 命令（不是 USE 语句）
func TestE2E_COM_INIT_DB_Command(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 1. 初始查询应该返回 default
		var initialDB string
		err := conn.QueryRow("SELECT DATABASE()").Scan(&initialDB)
		if err != nil {
			return fmt.Errorf("initial SELECT DATABASE() failed: %w", err)
		}
		t.Logf("Initial database: %s", initialDB)
		assert.Equal(t, "default", initialDB)

		// 2. 使用 Exec 执行 USE 命令（会发送 COM_INIT_DB）
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 3. 验证当前数据库
		var currentDB string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&currentDB)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE failed: %w", err)
		}
		t.Logf("Current database after USE: %s", currentDB)
		t.Logf("Expected: information_schema, Got: %s", currentDB)

		// BUG: COM_INIT_DB 没有正确更新 OptimizedExecutor 的 currentDB
		if currentDB != "information_schema" {
			return fmt.Errorf("DATABASE() returned %q but expected 'information_schema' - BUG REPRODUCED", currentDB)
		}
		t.Logf("✓ DATABASE() correctly returned 'information_schema'")

		// 4. 验证可以查询 information_schema
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
