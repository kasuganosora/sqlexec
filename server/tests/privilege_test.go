package tests

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_PrivilegeTablesVisibility 测试权限表的可见性
func TestE2E_PrivilegeTablesVisibility(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器（使用不同端口避免冲突）
	port := 13307
	err := testServer.Start(port)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 创建自定义连接
	conn, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", port))
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	// 测试连接
	if err := conn.Ping(); err != nil {
		t.Fatalf("Failed to ping server: %v", err)
	}


	t.Run("Root用户可以看到所有表", func(t *testing.T) {
		// root 用户连接到 information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			t.Fatalf("USE information_schema failed: %v", err)
		}

		// 查询表列表
		rows, err := conn.Query("SHOW TABLES")
		if err != nil {
			t.Fatalf("SHOW TABLES failed: %v", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			tables = append(tables, tableName)
		}

		t.Logf("Root用户看到的表: %v", tables)

		// root 用户应该能看到所有表，包括权限表
		expectedTables := []string{
			"schemata",
			"tables",
			"columns",
			"table_constraints",
			"key_column_usage",
			"USER_PRIVILEGES",
			"SCHEMA_PRIVILEGES",
			"TABLE_PRIVILEGES",
			"COLUMN_PRIVILEGES",
		}

		for _, expected := range expectedTables {
			found := false
			for _, table := range tables {
				if table == expected {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("root用户看不到表: %s", expected)
			}
		}

		// 验证表数量
		if len(tables) != len(expectedTables) {
			t.Fatalf("期望 %d 个表，实际看到 %d 个", len(expectedTables), len(tables))
		}

		assert.Equal(t, len(expectedTables), len(tables))
	})

	assert.NoError(t, err)
}

// 注意：由于当前测试框架使用 root 用户连接，
// 测试非特权用户需要额外的用户管理功能，这里暂时跳过
// 未来可以扩展支持多用户测试
