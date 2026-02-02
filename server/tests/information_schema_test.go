package tests

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_ShowDatabases_BugReproduction 测试 SHOW DATABASES 命令（复现 bug）
func TestE2E_ShowDatabases_BugReproduction(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13308)
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

		// 收集所有数据库名
		var databases []string
		for rows.Next() {
			var dbName string
			if err := rows.Scan(&dbName); err != nil {
				return fmt.Errorf("scan database name failed: %w", err)
			}
			databases = append(databases, dbName)
		}

		// 验证结果
		assert.Contains(t, databases, "information_schema", "Should contain 'information_schema'")
		assert.Contains(t, databases, "default", "Should contain 'default'")

		t.Logf("Found databases: %v", databases)

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_ShowTablesInInformationSchema 测试在 information_schema 中执行 SHOW TABLES
func TestE2E_ShowTablesInInformationSchema(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13309)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 切换到 information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 执行 SHOW TABLES
		rows, err := conn.Query("SHOW TABLES")
		if err != nil {
			return fmt.Errorf("SHOW TABLES in information_schema failed: %w", err)
		}
		defer rows.Close()

		// 收集所有表名
		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("scan table name failed: %w", err)
			}
			tables = append(tables, tableName)
		}

		// 验证结果
		t.Logf("Found tables in information_schema: %v", tables)

		// information_schema 应该包含以下表
		expectedTables := []string{
			"schemata",
			"tables",
			"columns",
			"table_constraints",
			"key_column_usage",
			// 权限表（大写名称）
			"USER_PRIVILEGES",
			"SCHEMA_PRIVILEGES",
			"TABLE_PRIVILEGES",
			"COLUMN_PRIVILEGES",
		}

		for _, expected := range expectedTables {
			assert.Contains(t, tables, expected, "Should contain '%s' table", expected)
		}

		if len(tables) == 0 {
			return fmt.Errorf("SHOW TABLES in information_schema returned empty result - BUG REPRODUCED")
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_UseInformationSchema 测试 USE information_schema 命令
func TestE2E_UseInformationSchema(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13310)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 切换到 information_schema
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
			return fmt.Errorf("expected database 'information_schema', got '%s' - BUG REPRODUCED", dbName)
		}

		// 尝试查询 information_schema 的表
		rows, err := conn.Query("SELECT * FROM schemata")
		if err != nil {
			return fmt.Errorf("SELECT from information_schema.schemata failed: %w", err)
		}
		defer rows.Close()

		// 查询数据
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterating rows failed: %w", err)
		}

		t.Logf("Information schema schemata row count: %d", rowCount)

		if rowCount == 0 {
			return fmt.Errorf("information_schema.schemata is empty - BUG REPRODUCED")
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_InformationSchemaIntegration 测试 information_schema 完整功能
func TestE2E_InformationSchemaIntegration(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13311)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 1. SHOW DATABASES
		dbs, err := querySingleColumn(conn, "SHOW DATABASES")
		if err != nil {
			return err
		}
		t.Logf("Databases: %v", dbs)
		assert.Contains(t, dbs, "information_schema")

		// 2. USE information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 3. SHOW TABLES
		tables, err := querySingleColumn(conn, "SHOW TABLES")
		if err != nil {
			return err
		}
		t.Logf("Tables in information_schema: %v", tables)

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
				return fmt.Errorf("missing table '%s' in information_schema - BUG", expected)
			}
		}

		// 4. Query specific tables
		schemataRows, err := querySingleColumn(conn, "SELECT schema_name FROM schemata")
		if err != nil {
			return err
		}
		t.Logf("Schemata: %v", schemataRows)
		assert.Contains(t, schemataRows, "information_schema")

		return nil
	})

	assert.NoError(t, err)
}

// querySingleColumn 辅助函数：执行查询并返回单列结果
func querySingleColumn(conn *sql.DB, query string) ([]string, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		results = append(results, value)
	}
	return results, nil
}

