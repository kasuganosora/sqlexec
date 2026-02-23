package testing

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_ShowDatabases_BugReproduction tests SHOW DATABASES command (reproduce bug)
func TestE2E_ShowDatabases_BugReproduction(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 鍚姩鏈嶅姟鍣?
	err := testServer.Start(13308)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 鎵ц SHOW DATABASES
		rows, err := conn.Query("SHOW DATABASES")
		if err != nil {
			return fmt.Errorf("SHOW DATABASES failed: %w", err)
		}
		defer rows.Close()

		// 鏀堕泦鎵€鏈夋暟鎹簱鍚?
		var databases []string
		for rows.Next() {
			var dbName string
			if err := rows.Scan(&dbName); err != nil {
				return fmt.Errorf("scan database name failed: %w", err)
			}
			databases = append(databases, dbName)
		}

		// 楠岃瘉缁撴灉
		assert.Contains(t, databases, "information_schema", "Should contain 'information_schema'")
		assert.Contains(t, databases, "default", "Should contain 'default'")

		t.Logf("Found databases: %v", databases)

		return nil
	})

	assert.NoError(t, err)
}

// TestE2E_ShowTablesInInformationSchema 娴嬭瘯鍦?information_schema 涓墽琛?SHOW TABLES
func TestE2E_ShowTablesInInformationSchema(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 鍚姩鏈嶅姟鍣?
	err := testServer.Start(13309)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 鍒囨崲鍒?information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 鎵ц SHOW TABLES
		rows, err := conn.Query("SHOW TABLES")
		if err != nil {
			return fmt.Errorf("SHOW TABLES in information_schema failed: %w", err)
		}
		defer rows.Close()

		// 鏀堕泦鎵€鏈夎〃鍚?
		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("scan table name failed: %w", err)
			}
			tables = append(tables, tableName)
		}

		// 楠岃瘉缁撴灉
		t.Logf("Found tables in information_schema: %v", tables)

		// information_schema 搴旇鍖呭惈浠ヤ笅琛?
		expectedTables := []string{
			"schemata",
			"tables",
			"columns",
			"table_constraints",
			"key_column_usage",
			// 鏉冮檺琛紙澶у啓鍚嶇О锛?
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

// TestE2E_UseInformationSchema 娴嬭瘯 USE information_schema 鍛戒护
func TestE2E_UseInformationSchema(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 鍚姩鏈嶅姟鍣?
	err := testServer.Start(13310)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 鍒囨崲鍒?information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 楠岃瘉褰撳墠鏁版嵁搴?
		var dbName string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() failed: %w", err)
		}

		if dbName != "information_schema" {
			return fmt.Errorf("expected database 'information_schema', got '%s' - BUG REPRODUCED", dbName)
		}

		// 灏濊瘯鏌ヨ information_schema 鐨勮〃
		rows, err := conn.Query("SELECT * FROM schemata")
		if err != nil {
			return fmt.Errorf("SELECT from information_schema.schemata failed: %w", err)
		}
		defer rows.Close()

		// 鏌ヨ鏁版嵁
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

// TestE2E_InformationSchemaIntegration 娴嬭瘯 information_schema 瀹屾暣鍔熻兘
func TestE2E_InformationSchemaIntegration(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 鍚姩鏈嶅姟鍣?
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

// querySingleColumn 杈呭姪鍑芥暟锛氭墽琛屾煡璇㈠苟杩斿洖鍗曞垪缁撴灉
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
