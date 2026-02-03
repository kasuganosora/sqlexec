package testing

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_PrivilegeTablesVisibility tests visibility of privilege tables
func TestE2E_PrivilegeTablesVisibility(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server (use different port to avoid conflicts)
	port := 13307
	err := testServer.Start(port)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// Create custom connection
	conn, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", port))
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	// Test connection
	if err := conn.Ping(); err != nil {
		t.Fatalf("Failed to ping server: %v", err)
	}

	t.Run("Root user can see all tables", func(t *testing.T) {
		// Root user connects to information_schema
		_, err := conn.Exec("USE information_schema")
		if err != nil {
			t.Fatalf("USE information_schema failed: %v", err)
		}

		// Query table list
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

		t.Logf("Root user sees tables: %v", tables)

		// Root user should see all tables, including privilege tables
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
				t.Fatalf("Root user cannot see table: %s", expected)
			}
		}

		// Verify table count
		if len(tables) != len(expectedTables) {
			t.Fatalf("Expected %d tables, got %d", len(expectedTables), len(tables))
		}

		assert.Equal(t, len(expectedTables), len(tables))
	})

	assert.NoError(t, err)
}

// Note: Since current test framework uses root user connection,
// testing non-privileged users requires additional user management features, skipped for now
// Future extensions can support multi-user testing
