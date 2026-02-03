package testing

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestE2E_UseDatabaseAfterConnection tests if USE command correctly sets database after connection
func TestE2E_UseDatabaseAfterConnection(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// Use MySQL client connection
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 1. First query current database (should be default)
		var initialDB string
		err := conn.QueryRow("SELECT DATABASE()").Scan(&initialDB)
		if err != nil {
			return fmt.Errorf("initial SELECT DATABASE() failed: %w", err)
		}
		t.Logf("Initial database: %s", initialDB)
		assert.Equal(t, "default", initialDB, "Initial database should be default")

		// 2. Switch to information_schema
		_, err = conn.Exec("USE information_schema")
		if err != nil {
			return fmt.Errorf("USE information_schema failed: %w", err)
		}

		// 3. Verify current database is switched to information_schema
		var currentDB string
		err = conn.QueryRow("SELECT DATABASE()").Scan(&currentDB)
		if err != nil {
			return fmt.Errorf("SELECT DATABASE() after USE failed: %w", err)
		}
		t.Logf("Current database after USE: %s", currentDB)
		t.Logf("Expected: information_schema, Got: %s", currentDB)

		// BUG: Should return information_schema here, but actually returns default
		if currentDB != "information_schema" {
			return fmt.Errorf("DATABASE() returned %q but expected 'information_schema' - BUG REPRODUCED", currentDB)
		}
		t.Logf("DATABASE() correctly returned 'information_schema'")

		// 4. Verify can query information_schema tables
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

// TestE2E_MultipleDatabaseSwitching tests multiple database switching
func TestE2E_MultipleDatabaseSwitching(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server
	err := testServer.Start(13306)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// Use MySQL client connection
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Switch to information_schema
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

		// Switch back to `default` (using backticks)
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

		// Switch to information_schema again
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
