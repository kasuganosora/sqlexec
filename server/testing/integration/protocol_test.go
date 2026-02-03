package testing

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// =====================================================
// Protocol layer tests - Test correct handling of MySQL protocol packets
// =====================================================

// TestProtocol_Connection tests connection and authentication
func TestProtocol_Connection(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server
	err := testServer.Start(13301)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// Use MySQL client connection
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Test connection and Ping
		if err := conn.Ping(); err != nil {
			return fmt.Errorf("ping failed: %w", err)
		}
		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_SimpleQuery tests simple SELECT query
func TestProtocol_SimpleQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server
	err := testServer.Start(13300)
	assert.NoError(t, err)
	defer testServer.Stop()

	// Use MySQL client connection
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Execute simple query
		var result string
		err := conn.QueryRow("SELECT 1").Scan(&result)
		return err
	})

	assert.NoError(t, err)
}

// TestProtocol_ErrorPacket tests error packet serialization
func TestProtocol_ErrorPacket(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13302)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Test syntax error
		_, err := conn.Query("SELEC * FROM table")
		if err == nil {
			return errors.New("expected syntax error")
		}

		// Verify error message format
		if !strings.Contains(err.Error(), "1064") {
			return fmt.Errorf("error code mismatch: got %s, want 1064", err.Error())
		}
		if !strings.Contains(err.Error(), "42000") {
			return fmt.Errorf("SQLState mismatch: got %s, want 42000", err.Error())
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_EmptyQuery tests empty query handling
func TestProtocol_EmptyQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13303)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Execute empty query
		_, err := conn.Query("")
		if err == nil {
			return fmt.Errorf("expected error for empty query, got nil")
		}

		t.Logf("Correctly returned empty query error: %v", err)

		// Verify connection is still active
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should be active after empty query: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_PingKeepAlive tests Ping keep-alive
func TestProtocol_PingKeepAlive(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13304)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Multiple Ping tests for connection keep-alive
		for i := 0; i < 10; i++ {
			err := conn.Ping()
			if err != nil {
				return fmt.Errorf("ping %d failed: %w", i+1, err)
			}
		}

		t.Log("Successfully completed 10 Pings")
		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_ErrorReturn tests error return format
func TestProtocol_ErrorReturn(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// Start server
	err := testServer.Start(13305)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// Test 1: Syntax error
		t.Log("Test 1: show databaes (syntax error)")
		_, err := conn.Query("show databaes")
		if err == nil {
			return fmt.Errorf("expected error for 'show databaes', got nil")
		}
		t.Logf("Error: %v", err)

		// Verify error contains correct SQLState and message
		if !contains(err.Error(), "1064") {
			return fmt.Errorf("expected error code 1064, got: %s", err.Error())
		}
		if !contains(err.Error(), "42000") {
			return fmt.Errorf("expected SQLState 42000, got: %s", err.Error())
		}
		if !contains(err.Error(), "databaes") {
			return fmt.Errorf("expected error message to contain 'databaes', got: %s", err.Error())
		}

		// Test 2: Table doesn't exist
		t.Log("Test 2: SELECT * FROM non_existent_table")
		_, err = conn.Query("SELECT * FROM non_existent_table")
		if err == nil {
			return fmt.Errorf("expected error for non-existent table, got nil")
		}
		t.Logf("Error: %v", err)

		if !contains(err.Error(), "1146") {
			return fmt.Errorf("expected error code 1146, got: %s", err.Error())
		}

		return nil
	})

	assert.NoError(t, err)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && strings.Contains(s, substr)
}
