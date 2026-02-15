package mcp

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/security"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDeps(t *testing.T) *ToolDeps {
	t.Helper()

	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: false,
		DebugMode:    false,
	})
	require.NoError(t, err)

	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	})
	require.NoError(t, ds.Connect(nil))
	require.NoError(t, db.RegisterDataSource("default", ds))

	configDir := t.TempDir()

	return &ToolDeps{
		DB:          db,
		ConfigDir:   configDir,
		AuditLogger: security.NewAuditLogger(100),
	}
}

func makeCallToolRequest(args map[string]interface{}) mcp.CallToolRequest {
	var arguments interface{}
	if args != nil {
		arguments = map[string]any(args)
	}
	return mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: arguments,
		},
	}
}

func TestHandleQuery_Select(t *testing.T) {
	deps := setupTestDeps(t)

	// Create a table and insert data
	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE users (id INT, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "SELECT * FROM users",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check result contains data
	assert.False(t, result.IsError)
	assert.NotEmpty(t, result.Content)

	// Extract text content
	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "Alice")
	assert.Contains(t, textContent.Text, "Bob")
	assert.Contains(t, textContent.Text, "(2 rows)")
}

func TestHandleQuery_Insert(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE insert_test (id INT, value VARCHAR(100))")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "INSERT INTO insert_test (id, value) VALUES (1, 'hello')",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "Affected rows: 1")
}

func TestHandleQuery_EmptySQL(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.IsError)
}

func TestHandleListDatabases(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(nil)

	result, err := deps.HandleListDatabases(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "default")
}

func TestHandleListTables(t *testing.T) {
	deps := setupTestDeps(t)

	// Create some tables
	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE table_a (id INT)")
	require.NoError(t, err)
	_, err = session.Execute("CREATE TABLE table_b (id INT)")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"database": "default",
	})

	result, err := deps.HandleListTables(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "table_a")
	assert.Contains(t, textContent.Text, "table_b")
}

func TestHandleListTables_MissingDB(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{})

	result, err := deps.HandleListTables(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.IsError)
}

func TestHandleDescribeTable(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE desc_test (id INT, name VARCHAR(100), age INT)")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"database": "default",
		"table":    "desc_test",
	})

	result, err := deps.HandleDescribeTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "desc_test")
}

func TestHandleDescribeTable_MissingParams(t *testing.T) {
	deps := setupTestDeps(t)
	ctx := context.Background()

	t.Run("missing database", func(t *testing.T) {
		req := makeCallToolRequest(map[string]interface{}{
			"table": "test",
		})
		result, err := deps.HandleDescribeTable(ctx, req)
		require.NoError(t, err)
		assert.True(t, result.IsError)
	})

	t.Run("missing table", func(t *testing.T) {
		req := makeCallToolRequest(map[string]interface{}{
			"database": "default",
		})
		result, err := deps.HandleDescribeTable(ctx, req)
		require.NoError(t, err)
		assert.True(t, result.IsError)
	})
}

func TestAuditLogging_MCP(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE audit_mcp (id INT)")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "SELECT * FROM audit_mcp",
	})

	_, err = deps.HandleQuery(ctx, req)
	require.NoError(t, err)

	events := deps.AuditLogger.GetEventsByType(security.EventTypeMCPToolCall)
	assert.NotEmpty(t, events, "should have MCP audit events")
}

func TestHandleQuery_Update(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE update_test (id INT, val VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO update_test (id, val) VALUES (1, 'old')")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "UPDATE update_test SET val = 'new' WHERE id = 1",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "Affected rows:")
}

func TestHandleQuery_Delete(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE delete_test (id INT)")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO delete_test (id) VALUES (1)")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "DELETE FROM delete_test WHERE id = 1",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "Affected rows:")
}

func TestHandleQuery_SelectError(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "SELECT * FROM nonexistent_table_xyz",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "query failed")
}

func TestHandleQuery_ExecuteError(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "INSERT INTO nonexistent_table_xyz (id) VALUES (1)",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.IsError)

	textContent, ok := result.Content[0].(mcp.TextContent)
	require.True(t, ok)
	assert.Contains(t, textContent.Text, "execute failed")
}

func TestHandleQuery_WithDatabase(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE dbparam_test (id INT)")
	require.NoError(t, err)
	session.Close()

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"sql":      "SELECT * FROM dbparam_test",
		"database": "default",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)
}

func TestHandleQuery_WithAuthClient(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE auth_test (id INT)")
	require.NoError(t, err)
	session.Close()

	client := &config_schema.APIClient{Name: "mcp_user", APIKey: "key-1", Enabled: true}
	ctx := context.WithValue(context.Background(), ctxKeyMCPClient, client)
	req := makeCallToolRequest(map[string]interface{}{
		"sql": "SELECT * FROM auth_test",
	})

	result, err := deps.HandleQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)

	// Verify audit log captured the client name
	events := deps.AuditLogger.GetEventsByType(security.EventTypeMCPToolCall)
	assert.NotEmpty(t, events)
	if len(events) > 0 {
		assert.Equal(t, "mcp_user", events[len(events)-1].User)
	}
}

func TestHandleDescribeTable_NonexistentTable(t *testing.T) {
	deps := setupTestDeps(t)

	ctx := context.Background()
	req := makeCallToolRequest(map[string]interface{}{
		"database": "default",
		"table":    "nonexistent_table_xyz",
	})

	result, err := deps.HandleDescribeTable(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	// SHOW COLUMNS returns empty result for nonexistent table (no error)
	assert.False(t, result.IsError)
}

func TestLogToolCall_NilAuditLogger(t *testing.T) {
	deps := &ToolDeps{AuditLogger: nil}
	// Should not panic
	deps.logToolCall("client", "ip", "tool", nil, 0, true)
}

func TestGetClient_NoClient(t *testing.T) {
	client := getClient(context.Background())
	assert.Nil(t, client)
}

func TestNewServer_Constructor(t *testing.T) {
	db, err := api.NewDB(&api.DBConfig{CacheEnabled: false, DebugMode: false})
	require.NoError(t, err)

	s := NewServer(db, t.TempDir(), nil, nil)
	assert.NotNil(t, s)
	assert.Equal(t, db, s.db)
}

func TestHandleListDatabases_WithClient(t *testing.T) {
	deps := setupTestDeps(t)

	client := &config_schema.APIClient{Name: "db_lister", APIKey: "key-2", Enabled: true}
	ctx := context.WithValue(context.Background(), ctxKeyMCPClient, client)
	req := makeCallToolRequest(nil)

	result, err := deps.HandleListDatabases(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)
}

func TestHandleListTables_WithClient(t *testing.T) {
	deps := setupTestDeps(t)

	session := deps.DB.Session()
	_, err := session.Execute("CREATE TABLE client_tables_test (id INT)")
	require.NoError(t, err)
	session.Close()

	client := &config_schema.APIClient{Name: "tbl_lister", APIKey: "key-3", Enabled: true}
	ctx := context.WithValue(context.Background(), ctxKeyMCPClient, client)
	req := makeCallToolRequest(map[string]interface{}{
		"database": "default",
	})

	result, err := deps.HandleListTables(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsError)
}

func TestAuthContextFunc(t *testing.T) {
	db, err := api.NewDB(&api.DBConfig{CacheEnabled: false, DebugMode: false})
	require.NoError(t, err)
	s := NewServer(db, t.TempDir(), nil, nil)

	t.Run("no auth header", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return nil, nil
		})
		r, _ := http.NewRequest("GET", "/", nil)
		ctx := authFn(context.Background(), r)
		assert.Nil(t, getClient(ctx))
	})

	t.Run("invalid auth format", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return nil, nil
		})
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Basic abc123")
		ctx := authFn(context.Background(), r)
		assert.Nil(t, getClient(ctx))
	})

	t.Run("load clients error", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return nil, fmt.Errorf("disk error")
		})
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer test-key")
		ctx := authFn(context.Background(), r)
		assert.Nil(t, getClient(ctx))
	})

	t.Run("key not found", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return []config_schema.APIClient{
				{Name: "c1", APIKey: "other-key", Enabled: true},
			}, nil
		})
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer wrong-key")
		ctx := authFn(context.Background(), r)
		assert.Nil(t, getClient(ctx))
	})

	t.Run("key found but disabled", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return []config_schema.APIClient{
				{Name: "disabled_client", APIKey: "disabled-key", Enabled: false},
			}, nil
		})
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer disabled-key")
		ctx := authFn(context.Background(), r)
		assert.Nil(t, getClient(ctx))
	})

	t.Run("valid key", func(t *testing.T) {
		authFn := s.authContextFunc(func(dir string) ([]config_schema.APIClient, error) {
			return []config_schema.APIClient{
				{Name: "valid_client", APIKey: "valid-key", Enabled: true},
			}, nil
		})
		r, _ := http.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer valid-key")
		ctx := authFn(context.Background(), r)
		client := getClient(ctx)
		require.NotNil(t, client)
		assert.Equal(t, "valid_client", client.Name)
	})
}

func TestContextWithClient(t *testing.T) {
	client := &config_schema.APIClient{
		Name:    "test_mcp_client",
		APIKey:  "key-123",
		Enabled: true,
	}

	ctx := context.WithValue(context.Background(), ctxKeyMCPClient, client)
	got := getClient(ctx)
	require.NotNil(t, got)
	assert.Equal(t, "test_mcp_client", got.Name)

	// No client in context
	got = getClient(context.Background())
	assert.Nil(t, got)
}
