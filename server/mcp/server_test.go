package mcp

import (
	"context"
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
