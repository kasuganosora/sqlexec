package mcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/security"
	"github.com/mark3labs/mcp-go/mcp"
)

type contextKey string

const ctxKeyMCPClient contextKey = "mcp_client"

// ToolDeps holds shared dependencies for MCP tool handlers
type ToolDeps struct {
	DB          *api.DB
	ConfigDir   string
	AuditLogger *security.AuditLogger
}

// HandleQuery executes an arbitrary SQL query
func (d *ToolDeps) HandleQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sql := request.GetString("sql", "")
	database := request.GetString("database", "")

	if sql == "" {
		return mcp.NewToolResultError("sql parameter is required"), nil
	}

	client := getClient(ctx)
	clientName := ""
	clientIP := ""
	if client != nil {
		clientName = client.Name
	}

	start := time.Now()

	session := d.DB.Session()
	defer session.Close()
	session.SetConfigDir(d.ConfigDir)
	if clientName != "" {
		session.SetUser(clientName)
	}
	if database != "" {
		session.SetCurrentDB(database)
	}

	sqlUpper := strings.TrimSpace(strings.ToUpper(sql))
	isRead := strings.HasPrefix(sqlUpper, "SELECT") ||
		strings.HasPrefix(sqlUpper, "SHOW") ||
		strings.HasPrefix(sqlUpper, "DESCRIBE") ||
		strings.HasPrefix(sqlUpper, "DESC ") ||
		strings.HasPrefix(sqlUpper, "EXPLAIN")

	var resultText string

	if isRead {
		query, err := session.Query(sql)
		if err != nil {
			d.logToolCall(clientName, clientIP, "query", map[string]interface{}{"sql": sql, "database": database}, time.Since(start).Milliseconds(), false)
			return mcp.NewToolResultError(fmt.Sprintf("query failed: %v", err)), nil
		}
		defer query.Close()

		var sb strings.Builder
		// Write column headers
		cols := query.Columns()
		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name
		}
		sb.WriteString(strings.Join(colNames, "\t"))
		sb.WriteString("\n")

		rowCount := 0
		for query.Next() {
			row := query.Row()
			vals := make([]string, len(colNames))
			for i, col := range colNames {
				vals[i] = fmt.Sprintf("%v", row[col])
			}
			sb.WriteString(strings.Join(vals, "\t"))
			sb.WriteString("\n")
			rowCount++
		}
		sb.WriteString(fmt.Sprintf("\n(%d rows)", rowCount))
		resultText = sb.String()
	} else {
		result, err := session.Execute(sql)
		if err != nil {
			d.logToolCall(clientName, clientIP, "query", map[string]interface{}{"sql": sql, "database": database}, time.Since(start).Milliseconds(), false)
			return mcp.NewToolResultError(fmt.Sprintf("execute failed: %v", err)), nil
		}
		resultText = fmt.Sprintf("Affected rows: %d", result.RowsAffected)
	}

	d.logToolCall(clientName, clientIP, "query", map[string]interface{}{"sql": sql, "database": database}, time.Since(start).Milliseconds(), true)
	return mcp.NewToolResultText(resultText), nil
}

// HandleListDatabases lists all available databases
func (d *ToolDeps) HandleListDatabases(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	client := getClient(ctx)
	clientName := ""
	if client != nil {
		clientName = client.Name
	}
	start := time.Now()

	session := d.DB.Session()
	defer session.Close()
	session.SetConfigDir(d.ConfigDir)
	if clientName != "" {
		session.SetUser(clientName)
	}

	query, err := session.Query("SHOW DATABASES")
	if err != nil {
		d.logToolCall(clientName, "", "list_databases", nil, time.Since(start).Milliseconds(), false)
		return mcp.NewToolResultError(fmt.Sprintf("failed to list databases: %v", err)), nil
	}
	defer query.Close()

	var sb strings.Builder
	sb.WriteString("Databases:\n")
	for query.Next() {
		row := query.Row()
		for _, v := range row {
			sb.WriteString(fmt.Sprintf("- %v\n", v))
		}
	}

	d.logToolCall(clientName, "", "list_databases", nil, time.Since(start).Milliseconds(), true)
	return mcp.NewToolResultText(sb.String()), nil
}

// HandleListTables lists tables in a given database
func (d *ToolDeps) HandleListTables(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	database := request.GetString("database", "")
	if database == "" {
		return mcp.NewToolResultError("database parameter is required"), nil
	}

	client := getClient(ctx)
	clientName := ""
	if client != nil {
		clientName = client.Name
	}
	start := time.Now()

	session := d.DB.Session()
	defer session.Close()
	session.SetConfigDir(d.ConfigDir)
	if clientName != "" {
		session.SetUser(clientName)
	}
	session.SetCurrentDB(database)

	query, err := session.Query("SHOW TABLES")
	if err != nil {
		d.logToolCall(clientName, "", "list_tables", map[string]interface{}{"database": database}, time.Since(start).Milliseconds(), false)
		return mcp.NewToolResultError(fmt.Sprintf("failed to list tables: %v", err)), nil
	}
	defer query.Close()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Tables in %s:\n", database))
	for query.Next() {
		row := query.Row()
		for _, v := range row {
			sb.WriteString(fmt.Sprintf("- %v\n", v))
		}
	}

	d.logToolCall(clientName, "", "list_tables", map[string]interface{}{"database": database}, time.Since(start).Milliseconds(), true)
	return mcp.NewToolResultText(sb.String()), nil
}

// HandleDescribeTable returns the schema of a table
func (d *ToolDeps) HandleDescribeTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	database := request.GetString("database", "")
	table := request.GetString("table", "")

	if database == "" {
		return mcp.NewToolResultError("database parameter is required"), nil
	}
	if table == "" {
		return mcp.NewToolResultError("table parameter is required"), nil
	}

	client := getClient(ctx)
	clientName := ""
	if client != nil {
		clientName = client.Name
	}
	start := time.Now()

	session := d.DB.Session()
	defer session.Close()
	session.SetConfigDir(d.ConfigDir)
	if clientName != "" {
		session.SetUser(clientName)
	}
	session.SetCurrentDB(database)

	query, err := session.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", table))
	if err != nil {
		d.logToolCall(clientName, "", "describe_table", map[string]interface{}{"database": database, "table": table}, time.Since(start).Milliseconds(), false)
		return mcp.NewToolResultError(fmt.Sprintf("failed to describe table: %v", err)), nil
	}
	defer query.Close()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Table: %s.%s\n\n", database, table))

	cols := query.Columns()
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}
	sb.WriteString(strings.Join(colNames, "\t"))
	sb.WriteString("\n")

	for query.Next() {
		row := query.Row()
		vals := make([]string, len(colNames))
		for i, col := range colNames {
			vals[i] = fmt.Sprintf("%v", row[col])
		}
		sb.WriteString(strings.Join(vals, "\t"))
		sb.WriteString("\n")
	}

	d.logToolCall(clientName, "", "describe_table", map[string]interface{}{"database": database, "table": table}, time.Since(start).Milliseconds(), true)
	return mcp.NewToolResultText(sb.String()), nil
}

func (d *ToolDeps) logToolCall(clientName, ip, toolName string, args map[string]interface{}, duration int64, success bool) {
	if d.AuditLogger != nil {
		d.AuditLogger.LogMCPToolCall(clientName, ip, toolName, args, duration, success)
	}
}

func getClient(ctx context.Context) *config_schema.APIClient {
	client, _ := ctx.Value(ctxKeyMCPClient).(*config_schema.APIClient)
	return client
}
