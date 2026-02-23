package mcp

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/security"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Server is the MCP protocol server
type Server struct {
	db          *api.DB
	configDir   string
	vdbRegistry *virtual.VirtualDatabaseRegistry
	cfg         *config.MCPConfig
	auditLogger *security.AuditLogger
}

// SetVirtualDBRegistry sets the virtual database registry
func (s *Server) SetVirtualDBRegistry(registry *virtual.VirtualDatabaseRegistry) {
	s.vdbRegistry = registry
}

// NewServer creates a new MCP server
func NewServer(db *api.DB, configDir string, cfg *config.MCPConfig, auditLogger *security.AuditLogger) *Server {
	return &Server{
		db:          db,
		configDir:   configDir,
		cfg:         cfg,
		auditLogger: auditLogger,
	}
}

// Start starts the MCP server (blocking)
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)

	deps := &ToolDeps{
		DB:          s.db,
		ConfigDir:   s.configDir,
		VDBRegistry: s.vdbRegistry,
		AuditLogger: s.auditLogger,
	}

	// Create MCP server
	mcpSrv := mcpserver.NewMCPServer(
		"sqlexec",
		"1.0.0",
		mcpserver.WithToolCapabilities(true),
		mcpserver.WithRecovery(),
	)

	// Register tools
	queryTool := mcp.NewTool("query",
		mcp.WithDescription("Execute a SQL query against the database. Supports SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, SHOW, DESCRIBE, and other SQL statements."),
		mcp.WithString("sql", mcp.Description("The SQL query to execute"), mcp.Required()),
		mcp.WithString("database", mcp.Description("The database to query (optional, uses default if not specified)")),
		mcp.WithString("trace_id", mcp.Description("Optional trace ID for request tracing and audit logging")),
	)

	listDBTool := mcp.NewTool("list_databases",
		mcp.WithDescription("List all available databases"),
	)

	listTablesTool := mcp.NewTool("list_tables",
		mcp.WithDescription("List all tables in a specified database"),
		mcp.WithString("database", mcp.Description("The database name"), mcp.Required()),
	)

	describeTableTool := mcp.NewTool("describe_table",
		mcp.WithDescription("Get the schema/structure of a table, including column names, types, and constraints"),
		mcp.WithString("database", mcp.Description("The database name"), mcp.Required()),
		mcp.WithString("table", mcp.Description("The table name"), mcp.Required()),
	)

	mcpSrv.AddTool(queryTool, deps.HandleQuery)
	mcpSrv.AddTool(listDBTool, deps.HandleListDatabases)
	mcpSrv.AddTool(listTablesTool, deps.HandleListTables)
	mcpSrv.AddTool(describeTableTool, deps.HandleDescribeTable)

	// Create Streamable HTTP transport with auth
	clientStore := config_schema.LoadAPIClients
	httpServer := mcpserver.NewStreamableHTTPServer(
		mcpSrv,
		mcpserver.WithEndpointPath("/mcp"),
		mcpserver.WithHTTPContextFunc(s.authContextFunc(clientStore)),
	)

	log.Printf("[MCP] 启动 MCP 服务器: %s", addr)
	return httpServer.Start(addr)
}

// authContextFunc returns an HTTP context function that validates Bearer token auth
// and stores the HTTP request in context for IP extraction.
func (s *Server) authContextFunc(loadClients func(string) ([]config_schema.APIClient, error)) mcpserver.HTTPContextFunc {
	return func(ctx context.Context, r *http.Request) context.Context {
		// Always store the HTTP request so tool handlers can extract client IP.
		ctx = context.WithValue(ctx, ctxKeyMCPRequest, r)

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return ctx
		}

		// Expect "Bearer <api_key>"
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			return ctx
		}

		apiKey := parts[1]

		clients, err := loadClients(s.configDir)
		if err != nil {
			log.Printf("[MCP] failed to load API clients: %v", err)
			return ctx
		}

		for _, c := range clients {
			if c.APIKey == apiKey && c.Enabled {
				clientCopy := c
				ctx = context.WithValue(ctx, ctxKeyMCPClient, &clientCopy)
				return ctx
			}
		}

		return ctx
	}
}
