# MCP Server

SQLExec includes a built-in MCP (Model Context Protocol) server, enabling AI tools to directly access and operate on databases.

## What is MCP

MCP (Model Context Protocol) is an open protocol that standardizes communication between AI applications and external data sources and tools. Through MCP, AI assistants (such as Claude, Cursor) can securely access databases, execute queries, and retrieve structured results.

## Enabling Configuration

Enable the MCP Server in the configuration file:

```json
{
  "mcp": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8081
  }
}
```

## Transport Method

The MCP Server uses the **Streamable HTTP** transport protocol, with the endpoint path:

```
http://<host>:<port>/mcp
```

## Authentication

Authentication is performed via Bearer Token:

```
Authorization: Bearer <your-token>
```

## Available Tools

The MCP Server exposes the following 4 tools. AI clients can use them through standard MCP tool calls.

### query

Execute an SQL statement and return the results.

**Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | Yes | The SQL statement to execute |
| `database` | string | No | Target data source name |
| `trace_id` | string | No | Request trace ID |

**Call Example**

```json
{
  "tool": "query",
  "arguments": {
    "sql": "SELECT id, name, email FROM users WHERE age > 18 LIMIT 5",
    "database": "my_database",
    "trace_id": "mcp-20260215-abc123"
  }
}
```

**Response Example**

```json
{
  "content": [
    {
      "type": "text",
      "text": "{\"columns\":[\"id\",\"name\",\"email\"],\"rows\":[[1,\"张三\",\"zhangsan@example.com\"],[2,\"李四\",\"lisi@example.com\"]],\"total\":2}"
    }
  ]
}
```

---

### list_databases

List all registered data sources.

**Parameters**

No parameters required.

**Call Example**

```json
{
  "tool": "list_databases",
  "arguments": {}
}
```

**Response Example**

```json
{
  "content": [
    {
      "type": "text",
      "text": "{\"databases\":[\"my_database\",\"analytics_db\",\"user_db\"]}"
    }
  ]
}
```

---

### list_tables

List all tables in a specified data source.

**Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `database` | string | Yes | Data source name |

**Call Example**

```json
{
  "tool": "list_tables",
  "arguments": {
    "database": "my_database"
  }
}
```

**Response Example**

```json
{
  "content": [
    {
      "type": "text",
      "text": "{\"tables\":[\"users\",\"orders\",\"products\"]}"
    }
  ]
}
```

---

### describe_table

Retrieve the schema of a specified table, including column names, types, and more.

**Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `database` | string | Yes | Data source name |
| `table` | string | Yes | Table name |

**Call Example**

```json
{
  "tool": "describe_table",
  "arguments": {
    "database": "my_database",
    "table": "users"
  }
}
```

**Response Example**

```json
{
  "content": [
    {
      "type": "text",
      "text": "{\"columns\":[{\"name\":\"id\",\"type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"type\":\"TEXT\"},{\"name\":\"age\",\"type\":\"INTEGER\"},{\"name\":\"email\",\"type\":\"TEXT\"}]}"
    }
  ]
}
```

## Integrating with Claude Desktop

Add the MCP server in the Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "sqlexec": {
      "url": "http://127.0.0.1:8081/mcp",
      "headers": {
        "Authorization": "Bearer your-token-here"
      }
    }
  }
}
```

After configuration, restart Claude Desktop and you can query the database directly in conversations:

> "How many users in the users table are older than 25?"

Claude will automatically call the `query` tool to execute the corresponding SQL query and return the results.

## Integrating with Cursor

Add the MCP server in Cursor:

1. Open Cursor Settings
2. Navigate to the **MCP** configuration section
3. Add a new MCP server:

```json
{
  "name": "sqlexec",
  "url": "http://127.0.0.1:8081/mcp",
  "headers": {
    "Authorization": "Bearer your-token-here"
  }
}
```

Once configured, Cursor's AI assistant can directly access the database to assist with code writing and data analysis.

## Typical Workflow

The typical workflow of an AI tool interacting with SQLExec via MCP is as follows:

```
AI Tool                               SQLExec MCP Server
  │                                      │
  │  1. list_databases()                 │
  │ ──────────────────────────────────►  │
  │  ◄──────── ["my_db", "analytics"]    │
  │                                      │
  │  2. list_tables("my_db")             │
  │ ──────────────────────────────────►  │
  │  ◄──────── ["users", "orders"]       │
  │                                      │
  │  3. describe_table("my_db","users")  │
  │ ──────────────────────────────────►  │
  │  ◄──────── [column definitions...]   │
  │                                      │
  │  4. query("SELECT ...")              │
  │ ──────────────────────────────────►  │
  │  ◄──────── {columns, rows, total}    │
  │                                      │
```

The AI tool first explores the database schema, then generates and executes SQL queries based on the user's natural language requests.
