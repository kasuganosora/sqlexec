# Standalone Server Overview

SQLExec can run as a standalone server, offering three protocol access methods simultaneously to meet data access needs across different scenarios.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                          │
│                                                         │
│   MySQL CLI / App        HTTP Client        AI Tools    │
│   (mysql, Go, Python,    (curl, fetch,      (Claude,    │
│    Java, Node.js)         Postman)           Cursor)    │
└────────┬─────────────────────┬──────────────────┬───────┘
         │                     │                  │
         ▼                     ▼                  ▼
┌────────────────┐  ┌──────────────────┐  ┌──────────────┐
│ MySQL Protocol │  │ HTTP REST API    │  │ MCP Server   │
│ Default: 3306  │  │ Default: 8080    │  │ Default: 8081│
└────────┬───────┘  └────────┬─────────┘  └──────┬───────┘
         │                   │                    │
         └───────────────────┼────────────────────┘
                             │
                             ▼
              ┌──────────────────────────┐
              │   Shared SQL Query Engine │
              │  (Parse → Optimize → Exec)│
              └─────────────┬────────────┘
                            │
                            ▼
              ┌──────────────────────────┐
              │    Data Source Manager    │
              │                          │
              │  ┌──────┐  ┌──────────┐  │
              │  │Built-in│  │MySQL/PG  │  │
              │  │Storage │  │          │  │
              │  └──────┘  └──────────┘  │
              └──────────────────────────┘
```

All three protocols share the same SQL query engine and data sources, ensuring consistent data access behavior.

## Protocol Comparison

| Feature | MySQL Protocol | HTTP REST API | MCP Server |
|---------|---------------|---------------|------------|
| Default Port | 3306 | 8080 | 8081 |
| Use Cases | Traditional database clients, ORM frameworks | Web applications, microservices | AI tools (Claude, Cursor) |
| Authentication | mysql_native_password | Bearer Token / API Key | Bearer Token |
| Transport Protocol | MySQL Wire Protocol | HTTP/1.1, HTTP/2 | Streamable HTTP |
| Session State | Stateful (connection-level sessions) | Stateless | Stateless |
| Transaction Support | Supported | Single statement | Single statement |

## Quick Start

### Build

```bash
go build -o sqlexec ./cmd/service
```

### Run

```bash
./sqlexec
```

After the server starts, the three protocols are automatically enabled based on the settings in the configuration file.

### Configuration File

Control which protocols are enabled via the configuration file:

```json
{
  "mysql": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 3306
  },
  "http_api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080
  },
  "mcp": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8081
  }
}
```

Set `enabled` to `false` to disable any protocol you do not need.

## Shared Engine

The core advantage of the three protocols is that they share the same query engine:

- **Unified SQL Parser** -- All protocols use the same SQL syntax parser
- **Unified Query Optimizer** -- Query plan generation and optimization logic is consistent
- **Unified Data Sources** -- Specify data sources via `USE database_name` or request parameters; all protocols can access the same data
- **Unified Permission Model** -- RBAC access control applies to all protocols
- **Unified Audit Logging** -- Operations from all protocols are recorded in the same audit log system
