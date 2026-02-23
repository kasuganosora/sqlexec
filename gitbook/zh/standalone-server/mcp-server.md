# MCP Server

SQLExec 内置 MCP（Model Context Protocol）服务器，使 AI 工具能够直接访问和操作数据库。

## 什么是 MCP

MCP（Model Context Protocol）是一种开放协议，用于标准化 AI 应用与外部数据源及工具之间的通信。通过 MCP，AI 助手（如 Claude、Cursor）可以安全地访问数据库、执行查询并获取结构化结果。

## 启用配置

在配置文件中启用 MCP Server：

```json
{
  "mcp": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8081
  }
}
```

## 传输方式

MCP Server 使用 **Streamable HTTP** 传输协议，端点路径为：

```
http://<host>:<port>/mcp
```

## 认证

通过 Bearer Token 进行认证：

```
Authorization: Bearer <your-token>
```

> **注意：** 认证是必须的。所有没有有效 Bearer Token 的工具调用将被拒绝，返回 "unauthorized" 错误。

## 安全机制

- **SQL 注入防护**：`list_tables` 和 `describe_table` 中的 `database` 和 `table` 参数会被严格校验，仅允许安全的标识符字符（字母、数字、下划线）。非法名称会在查询执行前被拒绝。
- **结果大小限制**：读取查询（SELECT、SHOW 等）最多返回 **10,000 行**，以防止内存溢出。超过限制时，结果将被截断并在响应中注明。
- **审计日志**：所有工具调用都会记录客户端名称、客户端 IP 地址、工具名称、参数、耗时及成功/失败状态。

## 提供的工具

MCP Server 对外暴露以下 4 个工具，AI 客户端可以通过标准的 MCP 工具调用来使用它们。

### query

执行 SQL 语句并返回结果。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `sql` | string | 是 | 要执行的 SQL 语句 |
| `database` | string | 否 | 目标数据源名称 |
| `trace_id` | string | 否 | 请求追踪 ID |

**调用示例**

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

**响应示例**

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

列出所有已注册的数据源。

**参数**

无需参数。

**调用示例**

```json
{
  "tool": "list_databases",
  "arguments": {}
}
```

**响应示例**

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

列出指定数据源中的所有表。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database` | string | 是 | 数据源名称 |

**调用示例**

```json
{
  "tool": "list_tables",
  "arguments": {
    "database": "my_database"
  }
}
```

**响应示例**

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

获取指定表的结构信息，包括列名、类型等。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database` | string | 是 | 数据源名称 |
| `table` | string | 是 | 表名 |

**调用示例**

```json
{
  "tool": "describe_table",
  "arguments": {
    "database": "my_database",
    "table": "users"
  }
}
```

**响应示例**

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

## 集成 Claude Desktop

在 Claude Desktop 的配置文件中添加 MCP 服务器：

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

配置完成后重启 Claude Desktop，即可在对话中直接查询数据库：

> "帮我查一下 users 表中年龄大于 25 岁的用户有多少"

Claude 会自动调用 `query` 工具执行相应的 SQL 查询并返回结果。

## 集成 Cursor

在 Cursor 中添加 MCP 服务器：

1. 打开 Cursor 设置
2. 进入 **MCP** 配置部分
3. 添加新的 MCP 服务器：

```json
{
  "name": "sqlexec",
  "url": "http://127.0.0.1:8081/mcp",
  "headers": {
    "Authorization": "Bearer your-token-here"
  }
}
```

配置完成后，Cursor 的 AI 助手即可直接访问数据库，辅助代码编写和数据分析。

## 典型使用流程

AI 工具通过 MCP 与 SQLExec 交互的典型流程如下：

```
AI 工具                           SQLExec MCP Server
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
  │  ◄──────── [列定义...]               │
  │                                      │
  │  4. query("SELECT ...")              │
  │ ──────────────────────────────────►  │
  │  ◄──────── {columns, rows, total}    │
  │                                      │
```

AI 工具首先了解数据库结构，然后基于用户的自然语言请求生成并执行 SQL 查询。
