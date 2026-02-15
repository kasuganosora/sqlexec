# SQLExec

SQLExec 是一个使用 Go 实现的兼容 MySQL 协议的数据库服务器。它基于 TiDB SQL Parser 进行 SQL 解析，支持内存 MVCC 存储引擎，并可通过插件和数据源配置连接外部 MySQL、PostgreSQL、HTTP API 等数据源。

## 核心特性

- **MySQL 协议兼容**：支持标准 MySQL 客户端连接，实现 COM_QUERY、COM_PING、COM_INIT_DB 等命令
- **TiDB SQL Parser**：完整的 SQL 解析能力，支持 SELECT/INSERT/UPDATE/DELETE/DDL 等语句
- **MVCC 存储引擎**：PostgreSQL 风格的多版本并发控制，支持事务隔离级别
- **向量搜索**：支持 HNSW、IVF 等向量索引，兼容 cosine/L2/inner product 距离度量
- **全文搜索**：基于 BM25 的倒排索引，支持中日韩文分词（Jieba）
- **多数据源**：统一 DataSource 接口，支持 Memory、MySQL、PostgreSQL、HTTP、CSV、JSON、Excel、Parquet
- **查询优化器**：基于代价的优化器，支持 JOIN 算法选择、谓词下推、并行执行
- **HTTP API**：RESTful 查询接口，支持 Bearer Token 认证
- **MCP Server**：Model Context Protocol 服务器，供 AI 工具调用
- **Trace-ID 机制**：贯穿 MySQL/HTTP/MCP 三个入口的请求追踪和审计日志
- **插件系统**：通过 DLL/SO 共享库动态加载数据源插件
- **UDF 支持**：用户自定义函数注册

## 快速开始

```bash
# 编译
go build -o sqlexec ./cmd/service

# 启动（默认监听 3306 端口）
./sqlexec

# 连接
mysql -h 127.0.0.1 -P 3306 -u root
```

## 配置

### config.json

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 3306
  },
  "database": {
    "max_connections": 100
  }
}
```

### datasources.json

通过 `datasources.json` 配置外部数据源：

```json
[
  {
    "type": "mysql",
    "name": "remote_mysql",
    "host": "mysql.example.com",
    "port": 3306,
    "username": "app",
    "password": "secret",
    "database": "app_db",
    "writable": true,
    "options": {
      "max_open_conns": 50,
      "charset": "utf8mb4"
    }
  },
  {
    "type": "postgresql",
    "name": "analytics_pg",
    "host": "pg.example.com",
    "port": 5432,
    "username": "analyst",
    "password": "secret",
    "database": "analytics",
    "writable": false,
    "options": {
      "schema": "public",
      "ssl_mode": "require"
    }
  }
]
```

配置后即可通过 `USE remote_mysql` 切换到外部数据源执行查询。

## 多协议接入

| 协议 | 端口 | 用途 |
|------|------|------|
| MySQL Protocol | 3306 | 标准 SQL 客户端连接 |
| HTTP API | 配置指定 | RESTful 查询，`POST /api/v1/query` |
| MCP | 配置指定 | AI 工具调用（Claude、Cursor 等） |

## 文档目录

| 文档 | 说明 |
|------|------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 项目架构与模块依赖 |
| [SQL_PARSING.md](SQL_PARSING.md) | SQL 解析与执行管线 |
| [QUERY_OPTIMIZER.md](QUERY_OPTIMIZER.md) | 查询优化器原理 |
| [MVCC.md](MVCC.md) | MVCC 并发控制原理 |
| [VECTOR_SEARCH.md](VECTOR_SEARCH.md) | 向量搜索原理 |
| [FULLTEXT_SEARCH.md](FULLTEXT_SEARCH.md) | 全文搜索原理 |
| [DATASOURCE.md](DATASOURCE.md) | 数据源架构 |
| [FUNCTIONS.md](FUNCTIONS.md) | 函数系统 |
| [TRACE_AUDIT.md](TRACE_AUDIT.md) | 追踪与审计机制 |

## 技术栈

- **语言**：Go 1.24+
- **SQL 解析**：TiDB Parser
- **中文分词**：Jieba (gojieba)
- **MySQL 驱动**：go-sql-driver/mysql
- **PostgreSQL 驱动**：lib/pq
- **MCP 协议**：mcp-go
- **ICU 支持**：golang.org/x/text
