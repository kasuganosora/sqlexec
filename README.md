# SQLExec

SQLExec 是一个使用 Go 语言实现的 MySQL 兼容数据库引擎。它既可以作为**独立服务器**运行，也可以作为**嵌入式库**集成到你的 Go 应用中。

SQLExec is a MySQL-compatible database engine written in Go. It can run as a **standalone server** or be embedded as an **in-process library** in your Go applications.

---

## 特性 / Features

- **MySQL 协议兼容 / MySQL Protocol Compatible** — 支持标准 MySQL 客户端连接 / Connect with any standard MySQL client
- **多协议接入 / Multi-Protocol Access** — MySQL 协议、HTTP REST API、MCP（AI 工具集成） / MySQL protocol, HTTP REST API, and MCP (AI tool integration)
- **多数据源 / Multi-Source Queries** — 统一 SQL 接口查询 Memory、MySQL、PostgreSQL、HTTP API、CSV、JSON、JSONL、Excel、Parquet
- **MVCC 存储引擎 / MVCC Storage Engine** — PostgreSQL 风格的多版本并发控制，支持 4 种事务隔离级别
- **向量搜索 / Vector Search** — 10 种向量索引算法（HNSW、IVF 等），支持 cosine/L2/inner product
- **全文搜索 / Full-Text Search** — BM25 评分的倒排索引，内置中文分词（Jieba）
- **查询优化器 / Query Optimizer** — 基于代价的优化器，谓词下推、索引选择、JOIN 重排序
- **GORM 集成 / GORM Integration** — 完整的 GORM Dialector，支持 AutoMigrate 和 ORM 操作
- **插件系统 / Plugin System** — 自定义数据源、用户自定义函数（UDF）、原生插件（DLL/SO）
- **审计追踪 / Audit Trail** — Trace-ID 贯穿所有入口，完整的请求追踪和审计日志

## 快速开始 / Quick Start

### 独立服务器 / Standalone Server

```bash
# 编译并启动 / Build and start
go build -o sqlexec ./cmd/service
./sqlexec

# 使用 MySQL 客户端连接 / Connect with MySQL client
mysql -h 127.0.0.1 -P 3306 -u root
```

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    age INT
);

INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);

SELECT * FROM users WHERE age > 20 ORDER BY name;
```

### 嵌入式使用 / Embedded Usage

```go
package main

import "github.com/kasuganosora/sqlexec/pkg/api"

func main() {
    db := api.NewDB(nil)
    defer db.Close()

    session := db.Session()
    defer session.Close()

    session.Execute("CREATE TABLE t (id INT, name TEXT)")
    session.Execute("INSERT INTO t (id, name) VALUES (1, 'hello')")

    query, _ := session.Query("SELECT * FROM t")
    defer query.Close()
    for query.Next() {
        row := query.Row()
        // row["id"], row["name"]
        _ = row
    }
}
```

### 多数据源查询 / Multi-Source Queries

支持 9 种数据源 / Supports 9 data source types：Memory, MySQL, PostgreSQL, CSV, JSON, JSONL, Excel, Parquet, HTTP

**方式一 / Method 1：`datasources.json`**

```json
{
  "datasources": [
    {
      "name": "default",
      "type": "memory",
      "writable": true
    },
    {
      "name": "mydb",
      "type": "mysql",
      "host": "10.0.0.1",
      "port": 3306,
      "username": "reader",
      "password": "secret",
      "database": "production"
    },
    {
      "name": "logs",
      "type": "csv",
      "options": {
        "path": "/data/access_logs.csv"
      }
    },
    {
      "name": "analytics",
      "type": "postgresql",
      "host": "pg.example.com",
      "port": 5432,
      "username": "analyst",
      "password": "pass",
      "database": "analytics_db",
      "options": {
        "schema": "public",
        "ssl_mode": "require"
      }
    }
  ]
}
```

> **Note**: 文件型数据源（CSV/JSON/JSONL/Excel/Parquet）的文件路径通过 `options.path` 指定 / File paths for file-based sources go in `options.path`

**方式二 / Method 2：SQL 动态管理 / Runtime SQL Management**

```sql
-- 查看所有数据源 / List all data sources
SELECT * FROM config.datasource;

-- 添加 MySQL 数据源 / Add MySQL data source
INSERT INTO config.datasource (name, type, host, port, username, password, database_name, writable)
VALUES ('production', 'mysql', 'db.example.com', 3306, 'app_user', 'secret', 'myapp', true);

-- 添加 CSV 数据源 / Add CSV data source
INSERT INTO config.datasource (name, type, options)
VALUES ('logs', 'csv', '{"path": "/data/access_logs.csv"}');
```

**查询数据源 / Query Data Sources**

```sql
USE logs;
SELECT * FROM csv_data WHERE status_code = 500;

USE mydb;
SELECT * FROM orders WHERE created_at > '2025-01-01';
```

### 向量搜索 / Vector Search

```sql
CREATE TABLE docs (
    id INT PRIMARY KEY,
    title TEXT,
    embedding VECTOR(384)
);

CREATE VECTOR INDEX idx_emb ON docs(embedding) USING HNSW WITH (metric = 'cosine');

SELECT id, title, COSINE_SIMILARITY(embedding, '[0.1, 0.2, ...]') AS score
FROM docs
ORDER BY score DESC
LIMIT 10;
```

### 全文搜索 / Full-Text Search

```sql
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title TEXT,
    content TEXT
);

CREATE FULLTEXT INDEX idx_ft ON articles(content) WITH (analyzer = 'jieba');

SELECT id, title, MATCH(content) AGAINST('数据库 搜索') AS score
FROM articles
WHERE MATCH(content) AGAINST('数据库 搜索')
ORDER BY score DESC;
```

## 安装 / Installation

```bash
# 作为独立服务器 / As standalone server
go build -o sqlexec ./cmd/service

# 作为库引入 / As a library
go get github.com/kasuganosora/sqlexec
```

**要求 / Requirements:** Go 1.24+

## 文档 / Documentation

| 中文文档 | English Docs |
|---------|--------------|
| [快速开始](gitbook/zh/getting-started/quick-start.md) | [Quick Start](gitbook/en/getting-started/quick-start.md) |
| [配置详解](gitbook/zh/getting-started/configuration.md) | [Configuration](gitbook/en/getting-started/configuration.md) |
| [独立服务器](gitbook/zh/standalone-server/overview.md) | [Standalone Server](gitbook/en/standalone-server/overview.md) |
| [嵌入式使用](gitbook/zh/embedded/overview.md) | [Embedded Usage](gitbook/en/embedded/overview.md) |
| [数据源](gitbook/zh/datasources/overview.md) | [Data Sources](gitbook/en/datasources/overview.md) |
| [SQL 参考](gitbook/zh/sql-reference/overview.md) | [SQL Reference](gitbook/en/sql-reference/overview.md) |
| [函数参考](gitbook/zh/functions/overview.md) | [Function Reference](gitbook/en/functions/overview.md) |
| [向量搜索](gitbook/zh/advanced/vector-search.md) | [Vector Search](gitbook/en/advanced/vector-search.md) |
| [全文搜索](gitbook/zh/advanced/fulltext-search.md) | [Full-Text Search](gitbook/en/advanced/fulltext-search.md) |
| [GORM 驱动](gitbook/zh/embedded/gorm-driver.md) | [GORM Driver](gitbook/en/embedded/gorm-driver.md) |
| [插件开发](gitbook/zh/plugin-development/overview.md) | [Plugin Development](gitbook/en/plugin-development/overview.md) |

## 技术栈 / Tech Stack

| 组件 / Component | 技术 / Technology |
|---------|-----------|
| Language | Go 1.24+ |
| SQL Parser | TiDB Parser |
| Chinese Tokenizer | Jieba (gojieba) |
| MySQL Driver | go-sql-driver/mysql |
| PostgreSQL Driver | lib/pq |
| MCP Protocol | mcp-go |
| ORM | GORM v2 |

## 协议 / License

[MIT](LICENSE)
