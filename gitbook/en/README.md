# SQLExec

SQLExec is a MySQL-compatible database engine written in Go. It can run as a **standalone server** or be embedded as an **in-process library** in your Go applications.

## Key Features

- **MySQL Protocol Compatible** — Connect with any standard MySQL client, no tooling changes needed
- **Multi-Protocol Access** — MySQL protocol, HTTP REST API, and MCP (AI tool integration)
- **Multi-Source Queries** — Unified SQL interface across Memory, MySQL, PostgreSQL, HTTP APIs, CSV, JSON, JSONL, Excel, and Parquet
- **MVCC Storage Engine** — PostgreSQL-style multi-version concurrency control with 4 isolation levels
- **Vector Search** — 10 vector index algorithms (HNSW, IVF, etc.) with cosine/L2/inner product metrics
- **Full-Text Search** — BM25-scored inverted index with Chinese tokenization (Jieba)
- **Query Optimizer** — Cost-based optimizer with predicate pushdown, index selection, and join reordering
- **GORM Integration** — Full GORM Dialector with AutoMigrate and ORM support
- **Plugin System** — Custom data sources, user-defined functions (UDF), and native plugins (DLL/SO)
- **Audit Trail** — Trace-ID propagation across all entry points with comprehensive audit logging

## Two Usage Scenarios

| Feature | Standalone Server | Embedded Library |
|---------|------------------|-----------------|
| Deployment | Separate process, listens on ports | Integrated into your Go app |
| Access | MySQL client / HTTP / MCP | Direct Go API calls |
| Use Cases | Data analytics, multi-source gateway | Testing, CLI tools, in-app SQL engine |
| Data Source Config | datasources.json file | Register in code |
| Multi-User | Supported | Single process |

## Quick Navigation

| I want to... | Go to... |
|--------------|----------|
| Get running in 5 minutes | [Quick Start](getting-started/quick-start.md) |
| Deploy a standalone server | [Standalone Server Overview](standalone-server/overview.md) |
| Embed in my Go project | [Embedded Usage Overview](embedded/overview.md) |
| See supported data sources | [Data Sources Overview](datasources/overview.md) |
| Look up SQL syntax | [SQL Reference](sql-reference/overview.md) |
| Look up functions | [Function Reference](functions/overview.md) |
| Use vector search | [Vector Search](advanced/vector-search.md) |
| Use full-text search | [Full-Text Search](advanced/fulltext-search.md) |
| Build custom plugins | [Plugin Development](plugin-development/overview.md) |
| Use with GORM | [GORM Driver](embedded/gorm-driver.md) |

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.24+ |
| SQL Parser | TiDB Parser |
| Chinese Tokenizer | Jieba (gojieba) |
| MySQL Driver | go-sql-driver/mysql |
| PostgreSQL Driver | lib/pq |
| MCP Protocol | mcp-go |
| ORM Support | GORM v2 |
