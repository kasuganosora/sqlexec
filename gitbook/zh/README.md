# SQLExec

SQLExec 是一个使用 Go 实现的 MySQL 兼容数据库引擎。它既可以作为**独立服务器**运行，也可以作为**嵌入式库**集成到你的 Go 应用中。

## 核心特性

- **MySQL 协议兼容** — 支持标准 MySQL 客户端连接，无需修改现有工具链
- **多协议接入** — MySQL 协议、HTTP REST API、MCP（AI 工具集成）三种访问方式
- **多数据源** — 统一 SQL 接口查询 Memory、MySQL、PostgreSQL、HTTP API、CSV、JSON、JSONL、Excel、Parquet
- **MVCC 存储引擎** — PostgreSQL 风格的多版本并发控制，支持 4 种事务隔离级别
- **向量搜索** — 10 种向量索引算法（HNSW、IVF 等），支持 cosine/L2/inner product
- **全文搜索** — BM25 评分的倒排索引，内置中文分词（Jieba）
- **查询优化器** — 基于代价的优化器，谓词下推、索引选择、JOIN 重排序
- **GORM 集成** — 完整的 GORM Dialector，支持 AutoMigrate 和 ORM 操作
- **插件系统** — 支持自定义数据源、用户自定义函数（UDF）、原生插件（DLL/SO）
- **审计追踪** — Trace-ID 贯穿所有入口，完整的请求追踪和审计日志

## 两种使用场景

| 特性 | 独立服务器 | 嵌入式库 |
|------|-----------|---------|
| 部署方式 | 独立进程，监听端口 | 集成到 Go 应用中 |
| 访问方式 | MySQL 客户端 / HTTP / MCP | Go API 直接调用 |
| 适用场景 | 数据分析平台、多源查询网关 | 测试、CLI 工具、应用内 SQL 引擎 |
| 数据源配置 | datasources.json 文件 | 代码中注册 |
| 多用户 | 支持 | 单进程内 |

## 快速导航

| 我想... | 去看... |
|---------|--------|
| 5 分钟跑起来 | [快速开始](getting-started/quick-start.md) |
| 部署独立服务器 | [独立服务器概述](standalone-server/overview.md) |
| 嵌入到我的 Go 项目 | [嵌入式使用概述](embedded/overview.md) |
| 了解支持哪些数据源 | [数据源概述](datasources/overview.md) |
| 查 SQL 语法 | [SQL 参考](sql-reference/overview.md) |
| 查函数列表 | [函数参考](functions/overview.md) |
| 使用向量搜索 | [向量搜索](advanced/vector-search.md) |
| 使用全文搜索 | [全文搜索](advanced/fulltext-search.md) |
| 开发自定义插件 | [插件开发](plugin-development/overview.md) |
| 配合 GORM 使用 | [GORM 驱动](embedded/gorm-driver.md) |

## 技术栈

| 组件 | 技术 |
|------|------|
| 语言 | Go 1.24+ |
| SQL 解析 | TiDB Parser |
| 中文分词 | Jieba (gojieba) |
| MySQL 驱动 | go-sql-driver/mysql |
| PostgreSQL 驱动 | lib/pq |
| MCP 协议 | mcp-go |
| ORM 支持 | GORM v2 |
