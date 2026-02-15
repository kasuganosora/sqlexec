# 项目架构

## 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                        客户端接入层                           │
│  MySQL Client ──→ MySQL Protocol (TCP 3306)                  │
│  HTTP Client  ──→ HTTP API (RESTful)                         │
│  AI Tool      ──→ MCP Server (Streamable HTTP)               │
└──────────────┬────────────────┬──────────────┬───────────────┘
               │                │              │
               ▼                ▼              ▼
┌──────────────────────────────────────────────────────────────┐
│                        服务器层 (server/)                     │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐     │
│  │ MySQL Server │  │  HTTP API    │  │   MCP Server    │     │
│  │  handler/    │  │  httpapi/    │  │   mcp/          │     │
│  │  protocol/   │  │              │  │                 │     │
│  │  response/   │  │              │  │                 │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬──────────┘     │
│         │                 │                 │                │
│         └────────┬────────┴────────┬────────┘                │
│                  ▼                 ▼                         │
│         ┌──────────────┐  ┌──────────────┐                  │
│         │   Session    │  │ Audit Logger │                   │
│         │ (Trace-ID)   │  │ (security/)  │                   │
│         └──────┬───────┘  └──────────────┘                  │
└────────────────┼────────────────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────────────────┐
│                      API 层 (pkg/api/)                       │
│  DB ──→ Session ──→ Query() / Execute()                     │
│                      ↓                                       │
│               CoreSession (pkg/session/)                     │
│                      ↓                                       │
│               SQL Adapter (pkg/parser/)                      │
│                   TiDB Parser                                │
│                      ↓                                       │
│              OptimizedExecutor (pkg/optimizer/)               │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                    数据访问层                                  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  DataSourceManager (pkg/resource/application/)         │  │
│  │  ├── Registry (工厂注册)                                │  │
│  │  └── DataSource 路由                                    │  │
│  └────────────────────────┬───────────────────────────────┘  │
│                           │                                  │
│  ┌────────────┬───────────┼───────────┬──────────────────┐  │
│  ▼            ▼           ▼           ▼                  ▼  │
│ Memory     MySQL       PostgreSQL    HTTP          File      │
│ (MVCC)     (remote)    (remote)     (remote)    (CSV/JSON)  │
│ pkg/       server/     server/      server/     pkg/        │
│ resource/  datasource/ datasource/  datasource/ resource/   │
│ memory/    mysql/      postgresql/  http/       csv/json/   │
└──────────────────────────────────────────────────────────────┘
```

## 目录结构

```
cmd/
  service/              入口点 main.go

server/                 服务器层
  server.go             MySQL 服务器主体，连接处理，组件初始化
  handler/              MySQL 命令处理器（Handler Chain 模式）
    handler.go          HandlerContext、HandlerRegistry
    query/              COM_QUERY 处理（SQL 查询执行）
    simple/             COM_PING、COM_QUIT 等简单命令
    handshake/          MySQL 握手认证
    process/            KILL 命令处理
    packet_parsers/     MySQL 协议包解析器
  protocol/             MySQL 协议编解码
  response/             MySQL 结果集构建
  acl/                  访问控制列表
  httpapi/              HTTP API 服务器
  mcp/                  MCP (Model Context Protocol) 服务器
  datasource/           外部数据源实现
    sql/                MySQL/PostgreSQL 共享基础（Dialect 抽象）
    mysql/              MySQL 数据源
    postgresql/         PostgreSQL 数据源
    http/               HTTP 远程数据源

pkg/                    核心库
  api/                  公共 API 层（DB、Session、Query）
  session/              会话管理（CoreSession、QueryContext、Trace-ID）
  parser/               SQL 解析适配器（封装 TiDB Parser）
  optimizer/            查询优化器
    core/               核心优化逻辑
    cost/               代价模型
    statistics/         统计信息
    join/               JOIN 算法
    parallel/           并行执行
    plan/               执行计划
    physical/           物理计划
    index/              索引优化
  executor/             查询执行器
    operators/          算子实现
    parallel/           并行执行
  resource/             数据源抽象
    domain/             领域模型（DataSource 接口、TableInfo、Filter 等）
    application/        DataSourceManager、Registry
    memory/             内存 MVCC 存储引擎
    csv/json/excel/parquet/  文件数据源
    slice/              Go Slice 适配器
    file/               文件数据源基类
  mvcc/                 MVCC 事务引擎
  fulltext/             全文搜索引擎
    analyzer/           分词器
    bm25/               BM25 评分
    index/              倒排索引
    query/              全文查询
  builtin/              内建函数注册
  extensibility/        UDF 扩展
  security/             安全（审计日志、SQL 注入检测）
  plugin/               插件加载器（DLL/SO）
  config/               配置管理
  config_schema/        配置文件模式（datasources.json、api_clients.json）
  information_schema/   INFORMATION_SCHEMA 虚拟数据库
  virtual/              虚拟数据库
  pool/                 对象池与 Goroutine 池
  workerpool/           工作池
  json/                 JSON 函数支持
  monitor/              性能监控
  reliability/          可靠性（熔断、限流）
  types/                类型系统
  utils/                工具函数
```

## 模块依赖关系

```
server/
  ├── pkg/api          (DB, Session)
  ├── pkg/session      (SessionMgr, CoreSession)
  ├── pkg/config       (配置)
  ├── pkg/security     (审计日志)
  ├── pkg/plugin       (插件加载)
  └── server/datasource/* (数据源工厂)

pkg/api
  ├── pkg/session      (CoreSession)
  └── pkg/resource     (DataSourceManager)

pkg/session (CoreSession)
  ├── pkg/parser       (SQL Adapter)
  └── pkg/optimizer    (OptimizedExecutor)

pkg/optimizer
  ├── pkg/executor     (执行器)
  ├── pkg/resource     (数据源访问)
  ├── pkg/fulltext     (全文搜索)
  └── pkg/dataaccess   (数据访问服务)

pkg/resource/memory
  └── pkg/mvcc         (MVCC 事务引擎)
```

## 请求处理流程

### MySQL 协议查询

```
TCP 连接 → handleConnection()
  → 创建 Session（自动生成 Trace-ID）
  → 握手认证
  → 命令循环:
      → 读取 MySQL Packet
      → PacketParserRegistry.Parse() → CommandPack
      → HandlerRegistry.Handle() → Handler
      → QueryHandler:
          → session.Query(sql) / session.Execute(sql)
          → CoreSession.ExecuteQuery()
              → ExtractTraceID(sql)     // SQL 注释中的 trace_id
              → SQLAdapter.Parse(sql)   // TiDB Parser
              → OptimizedExecutor.Execute(stmt)
                  → 代价优化 → 物理计划 → 数据源执行
              → 审计日志记录
          → 构建 MySQL ResultSet 响应
```

### HTTP API 查询

```
POST /api/v1/query
  → Auth Middleware（Bearer Token）
  → QueryHandler.ServeHTTP()
      → 解析 JSON Body
      → 解析 Trace-ID（body > X-Trace-ID header > 自动生成）
      → 创建临时 Session
      → session.Query() / session.Execute()
      → 审计日志记录
      → JSON 响应
```

### MCP 工具调用

```
POST /mcp (Streamable HTTP)
  → Bearer Token 认证
  → MCP 协议解析
  → HandleQuery()
      → 解析 trace_id 工具参数
      → 创建临时 Session
      → session.Query() / session.Execute()
      → 审计日志记录
      → MCP TextContent 响应
```

## 核心设计模式

- **Handler Chain**：MySQL 命令通过 HandlerRegistry 分发到对应的 Handler
- **Factory + Registry**：DataSourceFactory 注册到 Registry，按类型创建数据源
- **Dialect 抽象**：MySQL/PostgreSQL 共享 SQLCommonDataSource，通过 Dialect 接口分离差异
- **Domain-Driven Design**：`pkg/resource/domain/` 定义核心接口，各实现包独立实现
- **Session 分层**：协议 Session → API Session → CoreSession，各层职责清晰
