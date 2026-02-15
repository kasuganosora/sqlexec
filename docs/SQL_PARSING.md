# SQL 解析与执行管线技术原理

本文档详细描述 SQLExec 项目中 SQL 语句从文本输入到最终执行的完整技术管线。

## 目录

1. [SQL 解析架构](#1-sql-解析架构)
2. [语句类型支持](#2-语句类型支持)
3. [执行管线](#3-执行管线)
4. [Session 分层架构](#4-session-分层架构)
5. [QueryContext 查询上下文](#5-querycontext-查询上下文)
6. [参数绑定](#6-参数绑定)
7. [查询超时与 KILL 机制](#7-查询超时与-kill-机制)

---

## 1. SQL 解析架构

### 1.1 TiDB Parser 集成

SQLExec 的 SQL 解析器基于 [TiDB Parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) 构建。TiDB Parser 是一个工业级、经过大规模生产验证的 MySQL 兼容 SQL 解析器，能够将 SQL 文本解析为 AST（抽象语法树）。

系统中有两个层面封装了 TiDB Parser：

**底层封装 — `Parser` (`pkg/parser/parser.go`)**

```go
type Parser struct {
    parser *parser.Parser  // TiDB 原生 parser
}
```

`Parser` 是对 TiDB `parser.Parser` 的直接封装，提供类型安全的解析方法：

- `ParseSQL(sql)` — 解析 SQL 文本，返回 `[]ast.StmtNode`（TiDB AST 节点列表）
- `ParseOneStmt(sql)` — 解析单条 SQL，返回 `ast.StmtNode`
- 各类型安全方法：`ParseSelectStmt`、`ParseInsertStmt`、`ParseUpdateStmt` 等

还提供了语句分类的辅助函数：

- `GetStmtType(stmt)` — 返回语句类型字符串（`"SELECT"`, `"INSERT"` 等）
- `IsWriteOperation(stmt)` — 判断是否为写操作
- `IsReadOperation(stmt)` — 判断是否为读操作
- `IsTransactionOperation(stmt)` — 判断是否为事务操作

**WITH 子句预处理**

TiDB Parser 原生不支持 `CREATE INDEX ... WITH (param=value)` 语法。系统在解析前会对 SQL 进行预处理，将 `WITH (...)` 子句转换为 `COMMENT '...'` 子句，从而让 TiDB Parser 能正常解析：

```
-- 输入
CREATE VECTOR INDEX idx ON t(embedding) USING HNSW WITH (metric='cosine', dim=768)

-- 预处理后
CREATE VECTOR INDEX idx ON t(embedding) USING HNSW COMMENT 'metric=cosine, dim=768'
```

此逻辑在 `preprocessWithClause()` 函数中实现（`pkg/parser/parser.go`）。

### 1.2 SQLAdapter — 高级解析适配器

`SQLAdapter`（`pkg/parser/adapter.go`）是 SQL 执行管线中真正使用的核心解析器。它在 TiDB Parser 的 AST 之上构建了一套自有的中间表示（IR），将 TiDB 的 AST 节点转换为系统内部的 `SQLStatement` 结构体树。

```go
type SQLAdapter struct {
    parser *parser.Parser  // TiDB 原生 parser
}
```

**核心方法：**

- `Parse(sql string) (*ParseResult, error)` — 解析单条 SQL 语句
- `ParseMulti(sql string) ([]*ParseResult, error)` — 解析多条 SQL 语句

**解析流程（`Parse` 方法）：**

```
SQL 文本
  │
  ▼
preprocessWithClause()       // 预处理 WITH 子句
  │
  ▼
TiDB parser.Parse()          // TiDB Parser 解析为 AST
  │
  ▼
convertToStatement()         // AST → 内部 SQLStatement
  │
  ▼
ParseResult{Statement, Success}
```

`convertToStatement()` 是核心转换方法。它对 `ast.StmtNode` 进行 type switch，针对每种语句类型调用相应的转换函数：

| TiDB AST 类型 | 转换方法 | 目标结构体 |
|---|---|---|
| `*ast.SelectStmt` | `convertSelectStmt` | `SelectStatement` |
| `*ast.InsertStmt` | `convertInsertStmt` | `InsertStatement` |
| `*ast.UpdateStmt` | `convertUpdateStmt` | `UpdateStatement` |
| `*ast.DeleteStmt` | `convertDeleteStmt` | `DeleteStatement` |
| `*ast.CreateTableStmt` | `convertCreateTableStmt` | `CreateStatement` |
| `*ast.DropTableStmt` | `convertDropTableStmt` / `convertDropViewStmt` | `DropStatement` / `DropViewStatement` |
| `*ast.AlterTableStmt` | `convertAlterTableStmt` | `AlterStatement` |
| `*ast.CreateIndexStmt` | `convertCreateIndexStmt` | `CreateIndexStatement` |
| `*ast.DropIndexStmt` | `convertDropIndexStmt` | `DropIndexStatement` |
| `*ast.ShowStmt` | `convertShowStmt` | `ShowStatement` |
| `*ast.ExplainStmt` | 内联处理 | `ExplainStatement` / `DescribeStatement` |
| `*ast.UseStmt` | `convertUseStmt` | `UseStatement` |
| `*ast.CreateViewStmt` | `convertCreateViewStmt` | `CreateViewStatement` |

### 1.3 内部类型系统

所有类型定义在 `pkg/parser/types.go` 中。

**`SQLStatement`** — 语句级顶层容器：

```go
type SQLStatement struct {
    Type       SQLType               // 语句类型枚举
    RawSQL     string                // 原始 SQL 文本
    Select     *SelectStatement      // SELECT 相关
    Insert     *InsertStatement      // INSERT 相关
    Update     *UpdateStatement      // UPDATE 相关
    Delete     *DeleteStatement      // DELETE 相关
    Create     *CreateStatement      // CREATE TABLE 相关
    CreateView *CreateViewStatement  // CREATE VIEW 相关
    Drop       *DropStatement        // DROP TABLE 相关
    DropView   *DropViewStatement    // DROP VIEW 相关
    Alter      *AlterStatement       // ALTER TABLE 相关
    CreateIndex *CreateIndexStatement // CREATE INDEX 相关
    DropIndex  *DropIndexStatement   // DROP INDEX 相关
    Show       *ShowStatement        // SHOW 相关
    Describe   *DescribeStatement    // DESCRIBE 相关
    Explain    *ExplainStatement     // EXPLAIN 相关
    Use        *UseStatement         // USE 相关
    // ... 更多类型
}
```

**`Expression`** — 统一的表达式表示：

```go
type Expression struct {
    Type     ExprType      // COLUMN, VALUE, OPERATOR, FUNCTION, LIST
    Column   string        // 列名（Type=COLUMN 时）
    Value    interface{}   // 值（Type=VALUE 时）
    Operator string        // 运算符（Type=OPERATOR 时，如 "=", "AND", "LIKE"）
    Left     *Expression   // 左操作数
    Right    *Expression   // 右操作数
    Args     []Expression  // 函数参数（Type=FUNCTION 时）
    Function string        // 函数名（Type=FUNCTION 时）
}
```

表达式转换在 `convertExpression()` 方法中完成，支持以下 AST 节点类型：

- `*ast.BinaryOperationExpr` → `ExprTypeOperator`（二元运算：`=`, `AND`, `>`, `+` 等）
- `*ast.ColumnNameExpr` → `ExprTypeColumn`（列引用）
- `ast.ValueExpr` → `ExprTypeValue`（字面量值）
- `*ast.FuncCallExpr` → `ExprTypeFunction`（函数调用）
- `*ast.PatternLikeOrIlikeExpr` → `ExprTypeOperator`（`LIKE` / `NOT LIKE`）
- `*ast.BetweenExpr` → `ExprTypeOperator`（`BETWEEN` / `NOT BETWEEN`）

### 1.4 AST Visitor 模式

`pkg/parser/visitor.go` 实现了 TiDB 的 AST Visitor 模式，用于遍历 AST 树并提取元信息：

```go
type SQLVisitor struct {
    info *SQLInfo
}

type SQLInfo struct {
    Tables       []string
    Columns      []string
    Databases    []string
    WhereExpr    ast.ExprNode
    LimitExpr    *ast.Limit
    OrderByItems []*ast.ByItem
    GroupByItems []*ast.ByItem
    // ...
}
```

`ExtractSQLInfo()` 函数通过 Visitor 模式遍历 AST，收集涉及的表名、列名和数据库名。

### 1.5 Hints 解析

`pkg/parser/hints_parser.go` 实现了 SQL Hint 的解析功能。Hints 以 `/*+ ... */` 注释形式嵌入 SQL 中，支持：

- **JOIN Hints**：`HASH_JOIN(t)`, `MERGE_JOIN(t)`, `INL_JOIN(t)`, `LEADING(t1, t2)`, `STRAIGHT_JOIN` 等
- **INDEX Hints**：`USE_INDEX(t@idx)`, `FORCE_INDEX(t@idx)`, `IGNORE_INDEX(t@idx)`, `ORDER_INDEX(t@idx)` 等
- **AGG Hints**：`HASH_AGG`, `STREAM_AGG`
- **Subquery Hints**：`SEMI_JOIN_REWRITE`, `NO_DECORRELATE`, `USE_TOJA`
- **全局 Hints**：`MAX_EXECUTION_TIME(1000ms)`, `MEMORY_QUOTA(1024)`, `QB_NAME(qb1)`, `RESOURCE_GROUP(rg1)`

### 1.6 向量索引扩展

`SQLAdapter` 对向量列和向量索引提供了完整的解析支持：

- **向量列类型**：`VECTOR(dim)` 或 `ARRAY<FLOAT, dim>`，通过 `isVectorType()` 和 `extractVectorDimension()` 检测
- **向量索引创建**：支持 TiDB 的 `CREATE VECTOR INDEX` 语法，以及通过 `USING HNSW` 等子句指定索引类型
- **向量距离函数**：解析 `VEC_COSINE_DISTANCE(col)`, `VEC_L2_DISTANCE(col)`, `VEC_INNER_PRODUCT(col)` 函数表达式
- **索引参数**：从 `WITH (metric='cosine', dim=768, M=16)` 子句（预处理后为 COMMENT）中提取向量索引参数

---

## 2. 语句类型支持

### 2.1 SQLType 枚举

系统通过 `SQLType` 枚举定义了所有支持的语句类型（`pkg/parser/types.go`）：

| SQLType | 说明 | 示例 |
|---|---|---|
| `SQLTypeSelect` | 查询 | `SELECT * FROM users` |
| `SQLTypeInsert` | 插入 | `INSERT INTO users (name) VALUES ('Alice')` |
| `SQLTypeUpdate` | 更新 | `UPDATE users SET name='Bob' WHERE id=1` |
| `SQLTypeDelete` | 删除 | `DELETE FROM users WHERE id=1` |
| `SQLTypeCreate` | 创建表 | `CREATE TABLE users (id INT, name VARCHAR(100))` |
| `SQLTypeCreateView` | 创建视图 | `CREATE VIEW v AS SELECT * FROM users` |
| `SQLTypeDrop` | 删除表 | `DROP TABLE users` |
| `SQLTypeDropView` | 删除视图 | `DROP VIEW v` |
| `SQLTypeAlter` | 修改表 | `ALTER TABLE users ADD COLUMN age INT` |
| `SQLTypeTruncate` | 截断表 | `TRUNCATE TABLE users` |
| `SQLTypeShow` | 元数据查询 | `SHOW TABLES`, `SHOW DATABASES`, `SHOW PROCESSLIST` |
| `SQLTypeDescribe` | 表结构查询 | `DESC users`, `DESCRIBE users` |
| `SQLTypeExplain` | 执行计划 | `EXPLAIN SELECT * FROM users` |
| `SQLTypeUse` | 切换数据库 | `USE mydb` |
| `SQLTypeBegin` | 开始事务 | `BEGIN`, `START TRANSACTION` |
| `SQLTypeCommit` | 提交事务 | `COMMIT` |
| `SQLTypeRollback` | 回滚事务 | `ROLLBACK` |
| `SQLTypeCreateUser` | 创建用户 | `CREATE USER 'alice'@'%' IDENTIFIED BY '123'` |
| `SQLTypeDropUser` | 删除用户 | `DROP USER 'alice'@'%'` |
| `SQLTypeGrant` | 授权 | `GRANT SELECT ON db.* TO 'alice'@'%'` |
| `SQLTypeRevoke` | 撤销权限 | `REVOKE SELECT ON db.* FROM 'alice'@'%'` |
| `SQLTypeSetPasswd` | 修改密码 | `SET PASSWORD FOR 'alice'@'%' = PASSWORD('456')` |

### 2.2 SELECT 语句

`SelectStatement` 结构支持完整的 SELECT 语法：

- **列选择**：支持 `*` 通配符、列名、别名、函数调用表达式
- **FROM**：表名，支持 `schema.table` 格式
- **JOIN**：`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`（通过递归 `convertJoinTree` 处理多表连接树）
- **WHERE**：完整的表达式树，支持 `AND/OR` 逻辑运算、比较运算、`LIKE`、`BETWEEN`、函数调用
- **GROUP BY**：分组列列表
- **HAVING**：分组后过滤条件
- **ORDER BY**：排序列列表，支持 `ASC/DESC`，支持函数表达式（如 `vec_cosine_distance(...)`）
- **LIMIT / OFFSET**：分页

### 2.3 DML 语句

- **INSERT**：表名、列名列表、值列表（支持多行 `VALUES`）
- **UPDATE**：表名、`SET` 键值对 (`map[string]interface{}`)、WHERE、ORDER BY、LIMIT
- **DELETE**：表名、WHERE、ORDER BY、LIMIT

### 2.4 DDL 语句

- **CREATE TABLE**：列定义（名称、类型、约束）、向量列支持、生成列（`GENERATED ALWAYS AS` — `STORED`/`VIRTUAL`）
- **CREATE INDEX**：索引名、表名、列名、索引类型（BTREE/HASH/FULLTEXT/VECTOR）、向量索引参数
- **DROP TABLE / INDEX**：支持 `IF EXISTS`
- **ALTER TABLE**：操作列表（`ADD`/`DROP`/`MODIFY`/`CHANGE` 等）
- **CREATE VIEW**：支持 `OR REPLACE`、`Algorithm`、`Definer`、`Security`、`CheckOption`

---

## 3. 执行管线

### 3.1 完整管线概览

一条 SQL 从客户端发送到结果返回，经历以下完整路径：

```
MySQL Client
  │
  │ TCP (MySQL Protocol)
  │
  ▼
┌─────────────────────────────────────────────────┐
│ Server Layer                                     │
│                                                   │
│  COM_QUERY Packet                                 │
│      │                                            │
│      ▼                                            │
│  QueryHandler.Handle()                            │
│  (server/handler/query/query_handler.go)          │
│      │                                            │
│      │  提取 SQL 文本                              │
│      │  获取 Protocol Session → API Session        │
│      ▼                                            │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│ API Layer (pkg/api/)                              │
│                                                   │
│  Session.Query(sql, args...)                      │
│  Session.Execute(sql, args...)                    │
│      │                                            │
│      │  1. 参数绑定 (bindParams)                   │
│      │  2. 缓存检查                                │
│      ▼                                            │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│ Session Layer (pkg/session/)                      │
│                                                   │
│  CoreSession.ExecuteQuery / ExecuteInsert / ...   │
│      │                                            │
│      │  1. ExtractTraceID (提取 trace_id 注释)     │
│      │  2. createQueryContext (创建超时/取消上下文)  │
│      │  3. 注册到 GlobalQueryRegistry              │
│      │  4. SQLAdapter.Parse(sql)                   │
│      ▼                                            │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│ Parser Layer (pkg/parser/)                        │
│                                                   │
│  SQLAdapter.Parse(sql)                            │
│      │                                            │
│      │  1. preprocessWithClause()                  │
│      │  2. TiDB parser.Parse() → AST              │
│      │  3. convertToStatement() → SQLStatement     │
│      ▼                                            │
│  返回 ParseResult{Statement, Success}              │
│                                                   │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│ Optimizer / Executor Layer (pkg/optimizer/)        │
│                                                   │
│  OptimizedExecutor.ExecuteSelect / ExecuteInsert  │
│      │                                            │
│      ├─ information_schema? → QueryBuilder 路径    │
│      ├─ config 虚拟数据库? → Config 虚拟数据源       │
│      ├─ 启用优化器? → executeWithOptimizer()       │
│      │      │                                     │
│      │      │  1. 构建 SQLStatement                │
│      │      │  2. EnhancedOptimizer.Optimize()     │
│      │      │     → 逻辑计划 → 优化规则 → 物理计划   │
│      │      │  3. 执行物理计划                      │
│      │      ▼                                     │
│      └─ 否 → executeWithBuilder() (QueryBuilder)  │
│                                                   │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│ DataSource Layer (pkg/resource/domain/)            │
│                                                   │
│  domain.DataSource 接口                            │
│      │                                            │
│      ├── memory.MVCCDataSource (内存 MVCC)         │
│      ├── MySQL DataSource (外部 MySQL)              │
│      ├── PostgreSQL DataSource (外部 PostgreSQL)    │
│      └── virtual.VirtualDataSource (虚拟表)         │
│                                                   │
│  QueryRows / InsertRow / UpdateRows / DeleteRows   │
│                                                   │
└──────┬──────────────────────────────────────────────┘
       │
       ▼
  domain.QueryResult{Columns, Rows, Total}
       │
       │ (逆向传播到各层)
       ▼
  MySQL ResultSet Packets → Client
```

### 3.2 读查询路径（SELECT / SHOW）

以 `SELECT * FROM users WHERE id = 1` 为例：

1. **QueryHandler** 从 `COM_QUERY` 包中提取 SQL 文本
2. **API Session.Query()** 执行参数绑定（如有 `?` 占位符），检查缓存
3. **CoreSession.ExecuteQuery()** 创建 `QueryContext`（含超时和取消），注册到全局注册表
4. **SQLAdapter.Parse()** 解析 SQL → `ParseResult.Statement.Select`
5. **OptimizedExecutor.ExecuteSelect()** 根据条件选择执行路径：
   - 若为 `information_schema` 查询 → `QueryBuilder` 路径
   - 若为 `config` 虚拟数据库查询 → Config 虚拟数据源路径
   - 若启用优化器 → `executeWithOptimizer()`：调用 `EnhancedOptimizer.Optimize()` 生成物理计划，再执行
   - 否则 → `executeWithBuilder()`：直接通过 `QueryBuilder` 执行
6. 返回 `domain.QueryResult`
7. **QueryHandler** 将结果序列化为 MySQL 协议的 ResultSet 包（列定义 → 行数据 → EOF）

### 3.3 写操作路径（INSERT / UPDATE / DELETE）

以 `INSERT INTO users (name) VALUES ('Alice')` 为例：

1. **QueryHandler** 提取 SQL 文本（注意：MySQL 协议中 INSERT 也通过 COM_QUERY 发送）
2. **API Session.Execute()** 解析语句类型，路由到对应的 `CoreSession.ExecuteInsert()`
3. **CoreSession.ExecuteInsert()** 创建 `QueryContext`，注册查询
4. **SQLAdapter.Parse()** 解析 SQL → `ParseResult.Statement.Insert`
5. **OptimizedExecutor.ExecuteInsert()** 执行插入操作
6. 返回 `domain.QueryResult`（`Total` 字段表示影响行数）
7. 清除相关表的缓存

### 3.4 DDL 操作路径

DDL 操作（CREATE / DROP / ALTER）的路径类似，但通过 `API Session.Execute()` 的 type switch 分发到对应的 `CoreSession.ExecuteCreate()` / `ExecuteDrop()` / `ExecuteAlter()` / `ExecuteCreateIndex()` / `ExecuteDropIndex()`。

### 3.5 SHOW 语句路径

`SHOW` 语句在 `CoreSession.ExecuteQuery()` 中被识别后，调用 `OptimizedExecutor.ExecuteShow()`。`ShowExecutor` 将 SHOW 语句转换为等价的 `information_schema` 查询：

- `SHOW TABLES` → 查询 `information_schema.tables`
- `SHOW DATABASES` → 查询 `information_schema.schemata`
- `SHOW COLUMNS FROM t` → 查询 `information_schema.columns`
- `SHOW CREATE TABLE t` → 从表元数据重建 DDL
- `SHOW PROCESSLIST` → 从全局 `QueryRegistry` 获取活跃查询列表

---

## 4. Session 分层架构

系统有三层 Session，各司其职：

### 4.1 Protocol Session (`pkg/session/session.go`)

协议层 Session 管理 TCP 连接的生命周期：

```go
type Session struct {
    ID         string       // 会话ID（MD5(addr+port)）
    ThreadID   uint32       // 线程ID（用于 KILL 命令）
    TraceID    string       // 追踪ID（格式: {ThreadID}-{timestamp}）
    User       string       // 登录用户名
    Created    time.Time    // 创建时间
    LastUsed   time.Time    // 最后使用时间（用于 GC）
    RemoteIP   string       // 客户端 IP
    RemotePort string       // 客户端端口
    SequenceID uint8        // MySQL 协议包序列号
    APISession interface{}  // 关联的 API Session
}
```

**SessionMgr** 负责管理所有 Protocol Session：
- `GetOrCreateSession(addr, port)` — 获取或创建会话
- `GenerateSessionID(addr, port)` — 生成会话 ID
- `GetThreadId()` — 分配唯一线程 ID（递增扫描可用 ID）
- `GC()` — 定期清理过期会话（默认 24 小时过期，1 分钟 GC 间隔）

**会话变量支持**

Protocol Session 支持通过 `SetVariable`/`GetVariable` 存储会话级变量。特殊变量 `trace_id` 会被拦截并同步到 API Session（CoreSession）。

### 4.2 API Session (`pkg/api/session_types.go`, `pkg/api/session_query.go`, `pkg/api/session_dml.go`)

API 层 Session 是面向应用层的高级接口：

```go
type Session struct {
    db           *DB                   // 关联的数据库实例
    coreSession  *session.CoreSession  // 底层核心 Session
    options      *SessionOptions       // 会话选项
    cacheEnabled bool                  // 是否启用查询缓存
    logger       Logger                // 日志记录器
    queryTimeout time.Duration         // 查询超时
    threadID     uint32                // 线程 ID
}
```

**主要方法：**

- `Query(sql, args...)` — 执行 SELECT / SHOW / DESCRIBE，返回 `*Query` 迭代器
- `QueryAll(sql, args...)` — 执行查询并返回所有行
- `QueryOne(sql, args...)` — 执行查询并返回第一行
- `Execute(sql, args...)` — 执行 INSERT / UPDATE / DELETE / DDL，返回 `*Result`
- `Explain(sql, args...)` — 生成执行计划

**API Session 负责：**

1. 参数绑定（`?` 占位符替换）
2. 查询缓存检查与更新
3. 语句类型路由（读操作 vs 写操作）
4. 错误分类与包装

### 4.3 CoreSession (`pkg/session/core.go`)

核心 Session 是 SQL 执行的核心：

```go
type CoreSession struct {
    dataSource   domain.DataSource              // 数据源
    dsManager    *application.DataSourceManager  // 数据源管理器
    executor     *optimizer.OptimizedExecutor     // 优化执行器
    adapter      *parser.SQLAdapter              // SQL 解析适配器
    currentDB    string                          // 当前数据库
    user         string                          // 当前用户
    host         string                          // 客户端主机
    txn          domain.Transaction              // 活跃事务
    tempTables   []string                        // 会话级临时表
    queryTimeout time.Duration                   // 查询超时
    threadID     uint32                          // 线程 ID
    traceID      string                          // 追踪 ID
}
```

**CoreSession 职责：**

1. SQL 解析（调用 `SQLAdapter.Parse()`）
2. 创建 `QueryContext`（超时、取消、追踪）
3. 查询注册与注销（全局 `QueryRegistry`）
4. 调用 `OptimizedExecutor` 执行语句
5. 处理超时和取消错误
6. 事务管理（`BeginTx` / `CommitTx` / `RollbackTx`）
7. 临时表管理
8. 会话关闭时的资源清理

### 4.4 三层关系

```
Protocol Session (TCP 连接生命周期)
    │
    │ SetAPISession() / GetAPISession()
    ▼
API Session (应用层接口：缓存、参数绑定、错误包装)
    │
    │ s.coreSession
    ▼
CoreSession (核心执行：解析、优化、超时、事务)
    │
    │ s.executor
    ▼
OptimizedExecutor → DataSource
```

线程 ID 和追踪 ID 在三层之间传播：

- Protocol Session 创建时生成 `ThreadID` 和 `TraceID`
- 通过 `SetThreadID()` / `SetTraceID()` 传递到 API Session
- API Session 进一步传递到 CoreSession
- CoreSession 在每个查询中将这些信息注入 `QueryContext`

---

## 5. QueryContext 查询上下文

### 5.1 结构定义

`QueryContext`（`pkg/session/query_context.go`）是每个查询的运行时上下文：

```go
type QueryContext struct {
    QueryID    string             // 查询唯一 ID（格式: {ThreadID}_{timestamp}_{sequence}）
    ThreadID   uint32             // 关联的线程 ID
    TraceID    string             // 追踪 ID
    SQL        string             // 执行的 SQL
    StartTime  time.Time          // 开始时间
    CancelFunc context.CancelFunc // 取消函数
    User       string             // 执行用户
    Host       string             // 客户端主机
    DB         string             // 当前数据库
    canceled   bool               // 是否被取消
    timeout    bool               // 是否超时
}
```

### 5.2 QueryID 生成

QueryID 由 `GenerateQueryID()` 生成（`pkg/session/query_registry.go`），格式为 `{ThreadID}_{UnixNano}_{AtomicSequence}`，保证全局唯一：

```go
func GenerateQueryID(threadID uint32) string {
    timestamp := time.Now().UnixNano()
    seq := atomic.AddUint64(&querySequence, 1)
    return fmt.Sprintf("%d_%d_%d", threadID, timestamp, seq)
}
```

### 5.3 创建流程

`CoreSession.createQueryContext()` 创建查询上下文：

1. 读取会话级别的 `queryTimeout`、`threadID`、`traceID`、`user`、`host`、`currentDB`
2. 创建可取消的 `context.Context`（`context.WithCancel`）
3. 生成唯一 `QueryID`
4. 如果设置了超时，包装为 `context.WithTimeout`，并组合两个 cancel 函数
5. 返回 `(ctx, cancelFunc, *QueryContext)`

### 5.4 TraceID 来源优先级

TraceID 有三个来源，优先级从高到低：

1. **SQL 注释**：`/*trace_id=abc123*/ SELECT ...` — 最高优先级，通过 `ExtractTraceID()` 提取
2. **会话变量**：`SET @trace_id = 'abc123'` — Protocol Session 级别
3. **自动生成**：Protocol Session 创建时自动生成 `{ThreadID}-{timestamp}`

### 5.5 全局查询注册表

`QueryRegistry`（`pkg/session/query_registry.go`）是一个全局单例，维护所有活跃查询的注册表：

```go
type QueryRegistry struct {
    queries   map[string]*QueryContext  // QueryID → QueryContext
    threadMap map[uint32]*QueryContext  // ThreadID → 当前查询
}
```

核心操作：

- `RegisterQuery(qc)` — 注册查询。如果该 ThreadID 已有活跃查询，先取消旧查询
- `UnregisterQuery(queryID)` — 注销查询（查询完成后 defer 调用）
- `KillQueryByThreadID(threadID)` — 通过 ThreadID 终止查询（KILL 命令使用）
- `GetAllQueries()` — 获取所有活跃查询（SHOW PROCESSLIST 使用）

---

## 6. 参数绑定

### 6.1 绑定机制

参数绑定在 API 层实现（`pkg/api/params.go`），采用文本替换策略：

```go
func bindParams(sql string, params []interface{}) (string, error)
```

**工作原理：**

1. 统计 SQL 中 `?` 占位符的数量
2. 校验占位符数量与参数数量是否匹配（不匹配则报错）
3. 逐个替换 `?` 为参数的 SQL 字面量表示

### 6.2 类型转换规则

`paramToSQLLiteral()` 函数将 Go 类型转换为 SQL 字面量：

| Go 类型 | SQL 字面量 | 示例 |
|---|---|---|
| `nil` | `NULL` | `NULL` |
| `string` | 单引号包裹，内部单引号双写转义 | `'hello'`, `'it''s'` |
| `int`, `int64` 等 | 数字文本 | `42` |
| `uint`, `uint64` 等 | 数字文本 | `100` |
| `float32`, `float64` | 浮点文本 | `3.14` |
| `bool` | `TRUE` / `FALSE` | `TRUE` |
| `[]byte` | 十六进制 | `0x48656c6c6f` |
| 其他类型 | `fmt.Sprintf("%v", val)` 后引号包裹 | `'...'` |

### 6.3 使用示例

```go
// API 层调用
session.Query("SELECT * FROM users WHERE id = ? AND name = ?", 1, "Alice")

// 内部绑定后变为
// SELECT * FROM users WHERE id = 1 AND name = 'Alice'
```

---

## 7. 查询超时与 KILL 机制

### 7.1 查询超时

**设置超时**

超时可在多个层级设置：

- **会话级别**：通过 `SessionOptions.QueryTimeout` 或 `CoreSession.SetQueryTimeout()` 设置
- **Hint 级别**：通过 `/*+ MAX_EXECUTION_TIME(1000ms) */` SQL Hint 设置

**超时实现**

`CoreSession.createQueryContext()` 中，如果 `queryTimeout > 0`，使用 `context.WithTimeout()` 包装上下文：

```go
if timeout > 0 {
    ctx, timeoutCancel = context.WithTimeout(baseCtx, timeout)
    // 组合 cancel 函数，同时释放超时计时器和基础上下文
    combinedCancel := func() {
        timeoutCancel()
        cancel()
    }
    return ctx, combinedCancel, queryCtx
}
```

超时的 context 被传递给 `OptimizedExecutor` 和 `DataSource`，当 context 过期时，底层操作会通过 `context.DeadlineExceeded` 错误终止。

**超时错误处理**

`CoreSession.ExecuteQuery()` 等方法统一检查超时和取消错误：

```go
if errors.Is(err, context.DeadlineExceeded) {
    qc.SetTimeout()
    return nil, fmt.Errorf("query execution timed out after %v", qc.GetDuration())
}
if errors.Is(err, context.Canceled) {
    if qc.IsCanceled() {
        return nil, fmt.Errorf("query was killed")
    }
    return nil, fmt.Errorf("query execution cancelled")
}
```

### 7.2 KILL 命令

**KILL 机制实现**

KILL 命令通过全局 `QueryRegistry` 实现：

```go
func (r *QueryRegistry) KillQueryByThreadID(threadID uint32) error {
    qc, ok := r.threadMap[threadID]
    if !ok {
        return fmt.Errorf("query not found for thread %d", threadID)
    }
    qc.SetCanceled()     // 标记为已取消
    qc.CancelFunc()      // 触发 context 取消
    return nil
}
```

**全局便捷函数：**

```go
// 直接使用全局注册表
session.KillQueryByThreadID(threadID)
```

**KILL 的传播路径：**

1. 客户端发送 `KILL <thread_id>` 命令
2. 服务器调用 `KillQueryByThreadID(threadID)`
3. 全局 `QueryRegistry` 查找该 ThreadID 对应的 `QueryContext`
4. 调用 `qc.SetCanceled()` 标记取消状态
5. 调用 `qc.CancelFunc()` 触发 Go context 的取消
6. 正在执行的查询通过 `context.Canceled` 错误感知到取消
7. `CoreSession` 检测到 `context.Canceled` 且 `qc.IsCanceled() == true`，返回 `"query was killed"` 错误

### 7.3 SHOW PROCESSLIST

`SHOW PROCESSLIST` 通过全局 `QueryRegistry` 获取所有活跃查询的快照：

```go
func GetProcessListForOptimizer() []interface{} {
    registry := GetGlobalQueryRegistry()
    queries := registry.GetAllQueries()
    // 转换为 map 列表，包含 QueryID, ThreadID, SQL, Duration, Status, User, Host, DB
}
```

每个 `QueryContext` 提供 `GetStatus()` 方法，返回当前状态（`"running"`, `"canceled"`, `"timeout"`）、持续时间等信息。

---

## 附录：关键源文件索引

| 文件路径 | 说明 |
|---|---|
| `pkg/parser/parser.go` | TiDB Parser 封装、预处理、语句类型判断 |
| `pkg/parser/adapter.go` | SQLAdapter — 核心解析器，AST → SQLStatement 转换 |
| `pkg/parser/types.go` | 所有内部类型定义（SQLStatement, Expression, ColumnInfo 等） |
| `pkg/parser/expr_converter.go` | AST 表达式转换器 |
| `pkg/parser/visitor.go` | AST Visitor 模式实现 |
| `pkg/parser/hints_parser.go` | SQL Hint 解析器 |
| `pkg/parser/builder.go` | QueryBuilder — 传统执行路径 |
| `pkg/parser/handler.go` | 语句处理器链（HandlerChain） |
| `pkg/session/core.go` | CoreSession — 核心会话实现 |
| `pkg/session/query_context.go` | QueryContext — 查询上下文定义 |
| `pkg/session/query_registry.go` | QueryRegistry — 全局查询注册表 |
| `pkg/session/trace.go` | TraceID 提取（SQL 注释解析） |
| `pkg/session/session.go` | Protocol Session — 协议层会话管理 |
| `pkg/api/session_types.go` | API Session 类型定义 |
| `pkg/api/session_query.go` | API 层查询方法（Query, QueryAll, Explain） |
| `pkg/api/session_dml.go` | API 层 DML 方法（Execute） |
| `pkg/api/params.go` | 参数绑定实现 |
| `pkg/optimizer/optimized_executor.go` | OptimizedExecutor — 优化执行器 |
| `server/handler/query/query_handler.go` | COM_QUERY 处理器 |
| `server/handler/query/init_db_handler.go` | COM_INIT_DB 处理器 |
