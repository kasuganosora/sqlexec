# SQLExec 项目架构图

> 从 server 包开始的完整依赖关系图

## 项目目录结构

```
sqlexec/
├── cmd/                 # 命令行入口
├── server/               # 服务器核心模块 ⬅️ 入口点
│   ├── acl/             # 访问控制 (ACL)
│   ├── handler/          # 命令处理器
│   ├── protocol/         # 协议处理
│   ├── response/         # 响应构建器
│   └── testing/          # 服务器测试
├── pkg/                 # 核心包目录
│   ├── api/             # API 层
│   ├── builtin/          # 内置函数库
│   ├── config/           # 配置管理
│   ├── dataaccess/       # 数据访问层
│   ├── executor/         # 执行器
│   ├── extensibility/    # 扩展性支持
│   ├── information_schema/  # 信息模式
│   ├── json/            # JSON 支持
│   ├── monitor/          # 监控指标
│   ├── mvcc/            # MVCC 并发控制
│   ├── optimizer/        # 查询优化器 ⭐
│   ├── parser/           # SQL 解析器
│   ├── pool/            # 连接池
│   ├── reliability/      # 可靠性保障
│   ├── resource/         # 资源管理
│   ├── security/         # 安全模块
│   ├── session/          # 会话管理
│   ├── testutils/       # 测试工具
│   ├── types/           # 类型定义
│   ├── utils/           # 工具函数
│   └── virtual/         # 虚拟表
├── integration/          # 集成测试
└── docs/               # 文档目录
```

---

## 核心模块详解

### 1. Server 模块 (server/) - 入口点

**作用**: MySQL 协议服务器，处理客户端连接、认证和命令分发

**职责**:
- TCP 监听和连接管理
- MySQL 协议握手
- 命令路由和处理器调度
- 会话管理集成
- ACL 权限控制集成

---

#### server/acl/ - 访问控制
- `authenticator.go` - 身份认证器
- `manager.go` - ACL 管理器
- `permission_manager.go` - 权限管理器
- `user_manager.go` - 用户管理器
- `mysql_schema.go` - MySQL 系统表映射

**依赖**: `pkg/session`, `pkg/information_schema`

---

#### server/handler/ - 命令处理器

**核心接口**:
```go
type Handler interface {
    Handle(ctx *HandlerContext, packet interface{}) error
    Command() uint8
    Name() string
}
```

**子包**:
- `simple/` - 简单命令 (PING, QUIT, DEBUG, SET OPTION, REFRESH, STATISTICS, SHUTDOWN)
- `query/` - 查询命令 (SELECT, INSERT, UPDATE, DELETE, KILL QUERY)
- `process/` - 进程控制 (KILL)
- `packet_parsers/` - 数据包解析器
- `handshake/` - 握手流程

**依赖**: `pkg/optimizer`, `pkg/executor`, `pkg/resource/domain`, `pkg/session`, `server/protocol`

---

#### server/protocol/ - 协议层
- `type.go` - 协议类型定义
- `packet.go` - 数据包处理
- `charset.go` - 字符集支持
- `replication.go` - 复制协议
- `helper.go` - 协议辅助函数

**依赖**: 外部 MySQL 协议规范

---

#### server/response/ - 响应构建器
- `builder.go` - 通用响应构建器
- `error_builder.go` - 错误响应
- `ok_builder.go` - OK 响应
- `eof_builder.go` - EOF 响应

**依赖**: `server/protocol`

---

#### server/testing/ - 服务器测试
- `unit/` - handler, parser, packet_parsers 单元测试
- `mock/` - session, protocol, connection mock
- `resource/` - 资源操作测试
- `integration/` - 完整流程测试

**依赖**: 所有 server 子模块, pkg 包

---

### 2. Optimizer 模块 (pkg/optimizer/) ⭐

**作用**: SQL 查询优化器，负责生成最优执行计划

**职责**:
- SQL 语句解析和逻辑计划生成
- 代价估算和统计信息
- 物理计划生成和优化
- 索引选择和提示处理
- JOIN 重排序和谓词下推

---

#### optimizer/container/ - 依赖注入容器 ⭐ NEW
- `interfaces.go` - 容器接口定义
- `default_container.go` - 默认容器实现
- `builder.go` - 构建器模式

**提供的 Build 方法**:
- `BuildOptimizer()` - 构建基础优化器
- `BuildEnhancedOptimizer(parallelism)` - 构建增强优化器
- `BuildExecutor()` - 构建执行器
- `BuildOptimizedExecutor(useOptimizer)` - 构建优化执行器
- `BuildOptimizedExecutorWithDSManager()` - 带数据源管理器的执行器
- `BuildShowProcessor()` - SHOW 处理器
- `BuildVariableManager()` - 变量管理器
- `BuildExpressionEvaluator()` - 表达式求值器
- `GetCostModel()` - 获取成本模型
- `GetIndexSelector()` - 获取索引选择器
- `GetStatisticsCache()` - 获取统计缓存

**依赖**: `pkg/optimizer/cost`, `pkg/optimizer/index`, `pkg/optimizer/statistics`, `pkg/executor`, `pkg/builtin`, `pkg/dataaccess`, `pkg/optimizer`, `pkg/resource/application`, `pkg/resource/domain`

---

#### optimizer/cost/ - 成本模型
- `interfaces.go` - 成本模型接口
- `adaptive_model.go` - 自适应成本模型
- `hardware_profile.go` - 硬件配置

**接口**:
```go
type CostModel interface {
    ScanCost(tableName string, rowCount int64, useIndex bool) float64
    FilterCost(inputRows int64, selectivity float64, filters []interface{}) float64
    JoinCost(left, right interface{}, joinType JoinType, conditions []*parser.Expression) float64
    AggregateCost(inputRows int64, groupByCols, aggFuncs int) float64
    ProjectCost(inputRows int64, projCols int) float64
    SortCost(inputRows int64, sortCols int) float64
}
```

**依赖**: `pkg/parser`, `pkg/resource/domain`

---

#### optimizer/statistics/ - 统计信息
- `collector.go` - 统计信息收集器
- `cache.go` - 统计缓存
- `estimator.go` - 基数估算器
- `histogram.go` - 直方图实现
- `enhanced_cardinality.go` - 增强基数估算

**依赖**: `pkg/parser`, `pkg/resource/domain`, `pkg/optimizer/cost`

---

#### optimizer/index/ - 索引选择
- `interfaces.go` - 索引选择器接口
- `selector.go` - 索引选择器实现

**依赖**: `pkg/resource/domain`, `pkg/optimizer/statistics`

---

#### optimizer/join/ - JOIN 优化
- `dp_join_reorder.go` - 动态规划 JOIN 重排序
- `bushy_tree.go` - Bushy 树构建器
- `merge_join.go` - Merge Join 算法

**依赖**: `pkg/optimizer/cost`, `pkg/optimizer/statistics`, `pkg/parser`, `pkg/resource/domain`

---

#### optimizer/parallel/ - 并行执行
- `optimized_parallel.go` - 优化的并行执行

**依赖**: `pkg/resource/domain`, `pkg/optimizer/cost`

---

#### optimizer/genetic/ - 遗传算法 ⭐ NEW
- `core.go` - 遗传算法核心
- `config.go` - 算法配置
- `operators.go` - 选择、交叉、变异算子
- `types.go` - 个体、种群类型

**测试**: `genetic_test.go` (11个测试，全部通过) ✅

**依赖**: `pkg/parser`, `pkg/resource/domain`

---

#### optimizer/physical/ - 物理算子 ⭐ NEW
- `interfaces.go` - 物理算子接口
- `table_scan.go` - 表扫描算子
- `selection.go` - 过滤算子
- `projection.go` - 投影算子
- `join.go` - JOIN 算子 (PhysicalHashJoin)
- `aggregate.go` - 聚合算子 (PhysicalHashAggregate)
- `limit.go` - 限制算子

**测试**: `physical_test.go` (10个测试，全部通过) ✅

**依赖**: `pkg/parser`, `pkg/resource/domain`, `pkg/optimizer`

---

#### 核心优化器文件

**逻辑算子**:
- `logical_scan.go`, `logical_selection.go`, `logical_projection.go`
- `logical_join.go`, `logical_aggregate.go`, `logical_sort.go`
- `logical_limit.go`, `logical_delete.go`, `logical_insert.go`
- `logical_window.go`, `logical_datasource.go`, `logical_apply.go`
- `logical_topn.go`, `logical_union.go`

**优化规则**:
- `rules.go`, `join_elimination.go`, `semi_join_rewrite.go`
- `subquery_flattening.go`, `subquery_materialization.go`, `or_to_union.go`
- `maxmin_elimination.go`, `decorrelate.go`

**提示处理**:
- `hint_index.go`, `hint_join.go`, `hint_agg.go`
- `hint_orderby.go`, `hint_subquery.go`

**特殊功能**:
- `index_advisor.go` - 索引建议器
- `hypothetical_stats.go` - 假设统计
- `hypothetical_index_store.go` - 假设索引存储
- `fulltext_index_support.go` - 全文索引支持
- `spatial_index_support.go` - 空间索引支持
- `write_trigger.go` - 写触发器
- `expression_evaluator.go` - 表达式求值
- `variable_manager.go` - 变量管理器
- `show_processor.go` - SHOW 处理器
- `system_views.go` - 系统视图
- `view_executor.go` - 视图执行器
- `view_rewrite.go` - 视图重写
- `procedure_executor.go` - 存储过程执行器

**依赖**: `pkg/executor`, `pkg/parser`, `pkg/resource/domain`, `pkg/dataaccess`, `pkg/builtin`, `pkg/information_schema`, `pkg/session`, `pkg/utils`, `pkg/monitor`

---

### 3. Executor 模块 (pkg/executor/)

**作用**: 查询执行器，负责执行优化器生成的执行计划

**核心接口**:
```go
type Executor interface {
    Execute(ctx context.Context, plan *plan.Plan) (*domain.QueryResult, error)
}
```

**文件**:
- `executor.go` - 执行器主文件
- `runtime.go` - 执行运行时
- `operators/` - 物理操作符实现

**operators/ 子目录**:
- `scan.go` - 扫描操作符
- `filter.go` - 过滤操作符
- `project.go` - 投影操作符
- `join.go` - JOIN 操作符
- `aggregate.go` - 聚合操作符
- `sort.go` - 排序操作符
- `limit.go` - 限制操作符
- `union.go` - UNION 操作符

**依赖**: `pkg/dataaccess`, `pkg/optimizer/plan`, `pkg/resource/domain`

---

### 4. Parser 模块 (pkg/parser/)

**作用**: SQL 解析器，封装 TiDB Parser

**核心接口**:
```go
type Parser struct {
    parser *parser.Parser
}

func (p *Parser) ParseSQL(sql string) ([]ast.StmtNode, error)
func (p *Parser) ParseOneStmt(sql string) (ast.StmtNode, error)
```

**依赖**:
- `github.com/pingcap/tidb/pkg/parser` (外部依赖)
- `github.com/pingcap/tidb/pkg/parser/ast` (外部依赖)

---

### 5. Session 模块 (pkg/session/)

**作用**: 会话管理，负责客户端会话生命周期

**核心类型**:
```go
type SessionMgr struct {
    driver SessionDriver
}

type Session struct {
    ID          string
    ConnectionID uint32
    Database     string
    User        string
}
```

**依赖**: `pkg/config`

---

### 6. Resource 模块 (pkg/resource/)

**作用**: 资源管理和数据源抽象

---

#### resource/domain/ - 领域模型
- `datasource.go` - 数据源接口定义
- `table.go` - 表模型
- `column.go` - 列模型
- `index.go` - 索引模型
- `filter.go` - 过滤条件模型
- `result.go` - 查询结果模型
- `view.go` - 视图模型
- `mvcc.go` - MVCC 模型

**核心接口**:
```go
type DataSource interface {
    Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)
    Insert(ctx context.Context, tableName string, rows []Row) error
    Update(ctx context.Context, tableName string, set Map, filters []Filter) error
    Delete(ctx context.Context, tableName string, filters []Filter) error
}
```

**依赖**: `pkg/parser`

---

#### resource/memory/ - 内存数据源
- `mvcc_datasource.go` - MVCC 数据源
- `table.go` - 内存表实现
- `index.go` - 索引实现
- `transaction.go` - 事务管理
- `lock_manager.go` - 锁管理器
- `sequence.go` - 序列管理器
- `view.go` - 视图实现

**依赖**: `pkg/resource/domain`, `pkg/parser`

---

#### resource/csv/ - CSV 数据源
- `datasource.go` - CSV 数据源

#### resource/parquet/ - Parquet 数据源
- `datasource.go` - Parquet 数据源

#### resource/application/ - 应用层资源
- `datasource_manager.go` - 数据源管理器

---

### 7. Security 模块 (pkg/security/)

**作用**: 安全相关功能

**文件**:
- `sqlescape.go` - SQL 转义
- `encryption.go` - 加密支持
- `authorization.go` - 授权检查
- `audit_log.go` - 审计日志

**依赖**: `pkg/parser`, `pkg/session`

---

### 8. Monitor 模块 (pkg/monitor/)

**作用**: 监控和性能跟踪

**文件**:
- `metrics.go` - 指标收集
- `slow_query.go` - 慢查询日志
- `cache.go` - 缓存统计

**依赖**: `pkg/executor`

---

### 9. MVCC 模块 (pkg/mvcc/)

**作用**: 多版本并发控制支持

**文件**:
- `manager.go` - MVCC 管理器
- `transaction.go` - 事务管理
- `datasource.go` - MVCC 数据源
- `clog.go` - 提交日志

**依赖**: `pkg/resource/domain`

---

### 10. 其他核心模块

#### pkg/api/ - API 层
- 数据库 API (DB)
- 表操作 API

#### pkg/builtin/ - 内置函数
- 函数注册和实现
- `function_api.go` - 函数 API
- `registry.go` - 函数注册表

#### pkg/config/ - 配置管理
- 配置结构
- 默认值

#### pkg/dataaccess/ - 数据访问层
- 数据访问服务接口
- `service.go` - 数据访问服务

#### pkg/information_schema/ - 信息模式
- `tables.go` - 系统表实现
- `columns.go` - 列信息
- `views.go` - 视图信息

#### pkg/types/ - 类型定义
- 列类型
- 表类型
- 常量定义

#### pkg/utils/ - 工具函数
- 字符串工具
- 类型转换
- 验证函数

#### pkg/virtual/ - 虚拟表
- 序列列表虚拟表
- 虚拟表注册

---

## 依赖关系图

```
                    ┌─────────────────────────────────────────┐
                    │             server/               │
                    │  (MySQL 服务器入口)              │
                    └──────────────────┬────────────────┘
                                    │
                   ┌────────────────┼────────────────┐
                   │                │                │
              ┌────┴─────┐    │         ┌────┴─────┐
              │  handler/  │    │         │  protocol/ │
              │            │    │         │            │
         ┌────┴────┬─────┴────┴──────┬────┐
         │         │                       │         │
    ┌───┴───┐  │                       │    ┌───┴───┐
    │   acl/ │  │                       │    │ session/ │
    │        │  │                       │    │         │
    └───┬───┘  │                       │    └─────────┘
       │        │                       │
       │        │                       │
    ┌───┴───┐  │                 ┌───┴─────────┐
    │  config │  │                 │ information_schema/
    │         │  │                 │
    └─────────┘  │                 └──────────────┘
                    │
       ┌────────────┴────────────┬────────────────────┐
       │                         │                    │
  ┌────┴────┐          ┌─────┴─────┐    ┌─────┴─────┐
  │ optimizer/ │          │  executor/ │    │  dataaccess/│
  │           │          │            │    │             │
  └────┬──────┘          └─────┬──────┘    └──────┬──────┘
       │                        │                  │
       │                        │                  │
  ┌────┴────────┐    ┌──────┴──────┐    │
  │ resource/  │    │ parser/      │    │
  │            │    │              │    │
  └────┬───────┘    └──────┬───────┘    │
       │                       │             │
       │                       │             │
  ┌────┴──────┐          ┌──────┴────────┴───┐
  │  builtin/ │          │ types/              │
  │           │          │                     │
  └───────────┘          └─────────────────────┘
```

---

## 数据流向

### 查询执行流程

```
客户端 (MySQL Client)
    │
    ↓ TCP 连接
    │
┌─── server ─────────────────────────────┐
│    1. protocol: MySQL 握手            │
│    2. acl: 认证授权                │
│    3. handler: 命令分发              │
└─────────────────┬────────────────────┘
                │
    ↓ SQL 查询
┌─── optimizer ─────────────────────────────┐
│    4. parser: 解析 SQL              │
│    5. container: DI 注入组件          │
│    6. cost: 代价估算               │
│    7. statistics: 统计信息            │
│    8. index: 索引选择               │
│    9. join: JOIN 重排序            │
│    10. physical: 物理计划生成       │
└─────────────────┬────────────────────┘
                │
    ↓ 执行计划
┌─── executor ─────────────────────────────┐
│    11. operators: 执行物理算子      │
│    12. dataaccess: 访问数据源      │
└─────────────────┬────────────────────┘
                │
                ↓
┌─── resource ─────────────────────────────┐
│    13. memory/mvcc: 数据访问      │
│    14. domain: 结果封装           │
└─────────────────┬────────────────────┘
                │
                ↓ 查询结果
┌─── response ─────────────────────────────┐
│    15. 构建响应包               │
└─────────────────┬────────────────────┘
                │
                ↓ TCP 发送
            客户端
```

---

## 关键设计模式

### 1. 依赖注入 (DI) - optimizer/container/
- Container 接口 + Builder 模式
- 统一管理优化器依赖
- 支持组件替换和测试

### 2. 策略模式 - optimizer/ 多处
- CostModel 策略接口
- CardinalityEstimator 策略接口
- SelectionOperator 策略接口

### 3. 工厂模式 - server/handler/
- HandlerRegistry 注册机制
- 动态命令处理器创建

### 4. 模板方法模式 - executor/operators/
- Operator 接口
- 统一的 Execute 方法

---

## 扩展点

### 添加新的数据源类型
1. 实现 `resource/domain.DataSource` 接口
2. 在 `resource/application/datasource_manager.go` 注册
3. 实现 `resource/registry.go` 的注册逻辑

### 添加新的物理算子
1. 实现 `optimizer/physical/interfaces.go` 的 `PhysicalOperator` 接口
2. 在 `executor/operators/` 实现执行逻辑

### 添加新的优化规则
1. 在 `optimizer/rules.go` 添加规则
2. 在 `enhanced_optimizer.go` 的 ApplyRules 中调用

### 添加新的内置函数
1. 在 `pkg/builtin/registry.go` 注册
2. 实现 `Function` 接口

---

## 测试覆盖

### 单元测试
- server/acl: 5+ 测试
- server/handler: 10+ 测试
- server/protocol: 5+ 测试
- server/testing: 20+ 测试
- optimizer/cost: 5+ 测试
- optimizer/statistics: 5+ 测试
- optimizer/genetic: 11 测试 ✅ NEW
- optimizer/physical: 10 测试 ✅ NEW
- executor: 5+ 测试
- resource/memory: 5+ 测试
- security: 4+ 测试
- monitor: 2+ 测试
- mvcc: 2+ 测试

### 集成测试
- server/testing/integration/ - 完整流程测试
- integration/ - 跨模块集成测试

---

## 性能优化点

### 已实现的优化 ✅
1. 物理算子独立 (physical/)
2. 依赖注入支持 (container/builder.go)
3. 成本模型抽象 (cost/)
4. 统计信息缓存 (statistics/)
5. 并行扫描支持 (optimizer/parallel/)
6. JOIN 重排序 (optimizer/join/)
7. 谓词下推 (optimizer/enhanced_predicate_pushdown.go)
8. 列裁剪 (optimizer/enhanced_column_pruning.go)
9. 索引建议 (optimizer/index_advisor.go)
10. 提示处理 (optimizer/hint_*.go)

---

## 最近重要变更

### 2026-02-07: 重构清理
- ✅ 删除向后兼容代码
- ✅ 完善 Container Build 方法
- ✅ 添加 genetic 和 physical 单元测试
- ✅ 统一接口使用
- ✅ 更新成本模型接口

---

## 总结

SQLExec 项目采用了清晰的分层架构：

```
server (协议层)
    ↓
optimizer (优化层)
    ↓
executor (执行层)
    ↓
resource (数据层)
```

每一层都有明确的职责和接口定义，通过依赖注入实现松耦合。项目支持多种数据源（Memory、CSV、Parquet等），具备完整的 MySQL 协议兼容性，并提供了丰富的查询优化和执行能力。

关键特点：
- 🎯 **依赖注入**: optimizer/container/ 实现灵活的组件管理
- 📊 **优化器**: 完整的代价模型和统计信息
- ⚡ **执行器**: 物理算子并行执行
- 🔐 **安全性**: ACL 权限控制和加密支持
- 📈 **监控**: 性能指标和慢查询跟踪
- 🔧 **可扩展**: 基于接口的设计，易于添加新功能
