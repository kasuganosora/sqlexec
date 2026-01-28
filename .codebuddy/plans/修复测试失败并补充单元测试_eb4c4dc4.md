---
name: 修复测试失败并补充单元测试
overview: 修复monitor和security模块的测试失败问题，并为information_schema、mvcc、reliability、virtual模块编写贴近日常案例的单元测试，目标覆盖率80%以上。
todos:
  - id: fix-monitor-time-query
    content: 修复monitor模块TestGetSlowQueriesByTimeRange和TestGetSlowQueriesAfter时间边界问题
    status: completed
  - id: fix-security-injection
    content: 优化security模块SQL注入检测逻辑，避免对合法INSERT/UPDATE/OR语句误判
    status: completed
  - id: write-information-schema-tests
    content: 编写information_schema模块6个文件的单元测试（provider/schemata/tables/columns/constraints/keys）
    status: completed
    dependencies:
      - fix-monitor-time-query
      - fix-security-injection
  - id: write-mvcc-tests
    content: 编写mvcc模块5个文件的单元测试（manager/clog/datasource/transaction/types）
    status: completed
    dependencies:
      - write-information-schema-tests
  - id: write-reliability-tests
    content: 编写reliability模块3个文件的单元测试（backup/error_recovery/failover）
    status: completed
    dependencies:
      - write-mvcc-tests
  - id: write-virtual-tests
    content: 编写virtual模块2个文件的单元测试（datasource/table）
    status: completed
    dependencies:
      - write-reliability-tests
  - id: verify-coverage
    content: 运行所有测试并验证覆盖率达到80%以上
    status: completed
    dependencies:
      - write-virtual-tests
---

## 用户需求

修复当前测试失败的问题，并为未编写的测试模块进行单元测试编写，要求贴近日常案例，覆盖率达到80%以上。

## 核心问题

1. **monitor模块测试失败**：

- TestGetSlowQueriesByTimeRange：时间范围查询返回结果不符合预期
- TestGetSlowQueriesAfter：获取指定时间后的查询返回结果不正确

2. **security模块SQL注入误判**：

- 正常INSERT/UPDATE/OR查询被误判为注入攻击
- 例如 `INSERT INTO users (name) VALUES ('John')` 被标记为注入

3. **未测试模块（0%覆盖率）**：

- information_schema（6个文件）：provider.go, columns.go, constraints.go, keys.go, schemata.go, tables.go
- mvcc（5个文件）：manager.go, clog.go, datasource.go, transaction.go, types.go
- reliability（3个文件）：backup.go, error_recovery.go, failover.go
- virtual（2个文件）：datasource.go, table.go

## 技术栈

- 测试框架：Go标准库 `testing`
- 代码语言：Go 1.x
- 测试覆盖工具：`go test -cover`

## 修复方案

### 1. Monitor模块时间范围查询修复

**问题根源**：

- `GetSlowQueriesByTimeRange` 使用 `After(start) && Before(end)` 不包含边界
- 测试中动态使用 `time.Now()` 导致时间窗口不稳定

**修复方法**：

- 修改时间比较逻辑为 `!Before(start) && !After(end)` 包含边界
- 测试中固定时间点或使用 `time.Sleep` 确保时间窗口稳定

### 2. Security模块SQL注入检测优化

**问题根源**：

- 正则模式过于简单，缺少上下文判断
- 单引号、OR/AND 关键词在合法SQL中也会匹配

**修复方法**：

- 添加"上下文感知"的检测逻辑
- 区分SQL关键字出现在语句开头 vs 条件表达式中
- 对参数化查询（占位符）不误报
- 优化正则模式，避免匹配引号内的合法值

## 测试实现方案

### 测试编写原则

- 贴近日常使用场景（实际业务查询、真实备份恢复、事务并发）
- 每个测试覆盖正常、边界、错误场景
- 目标覆盖率80%以上
- 测试可独立运行，无依赖顺序

### information_schema模块测试（6个文件）

**provider_test.go**：

- Provider初始化和表注册
- GetVirtualTable/ListVirtualTables/HasTable
- TableNotFoundError处理
- 多数据源场景测试

**schemata_test.go**：

- 查询所有schema
- 过滤条件测试（=, !=, like）
- limit/offset分页
- 空数据源场景

**tables_test.go**：

- 查询所有表和表信息
- 按table_schema过滤
- LIKE模糊查询
- 表类型和引擎信息验证

**columns_test.go**：

- 列信息查询
- 列类型解析（VARCHAR/INT/TEXT等）
- 字符长度和数值精度提取
- 主键/唯一键列标记

**constraints_test.go**：

- PRIMARY KEY约束查询
- UNIQUE约束查询
- 约束名称生成
- 约束过滤和分页

**keys_test.go**：

- 主键列查询
- 唯一键列查询
- 外键列查询
- ordinal_position正确性

### mvcc模块测试（5个文件）

**types_test.go**：

- XID生成和比较（含环绕）
- TransactionStatus转换
- IsolationLevel解析
- Snapshot可见性规则（xmin/xmax/xip）
- TupleVersion可见性判断（核心MVCC逻辑）
- DataSourceFeatures能力检测

**clog_test.go**：

- CommitLog状态设置和查询
- IsCommitted/IsAborted/IsInProgress
- oldest XID维护
- GC清理过期条目
- SLRU缓存淘汰

**transaction_test.go**：

- Transaction属性访问（XID/Status/Level等）
- WriteCommand/DeleteCommand/UpdateCommand Apply/Rollback
- 命令幂等性（applied标志）

**datasource_test.go**：

- DataSourceRegistry注册和查询
- MemoryDataSource MVCC读写（ReadWithMVCC/WriteWithMVCC）
- NonMVCCDataSource降级场景
- DowngradeHandler检查逻辑
- 事务并发读写隔离

**manager_test.go**：

- Manager初始化和全局单例
- Begin/Commit/Rollback事务
- 快照管理（xmin/xmax/xip正确性）
- XID生成和环绕处理
- 非MVCC降级
- GC清理（快照和clog）
- 统计信息获取

### reliability模块测试（3个文件）

**backup_test.go**：

- 备份全量/增量/差异数据
- 恢复数据和校验和验证
- GetBackup/ListBackups/DeleteBackup
- CleanOldBackups保留策略
- 压缩/解压功能
- 元数据导入导出

**error_recovery_test.go**：

- ExecuteWithRetry重试逻辑
- backoff指数退避
- ExecuteWithFallback备用方案
- ErrorRecoveryManager错误日志
- CircuitBreaker断路器状态转换（Closed→Open→HalfOpen）
- GetErrorStats统计

**failover_test.go**：

- AddNode/RemoveNode节点管理
- GetActiveNode活跃节点选择
- health check故障检测
- ManualFailover手动切换
- LoadBalancer轮询/LeastLoadedNode
- ExecuteWithRetryAndFailover端到端场景
- 节点负载状态调整

### virtual模块测试（2个文件）

**table_test.go**：

- VirtualTable接口mock实现
- GetName/GetSchema/Query方法
- 过滤器和选项应用
- 空结果和错误处理

**datasource_test.go**：

- VirtualDataSource初始化
- Connect/Close/IsConnected行为
- GetTables/GetTableInfo/Query委托
- 只读操作验证（Insert/Update/Delete返回错误）
- CreateTable/DropTable/TruncateTable/Execute错误返回

## 目录结构

```
d:/code/db/pkg/
├── information_schema/
│   ├── provider_test.go        # [NEW] Provider管理和表注册测试
│   ├── schemata_test.go       # [NEW] Schema查询测试
│   ├── tables_test.go         # [NEW] 表信息查询测试
│   ├── columns_test.go        # [NEW] 列信息查询测试
│   ├── constraints_test.go     # [NEW] 约束查询测试
│   └── keys_test.go          # [NEW] 键列查询测试
├── mvcc/
│   ├── manager_test.go        # [NEW] 事务管理器测试
│   ├── clog_test.go          # [NEW] 提交日志测试
│   ├── datasource_test.go    # [NEW] 数据源和降级测试
│   ├── transaction_test.go    # [NEW] 事务和命令测试
│   └── types_test.go         # [NEW] 类型定义和可见性测试
├── reliability/
│   ├── backup_test.go        # [NEW] 备份恢复测试
│   ├── error_recovery_test.go # [NEW] 错误恢复和断路器测试
│   └── failover_test.go      # [NEW] 故障转移和负载均衡测试
├── virtual/
│   ├── datasource_test.go    # [NEW] 虚拟数据源测试
│   └── table_test.go         # [NEW] 虚拟表测试
├── monitor/
│   └── slow_query_test.go    # [MODIFY] 修复时间范围测试
└── security/
    └── sql_injection_test.go # [MODIFY] 优化注入检测测试
```