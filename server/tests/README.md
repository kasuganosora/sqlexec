# Server Tests 测试组织说明

## 当前测试文件

### 1. protocol_test.go - 协议层测试
测试MySQL协议包的正确处理。
- `TestProtocol_Connection` - 连接和认证
- `TestProtocol_SimpleQuery` - 简单SELECT查询
- `TestProtocol_ErrorPacket` - 错误包序列化
- `TestProtocol_EmptyQuery` - 空查询处理
- `TestProtocol_PingKeepAlive` - Ping保活
- `TestProtocol_ErrorReturn` - 错误返回格式验证

### 2. end_to_end_test.go - 端到端测试
使用标准MySQL客户端测试完整查询流程。
- `TestE2E_COM_INIT_DB` - USE命令测试
- `TestE2E_SelectDatabase` - SELECT DATABASE()函数测试
- `TestE2E_InformationSchemaQuery` - information_schema查询测试
- `TestE2E_ErrorPacket` - 错误包端到端测试
- `TestE2E_MultipleDBSwitching` - 多次数据库切换测试
- `TestE2E_ConnectionRecovery` - 连接恢复测试
- `TestE2E_InformationSchemaWithRealData` - information_schema实际数据测试
- `TestE2E_DatabaseContextCache` - 数据库上下文缓存测试
- `TestE2E_ShowDatabases` - SHOW DATABASES命令测试

### 3. scenarios_test.go - 场景测试
测试各种实际使用场景。
- `TestScenario_DatabaseSwitching` - 多次数据库切换场景
- `TestScenario_ErrorAfterInvalidQuery` - 错误后连接恢复场景
- `TestScenario_InvalidSQLSyntax` - 无效SQL语法场景
- `TestScenario_PingKeepAlive` - Ping保活场景
- `TestScenario_EmptyQuery` - 空查询场景
- `TestScenario_ConcurrentQueries` - 并发查询场景
- `TestScenario_ConnectionPool` - 连接池场景

### 4. generated_columns_phase2_test.go - 生成列测试
测试VIRTUAL和STORED生成列功能。
- `TestGeneratedColumnsPhase2V1` - VIRTUAL列基础测试
- `TestGeneratedColumnsPhase2V2` - STORED和VIRTUAL混合测试
- `TestGeneratedColumnsPhase2V3` - VIRTUAL列NULL传播测试
- `TestGeneratedColumnsPhase2V4` - 复杂表达式测试
- `TestGeneratedColumnsPhase2V5` - UPDATE操作级联更新测试
- `TestGeneratedColumnsPhase2V6` - SQL解析VIRTUAL列语法测试
- `TestGeneratedColumnsPhase2V7` - 性能测试
- `TestGeneratedColumnsPhase2V8` - 错误处理测试
- `TestGeneratedColumnsPhase2V9` - 混合STORED和VIRTUAL列的多级依赖测试
- `TestGeneratedColumnsPhase2V10` - VIRTUAL列在WHERE条件中测试
- `TestGeneratedColumnsPhase2V11` - VIRTUAL列与ORDER BY测试
- `TestGeneratedColumnsPhase2V12` - 复杂数学表达式测试

### 5. table_operations_test.go - 表操作测试
使用底层API测试数据存储和查询。
- `TestTableOperations` - 表操作（CREATE/DROP/TRUNCATE）
- `TestMultipleTables` - 多表操作测试

### 6. temporary_table_test.go - 临时表测试
测试临时表功能。
- `TestTemporaryTables` - 临时表完整功能测试

## 测试分类

### 协议层测试（protocol_test.go）
关注MySQL协议包的正确序列化和反序列化，不涉及业务逻辑。
- 连接和认证
- 错误包格式
- 空查询处理
- Ping保活

### 端到端测试（end_to_end_test.go）
使用标准MySQL客户端，测试完整的查询生命周期，验证系统整体功能。
- USE命令
- SELECT DATABASE()
- SHOW DATABASES
- information_schema查询
- 数据库切换
- 连接恢复

### 场景测试（scenarios_test.go）
模拟实际使用场景，测试复杂交互和边界条件。
- 错误处理
- 边界条件
- 并发查询
- 连接池
- 多次数据库切换

### 功能特性测试（generated_columns_phase2_test.go）
测试特定功能特性。
- VIRTUAL生成列
- STORED生成列
- 表达式计算
- NULL传播
- 性能测试

### 数据源测试（table_operations_test.go, temporary_table_test.go）
使用底层API直接测试数据存储和查询逻辑。
- 表创建/删除/清空
- 数据插入/查询/更新
- 临时表操作

## 端口分配

为了避免端口冲突，每个测试文件使用不同的端口范围：

- protocol_test.go: 13300-13309
- end_to_end_test.go: 13310-13319
- scenarios_test.go: 13320-13329
- generated_columns_phase2_test.go: 不需要端口（使用底层API）
- table_operations_test.go: 不需要端口（使用底层API）
- temporary_table_test.go: 不需要端口（使用底层API）

## 运行测试

```bash
# 运行所有测试
go test ./server/tests/...

# 运行特定类型的测试
go test ./server/tests/... -run TestProtocol
go test ./server/tests/... -run TestE2E
go test ./server/tests/... -run TestScenario
go test ./server/tests/... -run TestGeneratedColumns
go test ./server/tests/... -run TestTableOperations
go test ./server/tests/... -run TestTemporaryTables

# 运行特定测试
go test ./server/tests/... -run TestProtocol_Connection
go test ./server/tests/... -run TestE2E_COM_INIT_DB

# 运行并显示覆盖率
go test ./server/tests/... -cover

# 运行详细输出
go test ./server/tests/... -v
```

## 测试命名规范

- `TestProtocol_Xxx` - 协议层测试
- `TestE2E_Xxx` - 端到端测试
- `TestScenario_Xxx` - 场景测试
- `TestGeneratedColumnsPhase2Vx` - 生成列功能测试
- `TestTableOperations` / `TestMultipleTables` - 表操作测试
- `TestTemporaryTables` - 临时表测试

## 测试覆盖范围

当前测试覆盖了以下功能：

### 协议层
- ✅ 连接和认证
- ✅ 错误包序列化
- ✅ 空查询处理
- ✅ Ping保活
- ✅ 错误代码和SQLState

### 端到端
- ✅ USE命令
- ✅ SELECT DATABASE()
- ✅ SHOW DATABASES
- ✅ information_schema查询
- ✅ 数据库切换
- ✅ 连接恢复
- ✅ 数据库上下文缓存

### 场景
- ✅ 错误处理
- ✅ 边界条件
- ✅ 并发查询
- ✅ 连接池
- ✅ 多次数据库切换

### 功能特性
- ✅ VIRTUAL生成列
- ✅ STORED生成列
- ✅ 表达式计算
- ✅ NULL传播
- ✅ 性能测试
- ✅ 错误处理

### 数据源
- ✅ 表创建/删除/清空
- ✅ 数据插入/查询/更新
- ✅ 临时表操作
- ✅ 多表操作

## 重构建议

当前测试文件组织已经比较清晰，可以考虑以下优化：

1. **分离场景测试**
   - 将错误相关测试合并到协议层测试
   - 将并发测试独立为 concurrency_test.go

2. **添加性能测试文件**
   - 创建 performance_test.go 用于性能基准测试

3. **添加集成测试文件**
   - 对于复杂的端到端场景，可以创建 integration_test.go

4. **改进测试数据准备**
   - 添加测试数据准备的辅助函数
   - 统一测试表结构定义
