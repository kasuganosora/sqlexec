---
name: test_coverage_enhancement
overview: 提高pkg包的测试覆盖率，特别是memory(39%)、virtual(36.4%)、security(68.7%)和session(43.9%)等低覆盖率模块，目标是将整体覆盖率提升至80%以上，为后续优化重构提供可靠基准。
todos:
  - id: test-index-helper
    content: 创建index_helper_test.go测试文件覆盖所有IndexHelper方法
    status: completed
  - id: test-evaluator-functions
    content: 补充evaluator_test.go中evaluateFunctionCall和parseArguments的测试用例
    status: completed
  - id: test-constraints
    content: 创建constraints_test.go测试约束验证逻辑
    status: completed
  - id: test-mvcc-features
    content: 补充mvcc_datasource_test.go的MVCC特性测试
    status: completed
  - id: test-audit-log
    content: 补充audit_log_test.go的审计日志功能测试
    status: completed
  - id: test-authorization
    content: 补充authorization_test.go的权限验证测试
    status: completed
  - id: test-virtual-datasource
    content: 创建datasource_test.go测试虚拟数据源
    status: completed
  - id: test-integration-phase2
    content: 补充integration/generated_columns_phase2_test.go的复杂场景
    status: completed
---

## 用户需求

继续提高测试覆盖率，为后续的优化和重构工作提供良好的基准。当前整体覆盖率约79.1%，部分模块低于40%，目标是达到80%以上。

## 核心功能

- 补充 pkg/resource/generated 模块的缺失测试（evaluateFunctionCall、parseArguments、index_helper）
- 提高 pkg/resource/memory 模块的覆盖率（constraints、mvcc_datasource）
- 补充 pkg/security 模块的测试（audit_log、authorization）
- 补充 pkg/virtual 模块的测试
- 集成测试覆盖（integration/generated_columns_phase2_test.go）

## 技术栈

- 编程语言: Go 1.x
- 测试框架: testing + testify/assert
- 覆盖率工具: go test -coverprofile

## 实现方案

### 策略概述

采用增量式测试补充策略，优先覆盖核心业务逻辑和已有测试的缺失部分。通过分析覆盖率文件，识别未覆盖的代码路径，针对性地编写测试用例。

### 关键决策

1. **优先级排序**：按影响范围和业务重要性排序（generated > memory > security > virtual）
2. **复用现有模式**：参考现有测试的结构和风格（evaluator_test.go、validator_test.go、utils_test.go）
3. **覆盖关键路径**：重点测试核心函数、错误处理分支、边界条件

### 性能与可靠性

- 测试执行时间控制在合理范围内（避免过度复杂的集成测试）
- 使用表格驱动测试覆盖多种场景
- 保证测试的独立性和可重复性

### 避免技术债务

- 保持测试代码简洁明了
- 避免过度模拟，使用真实数据
- 测试命名遵循清晰规范

## 实现注意事项

### 生成列模块 (pkg/resource/generated)

- evaluateFunctionCall: 测试内置函数调用、函数不存在、参数解析错误等场景
- parseArguments: 测试空参数、单参数、多参数、嵌套表达式等情况
- index_helper: 创建完整测试文件，覆盖CanIndexGeneratedColumn、GetIndexValueForGenerated等方法

### 内存数据源模块 (pkg/resource/memory)

- constraints: 测试主键约束、外键约束、唯一约束等验证逻辑
- mvcc_datasource: 补充事务隔离级别、快照隔离等MVCC特性的测试

### 安全模块 (pkg/security)

- audit_log: 测试日志记录、查询、过滤等功能
- authorization: 测试权限验证、角色检查等

### 虚拟数据源模块 (pkg/virtual)

- datasource: 测试虚拟表的连接、查询等基础功能

### 架构设计

采用分层测试结构：

```
单元测试 (*_test.go) → 模块测试 → 集成测试 (integration/)
```

### 目录结构

```
d:/code/db/
├── pkg/resource/generated/
│   ├── index_helper_test.go           # [NEW] IndexHelper完整测试
│   ├── evaluator_test.go              # [MODIFY] 补充evaluateFunctionCall和parseArguments测试
│   └── virtual_calculator_test.go     # [MODIFY] 补充边界情况测试
├── pkg/resource/memory/
│   ├── constraints_test.go            # [NEW] 约束验证测试
│   └── mvcc_datasource_test.go        # [MODIFY] 补充MVCC特性测试
├── pkg/security/
│   ├── audit_log_test.go              # [MODIFY] 补充审计日志功能测试
│   └── authorization_test.go          # [MODIFY] 补充权限验证测试
├── pkg/virtual/
│   └── datasource_test.go            # [NEW] 虚拟数据源测试
└── integration/
    └── generated_columns_phase2_test.go  # [MODIFY] 补充复杂场景测试
```

## 关键代码结构

### IndexHelper测试函数签名

```
func TestCanIndexGeneratedColumn(t *testing.T)
func TestGetIndexValueForGenerated(t *testing.T)
func TestValidateIndexDefinition(t *testing.T)
func TestGetIndexableGeneratedColumns(t *testing.T)
func TestIsIndexableExpression(t *testing.T)
```

### Constraints测试用例

```
func TestValidatePrimaryKey(t *testing.T)
func TestValidateForeignKey(t *testing.T)
func TestValidateUniqueConstraint(t *testing.T)
func TestCheckConstraint(t *testing.T)
```