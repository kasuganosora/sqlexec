---
name: fix-bare-select-expression-evaluation
overview: 修复裸 SELECT 表达式求值问题，支持 SELECT 1+1、SELECT NOW() 等无 FROM 子句的查询
todos:
  - id: extend-optimized-executor
    content: 修改 OptimizedExecutor 结构体，添加 functionAPI 和 exprEvaluator 字段
    status: pending
  - id: update-constructors
    content: 修改构造函数，初始化 ExpressionEvaluator 和 FunctionAPI
    status: pending
    dependencies:
      - extend-optimized-executor
  - id: rewrite-handle-no-from
    content: 重写 handleNoFromQuery 函数，使用表达式求值器处理所有表达式类型
    status: pending
    dependencies:
      - update-constructors
  - id: test-naked-select
    content: 编写测试验证裸 SELECT 查询功能（算术运算、函数调用、组合表达式）
    status: pending
    dependencies:
      - rewrite-handle-no-from
  - id: regression-test
    content: 运行回归测试确保现有功能（SELECT DATABASE() 等）未被破坏
    status: pending
    dependencies:
      - test-naked-select
---

## 用户需求

用户反馈裸 SELECT 查询不支持，例如：

- 算术运算：`SELECT 1+1`, `SELECT 2*3`, `SELECT 10/2`
- 函数调用：`SELECT NOW()`, `SELECT RAND()`, `SELECT CURDATE()`, `SELECT CURTIME()`
- 组合表达式：`SELECT NOW()+1`, `SELECT 1+2*3`
- 常量：`SELECT 1`（已支持）

## 产品概述

扩展 MySQL 兼容的 SQL 执行引擎，支持无 FROM 子句的表达式查询（裸 SELECT），使其能够正确处理算术运算、内置函数调用及复杂表达式组合。

## 核心功能

- 支持算术运算符：`+`, `-`, `*`, `/`
- 支持内置函数调用：NOW(), RAND(), CURDATE(), CURTIME() 等
- 支持复杂表达式组合：函数与运算符混合使用
- 保持向后兼容：不破坏现有功能（SELECT DATABASE()、SELECT 1等）

## 技术栈

- 语言：Go 1.21+
- 解析器：github.com/pingcap/tidb/pkg/parser
- 表达式求值器：pkg/optimizer/expression_evaluator.go（已存在）
- 内置函数系统：pkg/builtin/（已存在）

## 实现方案

### 方案概述

在 `OptimizedExecutor` 中集成现有的 `ExpressionEvaluator` 和 `FunctionAPI`，让 `handleNoFromQuery` 函数使用表达式求值器来处理所有类型的表达式，而不是硬编码特定情况。

### 技术决策

1. **复用现有组件**：使用已存在的 `ExpressionEvaluator` 和 `FunctionAPI`，避免重复实现
2. **最小侵入性**：只修改 `OptimizedExecutor` 的初始化和 `handleNoFromQuery` 函数
3. **函数 API 初始化**：通过 `builtin.NewFunctionAPI()` 获取内置函数支持
4. **空 Row 上下文**：裸 SELECT 没有表数据，传入空 map 作为 Row

### 实现细节

#### 1. 扩展 OptimizedExecutor 结构

在 `pkg/optimizer/optimized_executor.go` 的 `OptimizedExecutor` 结构中添加字段：

- `functionAPI *builtin.FunctionAPI` - 函数 API（用于调用内置函数）
- `exprEvaluator *ExpressionEvaluator` - 表达式求值器

#### 2. 修改构造函数

修改两个构造函数：

- `NewOptimizedExecutor()` - 初始化 functionAPI 和 exprEvaluator
- `NewOptimizedExecutorWithDSManager()` - 初始化 functionAPI 和 exprEvaluator

#### 3. 重写 handleNoFromQuery 函数

完全重写 `handleNoFromQuery` 函数，使用 `ExpressionEvaluator.Evaluate()` 方法统一处理所有表达式类型：

- 创建空 Row：`row := make(parser.Row)`
- 调用求值器：`value, err := e.exprEvaluator.Evaluate(col.Expr, row)`
- 处理多列情况：遍历所有列进行求值
- 推断类型：根据返回值类型设置列类型（string/int/float）
- 生成列名：使用别名或表达式字符串作为列名

#### 4. 错误处理

- 如果 `exprEvaluator` 未初始化，返回友好的错误提示
- 如果表达式求值失败，返回具体的错误信息
- 保持错误日志记录用于调试

### 性能与可靠性

- **性能影响**：初始化 FunctionAPI 只在构造时执行一次，表达式求值器已有优化实现
- **内存开销**：新增两个指针字段（16字节），可忽略不计
- **错误处理**：完整的错误传播和日志记录，便于排查问题

## 架构设计

```mermaid
graph TB
    A[Client: SELECT NOW+1] --> B[TiDB Parser]
    B --> C[SQLAdapter]
    C --> D[SelectStatement<br/>Columns: [NOW+1]]
    D --> E[OptimizedExecutor.ExecuteSelect]
    E --> F[检查 From == 空字符串]
    F --> G[handleNoFromQuery]
    G --> H[创建空 Row]
    H --> I[ExpressionEvaluator.Evaluate]
    I --> J[检查表达式类型]
    J --> K[ExprTypeOperator]
    J --> L[ExprTypeFunction]
    J --> M[ExprTypeValue]
    K --> N[evaluateOperator]
    N --> O[递归求值左右操作数]
    L --> P[evaluateFunction]
    P --> Q[FunctionAPI.GetFunction]
    Q --> R[调用内置函数处理器]
    R --> S[返回函数结果]
    O --> T[返回运算结果]
    M --> U[返回常量值]
    T --> V[生成 QueryResult]
    S --> V
    U --> V
    V --> W[返回给客户端]
```

## 目录结构

```
pkg/optimizer/
└── optimized_executor.go  # [MODIFY] 扩展 OptimizedExecutor 结构，重写 handleNoFromQuery 函数
```

### 文件修改详情

**pkg/optimizer/optimized_executor.go**

- **目的**：优化执行器，支持裸 SELECT 表达式求值
- **修改内容**：

1. 在 `OptimizedExecutor` 结构体中添加 `functionAPI *builtin.FunctionAPI` 和 `exprEvaluator *ExpressionEvaluator` 字段
2. 修改 `NewOptimizedExecutor()` 构造函数，初始化新增字段
3. 修改 `NewOptimizedExecutorWithDSManager()` 构造函数，初始化新增字段
4. 完全重写 `handleNoFromQuery()` 函数（约77行），使用表达式求值器处理所有表达式类型

- **实现要求**：
- 导入 `github.com/kasuganosora/sqlexec/pkg/builtin` 包
- 使用 `builtin.NewFunctionAPI()` 初始化函数 API
- 使用 `NewExpressionEvaluator(functionAPI)` 初始化求值器
- 处理多列情况（支持 `SELECT 1+1, 2+2`）
- 根据返回值类型推断列类型（interface{} → string/int/float）
- 使用别名或表达式文本作为列名
- 保持现有的 SELECT DATABASE() 特殊处理逻辑
- **测试覆盖**：
- 算术运算：SELECT 1+1, SELECT 2*3, SELECT 10/2
- 函数调用：SELECT NOW(), SELECT RAND(), SELECT CURDATE()
- 组合表达式：SELECT NOW()+1, SELECT 1+2*3
- 多列查询：SELECT 1+1, 2*2, NOW()
- 别名测试：SELECT 1+1 AS result