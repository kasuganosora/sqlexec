---
name: mysql_view_implementation
overview: 实现完整的 MySQL VIEW 功能，包括 CREATE/DROP/ALTER VIEW 语句、INFORMATION_SCHEMA.VIEWS 表、视图查询重写和权限检查，确保与官方 MySQL 使用体验100%一致
todos:
  - id: add-view-parser
    content: 扩展 Parser 层添加视图 AST 节点和解析逻辑
    status: completed
  - id: extend-domain-models
    content: 扩展 Domain 层添加 ViewInfo 结构和常量
    status: completed
    dependencies:
      - add-view-parser
  - id: implement-views-table
    content: 实现 INFORMATION_SCHEMA.VIEWS 虚拟表
    status: completed
    dependencies:
      - extend-domain-models
  - id: implement-view-rewrite
    content: 实现视图查询重写逻辑（MERGE 算法）
    status: completed
    dependencies:
      - extend-domain-models
  - id: implement-view-executor
    content: 实现视图执行器（TEMPTABLE 算法）
    status: completed
    dependencies:
      - implement-view-rewrite
  - id: integrate-view-ddl
    content: 集成视图 DDL 执行到 Builder 和 Session 层
    status: completed
    dependencies:
      - implement-views-table
      - implement-view-rewrite
  - id: write-comprehensive-tests
    content: 编写单元测试确保 80% 覆盖率
    status: completed
    dependencies:
      - integrate-view-ddl
---

## 产品概述

在当前项目 (d:/code/db/pkg) 中实现 MySQL VIEW 功能，支持视图的创建、删除、修改和查询操作。

## 核心功能

- 支持 CREATE VIEW 语法（OR REPLACE、ALGORITHM、DEFINER、SECURITY、WITH CHECK OPTION）
- 支持 DROP VIEW 语法（IF EXISTS）
- 支持 ALTER VIEW 语法
- 实现 INFORMATION_SCHEMA.VIEWS 表（10 个标准字段）
- 实现视图查询处理（MERGE 和 TEMPTABLE 算法）
- 实现权限检查（CREATE VIEW、底层表权限）
- 实现 WITH CHECK OPTION 验证
- 单元测试覆盖率至少 80%

## 技术栈

- 开发语言：Go 1.20+
- 基础架构：分层架构（Parser → Optimizer → Session → API）
- AST 解析：TiDB Parser（已集成）
- 数据存储：内存数据源（MVCC）+ 虚拟数据源
- 测试框架：testing + testify

## 实现方式

### 高层策略

采用分层模块化设计，在现有架构基础上添加视图支持：

1. Parser 层：添加视图相关 AST 节点和解析逻辑
2. Domain 层：扩展 TableInfo 支持视图元数据
3. Information Schema 层：实现 VIEWS 虚拟表
4. Optimizer 层：实现视图查询重写和执行逻辑
5. Builder 层：添加视图 DDL 执行方法

### 关键技术决策

- **视图存储方案**：视图元数据存储在 TableInfo.Atts（map[string]interface{}）中，键为 "**view**"，与现有 Generated Columns 设计保持一致
- **视图算法支持**：初期支持 MERGE 算法（直接重写查询），TEMPTABLE 算法（临时表存储）作为第二阶段
- **查询重写策略**：在 OptimizedExecutor 的 ExecuteSelect 中检测视图，将视图引用重写为底层 SELECT 语句
- **权限检查**：复用现有 ACL Manager 检查底层表权限
- **WITH CHECK OPTION**：实现 CASCADED 和 LOCAL 两种检查模式

### 性能与可靠性

- **复杂度**：视图重写 O(n) 其中 n 是视图嵌套层数，深度限制为 10 层防止无限递归
- **瓶颈与缓解**：视图元数据缓存，避免重复解析；视图定义预编译
- **错误处理**：循环依赖检测、权限不足错误、视图不存在错误、WITH CHECK OPTION 违规错误

### 避免技术债务

- 复用现有虚拟表模式（information_schema/provider.go）
- 复用现有 DDL 执行流程（parser/builder.go）
- 复用现有查询重写逻辑（optimizer/optimized_executor.go）
- 与现有 TableInfo Atts 机制保持一致

## 实现注意事项

### 执行细节（基于探索结果）

- **Parser 层**：参考 tidb/pkg/parser/ast/ddl.go 的 CreateViewStmt 结构，在 types.go 添加对应字段
- **Builder 层**：参考 builder.go 的 executeCreate 方法，添加 executeCreateView 和 executeDropView 方法
- **Information Schema**：参考 privileges.go 的实现模式，创建 views.go
- **Optimizer 层**：在 ExecuteSelect 开始处添加视图检测和重写逻辑，调用新增的 view_rewrite.go

### 性能相关

- 视图元数据解析：缓存解析结果，避免每次查询重新解析视图定义
- 视图重写：预编译视图 SELECT 语句，减少重复解析开销

### 日志相关

- 复用现有 logger，避免记录敏感信息（视图定义、用户凭证）
- 视图创建/删除记录 info 级别日志
- 视图重写失败记录 error 级别日志（包含视图名和错误信息）

### 影响范围控制

- 保持向后兼容：不影响现有 TABLE 操作
- 视图 DDL 新增字段：使用可选参数，默认值兼容旧语法
- 视图查询：仅当检测到视图时启用重写逻辑

## 架构设计

### 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                             │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                     Session Layer                        │
│  (pkg/session/core.go)                                    │
│  - ExecuteCreate → ExecuteCreateView                        │
│  - ExecuteDrop → ExecuteDropView                            │
│  - ExecuteAlter → ExecuteAlterView                          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                   Optimizer Layer                        │
│  (pkg/optimizer/)                                        │
│  - optimized_executor.go: View detection and rewriting       │
│  - view_rewrite.go: MERGE algorithm implementation          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                   Parser Layer                           │
│  (pkg/parser/)                                           │
│  - types.go: CreateViewStatement, DropViewStatement         │
│  - parser.go: View parsing logic                           │
│  - builder.go: executeCreateView, executeDropView          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                Domain Layer                               │
│  (pkg/resource/domain/models.go)                          │
│  - TableInfo.Atts["__view__"]: ViewInfo metadata          │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│           Information Schema Layer                         │
│  (pkg/information_schema/views.go)                         │
│  - VIEWS virtual table implementation                      │
└─────────────────────────────────────────────────────────────┘
```

## 目录结构

### 目录结构概览

本实现添加视图支持，包括 Parser 扩展、Domain 模型扩展、Information Schema 虚拟表、Optimizer 查询重写、Builder DDL 执行、Session 集成和全面测试。

```
pkg/
├── parser/
│   ├── types.go                                    # [MODIFY] 添加视图相关 AST 节点类型
│   ├── parser.go                                   # [MODIFY] 添加视图解析逻辑
│   └── builder.go                                  # [MODIFY] 添加视图 DDL 执行方法
├── resource/domain/
│   └── models.go                                  # [MODIFY] 添加 ViewInfo 结构和常量
├── information_schema/
│   ├── views.go                                   # [NEW] 实现 VIEWS 虚拟表
│   ├── views_test.go                              # [NEW] VIEWS 表单元测试
│   └── provider.go                               # [MODIFY] 注册 VIEWS 虚拟表
├── optimizer/
│   ├── view_rewrite.go                            # [NEW] 视图查询重写逻辑
│   ├── view_rewrite_test.go                       # [NEW] 视图重写单元测试
│   ├── optimized_executor.go                      # [MODIFY] 集成视图检测和重写
│   └── view_executor.go                          # [NEW] 视图执行器（TEMPTABLE 算法）
├── session/
│   └── core.go                                   # [MODIFY] 集成视图 DDL 执行（通过 optimizer）
└── parser/
    └── view_test.go                              # [NEW] 视图解析单元测试
```

### 详细文件说明

#### pkg/parser/types.go [MODIFY]

**目的**：定义视图相关的 AST 节点类型
**功能**：

- 添加 CreateViewStatement 结构体（OR REPLACE、ALGORITHM、DEFINER、SECURITY、SELECT、WITH CHECK OPTION）
- 添加 DropViewStatement 结构体（IF EXISTS、视图名称列表）
- 添加 AlterViewStatement 结构体（语法同 CREATE VIEW）
- 添加 ViewAlgorithm、ViewSecurity、ViewCheckOption 枚举类型
- 在 SQLStatement 中添加 CreateView、DropView、AlterView 字段

#### pkg/parser/parser.go [MODIFY]

**目的**：解析视图相关 SQL 语法
**功能**：

- 在 convertToStatement 中添加视图语句转换逻辑
- 从 TiDB AST 节点提取视图属性（Algorithm、Definer、Security、SelectStmt、CheckOption、Columns）
- 处理 CREATE [OR REPLACE] [ALGORITHM = ...] [DEFINER = ...] [SQL SECURITY ...] VIEW ... AS ... [WITH ... CHECK OPTION]
- 处理 DROP VIEW [IF EXISTS] view_name [, view_name ...]
- 处理 ALTER VIEW ...（语法同 CREATE VIEW）

#### pkg/parser/builder.go [MODIFY]

**目的**：执行视图 DDL 操作
**功能**：

- 添加 executeCreateView 方法：创建视图元数据并存储到 TableInfo.Atts
- 添加 executeDropView 方法：删除视图元数据
- 添加 executeAlterView 方法：修改视图定义
- 在 ExecuteStatement 的 switch 中添加视图语句分支
- 实现视图名称验证（不存在、已存在、循环依赖检测）

#### pkg/resource/domain/models.go [MODIFY]

**目的**：定义视图元数据结构
**功能**：

- 添加 ViewInfo 结构体（Algorithm、Definer、Security、SelectStmt、CheckOption、Cols）
- 添加 ViewAlgorithm 常量（UNDEFINED、MERGE、TEMPTABLE）
- 添加 ViewSecurity 常量（DEFINER、INVOKER）
- 添加 ViewCheckOption 常量（NONE、CASCADED、LOCAL）
- 添加视图相关常量（ViewMetaKey = "**view**"、MaxViewDepth = 10）

#### pkg/information_schema/views.go [NEW]

**目的**：实现 INFORMATION_SCHEMA.VIEWS 虚拟表
**功能**：

- 定义 ViewsTable 结构体（dsManager *application.DataSourceManager）
- 实现 VirtualTable 接口：GetName、GetSchema、Query
- GetSchema 返回 10 个标准字段（TABLE_CATALOG、TABLE_SCHEMA、TABLE_NAME、VIEW_DEFINITION、CHECK_OPTION、IS_UPDATABLE、DEFINER、SECURITY_TYPE、CHARACTER_SET_CLIENT、COLLATION_CONNECTION）
- Query 方法：从所有数据源读取视图元数据，过滤并返回符合条件的视图信息

#### pkg/information_schema/views_test.go [NEW]

**目的**：VIEWS 虚拟表单元测试
**功能**：

- 测试 VIEWS 表的 Schema 正确性
- 测试 VIEWS 表的查询功能（简单视图、复杂视图、带 CHECK OPTION 视图）
- 测试过滤功能（按 TABLE_SCHEMA、TABLE_NAME 过滤）
- 测试 IS_UPDATABLE 计算逻辑

#### pkg/information_schema/provider.go [MODIFY]

**目的**：注册 VIEWS 虚拟表
**功能**：

- 在 initializeTables 方法中注册 views 表：p.tables["views"] = NewViewsTable(p.dsManager)

#### pkg/optimizer/view_rewrite.go [NEW]

**目的**：实现视图查询重写逻辑（MERGE 算法）
**功能**：

- 定义 ViewRewriter 结构体（executor *OptimizedExecutor）
- 实现 RewriteView 方法：将视图引用替换为底层 SELECT 语句
- 实现 DetectViewReferences 方法：检测查询中的视图引用
- 实现 ExpandViewDefinition 方法：展开视图定义
- 实现 ValidateViewCheckOption 方法：验证 WITH CHECK OPTION（CASCADED、LOCAL）
- 实现 CheckCircularDependency 方法：检测循环依赖（最大深度 10 层）

#### pkg/optimizer/view_rewrite_test.go [NEW]

**目的**：视图重写单元测试
**功能**：

- 测试简单视图重写（单表、无 WHERE、无 JOIN）
- 测试复杂视图重写（带 WHERE、JOIN、聚合函数）
- 测试嵌套视图重写（视图引用视图）
- 测试 WITH CHECK OPTION 验证
- 测试循环依赖检测

#### pkg/optimizer/view_executor.go [NEW]

**目的**：实现视图执行器（TEMPTABLE 算法）
**功能**：

- 定义 ViewExecutor 结构体
- 实现 ExecuteAsTempTable 方法：将视图查询结果存入临时表
- 实现 CreateTempTableForView 方法：创建临时表结构
- 实现 DropTempTableForView 方法：清理临时表

#### pkg/optimizer/optimized_executor.go [MODIFY]

**目的**：集成视图检测和重写
**功能**：

- 在 ExecuteSelect 方法开始处添加视图检测逻辑
- 调用 ViewRewriter.RewriteView 重写查询
- 如果视图使用 TEMPTABLE 算法，调用 ViewExecutor.ExecuteAsTempTable

#### pkg/parser/view_test.go [NEW]

**目的**：视图解析单元测试
**功能**：

- 测试 CREATE VIEW 语法（简单视图、带 OR REPLACE、带 ALGORITHM、带 DEFINER、带 SECURITY、带 WITH CHECK OPTION）
- 测试 DROP VIEW 语法（简单 DROP、带 IF EXISTS、多视图删除）
- 测试 ALTER VIEW 语法
- 测试视图列名定义（显式列名、隐式列名）

## 关键代码结构

### ViewInfo 结构（pkg/resource/domain/models.go）

```
// ViewInfo 视图元数据
type ViewInfo struct {
    Algorithm   ViewAlgorithm   `json:"algorithm"`   // MERGE, TEMPTABLE, UNDEFINED
    Definer     string          `json:"definer"`     // 'user'@'host'
    Security    ViewSecurity    `json:"security"`    // DEFINER, INVOKER
    SelectStmt  string          `json:"select_stmt"` // 视图定义的 SELECT 语句
    CheckOption ViewCheckOption `json:"check_option"` // NONE, CASCADED, LOCAL
    Cols        []string        `json:"cols"`       // 视图列名列表
}

// ViewAlgorithm 视图算法类型
type ViewAlgorithm string

const (
    ViewAlgorithmUndefined ViewAlgorithm = "UNDEFINED"
    ViewAlgorithmMerge    ViewAlgorithm = "MERGE"
    ViewAlgorithmTempTable ViewAlgorithm = "TEMPTABLE"
)

// ViewSecurity 视图安全类型
type ViewSecurity string

const (
    ViewSecurityDefiner  ViewSecurity = "DEFINER"
    ViewSecurityInvoker ViewSecurity = "INVOKER"
)

// ViewCheckOption 视图检查选项
type ViewCheckOption string

const (
    ViewCheckOptionNone      ViewCheckOption = "NONE"
    ViewCheckOptionCascaded  ViewCheckOption = "CASCADED"
    ViewCheckOptionLocal     ViewCheckOption = "LOCAL"
)

const (
    ViewMetaKey  = "__view__"  // 视图元数据在 TableInfo.Atts 中的键名
    MaxViewDepth = 10          // 视图嵌套最大深度
)
```

### CreateViewStatement 结构（pkg/parser/types.go）

```
// CreateViewStatement CREATE VIEW 语句
type CreateViewStatement struct {
    OrReplace    bool               `json:"or_replace"`
    Algorithm    ViewAlgorithm      `json:"algorithm,omitempty"`
    Definer      string             `json:"definer,omitempty"`
    Security     ViewSecurity       `json:"security,omitempty"`
    Name         string             `json:"name"`
    ColumnList   []string           `json:"column_list,omitempty"`
    Select       *SelectStatement   `json:"select"`
    CheckOption  ViewCheckOption    `json:"check_option,omitempty"`
}
```

## Agent Extensions

本实现计划不需要使用任何外部 Agent 扩展。所有功能都基于现有代码库架构和标准 Go 库实现。