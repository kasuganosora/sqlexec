---
name: generated_column_integration_tests
overview: 为api包的生成列功能设计并实现完整的集成测试，覆盖VIRTUAL/STORED列、表达式计算、级联依赖、错误场景和MySQL官网示例
todos:
  - id: create-test-file
    content: 创建pkg/api/generated_column_integration_test.go文件
    status: completed
  - id: implement-basic-tests
    content: 实现基础功能测试（CREATE/INSERT/UPDATE/SELECT）
    status: completed
    dependencies:
      - create-test-file
  - id: implement-mysql-examples
    content: 实现MySQL官网示例测试（triangle、full_name）
    status: completed
    dependencies:
      - create-test-file
  - id: implement-virtual-stored-tests
    content: 实现VIRTUAL vs STORED对比测试
    status: completed
    dependencies:
      - create-test-file
  - id: implement-expression-tests
    content: 实现表达式计算测试（算术、字符串、函数）
    status: completed
    dependencies:
      - create-test-file
  - id: implement-cascading-tests
    content: 实现级联生成列测试
    status: completed
    dependencies:
      - create-test-file
  - id: implement-null-tests
    content: 实现NULL传播测试
    status: completed
    dependencies:
      - create-test-file
  - id: implement-error-tests
    content: 实现错误场景测试
    status: completed
    dependencies:
      - create-test-file
  - id: run-all-tests
    content: 运行所有测试并验证结果
    status: completed
    dependencies:
      - implement-basic-tests
      - implement-mysql-examples
      - implement-virtual-stored-tests
      - implement-expression-tests
      - implement-cascading-tests
      - implement-null-tests
      - implement-error-tests
---

## 产品概述

为pkg/api包设计并实现完整的生成列集成测试，确保生成列功能正常工作。

## 核心功能

- 创建包含生成列（VIRTUAL/STORED）的表
- 插入数据并验证生成列计算正确性
- 更新基础列并验证生成列自动更新
- 查询数据并返回计算后的生成列值
- MySQL官网经典示例验证（triangle斜边、full_name拼接）
- 错误场景处理（循环依赖、引用AUTO_INCREMENT等）

## 技术栈

- 测试框架：Go testing + testify/assert
- 数据源：pkg/resource/memory (MVCCDataSource)
- 生成列实现：pkg/resource/generated
- API层：pkg/api (Session, DB)

## 技术架构

### 系统架构

```
pkg/api层
    ↓ Execute/Query
pkg/resource/memory (MVCCDataSource)
    ↓ Insert/Query
pkg/resource/generated
    ├─ VirtualCalculator (VIRTUAL列计算)
    ├─ GeneratedColumnEvaluator (表达式求值)
    └─ GeneratedColumnValidator (约束验证)
```

### 模块划分

- **测试初始化模块**：创建DB、Session、DataSource
- **基础场景测试**：CREATE/INSERT/UPDATE/SELECT基本流程
- **MySQL示例测试**：triangle、full_name等经典案例
- **VIRTUAL vs STORED测试**：对比两种生成列行为
- **级联生成列测试**：多级依赖关系
- **NULL传播测试**：处理NULL值的传播
- **错误场景测试**：非法表达式、循环依赖等

## 实现细节

### 核心目录结构

```
d:/code/db/pkg/api/
└── generated_column_integration_test.go  # [NEW] 生成列集成测试
```

### 测试文件组织

`generated_column_integration_test.go` 包含以下测试组：

1. **基础功能测试组** (TestGeneratedColumns_Basic*)

- 创建包含生成列的表
- 插入数据验证计算
- 更新基础列验证自动更新
- SELECT查询验证结果

2. **MySQL官网示例测试组** (TestGeneratedColumns_MySQLExamples*)

- triangle表：斜边计算
- full_name表：字符串拼接

3. **VIRTUAL vs STORED测试组** (TestGeneratedColumns_VirtualVsStored*)

- 创建VIRTUAL和STORED列的对比表
- 插入数据验证行为差异
- 存储空间对比

4. **表达式计算测试组** (TestGeneratedColumns_Expressions*)

- 算术运算：+ - * / %
- 字符串操作：CONCAT
- 函数调用：UPPER, LOWER等

5. **级联生成列测试组** (TestGeneratedColumns_Cascading*)

- 生成列依赖其他生成列
- 多级依赖链
- 更新基础列验证级联更新

6. **NULL传播测试组** (TestGeneratedColumns_NullPropagation*)

- 依赖列为NULL时的行为
- 多列NULL组合

7. **错误场景测试组** (TestGeneratedColumns_ErrorScenarios*)

- 循环依赖
- 引用AUTO_INCREMENT列
- 非法表达式
- 除零错误

### 测试工具函数

- `setupGeneratedColumnsTest()`：创建测试环境（DB, Session, DataSource）
- `teardownGeneratedColumnsTest()`：清理测试资源
- `assertColumnGenerated()`：验证列是否为生成列
- `assertGeneratedValue()`：验证生成列值是否正确

本任务不涉及UI设计，无需设计部分。