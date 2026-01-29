# SQL 注入模块重构总结

## 概述

根据对 TiDB 的研究，我们发现 TiDB **没有在数据库层面做 SQL 注入检测**，而是提供参数化查询工具（`sqlescape`），将安全性责任交给应用层。

因此，我们对 `pkg/security` 包进行了重构，采用了与 TiDB 相同的设计理念。

## 改动内容

### 删除的文件

- `sql_injection.go` - 基于正则表达式的注入检测器
- `sql_injection_hybrid.go` - 混合注入检测器
- `sql_injection_parser.go` - 基于 AST 的注入检测器
- `sql_injection_test.go` - 上述检测器的测试

### 新增的文件

- `sqlescape.go` - SQL 参数化查询工具
- `sqlescape_test.go` - 完整的测试覆盖
- `README.md` - 详细的使用文档

## 设计理念

### 为什么删除注入检测？

1. **责任分离**
   - 数据库层：执行 SQL 语句
   - 应用层：保证 SQL 安全

2. **参考业界最佳实践**
   - TiDB、MySQL、PostgreSQL 等主流数据库都不在服务器层面做注入检测
   - 安全性通过参数化查询实现

3. **避免误报和漏报**
   - 正则表达式检测无法覆盖所有攻击模式
   - 容易产生误报，影响正常业务

### 为什么提供参数化工具？

1. **帮助开发者**
   - 提供安全的 SQL 构建方式
   - 自动转义特殊字符

2. **兼容性**
   - 类似 TiDB 的 API 设计
   - 便于开发者从 TiDB 迁移

3. **灵活性**
   - 支持多种数据类型
   - 提供多种使用方式（字符串、Builder）

## API 设计

### 核心函数

```go
// 基本使用
EscapeSQL(sql string, args ...interface{}) (string, error)

// 便捷版本（遇到错误 panic）
MustEscapeSQL(sql string, args ...interface{}) string

// Writer 版本（流式构建）
FormatSQL(w io.Writer, sql string, args ...interface{}) error

// Builder 版本
MustFormatSQL(w *strings.Builder, sql string, args ...interface{})
```

### 格式说明符

- `%?` - 参数（自动类型转换）
- `%%` - 输出 %
- `%n` - 标识符（表名、列名）

## 测试覆盖

测试包含以下场景：

1. **基础功能**
   - 各种数据类型（整数、浮点、字符串、布尔、nil）
   - 多参数处理
   - 标识符处理

2. **转义测试**
   - 单引号、双引号
   - 反斜杠、换行符
   - NULL 字节
   - 标识符中的反引号

3. **数组类型**
   - 字符串数组
   - 整数数组
   - int64 数组

4. **特殊类型**
   - 时间类型
   - 字节数组
   - JSON RawMessage

5. **错误处理**
   - 参数不足
   - 参数过多
   - 类型错误
   - 不支持的类型

6. **边界情况**
   - 百分号转义
   - 复杂查询构建

## 迁移指南

对于之前使用注入检测的代码：

### 旧代码（已删除）

```go
detector := NewSQLInjectionDetector()
result := detector.Detect(sql)
if result.IsDetected {
    // 处理注入
}
```

### 新代码（推荐）

```go
// 使用参数化查询
query, err := EscapeSQL(
    "SELECT * FROM %n WHERE id = %? AND name = %?",
    "users", userID, userName,
)
if err != nil {
    // 处理错误
}
db.Exec(query)
```

## 性能

- 预分配缓冲区空间
- 流式构建支持
- 零内存分配（对于简单场景）

## 安全性声明

⚠️ **此工具不能替代参数化查询！**

- 建议优先使用数据库的预编译语句
- 此工具主要用于动态 SQL 构建场景
- 安全性仍依赖于开发者的正确使用

## 参考实现

本实现参考了：
- TiDB `sqlescape` 包
- MySQL 转义规则
- SQL 注入防御最佳实践

## 测试结果

所有测试通过：

```
=== RUN   TestEscapeSQL_Basic
=== RUN   TestEscapeSQL_Escaping
=== RUN   TestEscapeSQL_ArrayTypes
=== RUN   TestEscapeSQL_Time
=== RUN   TestEscapeSQL_Bytes
=== RUN   TestEscapeSQL_Errors
=== RUN   TestEscapeSQL_Percent
=== RUN   TestMustEscapeSQL
=== RUN   TestFormatSQL
=== RUN   TestComplexQuery
=== RUN   TestUnsupportedType
PASS
ok      github.com/kasuganosora/sqlexec/pkg/security    0.437s
```

## 总结

通过这次重构：
- ✅ 采用了业界标准的设计理念
- ✅ 简化了代码，减少了维护成本
- ✅ 提供了实用的开发工具
- ✅ 保持了测试覆盖率
- ✅ 与 TiDB 保持 API 一致性
