# 用户自定义函数（UDF）指南

## 概述

本系统提供了完整的用户自定义函数（UDF）支持，参考TiDB和MySQL的语法和实现方式，允许用户创建和使用自己的函数来扩展SQL查询能力。

## 特性

- ✅ **SQL表达式支持**: 支持简单的SQL表达式和函数调用
- ✅ **模板引擎**: 支持Go模板语法编写复杂逻辑
- ✅ **构建器模式**: 流式API简化UDF创建
- ✅ **类型安全**: 完整的类型检查和转换
- ✅ **并发安全**: 线程安全的函数管理
- ✅ **确定性标记**: 支持函数确定性标记，便于查询优化
- ✅ **元数据管理**: 完整的函数元数据和文档

## 快速开始

### 1. 创建简单的SQL表达式UDF

```go
import "mysql-proxy/mysql/builtin"

// 创建函数API
api := builtin.NewFunctionAPI()

// 创建简单的参数引用UDF
udf := builtin.NewUDFBuilder("get_x").
    WithParameter("x", "number", false).
    WithReturnType("number").
    WithBody("@x").
    WithDescription("获取参数x的值").
    Build()

// 注册UDF
api.RegisterUDF(udf)

// 使用UDF
fn, _ := api.GetFunction("get_x")
result, _ := fn.Handler([]interface{}{42})
// result = 42
```

### 2. 创建算术表达式UDF

```go
// 创建加法UDF
udf := builtin.NewUDFBuilder("add_numbers").
    WithParameter("a", "number", false).
    WithParameter("b", "number", false).
    WithReturnType("number").
    WithBody("@a + @b").
    WithDescription("加法运算").
    Build()

api.RegisterUDF(udf)

// 使用
fn, _ := api.GetFunction("add_numbers")
result, _ := fn.Handler([]interface{}{10, 20})
// result = "10 + 20" (注意：当前返回字符串)
```

### 3. 创建函数调用UDF

```go
// 创建调用内置函数的UDF
udf := builtin.NewUDFBuilder("double_abs").
    WithParameter("x", "number", false).
    WithReturnType("number").
    WithBody("abs(@x) * 2").
    WithDescription("计算绝对值的两倍").
    Build()

api.RegisterUDF(udf)

// 使用
fn, _ := api.GetFunction("double_abs")
result, _ := fn.Handler([]interface{}{-5})
// result = "abs(-5) * 2"
```

### 4. 创建模板表达式UDF

```go
// 创建模板UDF
udf := builtin.NewUDFBuilder("format_price").
    WithParameter("price", "number", false).
    WithParameter("currency", "string", false).
    WithReturnType("string").
    WithBody(`{{.price}} {{.currency}}`).
    WithLanguage("SQL").
    WithDescription("格式化价格").
    Build()

api.RegisterUDF(udf)

// 使用
fn, _ := api.GetFunction("format_price")
result, _ := fn.Handler([]interface{}{99.99, "USD"})
// result = "99.99 USD"
```

## API 参考

### UDFBuilder

UDF构建器提供流式API来创建UDF：

```go
builder := builtin.NewUDFBuilder(name)
```

#### 方法

**WithParameter(name, type string, optional bool)**
- 添加参数
- 参数：
  - `name`: 参数名称
  - `type`: 参数类型（string, number等）
  - `optional`: 是否可选

**WithReturnType(type string)**
- 设置返回类型

**WithBody(body string)**
- 设置函数体（SQL表达式或模板）

**WithLanguage(lang string)**
- 设置语言（SQL, GO）

**WithDeterminism(determinism bool)**
- 设置是否确定性（相同输入是否总是产生相同输出）

**WithDescription(desc string)**
- 设置函数描述

**WithAuthor(author string)**
- 设置作者

**Build()**
- 构建UDF对象

### FunctionAPI

UDF管理API：

#### 注册UDF

```go
// 注册UDF对象
api.RegisterUDF(udf *UDFFunction) error

// 通过构建器注册
api.RegisterUDFFromBuilder(builder *UDFBuilder) error
```

#### 查询UDF

```go
// 获取单个UDF
api.GetUDF(name string) (*UDFFunction, error)

// 列出所有UDF
api.ListUDFs() []*UDFFunction

// 检查是否存在
api.UDFExists(name string) bool

// 统计数量
api.CountUDFs() int
```

#### 管理UDF

```go
// 注销UDF
api.UnregisterUDF(name string) error

// 清除所有UDF
api.ClearUDFs()
```

## 函数体语法

### 1. 参数引用

使用 `@param` 或 `:param` 引用参数：

```sql
@param1
:param2
```

### 2. 算术表达式

支持基础算术运算：

```sql
@a + @b
@a - @b
@a * @b
@a / @b
```

### 3. 函数调用

调用内置函数或其他UDF：

```sql
abs(@x)
concat(@a, ' ', @b)
round(@price, 2)
```

### 4. 模板表达式

使用Go模板语法：

```
{{.param}}
{{.a}} + {{.b}}
{{if gt .age 18}}Adult{{else}}Minor{{end}}
```

## 最佳实践

### 1. 使用确定性标记

对于确定性的函数（相同输入总是产生相同输出），设置确定性标记：

```go
udf := builtin.NewUDFBuilder("square").
    WithParameter("x", "number", false).
    WithReturnType("number").
    WithBody("@x * @x").
    WithDeterminism(true).  // 标记为确定性
    Build()
```

### 2. 提供完整的元数据

```go
udf := builtin.NewUDFBuilder("calculate_tax").
    WithParameter("price", "number", false).
    WithParameter("tax_rate", "number", false).
    WithReturnType("number").
    WithBody("@price * @tax_rate").
    WithDescription("计算税费").
    WithAuthor("Finance Team").
    Build()
```

### 3. 使用有意义的名称

```go
// 好的命名
"apply_discount"
"calculate_gst"
"format_phone_number"

// 避免的命名
"func1"
"do_calc"
"x"
```

### 4. 参数验证

虽然当前实现有限，但应该在文档中说明参数限制：

```go
udf := builtin.NewUDFBuilder("divide").
    WithParameter("numerator", "number", false).
    WithParameter("denominator", "number", false).
    WithReturnType("number").
    WithBody("@numerator / @denominator").
    WithDescription("除法运算（注意：除数不能为零）").
    Build()
```

## 使用示例

### 商业场景

```go
// 折扣计算
discount := builtin.NewUDFBuilder("apply_discount").
    WithParameter("price", "number", false).
    WithParameter("discount_rate", "number", false).
    WithReturnType("number").
    WithBody(`{{.price}} - {{.price}} * {{.discount_rate}}`).
    WithDescription("计算折扣后价格").
    Build()

// 使用
result, _ := fn.Handler([]interface{}{100.0, 0.1})
// result = "100 - 100 * 0.1"
```

### 数据验证

```go
// 年龄验证
validate := builtin.NewUDFBuilder("is_adult").
    WithParameter("age", "number", false).
    WithReturnType("string").
    WithBody(`{{if ge .age 18}}Adult{{else}}Minor{{end}}`).
    WithDescription("验证是否成年").
    Build()

// 使用
result, _ := fn.Handler([]interface{}{21})
// result = "Adult"
```

### 字符串格式化

```go
// 用户名格式化
format := builtin.NewUDFBuilder("format_username").
    WithParameter("first_name", "string", false).
    WithParameter("last_name", "string", false).
    WithReturnType("string").
    WithBody(`{{.first_name}}.{{.last_name}}`).
    WithDescription("生成用户名格式").
    Build()

// 使用
result, _ := fn.Handler([]interface{}{"John", "Smith"})
// result = "John.Smith"
```

## 限制和注意事项

### 当前限制

1. **算术表达式求值**
   - 当前返回字符串形式
   - 不支持复杂的嵌套表达式
   - 需要改进表达式求值器

2. **函数调用**
   - 不支持UDF递归调用
   - 参数解析较为简单
   - 不支持位置参数

3. **错误处理**
   - 错误信息可能不够详细
   - 需要更友好的错误提示

### 性能考虑

1. **确定性函数**
   - 标记为确定性的函数可以被缓存
   - 提高查询性能

2. **模板表达式**
   - 每次调用都会重新解析
   - 复杂模板可能影响性能

3. **函数查找**
   - 使用map存储，查找速度O(1)
   - 大量函数时内存占用较大

## 未来改进

### 短期

1. 改进算术表达式求值
2. 支持更多SQL语法
3. 增强错误处理
4. 添加参数验证

### 中期

1. 支持UDF持久化
2. 支持UDF导入/导出
3. 添加性能分析工具
4. 支持UDF版本管理

### 长期

1. 支持外部语言（Python, JavaScript等）
2. 支持复杂SQL语句
3. 支持UDF依赖管理
4. 支持UDF市场和共享

## 示例文件

- `test_udf.go` - 基本功能测试
- `example_udf_sql.go` - SQL使用示例

## 相关文档

- [BUILTIN_FUNCTIONS_SUMMARY.md](./BUILTIN_FUNCTIONS_SUMMARY.md) - 内置函数文档
- [README_FUNCTION_API.md](./README_FUNCTION_API.md) - 函数API文档
- [FUNCTION_REGISTRY_COMPLETE.md](./FUNCTION_REGISTRY_COMPLETE.md) - 函数注册系统文档

## 许可证

本项目基于 Apache 2.0 许可证开源。
