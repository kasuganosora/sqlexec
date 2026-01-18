# Go层面函数注册系统

## 概述

这是一个完整的Go层面函数注册系统，允许将SQL查询能力内嵌到其他应用中。系统提供了灵活的API来注册、管理和使用自定义函数。

## 核心组件

### 1. FunctionRegistryExt（扩展注册表）

完整的函数注册表，支持：
- 标量函数
- 聚合函数
- 用户自定义函数
- 会话函数
- 函数别名

### 2. FunctionAPI（高级API）

提供友好的API接口：
- 函数注册
- 函数查询
- 函数搜索
- 文档生成

### 3. FunctionBuilder（构建器）

流式API，方便地构建复杂函数：
```go
builtin.MathFunctionBuilder("distance", "Distance", "计算距离").
    WithParameter("x1", "number", "X坐标", true).
    WithParameter("y1", "number", "Y坐标", true).
    WithExample("SELECT distance(0, 0, 3, 4)").
    WithHandler(distanceHandler).
    Register(api)
```

## 快速开始

### 基本使用

```go
package main

import (
    "fmt"
    "mysql-proxy/mysql/builtin"
)

func main() {
    // 创建函数API
    api := builtin.NewFunctionAPI()

    // 注册自定义函数
    err := builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
        "myfunc", "MyFunc", "我的函数", "number",
        func(args []interface{}) (interface{}, error) {
            return args[0], nil
        },
        1,
    )
    if err != nil {
        panic(err)
    }

    // 使用函数
    fn, ok := api.GetFunction("myfunc")
    if ok {
        result, _ := fn.Handler([]interface{}{42})
        fmt.Println(result) // 输出: 42
    }
}
```

### 应用程序集成

```go
package main

import (
    "mysql-proxy/mysql/builtin"
    "yourapp/app"
)

func main() {
    // 创建查询引擎
    engine := yourapp.NewQueryEngine()

    // 创建函数API
    fnAPI := builtin.NewFunctionAPI()

    // 注册应用特定函数
    fnAPI.RegisterScalarFunction("app_hash", "AppHash", "应用哈希",
        func(args []interface{}) (interface{}, error) {
            // 你的哈希逻辑
            return computeHash(args[0]), nil
        },
        builtin.WithCategory(builtin.CategoryString),
        builtin.WithReturnType("integer"),
    )

    // 将函数API集成到引擎
    engine.SetFunctionAPI(fnAPI)

    // 现在可以在SQL中使用app_hash()
    // SELECT app_hash(email) FROM users
}
```

## API 参考

### 注册函数

#### 简单标量函数

```go
builtin.RegisterSimpleScalar(
    api,                      // FunctionAPI实例
    builtin.CategoryMath,        // 类别
    "sqrt",                   // 函数名
    "Sqrt",                   // 显示名称
    "计算平方根",              // 描述
    "number",                 // 返回类型
    mySqrtHandler,            // 处理函数
    1,                        // 参数数
)
```

#### 可变参数函数

```go
builtin.RegisterVariadicScalar(
    api,
    builtin.CategoryString,
    "concat_all",             // 函数名
    "ConcatAll",             // 显示名称
    "连接所有字符串",          // 描述
    "string",                // 返回类型
    concatHandler,           // 处理函数
    1,                       // 最小参数数
)
```

#### 使用构建器

```go
builtin.MathFunctionBuilder("distance", "Distance", "计算距离").
    WithDescription("计算两点之间的距离").
    WithParameter("x1", "number", "X坐标", true).
    WithParameter("y1", "number", "Y坐标", true).
    WithParameter("x2", "number", "X坐标", true).
    WithParameter("y2", "number", "Y坐标", true).
    WithExample("SELECT distance(0, 0, 3, 4)").
    WithHandler(distanceHandler).
    Register(api)
```

### 查询函数

#### 获取单个函数

```go
fn, ok := api.GetFunction("abs")
if ok {
    fmt.Println(fn.Name, fn.Description)
}
```

#### 列出所有函数

```go
all := api.ListFunctions()
for _, fn := range all {
    fmt.Printf("%s: %s\n", fn.DisplayName, fn.Description)
}
```

#### 按类别列出

```go
mathFuncs := api.ListFunctionsByCategory(builtin.CategoryMath)
for _, fn := range mathFuncs {
    fmt.Printf("%s: %s\n", fn.DisplayName, fn.Description)
}
```

#### 按类型列出

```go
scalars := api.ListFunctionsByType(builtin.FunctionTypeScalar)
aggregates := api.ListFunctionsByType(builtin.FunctionTypeAggregate)
```

#### 搜索函数

```go
results := api.SearchFunctions("sqrt")
for _, fn := range results {
    fmt.Printf("%s: %s\n", fn.Name, fn.Description)
}
```

### 函数管理

#### 注销函数

```go
err := api.UnregisterFunction("myfunc")
if err != nil {
    log.Println("Error:", err)
}
```

#### 清除用户函数

```go
api.ClearUserFunctions()
```

#### 清除会话函数

```go
api.ClearSessionFunctions()
```

### 别名管理

```go
// 添加别名
err := api.AddFunctionAlias("sq", "square")

// 删除别名
api.RemoveFunctionAlias("sq")

// 获取所有别名
aliases := api.GetFunctionAliases()
```

### 生成文档

```go
// 生成Markdown文档
docs := api.GenerateDocumentation()
fmt.Println(docs)

// 生成JSON文档
jsonDoc, err := api.GenerateJSON()
if err == nil {
    fmt.Println(jsonDoc)
}
```

## 函数选项

系统提供了多个函数选项：

```go
builtin.WithCategory(builtin.CategoryMath)
builtin.WithReturnType("number")
builtin.WithVariadic()
builtin.WithMinArgs(1)
builtin.WithMaxArgs(3)
builtin.WithArgRange(1, 3)
builtin.WithParameter("x", "number", "参数", true)
builtin.WithExample("SELECT myfunc(42)")
builtin.WithTag("math")
builtin.WithTags([]string{"math", "custom"})
```

## 函数类型

### 标量函数 (FunctionTypeScalar)

返回单个值的函数，例如：
- 数学函数：ABS, ROUND, SQRT
- 字符串函数：CONCAT, UPPER, SUBSTRING
- 日期函数：NOW, YEAR, MONTH

### 聚合函数 (FunctionTypeAggregate)

对一组值进行计算，返回单个值，例如：
- COUNT, SUM, AVG
- MIN, MAX
- STDDEV, VARIANCE

### 窗口函数 (FunctionTypeWindow)

在窗口上计算的函数（计划中）：
- ROW_NUMBER, RANK
- LAG, LEAD

## 函数类别

```go
builtin.CategoryMath      // 数学函数
builtin.CategoryString    // 字符串函数
builtin.CategoryDate      // 日期时间函数
builtin.CategoryAggregate // 聚合函数
builtin.CategoryControl   // 控制函数
builtin.CategoryJSON      // JSON函数
builtin.CategorySystem    // 系统函数
```

## 函数作用域

```go
builtin.ScopeGlobal   // 全局函数（内置）
builtin.ScopeUser    // 用户自定义函数
builtin.ScopeSession // 会话函数
```

## 完整示例

参见 `example_function_api.go`，包含7个完整示例：

1. 注册自定义函数
2. 使用构建器创建复杂函数
3. 函数查询和搜索
4. 用户函数管理
5. 别名管理
6. 生成文档
7. 应用程序集成

## 运行示例

```bash
go run example_function_api.go
```

## 集成到其他应用

### 步骤1: 导入包

```go
import "mysql-proxy/mysql/builtin"
```

### 步骤2: 创建函数API

```go
fnAPI := builtin.NewFunctionAPI()
```

### 步骤3: 注册应用函数

```go
fnAPI.RegisterScalarFunction("your_func", "YourFunc", "描述",
    handler,
    builtin.WithCategory(builtin.CategoryMath),
)
```

### 步骤4: 集成到SQL引擎

```go
// 在SQL解析或执行时调用函数
fn, ok := fnAPI.GetFunction(expr.Function)
if ok {
    result, err := fn.Handler(args)
    // 使用结果...
}
```

## 性能考虑

1. **并发安全**：所有操作都使用读写锁保护
2. **快速查找**：函数查找是O(1)操作
3. **内存效率**：使用map存储，查找快速
4. **延迟加载**：函数按需注册，减少启动时间

## 扩展性

系统设计为高度可扩展：

1. **新增类别**：定义新的FunctionCategory
2. **新增类型**：定义新的FunctionType
3. **自定义元数据**：扩展FunctionMetadata结构
4. **自定义过滤器**：实现新的FunctionFilter
5. **插件系统**：结合extensibility包使用

## 注意事项

1. **函数名大小写**：函数名会自动转换为小写
2. **线程安全**：FunctionRegistryExt是线程安全的
3. **函数覆盖**：会话函数 > 用户函数 > 全局函数
4. **别名限制**：别名不能与现有函数名冲突

## 最佳实践

1. **函数命名**：使用小写，下划线分隔
2. **错误处理**：总是返回详细的错误信息
3. **文档完善**：提供描述、示例和参数说明
4. **类型安全**：进行必要的类型检查和转换
5. **性能优化**：避免不必要的计算和内存分配

## 未来计划

- [ ] 窗口函数支持
- [ ] 类型推导系统
- [ ] 函数依赖管理
- [ ] 函数性能分析
- [ ] 动态函数加载
- [ ] 函数权限控制
