# 自定义函数 (UDF)

SQLExec 支持注册自定义 SQL 函数，可在 SQL 查询中直接调用。

## 方式一：简单注册

最快的方式，适合简单的标量函数：

```go
import "github.com/kasuganosora/sqlexec/pkg/builtin"

builtin.RegisterGlobal(&builtin.FunctionInfo{
    Name:        "DOUBLE",
    Type:        builtin.FunctionTypeScalar,
    Category:    "custom",
    Description: "Doubles a numeric value",
    Handler: func(args []interface{}) (interface{}, error) {
        if len(args) == 0 || args[0] == nil {
            return nil, nil
        }
        switch v := args[0].(type) {
        case float64:
            return v * 2, nil
        case int64:
            return v * 2, nil
        default:
            return nil, fmt.Errorf("DOUBLE: unsupported type")
        }
    },
})
```

使用：

```sql
SELECT name, DOUBLE(price) AS double_price FROM products;
```

## 方式二：FunctionAPI

提供更丰富的元数据和校验：

```go
api := builtin.NewFunctionAPI()

// 标量函数
api.RegisterScalarFunction(
    "CELSIUS_TO_F",          // 函数名
    "CELSIUS_TO_F",          // 显示名
    "Convert Celsius to Fahrenheit",  // 描述
    func(args []interface{}) (interface{}, error) {
        c, _ := toFloat64(args[0])
        return c*9.0/5.0 + 32, nil
    },
    builtin.WithCategory(builtin.CategoryMath),
    builtin.WithArgRange(1, 1),        // 参数个数：最少1，最多1
    builtin.WithReturnType("float64"),
)
```

```sql
SELECT city, CELSIUS_TO_F(temperature) AS temp_f FROM weather;
```

## 方式三：聚合函数

```go
api.RegisterAggregateFunction(
    "PRODUCT",
    "PRODUCT",
    "Multiply all values together",
    // 聚合处理函数：接收当前值和累积值
    func(args []interface{}, accumulator interface{}) (interface{}, error) {
        val, _ := toFloat64(args[0])
        if accumulator == nil {
            return val, nil
        }
        acc, _ := toFloat64(accumulator)
        return acc * val, nil
    },
    // 结果提取函数
    func(accumulator interface{}) (interface{}, error) {
        return accumulator, nil
    },
    builtin.WithCategory(builtin.CategoryAggregate),
)
```

```sql
SELECT category, PRODUCT(growth_rate) AS compound_rate
FROM quarterly_results
GROUP BY category;
```

## 方式四：UDF Builder

适合定义 SQL 表达式函数：

```go
builder := builtin.NewUDFBuilder("BMI")
builder.WithParameter("weight_kg", "float64", false)
builder.WithParameter("height_m", "float64", false)
builder.WithReturnType("float64")
builder.WithBody("@weight_kg / (@height_m * @height_m)")
builder.WithLanguage("SQL")
builder.WithDescription("Calculate Body Mass Index")
builder.WithDeterminism(true)

udf := builder.Build()
builtin.GetGlobalUDFManager().Register(udf)
```

```sql
SELECT name, BMI(weight, height) AS bmi FROM patients;
```

## 函数类型

| 类型 | 常量 | 说明 |
|------|------|------|
| 标量函数 | `FunctionTypeScalar` | 每行返回一个值 |
| 聚合函数 | `FunctionTypeAggregate` | 多行聚合为一个值 |
| 窗口函数 | `FunctionTypeWindow` | 窗口内计算 |

## 函数作用域

| 作用域 | 常量 | 说明 |
|--------|------|------|
| 全局 | `ScopeGlobal` | 内置函数，始终可用 |
| 用户 | `ScopeUser` | 用户定义，持久化 |
| 会话 | `ScopeSession` | 仅当前会话有效 |

## 管理函数

```go
// 列出所有函数
functions := api.ListFunctions()

// 按类别列出
mathFuncs := api.ListFunctionsByCategory(builtin.CategoryMath)

// 搜索函数
results := api.SearchFunctions("convert")

// 获取函数信息
info := api.GetFunction("CELSIUS_TO_F")

// 注销函数
api.UnregisterFunction("CELSIUS_TO_F")
```

## 完整示例

```go
package main

import (
    "fmt"
    "strings"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/builtin"
)

func init() {
    // 注册自定义函数：首字母大写
    builtin.RegisterGlobal(&builtin.FunctionInfo{
        Name:     "TITLE_CASE",
        Type:     builtin.FunctionTypeScalar,
        Category: "custom",
        Handler: func(args []interface{}) (interface{}, error) {
            if len(args) == 0 || args[0] == nil {
                return nil, nil
            }
            s := fmt.Sprintf("%v", args[0])
            return strings.Title(s), nil
        },
    })
}

func main() {
    // ... 创建 db 和 session
    rows, _ := session.QueryAll("SELECT TITLE_CASE(name) AS title_name FROM users")
    for _, row := range rows {
        fmt.Println(row["title_name"])
    }
}
```
