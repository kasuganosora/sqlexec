# Custom Functions (UDF)

SQLExec supports registering custom SQL functions that can be called directly in SQL queries.

## Method 1: Simple Registration

The quickest approach, suitable for simple scalar functions:

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

Usage:

```sql
SELECT name, DOUBLE(price) AS double_price FROM products;
```

## Method 2: FunctionAPI

Provides richer metadata and validation:

```go
api := builtin.NewFunctionAPI()

// Scalar function
api.RegisterScalarFunction(
    "CELSIUS_TO_F",          // Function name
    "CELSIUS_TO_F",          // Display name
    "Convert Celsius to Fahrenheit",  // Description
    func(args []interface{}) (interface{}, error) {
        c, _ := toFloat64(args[0])
        return c*9.0/5.0 + 32, nil
    },
    builtin.WithCategory(builtin.CategoryMath),
    builtin.WithArgRange(1, 1),        // Argument count: min 1, max 1
    builtin.WithReturnType("float64"),
)
```

```sql
SELECT city, CELSIUS_TO_F(temperature) AS temp_f FROM weather;
```

## Method 3: Aggregate Functions

```go
api.RegisterAggregateFunction(
    "PRODUCT",
    "PRODUCT",
    "Multiply all values together",
    // Aggregate handler: receives the current value and the accumulator
    func(args []interface{}, accumulator interface{}) (interface{}, error) {
        val, _ := toFloat64(args[0])
        if accumulator == nil {
            return val, nil
        }
        acc, _ := toFloat64(accumulator)
        return acc * val, nil
    },
    // Result extractor
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

## Method 4: UDF Builder

Suitable for defining SQL expression functions:

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

## Function Types

| Type | Constant | Description |
|------|------|------|
| Scalar function | `FunctionTypeScalar` | Returns one value per row |
| Aggregate function | `FunctionTypeAggregate` | Aggregates multiple rows into one value |
| Window function | `FunctionTypeWindow` | Computes within a window |

## Function Scope

| Scope | Constant | Description |
|--------|------|------|
| Global | `ScopeGlobal` | Built-in functions, always available |
| User | `ScopeUser` | User-defined, persisted |
| Session | `ScopeSession` | Only valid for the current session |

## Managing Functions

```go
// List all functions
functions := api.ListFunctions()

// List by category
mathFuncs := api.ListFunctionsByCategory(builtin.CategoryMath)

// Search functions
results := api.SearchFunctions("convert")

// Get function info
info := api.GetFunction("CELSIUS_TO_F")

// Unregister a function
api.UnregisterFunction("CELSIUS_TO_F")
```

## Complete Example

```go
package main

import (
    "fmt"
    "strings"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/builtin"
)

func init() {
    // Register a custom function: title case
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
    // ... create db and session
    rows, _ := session.QueryAll("SELECT TITLE_CASE(name) AS title_name FROM users")
    for _, row := range rows {
        fmt.Println(row["title_name"])
    }
}
```
