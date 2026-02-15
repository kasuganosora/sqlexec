# Plugin Development Overview

SQLExec provides three extension mechanisms to meet different customization needs.

## Three Extension Methods

| Method | Use Case | Complexity | Language |
|------|---------|--------|------|
| [Custom Data Source](custom-datasource.md) | Connect new data backends (Redis, MongoDB, etc.) | Medium | Go |
| [Custom Functions (UDF)](custom-functions.md) | Add custom SQL functions | Simple | Go |
| [Native Plugins (DLL/SO)](native-plugin.md) | Cross-language, dynamic loading | Higher | C/Go/Any language |

## Selection Guide

### Custom Data Source

Use this when you need to connect a new data backend to the SQL query engine. For example:

- Use Redis as a SQL-queryable data source
- Connect to MongoDB and query with SQL
- Connect to a custom REST API
- Read files in special formats

Implement the `DataSource` + `DataSourceFactory` interfaces and register them with the Registry.

### Custom Functions (UDF)

Use this when you need custom computation logic in SQL. For example:

- Business-specific computation functions
- Custom string processing
- Special aggregate statistics
- External API call wrappers

Call `builtin.RegisterGlobal()` or use the `FunctionAPI` to register.

### Native Plugins (DLL/SO)

Use this when you need dynamic loading or non-Go language implementations. For example:

- Dynamically load data sources at runtime
- Implement high-performance data sources in C/C++/Rust
- Third-party independently developed extensions

Compile as a shared library and implement the required C export functions.

## Extension Integration Architecture

```
SQL Query
  |
SQL Parser -> Custom Functions (UDF)
  |
Query Optimizer
  |
Execution Engine -> Data Source Registry -> Built-in Data Sources
                                         -> Custom Data Sources
                                         -> Native Plugin Data Sources
```

All extensions are fully equivalent to built-in features and are transparent to the SQL engine.
