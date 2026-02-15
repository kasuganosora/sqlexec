# 插件开发概述

SQLExec 提供三种扩展机制，满足不同的自定义需求。

## 三种扩展方式

| 方式 | 适用场景 | 复杂度 | 语言 |
|------|---------|--------|------|
| [自定义数据源](custom-datasource.md) | 接入新的数据后端（Redis、MongoDB 等） | 中等 | Go |
| [自定义函数 (UDF)](custom-functions.md) | 添加自定义 SQL 函数 | 简单 | Go |
| [原生插件 (DLL/SO)](native-plugin.md) | 跨语言、动态加载 | 较高 | C/Go/任意语言 |

## 选择指南

### 自定义数据源

当你需要将新的数据后端接入 SQL 查询引擎时使用。例如：

- 将 Redis 作为 SQL 可查询的数据源
- 连接 MongoDB 并用 SQL 查询
- 接入自定义 REST API
- 读取特殊格式的文件

实现 `DataSource` + `DataSourceFactory` 接口，注册到 Registry 即可。

### 自定义函数 (UDF)

当你需要在 SQL 中使用自定义计算逻辑时使用。例如：

- 业务特定的计算函数
- 自定义字符串处理
- 特殊的聚合统计
- 外部 API 调用封装

调用 `builtin.RegisterGlobal()` 或 `FunctionAPI` 注册即可。

### 原生插件 (DLL/SO)

当你需要动态加载或使用非 Go 语言实现时使用。例如：

- 运行时动态加载数据源
- 使用 C/C++/Rust 实现高性能数据源
- 第三方独立开发的扩展

编译为共享库，实现约定的 C 导出函数即可。

## 扩展集成架构

```
SQL 查询
  ↓
SQL 解析器 → 自定义函数（UDF）
  ↓
查询优化器
  ↓
执行引擎 → 数据源 Registry → 内置数据源
                             → 自定义数据源
                             → 原生插件数据源
```

所有扩展与内置功能完全等价，对 SQL 引擎透明。
