# 配置详解

SQLExec 使用 JSON 格式的配置文件。

## 配置文件搜索路径

按以下优先级查找配置文件：

1. `SQLEXEC_CONFIG` 环境变量指定的路径
2. 当前目录下的 `config.json`
3. `./config/config.json`
4. `/etc/sqlexec/config.json`
5. 如果都不存在，使用内置默认值

## config.json

### 完整配置示例

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 3306,
    "server_version": "SqlExc",
    "keep_alive_period": "30s"
  },
  "database": {
    "max_connections": 100,
    "idle_timeout": 3600,
    "enabled_sources": ["memory", "csv", "excel", "json", "jsonl", "mysql", "postgresql", "parquet"]
  },
  "log": {
    "level": "info",
    "format": "text"
  },
  "pool": {
    "goroutine_pool": {
      "max_workers": 10,
      "queue_size": 1000
    },
    "object_pool": {
      "max_size": 100,
      "min_idle": 2,
      "max_idle": 50
    }
  },
  "cache": {
    "query_cache": {
      "max_size": 1000,
      "ttl": "5m"
    },
    "result_cache": {
      "max_size": 1000,
      "ttl": "10m"
    },
    "schema_cache": {
      "max_size": 100,
      "ttl": "1h"
    }
  },
  "monitor": {
    "slow_query": {
      "threshold": "1s",
      "max_entries": 1000
    }
  },
  "connection": {
    "max_open": 10,
    "max_idle": 5,
    "lifetime": "30m",
    "idle_timeout": "5m"
  },
  "mvcc": {
    "enable_warning": true,
    "auto_downgrade": true,
    "gc_interval": "5m",
    "gc_age_threshold": "1h",
    "xid_wrap_threshold": 100000,
    "max_active_txns": 10000
  },
  "session": {
    "max_age": "24h",
    "gc_interval": "1m"
  },
  "optimizer": {
    "enabled": true
  },
  "http_api": {
    "enabled": false,
    "host": "0.0.0.0",
    "port": 8080
  },
  "mcp": {
    "enabled": false,
    "host": "0.0.0.0",
    "port": 8081
  },
  "paging": {
    "enabled": true,
    "max_memory_mb": 0,
    "page_size": 4096,
    "spill_dir": "",
    "evict_interval": "5s"
  }
}
```

### 配置字段说明

#### server — 服务器

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `host` | string | `"0.0.0.0"` | 监听地址 |
| `port` | int | `3306` | MySQL 协议端口 |
| `server_version` | string | `"SqlExc"` | 服务器版本标识 |
| `keep_alive_period` | duration | `"30s"` | TCP Keep-Alive 周期 |

#### database — 数据库

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_connections` | int | `100` | 最大连接数 |
| `idle_timeout` | int | `3600` | 空闲连接超时（秒） |
| `enabled_sources` | []string | 全部 | 允许使用的数据源类型 |

#### cache — 缓存

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `query_cache.max_size` | int | `1000` | 查询计划缓存大小 |
| `query_cache.ttl` | duration | `"5m"` | 查询计划缓存 TTL |
| `result_cache.max_size` | int | `1000` | 结果缓存大小 |
| `result_cache.ttl` | duration | `"10m"` | 结果缓存 TTL |
| `schema_cache.max_size` | int | `100` | Schema 缓存大小 |
| `schema_cache.ttl` | duration | `"1h"` | Schema 缓存 TTL |

#### mvcc — 多版本并发控制

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enable_warning` | bool | `true` | 启用 MVCC 警告日志 |
| `auto_downgrade` | bool | `true` | 不支持时自动降级隔离级别 |
| `gc_interval` | duration | `"5m"` | 垃圾回收间隔 |
| `gc_age_threshold` | duration | `"1h"` | 旧版本保留时间 |
| `xid_wrap_threshold` | int | `100000` | 事务 ID 回绕阈值 |
| `max_active_txns` | int | `10000` | 最大活跃事务数 |

#### http_api — HTTP API

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `false` | 是否启用 HTTP API |
| `host` | string | `"0.0.0.0"` | 监听地址 |
| `port` | int | `8080` | HTTP 端口 |

#### mcp — MCP Server

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `false` | 是否启用 MCP Server |
| `host` | string | `"0.0.0.0"` | 监听地址 |
| `port` | int | `8081` | MCP 端口 |

#### paging — 分页溢出

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `true` | 启用分页溢出 |
| `max_memory_mb` | int | `0` | 内存上限（0=不限） |
| `page_size` | int | `4096` | 页大小 |
| `spill_dir` | string | `""` | 溢出目录 |

## datasources.json

通过 `datasources.json` 配置外部数据源。该文件位于 config.json 同目录下。

### 格式

```json
[
  {
    "type": "数据源类型",
    "name": "唯一名称",
    "host": "主机地址",
    "port": 3306,
    "username": "用户名",
    "password": "密码",
    "database": "数据库名或文件路径",
    "writable": true,
    "options": {}
  }
]
```

### 示例

```json
[
  {
    "type": "mysql",
    "name": "production",
    "host": "mysql.example.com",
    "port": 3306,
    "username": "app",
    "password": "secret",
    "database": "app_db",
    "writable": true,
    "options": {
      "max_open_conns": 50,
      "charset": "utf8mb4"
    }
  },
  {
    "type": "csv",
    "name": "sales_data",
    "database": "/data/sales.csv"
  },
  {
    "type": "json",
    "name": "config_data",
    "database": "/data/config.json",
    "writable": false
  }
]
```

配置完成后，可通过 `USE production` 切换到对应数据源。

## 嵌入式配置

嵌入式使用时，通过 `DBConfig` 结构体配置：

```go
db, err := api.NewDB(&api.DBConfig{
    CacheEnabled:         true,
    CacheSize:            1000,
    CacheTTL:             300,
    DefaultLogger:        api.NewDefaultLogger(api.LogInfo),
    DebugMode:            false,
    QueryTimeout:         30 * time.Second,
    UseEnhancedOptimizer: true,
})
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `CacheEnabled` | bool | `true` | 启用查询缓存 |
| `CacheSize` | int | `1000` | 缓存条目数 |
| `CacheTTL` | int | `300` | 缓存 TTL（秒） |
| `DefaultLogger` | Logger | nil | 日志实现 |
| `DebugMode` | bool | `false` | 调试模式 |
| `QueryTimeout` | Duration | `0` | 查询超时（0=不限） |
| `UseEnhancedOptimizer` | bool | `true` | 使用增强优化器 |
