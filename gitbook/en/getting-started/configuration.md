# Configuration Guide

SQLExec uses JSON-formatted configuration files.

## Configuration File Search Paths

Configuration files are searched in the following order of priority:

1. Path specified by the `SQLEXEC_CONFIG` environment variable
2. `config.json` in the current directory
3. `./config/config.json`
4. `/etc/sqlexec/config.json`
5. If none are found, built-in defaults are used

## config.json

### Full Configuration Example

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

### Configuration Field Reference

#### server -- Server

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | `"0.0.0.0"` | Listen address |
| `port` | int | `3306` | MySQL protocol port |
| `server_version` | string | `"SqlExc"` | Server version identifier |
| `keep_alive_period` | duration | `"30s"` | TCP Keep-Alive interval |

#### database -- Database

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_connections` | int | `100` | Maximum number of connections |
| `idle_timeout` | int | `3600` | Idle connection timeout (seconds) |
| `enabled_sources` | []string | all | Allowed data source types |

#### cache -- Cache

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query_cache.max_size` | int | `1000` | Query plan cache size |
| `query_cache.ttl` | duration | `"5m"` | Query plan cache TTL |
| `result_cache.max_size` | int | `1000` | Result cache size |
| `result_cache.ttl` | duration | `"10m"` | Result cache TTL |
| `schema_cache.max_size` | int | `100` | Schema cache size |
| `schema_cache.ttl` | duration | `"1h"` | Schema cache TTL |

#### mvcc -- Multi-Version Concurrency Control

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enable_warning` | bool | `true` | Enable MVCC warning logs |
| `auto_downgrade` | bool | `true` | Auto-downgrade isolation level when unsupported |
| `gc_interval` | duration | `"5m"` | Garbage collection interval |
| `gc_age_threshold` | duration | `"1h"` | Retention time for old versions |
| `xid_wrap_threshold` | int | `100000` | Transaction ID wraparound threshold |
| `max_active_txns` | int | `10000` | Maximum active transactions |

#### http_api -- HTTP API

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Whether to enable the HTTP API |
| `host` | string | `"0.0.0.0"` | Listen address |
| `port` | int | `8080` | HTTP port |

#### mcp -- MCP Server

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Whether to enable the MCP Server |
| `host` | string | `"0.0.0.0"` | Listen address |
| `port` | int | `8081` | MCP port |

#### paging -- Paging and Spill-to-Disk

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable paging and spill-to-disk |
| `max_memory_mb` | int | `0` | Memory limit (0 = unlimited) |
| `page_size` | int | `4096` | Page size |
| `spill_dir` | string | `""` | Spill directory |

## datasources.json

Configure external data sources via `datasources.json`. This file should be placed in the same directory as `config.json`.

### Format

```json
[
  {
    "type": "data source type",
    "name": "unique name",
    "host": "host address",
    "port": 3306,
    "username": "username",
    "password": "password",
    "database": "database name or file path",
    "writable": true,
    "options": {}
  }
]
```

### Example

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

Once configured, you can switch to a data source using `USE production`.

## Embedded Configuration

When using SQLExec as an embedded library, configure it via the `DBConfig` struct:

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

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `CacheEnabled` | bool | `true` | Enable query cache |
| `CacheSize` | int | `1000` | Number of cache entries |
| `CacheTTL` | int | `300` | Cache TTL (seconds) |
| `DefaultLogger` | Logger | nil | Logger implementation |
| `DebugMode` | bool | `false` | Debug mode |
| `QueryTimeout` | Duration | `0` | Query timeout (0 = unlimited) |
| `UseEnhancedOptimizer` | bool | `true` | Use the enhanced optimizer |
