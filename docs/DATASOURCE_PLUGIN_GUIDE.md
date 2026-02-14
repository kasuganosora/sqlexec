# 数据源插件开发指南

本文档描述如何为 sqlexec 开发自定义数据源插件。插件以共享库形式（Linux/macOS 为 `.so`，Windows 为 `.dll`）部署到 `datasource/` 目录，服务器启动时自动扫描加载。

## 目录

- [架构概览](#架构概览)
- [跨平台策略](#跨平台策略)
- [方式一：Go Plugin（Linux / macOS / FreeBSD）](#方式一go-pluginlinux--macos--freebsd)
- [方式二：DLL 插件（Windows）](#方式二dll-插件windows)
- [数据类型参考](#数据类型参考)
- [配置管理](#配置管理)
- [部署指南](#部署指南)
- [FAQ](#faq)

---

## 架构概览

```
┌───────────────────────────────────────────┐
│  Server 启动                               │
│  1. 加载 datasources.json 中的数据源       │
│  2. 扫描 datasource/ 目录                  │
│  3. 按平台选择加载器 (PluginLoader)         │
│  4. 注册工厂到 Registry                    │
│  5. 从 config.datasource 读取配置          │
│  6. 创建并注册插件数据源实例               │
├───────────────────────────────────────────┤
│  PluginManager                             │
│  ├── ScanAndLoad(pluginDir)                │
│  ├── LoadPlugin(path)                      │
│  └── createDatasourcesFromConfig()         │
├───────────────────────────────────────────┤
│            PluginLoader 接口                │
│       ┌─────────┴──────────┐               │
│  GoPluginLoader       DLLPluginLoader      │
│  (Linux/macOS)        (Windows)            │
│  plugin.Open()        syscall.LoadDLL()    │
│  原生 Go 接口          JSON-RPC over C ABI │
├───────────────────────────────────────────┤
│  DataSourceFactory 接口                    │
│  ├── GetType() DataSourceType              │
│  └── Create(config) (DataSource, error)    │
├───────────────────────────────────────────┤
│  DataSource 接口（15 个方法）              │
│  Connect, Close, IsConnected, IsWritable,  │
│  GetConfig, GetTables, GetTableInfo,       │
│  Query, Insert, Update, Delete,            │
│  CreateTable, DropTable, TruncateTable,    │
│  Execute                                   │
└───────────────────────────────────────────┘
```

---

## 跨平台策略

| 平台 | 文件后缀 | 加载机制 | 插件编写方式 |
|------|---------|---------|------------|
| Linux / macOS / FreeBSD | `.so` | Go `plugin.Open()` | 纯 Go 代码 |
| Windows | `.dll` | `syscall.LoadDLL()` | Go + CGo（`c-shared` 模式） |
| 其他 | — | 不支持 | — |

**重要**：两种方式最终都注册一个 `DataSourceFactory` 到系统中，对上层完全透明。

---

## 方式一：Go Plugin（Linux / macOS / FreeBSD）

### 接口规范

插件必须以 Go 包 `main` 编写，并导出以下变量：

| 导出符号 | 类型 | 必需 | 说明 |
|---------|------|------|------|
| `NewFactory` | `func() domain.DataSourceFactory` | **是** | 工厂构造函数 |
| `PluginVersion` | `string` | 否 | 插件版本号 |
| `PluginDescription` | `string` | 否 | 插件描述信息 |

### DataSourceFactory 接口

```go
// 位于 pkg/resource/domain/factory.go
type DataSourceFactory interface {
    // Create 根据配置创建数据源实例
    Create(config *DataSourceConfig) (DataSource, error)

    // GetType 返回此工厂支持的数据源类型标识
    GetType() DataSourceType
}
```

### DataSource 接口

```go
// 位于 pkg/resource/domain/datasource.go
type DataSource interface {
    // 连接管理
    Connect(ctx context.Context) error
    Close(ctx context.Context) error
    IsConnected() bool
    IsWritable() bool
    GetConfig() *DataSourceConfig

    // 表操作
    GetTables(ctx context.Context) ([]string, error)
    GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)
    CreateTable(ctx context.Context, tableInfo *TableInfo) error
    DropTable(ctx context.Context, tableName string) error
    TruncateTable(ctx context.Context, tableName string) error

    // 数据操作
    Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)
    Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)
    Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)
    Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)

    // 原始 SQL 执行
    Execute(ctx context.Context, sql string) (*QueryResult, error)
}
```

### 完整示例

```go
// my_redis_plugin.go
package main

import (
    "context"
    "fmt"

    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ── 可选：插件元数据 ──

var PluginVersion = "1.0.0"
var PluginDescription = "Redis datasource plugin for sqlexec"

// ── 必须导出：工厂构造函数 ──

var NewFactory = func() domain.DataSourceFactory {
    return &RedisFactory{}
}

// ── 工厂实现 ──

type RedisFactory struct{}

func (f *RedisFactory) GetType() domain.DataSourceType {
    return "redis"
}

func (f *RedisFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
    return &RedisDataSource{
        config: config,
    }, nil
}

// ── 数据源实现 ──

type RedisDataSource struct {
    config    *domain.DataSourceConfig
    connected bool
    // ... 你的 Redis 客户端
}

func (ds *RedisDataSource) Connect(ctx context.Context) error {
    host := ds.config.Host
    if host == "" {
        host = "localhost"
    }
    port := ds.config.Port
    if port == 0 {
        port = 6379
    }
    // 在这里初始化 Redis 连接
    // ds.client = redis.NewClient(...)
    ds.connected = true
    return nil
}

func (ds *RedisDataSource) Close(ctx context.Context) error {
    ds.connected = false
    return nil
}

func (ds *RedisDataSource) IsConnected() bool { return ds.connected }
func (ds *RedisDataSource) IsWritable() bool  { return ds.config.Writable }
func (ds *RedisDataSource) GetConfig() *domain.DataSourceConfig { return ds.config }

func (ds *RedisDataSource) GetTables(ctx context.Context) ([]string, error) {
    // Redis 中可以将 key pattern 映射为 "表"
    return []string{"keys"}, nil
}

func (ds *RedisDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
    return &domain.TableInfo{
        Name: tableName,
        Columns: []domain.ColumnInfo{
            {Name: "key", Type: "varchar(255)", Nullable: false, Primary: true},
            {Name: "value", Type: "text", Nullable: true},
            {Name: "ttl", Type: "bigint", Nullable: true},
        },
    }, nil
}

func (ds *RedisDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
    // 实现查询逻辑
    return &domain.QueryResult{
        Columns: []domain.ColumnInfo{
            {Name: "key", Type: "varchar(255)"},
            {Name: "value", Type: "text"},
        },
        Rows:  []domain.Row{},
        Total: 0,
    }, nil
}

func (ds *RedisDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
    // 实现插入逻辑（SET key value）
    return int64(len(rows)), nil
}

func (ds *RedisDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
    return 0, fmt.Errorf("update not supported for Redis")
}

func (ds *RedisDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
    // 实现删除逻辑（DEL key）
    return 0, nil
}

func (ds *RedisDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
    return nil // Redis 无需建表
}

func (ds *RedisDataSource) DropTable(ctx context.Context, tableName string) error {
    return nil
}

func (ds *RedisDataSource) TruncateTable(ctx context.Context, tableName string) error {
    // FLUSHDB
    return nil
}

func (ds *RedisDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
    return nil, fmt.Errorf("raw SQL not supported for Redis")
}
```

### 编译

```bash
# Linux / macOS
go build -buildmode=plugin -o redis_plugin.so my_redis_plugin.go

# 部署
cp redis_plugin.so /path/to/sqlexec/datasource/
```

**注意事项**：
- Go plugin 要求插件与主程序使用**完全相同的 Go 版本**编译。
- 所有共享依赖的版本必须一致（`go.mod` 中的间接依赖也算）。
- 建议在与主程序相同的构建环境中编译插件。

---

## 方式二：DLL 插件（Windows）

Windows 不支持 Go 的 `plugin` 包，因此采用 C-shared 模式 + JSON-RPC 协议：

- DLL 导出 3 个 C 函数
- 所有数据交换通过 JSON 字符串
- 服务端有 `DLLDataSource` 适配器自动将 Go 接口调用转换为 JSON-RPC

### 导出函数规范

DLL **必须**导出以下 3 个函数：

#### 1. `PluginGetInfo`

```c
// 返回插件元信息（JSON 字符串）
// 调用方会使用 PluginFreeString 释放返回值
char* PluginGetInfo();
```

返回值格式：

```json
{
    "type": "my_custom_db",
    "version": "1.0.0",
    "description": "My custom datasource plugin"
}
```

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `type` | string | **是** | 数据源类型标识（唯一） |
| `version` | string | 否 | 版本号 |
| `description` | string | 否 | 描述信息 |

#### 2. `PluginHandleRequest`

```c
// 处理来自 server 的 JSON-RPC 请求
// requestJSON: 请求的 JSON 字符串（以 null 结尾的 C 字符串）
// 返回: 响应的 JSON 字符串（由插件分配内存）
// 调用方会使用 PluginFreeString 释放返回值
char* PluginHandleRequest(char* requestJSON);
```

#### 3. `PluginFreeString`

```c
// 释放由插件分配的字符串内存
// 所有由 PluginGetInfo 和 PluginHandleRequest 返回的字符串
// 都必须通过此函数释放
void PluginFreeString(char* s);
```

### JSON-RPC 协议

#### 请求格式

```json
{
    "method": "方法名",
    "id": "实例ID",
    "params": { ... }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `method` | string | 要调用的方法（见下方方法列表） |
| `id` | string | 数据源实例 ID（由 `create` 时的 `config.name` 确定） |
| `params` | object | 方法参数 |

#### 响应格式

```json
{
    "result": { ... },
    "error": ""
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `result` | any | 方法返回的结果（成功时） |
| `error` | string | 错误消息（为空表示成功） |

### 方法列表

#### `create` — 创建数据源实例

由 `DataSourceFactory.Create()` 触发。插件应根据 config 初始化内部状态。

请求：
```json
{
    "method": "create",
    "id": "my_ds_1",
    "params": {
        "config": {
            "type": "my_custom_db",
            "name": "my_ds_1",
            "host": "localhost",
            "port": 9999,
            "username": "admin",
            "password": "secret",
            "database": "mydb",
            "writable": true,
            "options": {
                "custom_option": "value"
            }
        }
    }
}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `connect` — 连接数据源

请求：
```json
{"method": "connect", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `close` — 关闭连接

请求：
```json
{"method": "close", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `is_connected` — 检查连接状态

请求：
```json
{"method": "is_connected", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {"connected": true}, "error": ""}
```

#### `is_writable` — 检查可写性

请求：
```json
{"method": "is_writable", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {"writable": true}, "error": ""}
```

#### `get_tables` — 获取表列表

请求：
```json
{"method": "get_tables", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {"tables": ["users", "orders"]}, "error": ""}
```

#### `get_table_info` — 获取表结构

请求：
```json
{"method": "get_table_info", "id": "my_ds_1", "params": {"table": "users"}}
```

响应：
```json
{
    "result": {
        "name": "users",
        "columns": [
            {"name": "id", "type": "bigint", "nullable": false, "primary": true, "auto_increment": true},
            {"name": "name", "type": "varchar(100)", "nullable": false},
            {"name": "email", "type": "varchar(255)", "nullable": true}
        ]
    },
    "error": ""
}
```

列信息字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 列名 |
| `type` | string | SQL 类型（如 `varchar(255)`, `bigint`, `text`） |
| `nullable` | bool | 是否可为 NULL |
| `primary` | bool | 是否主键 |
| `default` | string | 默认值 |
| `unique` | bool | 是否唯一约束 |
| `auto_increment` | bool | 是否自动递增 |

#### `query` — 查询数据

请求：
```json
{
    "method": "query",
    "id": "my_ds_1",
    "params": {
        "table": "users",
        "options": {
            "filters": [
                {"field": "name", "operator": "=", "value": "alice"}
            ],
            "order_by": "id",
            "order": "ASC",
            "limit": 10,
            "offset": 0,
            "select_columns": ["id", "name", "email"]
        }
    }
}
```

响应：
```json
{
    "result": {
        "columns": [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "varchar(100)"},
            {"name": "email", "type": "varchar(255)"}
        ],
        "rows": [
            {"id": 1, "name": "alice", "email": "alice@example.com"}
        ],
        "total": 1
    },
    "error": ""
}
```

Filter 操作符：`=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `BETWEEN`

#### `insert` — 插入数据

请求：
```json
{
    "method": "insert",
    "id": "my_ds_1",
    "params": {
        "table": "users",
        "rows": [
            {"name": "bob", "email": "bob@example.com"},
            {"name": "carol", "email": "carol@example.com"}
        ],
        "options": {"replace": false}
    }
}
```

响应：
```json
{"result": {"affected": 2}, "error": ""}
```

#### `update` — 更新数据

请求：
```json
{
    "method": "update",
    "id": "my_ds_1",
    "params": {
        "table": "users",
        "filters": [
            {"field": "name", "operator": "=", "value": "bob"}
        ],
        "updates": {"email": "bob_new@example.com"},
        "options": {}
    }
}
```

响应：
```json
{"result": {"affected": 1}, "error": ""}
```

#### `delete` — 删除数据

请求：
```json
{
    "method": "delete",
    "id": "my_ds_1",
    "params": {
        "table": "users",
        "filters": [
            {"field": "id", "operator": "=", "value": 1}
        ],
        "options": {}
    }
}
```

响应：
```json
{"result": {"affected": 1}, "error": ""}
```

#### `create_table` — 创建表

请求：
```json
{
    "method": "create_table",
    "id": "my_ds_1",
    "params": {
        "table_info": {
            "name": "products",
            "columns": [
                {"name": "id", "type": "bigint", "nullable": false, "primary": true, "auto_increment": true},
                {"name": "name", "type": "varchar(200)", "nullable": false},
                {"name": "price", "type": "decimal(10,2)", "nullable": false}
            ]
        }
    }
}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `drop_table` — 删除表

请求：
```json
{"method": "drop_table", "id": "my_ds_1", "params": {"table": "products"}}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `truncate_table` — 清空表

请求：
```json
{"method": "truncate_table", "id": "my_ds_1", "params": {"table": "products"}}
```

响应：
```json
{"result": {}, "error": ""}
```

#### `execute` — 执行原始 SQL

请求：
```json
{"method": "execute", "id": "my_ds_1", "params": {"sql": "SELECT 1"}}
```

响应：
```json
{
    "result": {
        "columns": [{"name": "1", "type": "int"}],
        "rows": [{"1": 1}],
        "total": 1
    },
    "error": ""
}
```

#### `destroy` — 销毁实例

可选。服务器关闭时可能调用。

请求：
```json
{"method": "destroy", "id": "my_ds_1", "params": null}
```

响应：
```json
{"result": {}, "error": ""}
```

### DLL 完整示例（Go + CGo）

```go
// my_plugin.go
package main

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
    "encoding/json"
    "sync"
    "unsafe"
)

// ── 实例管理 ──

var (
    instances = make(map[string]*MyDataSource)
    mu        sync.Mutex
)

// ── 数据源实现 ──

type MyDataSource struct {
    ID        string
    Config    map[string]interface{}
    Connected bool
    Tables    map[string][]map[string]interface{} // table -> rows
}

// ── 导出函数 ──

//export PluginGetInfo
func PluginGetInfo() *C.char {
    info := map[string]string{
        "type":        "my_custom_db",
        "version":     "1.0.0",
        "description": "My custom datasource plugin",
    }
    data, _ := json.Marshal(info)
    return C.CString(string(data))
}

//export PluginHandleRequest
func PluginHandleRequest(reqC *C.char) *C.char {
    reqJSON := C.GoString(reqC)

    var req struct {
        Method string                 `json:"method"`
        ID     string                 `json:"id"`
        Params map[string]interface{} `json:"params"`
    }
    if err := json.Unmarshal([]byte(reqJSON), &req); err != nil {
        return makeErrorResponse("invalid request JSON: " + err.Error())
    }

    switch req.Method {
    case "create":
        return handleCreate(req.ID, req.Params)
    case "connect":
        return handleConnect(req.ID)
    case "close":
        return handleClose(req.ID)
    case "is_connected":
        return handleIsConnected(req.ID)
    case "is_writable":
        return makeResponse(map[string]interface{}{"writable": true})
    case "get_tables":
        return handleGetTables(req.ID)
    case "get_table_info":
        table, _ := req.Params["table"].(string)
        return handleGetTableInfo(req.ID, table)
    case "query":
        return handleQuery(req.ID, req.Params)
    case "insert":
        return handleInsert(req.ID, req.Params)
    case "update":
        return handleUpdate(req.ID, req.Params)
    case "delete":
        return handleDelete(req.ID, req.Params)
    case "create_table":
        return handleCreateTable(req.ID, req.Params)
    case "drop_table":
        table, _ := req.Params["table"].(string)
        return handleDropTable(req.ID, table)
    case "truncate_table":
        table, _ := req.Params["table"].(string)
        return handleTruncateTable(req.ID, table)
    case "execute":
        sql, _ := req.Params["sql"].(string)
        return handleExecute(req.ID, sql)
    case "destroy":
        return handleDestroy(req.ID)
    default:
        return makeErrorResponse("unknown method: " + req.Method)
    }
}

//export PluginFreeString
func PluginFreeString(s *C.char) {
    C.free(unsafe.Pointer(s))
}

// ── 辅助函数 ──

func makeResponse(result interface{}) *C.char {
    resp := map[string]interface{}{"result": result, "error": ""}
    data, _ := json.Marshal(resp)
    return C.CString(string(data))
}

func makeErrorResponse(errMsg string) *C.char {
    resp := map[string]interface{}{"result": nil, "error": errMsg}
    data, _ := json.Marshal(resp)
    return C.CString(string(data))
}

// ── 方法实现（简化版） ──

func handleCreate(id string, params map[string]interface{}) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds := &MyDataSource{
        ID:     id,
        Config: params,
        Tables: make(map[string][]map[string]interface{}),
    }
    instances[id] = ds
    return makeResponse(map[string]interface{}{})
}

func handleConnect(id string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }
    ds.Connected = true
    return makeResponse(map[string]interface{}{})
}

func handleClose(id string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }
    ds.Connected = false
    return makeResponse(map[string]interface{}{})
}

func handleIsConnected(id string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeResponse(map[string]interface{}{"connected": false})
    }
    return makeResponse(map[string]interface{}{"connected": ds.Connected})
}

func handleGetTables(id string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    tables := make([]string, 0, len(ds.Tables))
    for t := range ds.Tables {
        tables = append(tables, t)
    }
    return makeResponse(map[string]interface{}{"tables": tables})
}

func handleGetTableInfo(id, table string) *C.char {
    // 返回示例表结构
    return makeResponse(map[string]interface{}{
        "name": table,
        "columns": []map[string]interface{}{
            {"name": "id", "type": "bigint", "nullable": false, "primary": true},
            {"name": "data", "type": "text", "nullable": true},
        },
    })
}

func handleQuery(id string, params map[string]interface{}) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    table, _ := params["table"].(string)
    rows, _ := ds.Tables[table]

    return makeResponse(map[string]interface{}{
        "columns": []map[string]interface{}{
            {"name": "id", "type": "bigint"},
            {"name": "data", "type": "text"},
        },
        "rows":  rows,
        "total": len(rows),
    })
}

func handleInsert(id string, params map[string]interface{}) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    table, _ := params["table"].(string)
    rowsRaw, _ := params["rows"].([]interface{})

    for _, r := range rowsRaw {
        if row, ok := r.(map[string]interface{}); ok {
            ds.Tables[table] = append(ds.Tables[table], row)
        }
    }

    return makeResponse(map[string]interface{}{"affected": len(rowsRaw)})
}

func handleUpdate(id string, params map[string]interface{}) *C.char {
    // 简化实现
    return makeResponse(map[string]interface{}{"affected": 0})
}

func handleDelete(id string, params map[string]interface{}) *C.char {
    // 简化实现
    return makeResponse(map[string]interface{}{"affected": 0})
}

func handleCreateTable(id string, params map[string]interface{}) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    if info, ok := params["table_info"].(map[string]interface{}); ok {
        if name, ok := info["name"].(string); ok {
            ds.Tables[name] = []map[string]interface{}{}
        }
    }
    return makeResponse(map[string]interface{}{})
}

func handleDropTable(id, table string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    delete(ds.Tables, table)
    return makeResponse(map[string]interface{}{})
}

func handleTruncateTable(id, table string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    ds, ok := instances[id]
    if !ok {
        return makeErrorResponse("instance not found: " + id)
    }

    ds.Tables[table] = []map[string]interface{}{}
    return makeResponse(map[string]interface{}{})
}

func handleExecute(id, sql string) *C.char {
    return makeErrorResponse("raw SQL execution not supported")
}

func handleDestroy(id string) *C.char {
    mu.Lock()
    defer mu.Unlock()

    delete(instances, id)
    return makeResponse(map[string]interface{}{})
}

func main() {} // c-shared 模式必须有 main 函数
```

### 编译（Windows）

```powershell
# 需要 CGO_ENABLED=1 和 C 编译器（如 MinGW-w64）
set CGO_ENABLED=1
go build -buildmode=c-shared -o my_plugin.dll my_plugin.go
```

编译会同时生成 `my_plugin.dll` 和 `my_plugin.h`（C 头文件）。

### 编译（跨平台编译 DLL）

```bash
# 从 Linux 交叉编译到 Windows（需要 mingw-w64）
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc \
  go build -buildmode=c-shared -o my_plugin.dll my_plugin.go
```

---

## 数据类型参考

### DataSourceConfig

```json
{
    "type":     "string — 数据源类型标识（对应插件 GetType() 返回值）",
    "name":     "string — 数据源实例名称（唯一标识）",
    "host":     "string — 主机地址（可选）",
    "port":     "int    — 端口号（可选）",
    "username": "string — 用户名（可选）",
    "password": "string — 密码（可选）",
    "database": "string — 数据库名（可选）",
    "writable": "bool   — 是否可写",
    "options":  "object — 自定义选项键值对（可选）"
}
```

### QueryOptions

```json
{
    "filters": [
        {
            "field":    "列名",
            "operator": "操作符（=, !=, >, <, >=, <=, LIKE, IN, BETWEEN）",
            "value":    "过滤值"
        }
    ],
    "order_by": "排序列名",
    "order":    "ASC 或 DESC",
    "limit":    10,
    "offset":   0,
    "select_columns": ["id", "name"]
}
```

### QueryResult

```json
{
    "columns": [
        {"name": "id", "type": "bigint", "nullable": false, "primary": true}
    ],
    "rows": [
        {"id": 1, "name": "alice"}
    ],
    "total": 1
}
```

### Row

行数据是一个简单的 `map[string]interface{}`（JSON 中为 `object`），键为列名，值为列值。

支持的值类型：`string`, `int64` / `float64`, `bool`, `null`

---

## 配置管理

插件数据源的配置通过 `config.datasource` 虚拟表管理。

### 添加插件数据源配置

```sql
-- 连接到 config 虚拟数据库
USE config;

-- 添加配置
INSERT INTO datasource (name, type, host, port, username, password, database_name, writable)
VALUES ('my_redis', 'redis', 'localhost', 6379, '', '', '0', true);
```

配置会持久化到 `datasources.json` 文件。服务器下次启动时，如果插件已加载且配置存在，会自动创建对应的数据源实例。

### 查看已配置的数据源

```sql
USE config;
SELECT * FROM datasource;
```

### 修改配置

```sql
USE config;
UPDATE datasource SET host = '192.168.1.100', port = 6380 WHERE name = 'my_redis';
```

### 删除配置

```sql
USE config;
DELETE FROM datasource WHERE name = 'my_redis';
```

### 配置加载顺序

1. 服务器启动 → 加载 `datasources.json` 中的**内置类型**数据源
2. 扫描 `datasource/` 目录 → 加载插件 → 注册工厂到 Registry
3. 再次读取 `datasources.json` → 为匹配插件类型的配置创建数据源实例

---

## 部署指南

### 目录结构

```
sqlexec/
├── datasource/                 ← 插件目录
│   ├── redis_plugin.so         ← Linux/macOS 插件
│   ├── redis_plugin.dll        ← Windows 插件
│   └── elasticsearch_plugin.so
├── datasources.json            ← 数据源配置（由 config.datasource 管理）
├── acl.json                    ← ACL 配置
└── sqlexec                     ← 主程序
```

### 部署步骤

1. **编译插件**：使用与主程序相同的 Go 版本和依赖版本编译
2. **放置文件**：将编译好的 `.so` 或 `.dll` 文件放入 `datasource/` 目录
3. **添加配置**：通过 SQL 或直接编辑 `datasources.json` 添加数据源配置
4. **重启服务器**：服务器启动时自动加载插件

### datasources.json 格式

```json
[
    {
        "type": "redis",
        "name": "my_redis",
        "host": "localhost",
        "port": 6379,
        "writable": true,
        "options": {
            "pool_size": 10
        }
    }
]
```

---

## FAQ

### Q: Go Plugin 和 DLL 模式有什么区别？

| 特性 | Go Plugin (.so) | DLL (.dll) |
|------|----------------|------------|
| 平台 | Linux / macOS / FreeBSD | Windows |
| 性能 | 原生 Go 调用，零序列化开销 | JSON 序列化/反序列化开销 |
| Go 版本约束 | 必须与主程序完全一致 | 仅需 C ABI 兼容 |
| 依赖约束 | 所有 Go 依赖版本必须一致 | 无（JSON 隔离） |
| 开发复杂度 | 低（纯 Go） | 中（需要 CGo） |
| 非 Go 语言 | 不支持 | 支持（任何能生成 DLL 的语言） |

### Q: 插件可以用非 Go 语言编写吗？

**DLL 模式**可以。只要能编译出导出 `PluginGetInfo`、`PluginHandleRequest`、`PluginFreeString` 三个 C 函数的 DLL 即可。例如 C、C++、Rust（通过 `extern "C"`）。

**Go Plugin 模式**仅支持 Go。

### Q: 如何调试插件？

1. **日志输出**：在插件中使用 `log.Printf` 或写入日志文件
2. **DLL 模式**：可以独立测试 `PluginHandleRequest`，传入 JSON 字符串验证响应
3. **Go Plugin 模式**：建议先编写为普通 Go 包进行单元测试，确认无误后再编译为 plugin

### Q: 热加载 / 热更新？

目前不支持热加载。更新插件需要重启服务器。

### Q: 多个插件可以注册同一个 type 吗？

不可以。每个 `DataSourceType` 只能注册一次。如果有冲突，后加载的插件会跳过并打印警告日志。

### Q: options 字段如何传递自定义配置？

`DataSourceConfig.Options` 是一个 `map[string]interface{}`，可以放任何自定义键值对。插件在 `create` 方法中通过 `config.options` 获取。

```json
{
    "type": "my_custom_db",
    "name": "instance1",
    "options": {
        "connection_pool_size": 20,
        "timeout_ms": 5000,
        "custom_feature_enabled": true
    }
}
```
