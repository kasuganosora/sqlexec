# 原生插件 (DLL/SO)

SQLExec 支持通过共享库（DLL/SO/dylib）动态加载数据源插件。

## 插件接口规范

插件必须导出以下 C 函数：

| 导出函数 | 签名 | 说明 |
|---------|------|------|
| `PluginGetInfo` | `() → *char` | 返回插件元数据 JSON |
| `PluginHandleRequest` | `(*char) → *char` | 处理 JSON-RPC 请求 |
| `PluginFreeString` | `(*char) → void` | 释放返回的字符串内存 |

### PluginGetInfo 返回格式

```json
{
  "name": "my_plugin",
  "version": "1.0.0",
  "type": "datasource",
  "description": "My custom data source plugin"
}
```

### PluginHandleRequest JSON-RPC 协议

请求格式：

```json
{
  "method": "query",
  "params": {
    "table": "my_table",
    "filters": [],
    "limit": 100,
    "offset": 0
  }
}
```

响应格式：

```json
{
  "result": {
    "columns": [{"name": "id", "type": "int64"}],
    "rows": [{"id": 1}],
    "total": 1
  }
}
```

## 支持的方法

| 方法 | 说明 |
|------|------|
| `create` | 创建数据源实例 |
| `connect` | 连接 |
| `close` | 关闭连接 |
| `is_connected` | 检查连接状态 |
| `is_writable` | 检查写入支持 |
| `get_tables` | 获取表列表 |
| `get_table_info` | 获取表结构 |
| `query` | 查询数据 |
| `insert` | 插入数据 |
| `update` | 更新数据 |
| `delete` | 删除数据 |
| `create_table` | 创建表 |
| `drop_table` | 删除表 |
| `truncate_table` | 清空表 |
| `execute` | 执行 SQL |

## 编写插件（Go 示例）

```go
package main

import "C"
import (
    "encoding/json"
    "unsafe"
)

// 插件内部数据存储
var store = make(map[string][]map[string]interface{})

//export PluginGetInfo
func PluginGetInfo() *C.char {
    info := map[string]string{
        "name":        "demo_kv",
        "version":     "1.0.0",
        "type":        "datasource",
        "description": "Demo key-value store plugin",
    }
    data, _ := json.Marshal(info)
    return C.CString(string(data))
}

//export PluginHandleRequest
func PluginHandleRequest(requestJSON *C.char) *C.char {
    req := C.GoString(requestJSON)

    var request struct {
        Method string                 `json:"method"`
        Params map[string]interface{} `json:"params"`
    }
    json.Unmarshal([]byte(req), &request)

    var response interface{}
    switch request.Method {
    case "connect":
        response = map[string]bool{"success": true}
    case "get_tables":
        tables := make([]string, 0, len(store))
        for k := range store {
            tables = append(tables, k)
        }
        response = map[string]interface{}{"tables": tables}
    case "query":
        table := request.Params["table"].(string)
        rows := store[table]
        response = map[string]interface{}{
            "rows":  rows,
            "total": len(rows),
        }
    // ... 实现其他方法
    default:
        response = map[string]string{"error": "unsupported method"}
    }

    data, _ := json.Marshal(map[string]interface{}{"result": response})
    return C.CString(string(data))
}

//export PluginFreeString
func PluginFreeString(str *C.char) {
    C.free(unsafe.Pointer(str))
}

func main() {} // 共享库必须有 main
```

## 编译

```bash
# Linux
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.so ./plugin/

# Windows
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.dll ./plugin/

# macOS
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.dylib ./plugin/
```

编译会生成两个文件：共享库（`.so`/`.dll`/`.dylib`）和头文件（`.h`）。

## 加载插件

### 自动扫描

将插件放到 `datasource/` 目录下，服务器启动时自动扫描加载：

```
config/
├── config.json
├── datasources.json
└── datasource/          ← 插件目录
    ├── my_plugin.dll
    └── another_plugin.so
```

### 手动加载

```go
import "github.com/kasuganosora/sqlexec/pkg/plugin"

pm := plugin.NewPluginManager(registry, dsManager, configDir)

// 加载单个插件
err := pm.LoadPlugin("/path/to/my_plugin.dll")

// 扫描目录加载所有插件
err := pm.ScanAndLoad("/path/to/plugins/")

// 查看已加载的插件
plugins := pm.GetLoadedPlugins()
```

## 参考

完整的插件示例代码见项目 `examples/demo_plugin/main.go`。
