# Native Plugins (DLL/SO)

SQLExec supports dynamically loading data source plugins via shared libraries (DLL/SO/dylib).

## Plugin Interface Specification

Plugins must export the following C functions:

| Export Function | Signature | Description |
|---------|------|------|
| `PluginGetInfo` | `() -> *char` | Returns plugin metadata as JSON |
| `PluginHandleRequest` | `(*char) -> *char` | Handles a JSON-RPC request |
| `PluginFreeString` | `(*char) -> void` | Frees the memory of a returned string |

### PluginGetInfo Return Format

```json
{
  "name": "my_plugin",
  "version": "1.0.0",
  "type": "datasource",
  "description": "My custom data source plugin"
}
```

### PluginHandleRequest JSON-RPC Protocol

Request format:

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

Response format:

```json
{
  "result": {
    "columns": [{"name": "id", "type": "int64"}],
    "rows": [{"id": 1}],
    "total": 1
  }
}
```

## Supported Methods

| Method | Description |
|------|------|
| `create` | Create a data source instance |
| `connect` | Connect |
| `close` | Close the connection |
| `is_connected` | Check connection status |
| `is_writable` | Check write support |
| `get_tables` | Get the list of tables |
| `get_table_info` | Get table structure |
| `query` | Query data |
| `insert` | Insert data |
| `update` | Update data |
| `delete` | Delete data |
| `create_table` | Create a table |
| `drop_table` | Drop a table |
| `truncate_table` | Truncate a table |
| `execute` | Execute SQL |

## Writing a Plugin (Go Example)

```go
package main

import "C"
import (
    "encoding/json"
    "unsafe"
)

// Internal plugin data store
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
    // ... implement other methods
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

func main() {} // Shared libraries must have a main
```

## Compiling

```bash
# Linux
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.so ./plugin/

# Windows
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.dll ./plugin/

# macOS
CGO_ENABLED=1 go build -buildmode=c-shared -o my_plugin.dylib ./plugin/
```

Compilation produces two files: the shared library (`.so`/`.dll`/`.dylib`) and a header file (`.h`).

## Loading Plugins

### Automatic Scanning

Place plugins in the `datasource/` directory and the server will automatically scan and load them on startup:

```
config/
├── config.json
├── datasources.json
└── datasource/          <- Plugin directory
    ├── my_plugin.dll
    └── another_plugin.so
```

### Manual Loading

```go
import "github.com/kasuganosora/sqlexec/pkg/plugin"

pm := plugin.NewPluginManager(registry, dsManager, configDir)

// Load a single plugin
err := pm.LoadPlugin("/path/to/my_plugin.dll")

// Scan a directory and load all plugins
err := pm.ScanAndLoad("/path/to/plugins/")

// View loaded plugins
plugins := pm.GetLoadedPlugins()
```

## Reference

For the complete plugin example code, see `examples/demo_plugin/main.go` in the project.
