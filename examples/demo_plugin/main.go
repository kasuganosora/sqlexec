// Demo datasource plugin for testing the plugin loading system.
// This plugin implements an in-memory key-value store with table support.
//
// Build: CGO_ENABLED=1 go build -buildmode=c-shared -o demo_plugin.dll ./examples/demo_plugin/
package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"
)

// ── Instance management ──

var (
	instances = make(map[string]*Instance)
	mu        sync.Mutex
)

// Instance represents a single datasource instance
type Instance struct {
	Config    map[string]interface{}
	Connected bool
	Writable  bool
	Tables    map[string]*Table
}

// Table represents a table with schema and rows
type Table struct {
	Name    string                   `json:"name"`
	Columns []map[string]interface{} `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
}

// ── Exported C functions ──

//export PluginGetInfo
func PluginGetInfo() *C.char {
	info := map[string]interface{}{
		"type":        "demo",
		"version":     "1.0.0",
		"description": "Demo in-memory datasource plugin for testing",
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
		return errResp("invalid request JSON: " + err.Error())
	}

	mu.Lock()
	defer mu.Unlock()

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
		return handleIsWritable(req.ID)
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
		return handleExecute(req.ID, req.Params)
	case "destroy":
		return handleDestroy(req.ID)
	default:
		return errResp("unknown method: " + req.Method)
	}
}

//export PluginFreeString
func PluginFreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// ── Response helpers ──

func okResp(result interface{}) *C.char {
	resp := map[string]interface{}{"result": result, "error": ""}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

func errResp(msg string) *C.char {
	resp := map[string]interface{}{"result": nil, "error": msg}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

func getInstance(id string) (*Instance, *C.char) {
	inst, ok := instances[id]
	if !ok {
		return nil, errResp("instance not found: " + id)
	}
	return inst, nil
}

// ── Method handlers ──

func handleCreate(id string, params map[string]interface{}) *C.char {
	writable := true
	if cfg, ok := params["config"].(map[string]interface{}); ok {
		if w, ok := cfg["writable"].(bool); ok {
			writable = w
		}
	}
	instances[id] = &Instance{
		Config:    params,
		Connected: false,
		Writable:  writable,
		Tables:    make(map[string]*Table),
	}
	return okResp(map[string]interface{}{})
}

func handleConnect(id string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	inst.Connected = true
	return okResp(map[string]interface{}{})
}

func handleClose(id string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	inst.Connected = false
	return okResp(map[string]interface{}{})
}

func handleIsConnected(id string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	return okResp(map[string]interface{}{"connected": inst.Connected})
}

func handleIsWritable(id string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	return okResp(map[string]interface{}{"writable": inst.Writable})
}

func handleGetTables(id string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tables := make([]string, 0, len(inst.Tables))
	for name := range inst.Tables {
		tables = append(tables, name)
	}
	return okResp(map[string]interface{}{"tables": tables})
}

func handleGetTableInfo(id, tableName string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}
	return okResp(map[string]interface{}{
		"name":    tbl.Name,
		"columns": tbl.Columns,
	})
}

func handleQuery(id string, params map[string]interface{}) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tableName, _ := params["table"].(string)
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}

	// Apply filters if present
	rows := tbl.Rows
	if opts, ok := params["options"].(map[string]interface{}); ok {
		rows = applyFilters(rows, opts)
		rows = applyLimitOffset(rows, opts)
	}

	return okResp(map[string]interface{}{
		"columns": tbl.Columns,
		"rows":    rows,
		"total":   len(rows),
	})
}

func handleInsert(id string, params map[string]interface{}) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tableName, _ := params["table"].(string)
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}

	rawRows, _ := params["rows"].([]interface{})
	count := 0
	for _, r := range rawRows {
		if row, ok := r.(map[string]interface{}); ok {
			tbl.Rows = append(tbl.Rows, row)
			count++
		}
	}
	return okResp(map[string]interface{}{"affected": count})
}

func handleUpdate(id string, params map[string]interface{}) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tableName, _ := params["table"].(string)
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}

	filters := extractFilters(params)
	updates, _ := params["updates"].(map[string]interface{})

	affected := 0
	for i, row := range tbl.Rows {
		if matchFilters(row, filters) {
			for k, v := range updates {
				tbl.Rows[i][k] = v
			}
			affected++
		}
	}
	return okResp(map[string]interface{}{"affected": affected})
}

func handleDelete(id string, params map[string]interface{}) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tableName, _ := params["table"].(string)
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}

	filters := extractFilters(params)
	kept := make([]map[string]interface{}, 0)
	affected := 0
	for _, row := range tbl.Rows {
		if matchFilters(row, filters) {
			affected++
		} else {
			kept = append(kept, row)
		}
	}
	tbl.Rows = kept
	return okResp(map[string]interface{}{"affected": affected})
}

func handleCreateTable(id string, params map[string]interface{}) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}

	info, _ := params["table_info"].(map[string]interface{})
	name, _ := info["name"].(string)
	if name == "" {
		return errResp("table name is required")
	}

	if _, exists := inst.Tables[name]; exists {
		return errResp("table already exists: " + name)
	}

	// Parse columns
	var columns []map[string]interface{}
	if rawCols, ok := info["columns"].([]interface{}); ok {
		for _, c := range rawCols {
			if col, ok := c.(map[string]interface{}); ok {
				columns = append(columns, col)
			}
		}
	}

	inst.Tables[name] = &Table{
		Name:    name,
		Columns: columns,
		Rows:    make([]map[string]interface{}, 0),
	}
	return okResp(map[string]interface{}{})
}

func handleDropTable(id, tableName string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	if _, ok := inst.Tables[tableName]; !ok {
		return errResp("table not found: " + tableName)
	}
	delete(inst.Tables, tableName)
	return okResp(map[string]interface{}{})
}

func handleTruncateTable(id, tableName string) *C.char {
	inst, e := getInstance(id)
	if inst == nil {
		return e
	}
	tbl, ok := inst.Tables[tableName]
	if !ok {
		return errResp("table not found: " + tableName)
	}
	tbl.Rows = make([]map[string]interface{}, 0)
	return okResp(map[string]interface{}{})
}

func handleExecute(id string, params map[string]interface{}) *C.char {
	sql, _ := params["sql"].(string)
	return okResp(map[string]interface{}{
		"columns": []map[string]interface{}{
			{"name": "result", "type": "varchar(255)"},
		},
		"rows": []map[string]interface{}{
			{"result": fmt.Sprintf("executed: %s", sql)},
		},
		"total": 1,
	})
}

func handleDestroy(id string) *C.char {
	delete(instances, id)
	return okResp(map[string]interface{}{})
}

// ── Filter helpers ──

func extractFilters(params map[string]interface{}) []map[string]interface{} {
	var filters []map[string]interface{}
	if rawFilters, ok := params["filters"].([]interface{}); ok {
		for _, f := range rawFilters {
			if filter, ok := f.(map[string]interface{}); ok {
				filters = append(filters, filter)
			}
		}
	}
	return filters
}

func matchFilters(row map[string]interface{}, filters []map[string]interface{}) bool {
	for _, f := range filters {
		field, _ := f["field"].(string)
		op, _ := f["operator"].(string)
		value := f["value"]

		rowVal, exists := row[field]
		if !exists {
			return false
		}

		switch op {
		case "=":
			if !valuesEqual(rowVal, value) {
				return false
			}
		case "!=":
			if valuesEqual(rowVal, value) {
				return false
			}
		default:
			// For unsupported operators, treat as no match
			return false
		}
	}
	return true
}

func valuesEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func applyFilters(rows []map[string]interface{}, opts map[string]interface{}) []map[string]interface{} {
	rawFilters, ok := opts["filters"].([]interface{})
	if !ok || len(rawFilters) == 0 {
		return rows
	}

	var filters []map[string]interface{}
	for _, f := range rawFilters {
		if filter, ok := f.(map[string]interface{}); ok {
			filters = append(filters, filter)
		}
	}

	var result []map[string]interface{}
	for _, row := range rows {
		if matchFilters(row, filters) {
			result = append(result, row)
		}
	}
	return result
}

func applyLimitOffset(rows []map[string]interface{}, opts map[string]interface{}) []map[string]interface{} {
	offset := 0
	limit := 0
	if o, ok := opts["offset"].(float64); ok {
		offset = int(o)
	}
	if l, ok := opts["limit"].(float64); ok {
		limit = int(l)
	}

	if offset > len(rows) {
		return nil
	}
	if offset > 0 {
		rows = rows[offset:]
	}
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func main() {}
