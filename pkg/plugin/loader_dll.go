//go:build windows

package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"syscall"
	"unsafe"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// DLLPluginLoader loads DLL plugins on Windows
type DLLPluginLoader struct{}

func newPlatformLoader() PluginLoader {
	return &DLLPluginLoader{}
}

// SupportedExtension returns ".dll"
func (l *DLLPluginLoader) SupportedExtension() string {
	return ".dll"
}

// Load loads a DLL plugin from the given file path
// The DLL must export: PluginGetInfo, PluginHandleRequest, PluginFreeString
func (l *DLLPluginLoader) Load(path string) (domain.DataSourceFactory, PluginInfo, error) {
	dll, err := syscall.LoadDLL(path)
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("failed to load DLL '%s': %w", path, err)
	}

	// Find required exports
	getInfoProc, err := dll.FindProc("PluginGetInfo")
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s' missing 'PluginGetInfo' export: %w", path, err)
	}

	handleRequestProc, err := dll.FindProc("PluginHandleRequest")
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s' missing 'PluginHandleRequest' export: %w", path, err)
	}

	freeStringProc, err := dll.FindProc("PluginFreeString")
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s' missing 'PluginFreeString' export: %w", path, err)
	}

	// Call PluginGetInfo to get plugin metadata
	ret, _, _ := getInfoProc.Call()
	if ret == 0 {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s': PluginGetInfo returned null", path)
	}

	infoJSON := cStringToGoString(ret)
	freeStringProc.Call(ret)

	var info PluginInfo
	if err := json.Unmarshal([]byte(infoJSON), &info); err != nil {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s': failed to parse plugin info: %w", path, err)
	}
	info.FilePath = path

	if info.Type == "" {
		return nil, PluginInfo{}, fmt.Errorf("DLL '%s': plugin info missing 'type' field", path)
	}

	// Create a DLL-backed factory
	factory := &DLLDataSourceFactory{
		dll:              dll,
		handleRequestProc: handleRequestProc,
		freeStringProc:    freeStringProc,
		pluginType:        info.Type,
	}

	return factory, info, nil
}

// cStringToGoString converts a C string pointer to a Go string.
// maxLen prevents unbounded reads on corrupt memory.
const cStringMaxLen = 1 << 20 // 1 MB

func cStringToGoString(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}
	// Read bytes until null terminator, with a safety limit
	var bytes []byte
	for i := 0; i < cStringMaxLen; i++ {
		b := *(*byte)(unsafe.Pointer(ptr))
		if b == 0 {
			break
		}
		bytes = append(bytes, b)
		ptr++
	}
	return string(bytes)
}

// DLLDataSourceFactory implements DataSourceFactory by delegating to DLL calls
type DLLDataSourceFactory struct {
	dll               *syscall.DLL
	handleRequestProc *syscall.Proc
	freeStringProc    *syscall.Proc
	pluginType        domain.DataSourceType
}

// GetType returns the datasource type handled by this plugin
func (f *DLLDataSourceFactory) GetType() domain.DataSourceType {
	return f.pluginType
}

// GetMetadata returns the driver metadata for information_schema.ENGINES
func (f *DLLDataSourceFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "Plugin-based storage engine",
		Transactions: "NO",
		XA:           "NO",
		Savepoints:   "NO",
	}
}

// Create creates a new DLL-backed datasource
func (f *DLLDataSourceFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	ds := &DLLDataSource{
		handleRequestProc: f.handleRequestProc,
		freeStringProc:    f.freeStringProc,
		config:            config,
		instanceID:        config.Name,
	}

	// Send create request to the DLL
	resp, err := ds.callDLL("create", map[string]interface{}{
		"config": config,
	})
	if err != nil {
		return nil, fmt.Errorf("DLL create failed: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("DLL create error: %s", resp.Error)
	}

	return ds, nil
}

// DLLDataSource implements domain.DataSource by delegating to DLL via JSON-RPC
type DLLDataSource struct {
	handleRequestProc *syscall.Proc
	freeStringProc    *syscall.Proc
	config            *domain.DataSourceConfig
	instanceID        string
	connected         bool
}

// callDLL sends a JSON-RPC request to the DLL and returns the response
func (ds *DLLDataSource) callDLL(method string, params map[string]interface{}) (*PluginResponse, error) {
	req := PluginRequest{
		Method: method,
		ID:     ds.instanceID,
		Params: params,
	}

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Convert Go string to C string
	cReq, err := syscall.BytePtrFromString(string(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to create C string: %w", err)
	}

	ret, _, _ := ds.handleRequestProc.Call(uintptr(unsafe.Pointer(cReq)))
	if ret == 0 {
		return nil, fmt.Errorf("DLL returned null response for method '%s'", method)
	}

	respJSON := cStringToGoString(ret)
	ds.freeStringProc.Call(ret)

	var resp PluginResponse
	if err := json.Unmarshal([]byte(respJSON), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse DLL response: %w", err)
	}

	return &resp, nil
}

// Connect connects the datasource
func (ds *DLLDataSource) Connect(ctx context.Context) error {
	resp, err := ds.callDLL("connect", nil)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	ds.connected = true
	return nil
}

// Close closes the datasource
func (ds *DLLDataSource) Close(ctx context.Context) error {
	resp, err := ds.callDLL("close", nil)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	ds.connected = false
	return nil
}

// IsConnected returns whether the datasource is connected
func (ds *DLLDataSource) IsConnected() bool {
	resp, err := ds.callDLL("is_connected", nil)
	if err != nil {
		return ds.connected
	}
	if result, ok := resp.Result.(map[string]interface{}); ok {
		if c, ok := result["connected"].(bool); ok {
			return c
		}
	}
	return ds.connected
}

// IsWritable returns whether the datasource is writable
func (ds *DLLDataSource) IsWritable() bool {
	resp, err := ds.callDLL("is_writable", nil)
	if err != nil {
		return false
	}
	if result, ok := resp.Result.(map[string]interface{}); ok {
		if w, ok := result["writable"].(bool); ok {
			return w
		}
	}
	return false
}

// GetConfig returns the datasource config
func (ds *DLLDataSource) GetConfig() *domain.DataSourceConfig {
	return ds.config
}

// GetTables returns all table names
func (ds *DLLDataSource) GetTables(ctx context.Context) ([]string, error) {
	resp, err := ds.callDLL("get_tables", nil)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	// Parse result
	data, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, err
	}

	var result struct {
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result.Tables, nil
}

// GetTableInfo returns table schema information
func (ds *DLLDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	resp, err := ds.callDLL("get_table_info", map[string]interface{}{
		"table": tableName,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	data, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, err
	}

	var info domain.TableInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Query executes a query
func (ds *DLLDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	resp, err := ds.callDLL("query", map[string]interface{}{
		"table":   tableName,
		"options": options,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	data, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, err
	}

	var result domain.QueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Insert inserts rows
func (ds *DLLDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	resp, err := ds.callDLL("insert", map[string]interface{}{
		"table":   tableName,
		"rows":    rows,
		"options": options,
	})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}

	if result, ok := resp.Result.(map[string]interface{}); ok {
		if affected, ok := result["affected"].(float64); ok {
			return int64(affected), nil
		}
	}
	return 0, nil
}

// Update updates rows
func (ds *DLLDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	resp, err := ds.callDLL("update", map[string]interface{}{
		"table":   tableName,
		"filters": filters,
		"updates": updates,
		"options": options,
	})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}

	if result, ok := resp.Result.(map[string]interface{}); ok {
		if affected, ok := result["affected"].(float64); ok {
			return int64(affected), nil
		}
	}
	return 0, nil
}

// Delete deletes rows
func (ds *DLLDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	resp, err := ds.callDLL("delete", map[string]interface{}{
		"table":   tableName,
		"filters": filters,
		"options": options,
	})
	if err != nil {
		return 0, err
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("%s", resp.Error)
	}

	if result, ok := resp.Result.(map[string]interface{}); ok {
		if affected, ok := result["affected"].(float64); ok {
			return int64(affected), nil
		}
	}
	return 0, nil
}

// CreateTable creates a table
func (ds *DLLDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	resp, err := ds.callDLL("create_table", map[string]interface{}{
		"table_info": tableInfo,
	})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// DropTable drops a table
func (ds *DLLDataSource) DropTable(ctx context.Context, tableName string) error {
	resp, err := ds.callDLL("drop_table", map[string]interface{}{
		"table": tableName,
	})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// TruncateTable truncates a table
func (ds *DLLDataSource) TruncateTable(ctx context.Context, tableName string) error {
	resp, err := ds.callDLL("truncate_table", map[string]interface{}{
		"table": tableName,
	})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// Execute executes raw SQL
func (ds *DLLDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	resp, err := ds.callDLL("execute", map[string]interface{}{
		"sql": sql,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	data, err := json.Marshal(resp.Result)
	if err != nil {
		return nil, err
	}

	var result domain.QueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
