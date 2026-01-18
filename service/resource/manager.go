package resource

import (
	"context"
	"fmt"
	"sync"
)

// DataSourceManager 数据源管理器
type DataSourceManager struct {
	sources    map[string]DataSource
	defaultDS  string
	mu         sync.RWMutex
}

// NewDataSourceManager 创建数据源管理器
func NewDataSourceManager() *DataSourceManager {
	return &DataSourceManager{
		sources: make(map[string]DataSource),
	}
}

// Register 注册数据�?
func (m *DataSourceManager) Register(name string, ds DataSource) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.sources[name]; exists {
		return fmt.Errorf("data source %s already registered", name)
	}
	
	if m.defaultDS == "" {
		m.defaultDS = name
	}
	
	m.sources[name] = ds
	return nil
}

// Unregister 注销数据�?
func (m *DataSourceManager) Unregister(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, exists := m.sources[name]; !exists {
		return fmt.Errorf("data source %s not found", name)
	}
	
	// 关闭数据�?
	ds := m.sources[name]
	if err := ds.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close data source: %w", err)
	}
	
	delete(m.sources, name)
	
	// 如果删除的是默认数据源，重新设置默认�?
	if m.defaultDS == name {
		m.defaultDS = ""
		for n := range m.sources {
			m.defaultDS = n
			break
		}
	}
	
	return nil
}

// Get 获取数据�?
func (m *DataSourceManager) Get(name string) (DataSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	ds, ok := m.sources[name]
	if !ok {
		return nil, fmt.Errorf("data source %s not found", name)
	}
	return ds, nil
}

// GetDefault 获取默认数据�?
func (m *DataSourceManager) GetDefault() (DataSource, error) {
	if m.defaultDS == "" {
		return nil, fmt.Errorf("no default data source set")
	}
	return m.Get(m.defaultDS)
}

// SetDefault 设置默认数据�?
func (m *DataSourceManager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if _, ok := m.sources[name]; !ok {
		return fmt.Errorf("data source %s not found", name)
	}
	
	m.defaultDS = name
	return nil
}

// List 列出所有数据源
func (m *DataSourceManager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	names := make([]string, 0, len(m.sources))
	for name := range m.sources {
		names = append(names, name)
	}
	return names
}

// ConnectAll 连接所有数据源
func (m *DataSourceManager) ConnectAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	for name, ds := range m.sources {
		if !ds.IsConnected() {
			if err := ds.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect data source %s: %w", name, err)
			}
		}
	}
	return nil
}

// CloseAll 关闭所有数据源
func (m *DataSourceManager) CloseAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var lastErr error
	for name, ds := range m.sources {
		if err := ds.Close(ctx); err != nil {
			lastErr = fmt.Errorf("failed to close data source %s: %w", name, err)
		}
	}
	return lastErr
}

// CreateAndRegister 创建并注册数据源
func (m *DataSourceManager) CreateAndRegister(ctx context.Context, name string, config *DataSourceConfig) error {
	// 创建数据�?
	ds, err := CreateDataSource(config)
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}
	
	// 连接数据�?
	if err := ds.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect data source: %w", err)
	}
	
	// 注册数据�?
	if err := m.Register(name, ds); err != nil {
		ds.Close(ctx)
		return err
	}
	
	return nil
}

// GetTables 获取指定数据源的表列�?
func (m *DataSourceManager) GetTables(ctx context.Context, dsName string) ([]string, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.GetTables(ctx)
}

// GetTableInfo 获取指定数据源的表信�?
func (m *DataSourceManager) GetTableInfo(ctx context.Context, dsName, tableName string) (*TableInfo, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.GetTableInfo(ctx, tableName)
}

// Query 查询指定数据源的数据
func (m *DataSourceManager) Query(ctx context.Context, dsName, tableName string, options *QueryOptions) (*QueryResult, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.Query(ctx, tableName, options)
}

// Insert 向指定数据源插入数据
func (m *DataSourceManager) Insert(ctx context.Context, dsName, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Insert(ctx, tableName, rows, options)
}

// Update 更新指定数据源的数据
func (m *DataSourceManager) Update(ctx context.Context, dsName, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除指定数据源的数据
func (m *DataSourceManager) Delete(ctx context.Context, dsName, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Delete(ctx, tableName, filters, options)
}

// CreateTable 在指定数据源创建�?
func (m *DataSourceManager) CreateTable(ctx context.Context, dsName string, tableInfo *TableInfo) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.CreateTable(ctx, tableInfo)
}

// DropTable 在指定数据源删除�?
func (m *DataSourceManager) DropTable(ctx context.Context, dsName, tableName string) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.DropTable(ctx, tableName)
}

// TruncateTable 清空指定数据源的�?
func (m *DataSourceManager) TruncateTable(ctx context.Context, dsName, tableName string) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.TruncateTable(ctx, tableName)
}

// Execute 在指定数据源执行SQL
func (m *DataSourceManager) Execute(ctx context.Context, dsName, sql string) (*QueryResult, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.Execute(ctx, sql)
}

// GetStatus 获取数据源状�?
func (m *DataSourceManager) GetStatus() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	status := make(map[string]bool)
	for name, ds := range m.sources {
		status[name] = ds.IsConnected()
	}
	return status
}

// GetDefaultName 获取默认数据源名�?
func (m *DataSourceManager) GetDefaultName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultDS
}

// 全局数据源管理器实例
var defaultManager = NewDataSourceManager()

// GetDefaultManager 获取默认数据源管理器
func GetDefaultManager() *DataSourceManager {
	return defaultManager
}
