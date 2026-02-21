package application

import (
	"context"
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== 数据源管理器 ====================

// DataSourceManager 数据源管理器
type DataSourceManager struct {
	sources      map[string]domain.DataSource
	registry      *Registry
	defaultDS     string
	enabledTypes  map[domain.DataSourceType]bool
	mu            sync.RWMutex
}

// NewDataSourceManager 创建数据源管理器
func NewDataSourceManager() *DataSourceManager {
	return &DataSourceManager{
		sources:     make(map[string]domain.DataSource),
		registry:     NewRegistry(),
		enabledTypes: make(map[domain.DataSourceType]bool),
	}
}

// NewDataSourceManagerWithRegistry 使用指定注册表创建数据源管理器
func NewDataSourceManagerWithRegistry(registry *Registry) *DataSourceManager {
	return &DataSourceManager{
		sources:     make(map[string]domain.DataSource),
		registry:     registry,
		enabledTypes: make(map[domain.DataSourceType]bool),
	}
}

// SetEnabledTypes 设置启用的数据源类型
func (m *DataSourceManager) SetEnabledTypes(types []domain.DataSourceType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabledTypes = make(map[domain.DataSourceType]bool)
	for _, t := range types {
		m.enabledTypes[t] = true
	}
}

// Register 注册数据源
func (m *DataSourceManager) Register(name string, ds domain.DataSource) error {
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

// Unregister 注销数据源
func (m *DataSourceManager) Unregister(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sources[name]; !exists {
		return fmt.Errorf("data source %s not found", name)
	}

	// 关闭数据源
	ds := m.sources[name]
	if err := ds.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close data source: %w", err)
	}

	delete(m.sources, name)

	// 如果删除的是默认数据源，重新设置默认值
	if m.defaultDS == name {
		m.defaultDS = ""
		for n := range m.sources {
			m.defaultDS = n
			break
		}
	}

	return nil
}

// Get 获取数据源
func (m *DataSourceManager) Get(name string) (domain.DataSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ds, ok := m.sources[name]
	if !ok {
		return nil, fmt.Errorf("data source %s not found", name)
	}
	return ds, nil
}

// GetDefault 获取默认数据源
func (m *DataSourceManager) GetDefault() (domain.DataSource, error) {
	m.mu.RLock()
	name := m.defaultDS
	m.mu.RUnlock()
	if name == "" {
		return nil, fmt.Errorf("no default data source set")
	}
	return m.Get(name)
}

// CreateFromConfig 从配置创建数据源
func (m *DataSourceManager) CreateFromConfig(config *domain.DataSourceConfig) (domain.DataSource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查数据源类型是否启用
	if len(m.enabledTypes) > 0 && !m.enabledTypes[config.Type] {
		return nil, fmt.Errorf("data source type %s is not enabled", config.Type)
	}

	// 使用注册表创建数据源
	return m.registry.Create(config)
}

// IsTypeEnabled 检查数据源类型是否启用
func (m *DataSourceManager) IsTypeEnabled(t domain.DataSourceType) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.enabledTypes) == 0 {
		// 如果没有设置启用的类型，默认全部启用
		return true
	}

	return m.enabledTypes[t]
}

// SetDefault 设置默认数据源
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
	// Collect datasources under lock, then connect outside lock
	m.mu.RLock()
	type namedDS struct {
		name string
		ds   domain.DataSource
	}
	sources := make([]namedDS, 0, len(m.sources))
	for name, ds := range m.sources {
		sources = append(sources, namedDS{name, ds})
	}
	m.mu.RUnlock()

	for _, s := range sources {
		if !s.ds.IsConnected() {
			if err := s.ds.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect data source %s: %w", s.name, err)
			}
		}
	}
	return nil
}

// CloseAll 关闭所有数据源
func (m *DataSourceManager) CloseAll(ctx context.Context) error {
	m.mu.RLock()
	type namedDS struct {
		name string
		ds   domain.DataSource
	}
	sources := make([]namedDS, 0, len(m.sources))
	for name, ds := range m.sources {
		sources = append(sources, namedDS{name, ds})
	}
	m.mu.RUnlock()

	var lastErr error
	for _, s := range sources {
		if err := s.ds.Close(ctx); err != nil {
			lastErr = fmt.Errorf("failed to close data source %s: %w", s.name, err)
		}
	}
	return lastErr
}

// CreateAndRegister 创建并注册数据源
func (m *DataSourceManager) CreateAndRegister(ctx context.Context, name string, config *domain.DataSourceConfig) error {
	// 创建数据源
	ds, err := m.CreateFromConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}

	// 连接数据源
	if err := ds.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect data source: %w", err)
	}

	// 注册数据源
	if err := m.Register(name, ds); err != nil {
		ds.Close(ctx)
		return err
	}

	return nil
}

// GetRegistry 获取注册表
func (m *DataSourceManager) GetRegistry() *Registry {
	return m.registry
}

// ==================== 便捷查询方法 ====================

// GetTables 获取指定数据源的表列表
func (m *DataSourceManager) GetTables(ctx context.Context, dsName string) ([]string, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.GetTables(ctx)
}

// GetTableInfo 获取指定数据源的表信息
func (m *DataSourceManager) GetTableInfo(ctx context.Context, dsName, tableName string) (*domain.TableInfo, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.GetTableInfo(ctx, tableName)
}

// Query 查询指定数据源的数据
func (m *DataSourceManager) Query(ctx context.Context, dsName, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.Query(ctx, tableName, options)
}

// Insert 向指定数据源插入数据
func (m *DataSourceManager) Insert(ctx context.Context, dsName, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Insert(ctx, tableName, rows, options)
}

// Update 更新指定数据源的数据
func (m *DataSourceManager) Update(ctx context.Context, dsName, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除指定数据源的数据
func (m *DataSourceManager) Delete(ctx context.Context, dsName, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return 0, err
	}
	return ds.Delete(ctx, tableName, filters, options)
}

// CreateTable 在指定数据源创建表
func (m *DataSourceManager) CreateTable(ctx context.Context, dsName string, tableInfo *domain.TableInfo) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.CreateTable(ctx, tableInfo)
}

// DropTable 在指定数据源删除表
func (m *DataSourceManager) DropTable(ctx context.Context, dsName, tableName string) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.DropTable(ctx, tableName)
}

// TruncateTable 清空指定数据源的表
func (m *DataSourceManager) TruncateTable(ctx context.Context, dsName, tableName string) error {
	ds, err := m.Get(dsName)
	if err != nil {
		return err
	}
	return ds.TruncateTable(ctx, tableName)
}

// Execute 在指定数据源执行SQL
func (m *DataSourceManager) Execute(ctx context.Context, dsName, sql string) (*domain.QueryResult, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.Execute(ctx, sql)
}

// GetStatus 获取数据源状态
func (m *DataSourceManager) GetStatus() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]bool)
	for name, ds := range m.sources {
		status[name] = ds.IsConnected()
	}
	return status
}

// GetDefaultName 获取默认数据源名称
func (m *DataSourceManager) GetDefaultName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultDS
}

// GetAllDataSources returns all registered data sources with their configs
func (m *DataSourceManager) GetAllDataSources() map[string]domain.DataSource {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]domain.DataSource, len(m.sources))
	for name, ds := range m.sources {
		result[name] = ds
	}
	return result
}

// ==================== 全局数据源管理器 ====================

var defaultManager = NewDataSourceManager()

// GetDefaultManager 获取默认数据源管理器
func GetDefaultManager() *DataSourceManager {
	return defaultManager
}
