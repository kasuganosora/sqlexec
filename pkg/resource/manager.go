package resource

import (
	"context"
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// DataSourceManager 数据源管理器
type DataSourceManager struct {
	sources      map[string]DataSource
	factories    map[string]DataSourceFactory
	defaultDS    string
	mu           sync.RWMutex
	enabledTypes map[string]bool // 启用的数据源类型（使用字符串）
}

// NewDataSourceManager 创建数据源管理器
func NewDataSourceManager() *DataSourceManager {
	return &DataSourceManager{
		sources:      make(map[string]DataSource),
		factories:    make(map[string]DataSourceFactory),
		enabledTypes: make(map[string]bool),
	}
}

// SetEnabledTypes 设置启用的数据源类型（核心版本可以只启用部分数据源）
func (m *DataSourceManager) SetEnabledTypes(types []DataSourceType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabledTypes = make(map[string]bool)
	for _, t := range types {
		m.enabledTypes[t.String()] = true
	}
}

// RegisterFactory 注册数据源工厂
func (m *DataSourceManager) RegisterFactory(factory DataSourceFactory) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	factoryType := factory.GetType()
	typeStr := factoryType.String()
	if _, exists := m.factories[typeStr]; exists {
		return fmt.Errorf("factory %s already registered", factoryType)
	}

	m.factories[typeStr] = factory
	return nil
}

// Register 注册数据源
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
func (m *DataSourceManager) Get(name string) (DataSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ds, ok := m.sources[name]
	if !ok {
		return nil, fmt.Errorf("data source %s not found", name)
	}
	return ds, nil
}

// GetDefault 获取默认数据源
func (m *DataSourceManager) GetDefault() (DataSource, error) {
	if m.defaultDS == "" {
		return nil, fmt.Errorf("no default data source set")
	}
	return m.Get(m.defaultDS)
}

// CreateFromConfig 从配置创建数据源
func (m *DataSourceManager) CreateFromConfig(config *DataSourceConfig) (DataSource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查数据源类型是否启用
	if !m.enabledTypes[config.Type.String()] {
		return nil, fmt.Errorf("data source type %s is not enabled", config.Type)
	}

	// 查找工厂
	factory, ok := m.factories[config.Type.String()]
	if !ok {
		return nil, fmt.Errorf("no factory registered for type %s", config.Type)
	}

	// 使用工厂创建数据源
	return factory.Create(config)
}

// IsTypeEnabled 检查数据源类型是否启用
func (m *DataSourceManager) IsTypeEnabled(t DataSourceType) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.enabledTypes) == 0 {
		// 如果没有设置启用的类型，默认全部启用（核心版本）
		return true
	}

	return m.enabledTypes[t.String()]
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
	// 创建数据源
	ds, err := CreateDataSource(config)
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

// GetTables 获取指定数据源的表列表
func (m *DataSourceManager) GetTables(ctx context.Context, dsName string) ([]string, error) {
	ds, err := m.Get(dsName)
	if err != nil {
		return nil, err
	}
	return ds.GetTables(ctx)
}

// GetTableInfo 获取指定数据源的表信息
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

// CreateTable 在指定数据源创建表
func (m *DataSourceManager) CreateTable(ctx context.Context, dsName string, tableInfo *TableInfo) error {
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
func (m *DataSourceManager) Execute(ctx context.Context, dsName, sql string) (*QueryResult, error) {
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

// ==================== 能力接口支持 ====================

// IsWritable 检查数据源是否可写
// 使用domain.HasWriteSupport辅助函数
func (m *DataSourceManager) IsWritable(name string) (bool, error) {
	ds, err := m.Get(name)
	if err != nil {
		return false, err
	}
	
	// 使用辅助函数检查写支持
	return domain.HasWriteSupport(ds), nil
}

// SupportsMVCC 检查数据源是否支持MVCC
// 使用domain.HasMVCCSupport辅助函数
func (m *DataSourceManager) SupportsMVCC(name string) (bool, error) {
	ds, err := m.Get(name)
	if err != nil {
		return false, err
	}
	
	// 使用辅助函数检查MVCC支持
	return domain.HasMVCCSupport(ds), nil
}

// BeginTx 在指定数据源开始事务
// 如果数据源支持MVCC，使用其事务接口
// 否则返回错误
func (m *DataSourceManager) BeginTx(ctx context.Context, name string, readOnly bool) (int64, error) {
	ds, err := m.Get(name)
	if err != nil {
		return 0, err
	}
	
	// 检查是否支持MVCC
	mvccable, ok := domain.GetMVCCDataSource(ds)
	if !ok {
		return 0, fmt.Errorf("data source %s does not support MVCC", name)
	}
	
	// 调用事务方法
	return mvccable.BeginTx(ctx, readOnly)
}

// CommitTx 提交指定数据源的事务
func (m *DataSourceManager) CommitTx(ctx context.Context, name string, txnID int64) error {
	ds, err := m.Get(name)
	if err != nil {
		return err
	}
	
	// 检查是否支持MVCC
	mvccable, ok := domain.GetMVCCDataSource(ds)
	if !ok {
		return fmt.Errorf("data source %s does not support MVCC", name)
	}
	
	// 调用提交方法
	return mvccable.CommitTx(ctx, txnID)
}

// RollbackTx 回滚指定数据源的事务
func (m *DataSourceManager) RollbackTx(ctx context.Context, name string, txnID int64) error {
	ds, err := m.Get(name)
	if err != nil {
		return err
	}
	
	// 检查是否支持MVCC
	mvccable, ok := domain.GetMVCCDataSource(ds)
	if !ok {
		return fmt.Errorf("data source %s does not support MVCC", name)
	}
	
	// 调用回滚方法
	return mvccable.RollbackTx(ctx, txnID)
}

// ==================== 批量能力查询 ====================

// GetWritableSources 获取所有可写的数据源
func (m *DataSourceManager) GetWritableSources(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	writableSources := make([]string, 0)
	for name, ds := range m.sources {
		if domain.HasWriteSupport(ds) {
			writableSources = append(writableSources, name)
		}
	}
	return writableSources, nil
}

// GetMVCCSources 获取所有支持MVCC的数据源
func (m *DataSourceManager) GetMVCCSources(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	mvccSources := make([]string, 0)
	for name, ds := range m.sources {
		if domain.HasMVCCSupport(ds) {
			mvccSources = append(mvccSources, name)
		}
	}
	return mvccSources, nil
}

// GetSourceCapabilities 获取数据源的能力信息
func (m *DataSourceManager) GetSourceCapabilities(ctx context.Context, name string) (writable, mvcc bool, err error) {
	ds, err := m.Get(name)
	if err != nil {
		return false, false, err
	}
	
	writable = domain.HasWriteSupport(ds)
	mvcc = domain.HasMVCCSupport(ds)
	return writable, mvcc, nil
}

// 全局数据源管理器实例
var defaultManager = NewDataSourceManager()

// GetDefaultManager 获取默认数据源管理器
func GetDefaultManager() *DataSourceManager {
	return defaultManager
}
