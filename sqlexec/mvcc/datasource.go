package mvcc

import (
	"fmt"
	"log"
	"sync"
)

// ==================== MVCC数据源接口 ====================

// MVCCDataSource MVCC数据源接口
type MVCCDataSource interface {
	// GetFeatures 获取数据源特性
	GetFeatures() *DataSourceFeatures
	
	// ReadWithMVCC 使用MVCC读取数据
	ReadWithMVCC(key string, snapshot *Snapshot) (*TupleVersion, error)
	
	// WriteWithMVCC 使用MVCC写入数据
	WriteWithMVCC(key string, version *TupleVersion) error
	
	// DeleteWithMVCC 使用MVCC删除数据
	DeleteWithMVCC(key string, version *TupleVersion) error
	
	// BeginTransaction 开始事务
	BeginTransaction(xid XID, level IsolationLevel) (TransactionHandle, error)
	
	// CommitTransaction 提交事务
	CommitTransaction(xid XID) error
	
	// RollbackTransaction 回滚事务
	RollbackTransaction(xid XID) error
}

// ==================== 事务句柄 ====================

// TransactionHandle 事务句柄接口
type TransactionHandle interface {
	XID() XID
	Level() IsolationLevel
	IsMVCC() bool
	Commit() error
	Rollback() error
}

// ==================== 数据源注册表 ====================

// DataSourceRegistry 数据源注册表
type DataSourceRegistry struct {
	sources map[string]MVCCDataSource
	mu      sync.RWMutex
}

// NewDataSourceRegistry 创建数据源注册表
func NewDataSourceRegistry() *DataSourceRegistry {
	return &DataSourceRegistry{
		sources: make(map[string]MVCCDataSource),
	}
}

// Register 注册数据源
func (r *DataSourceRegistry) Register(name string, ds MVCCDataSource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.sources[name] = ds
	return nil
}

// Unregister 注销数据源
func (r *DataSourceRegistry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	delete(r.sources, name)
	return nil
}

// Get 获取数据源
func (r *DataSourceRegistry) Get(name string) (MVCCDataSource, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	ds, exists := r.sources[name]
	return ds, exists
}

// Exists 检查数据源是否存在
func (r *DataSourceRegistry) Exists(name string) bool {
	_, exists := r.Get(name)
	return exists
}

// List 列出所有数据源
func (r *DataSourceRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	names := make([]string, 0, len(r.sources))
	for name := range r.sources {
		names = append(names, name)
	}
	return names
}

// ==================== 降级处理器 ====================

// DowngradeHandler 降级处理器
type DowngradeHandler struct {
	manager   *Manager
	registry  *DataSourceRegistry
	logger    *log.Logger
	mu        sync.RWMutex
}

// NewDowngradeHandler 创建降级处理器
func NewDowngradeHandler(manager *Manager, registry *DataSourceRegistry) *DowngradeHandler {
	return &DowngradeHandler{
		manager:  manager,
		registry: registry,
		logger:   log.Default(),
	}
}

// SetLogger 设置日志器
func (h *DowngradeHandler) SetLogger(logger *log.Logger) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logger = logger
}

// CheckBeforeQuery 查询前检查
func (h *DowngradeHandler) CheckBeforeQuery(sources []string, readOnly bool) (bool, error) {
	// 如果是只读查询，允许降级
	if readOnly {
		return h.checkForReadOnlyQuery(sources)
	}
	
	// 如果不是只读查询，要求所有数据源支持MVCC
	return h.checkForReadWriteQuery(sources)
}

// checkForReadOnlyQuery 检查只读查询
func (h *DowngradeHandler) checkForReadOnlyQuery(sources []string) (bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	allSupportMVCC := true
	nonMVCCSources := make([]string, 0)
	
	for _, name := range sources {
		ds, exists := h.registry.Get(name)
		if !exists {
			return false, fmt.Errorf("data source '%s' not found", name)
		}
		
		features := ds.GetFeatures()
		if !features.HasMVCC() {
			allSupportMVCC = false
			nonMVCCSources = append(nonMVCCSources, name)
		}
	}
	
	// 如果有数据源不支持MVCC，输出警告
	if !allSupportMVCC {
		h.logger.Printf("[MVCC-WARN] Data sources do not support MVCC: %v", nonMVCCSources)
		return false, nil // 返回false表示不支持MVCC，但允许继续执行
	}
	
	return true, nil
}

// checkForReadWriteQuery 检查读写查询
func (h *DowngradeHandler) checkForReadWriteQuery(sources []string) (bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	nonMVCCSources := make([]string, 0)
	
	for _, name := range sources {
		ds, exists := h.registry.Get(name)
		if !exists {
			return false, fmt.Errorf("data source '%s' not found", name)
		}
		
		features := ds.GetFeatures()
		if !features.HasMVCC() {
			nonMVCCSources = append(nonMVCCSources, name)
		}
	}
	
	// 如果有数据源不支持MVCC
	if len(nonMVCCSources) > 0 {
		// 检查是否允许自动降级
		if h.manager.config.AutoDowngrade {
			h.logger.Printf("[MVCC-WARN] Auto-downgrading for sources: %v", nonMVCCSources)
			return false, nil // 返回false表示不支持MVCC，但允许降级执行
		}
		
		// 不允许降级，返回错误
		return false, fmt.Errorf("data sources do not support MVCC and auto-downgrade is disabled: %v", nonMVCCSources)
	}
	
	return true, nil
}

// CheckBeforeWrite 写入前检查
func (h *DowngradeHandler) CheckBeforeWrite(sources []string) (bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	nonMVCCSources := make([]string, 0)
	
	for _, name := range sources {
		ds, exists := h.registry.Get(name)
		if !exists {
			return false, fmt.Errorf("data source '%s' not found", name)
		}
		
		features := ds.GetFeatures()
		if !features.HasMVCC() {
			nonMVCCSources = append(nonMVCCSources, name)
		}
	}
	
	// 写入操作要求所有数据源支持MVCC
	if len(nonMVCCSources) > 0 {
		return false, fmt.Errorf("write operation requires MVCC support, but sources do not support it: %v", nonMVCCSources)
	}
	
	return true, nil
}

// ==================== 内存数据源（支持MVCC） ====================

// MemoryDataSource 内存数据源（支持MVCC）
type MemoryDataSource struct {
	name       string
	data       map[string][]*TupleVersion // key -> versions
	features   *DataSourceFeatures
	mu         sync.RWMutex
}

// NewMemoryDataSource 创建内存数据源
func NewMemoryDataSource(name string) *MemoryDataSource {
	return &MemoryDataSource{
		name:     name,
		data:     make(map[string][]*TupleVersion),
		features: NewDataSourceFeatures(name, CapabilityFull),
	}
}

// GetFeatures 获取数据源特性
func (ds *MemoryDataSource) GetFeatures() *DataSourceFeatures {
	return ds.features
}

// ReadWithMVCC 使用MVCC读取数据
func (ds *MemoryDataSource) ReadWithMVCC(key string, snapshot *Snapshot) (*TupleVersion, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	
	versions, exists := ds.data[key]
	if !exists || len(versions) == 0 {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	
	// 从后往前查找可见版本
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		if version.IsVisibleTo(snapshot) {
			return version, nil
		}
	}
	
	return nil, fmt.Errorf("no visible version for key: %s", key)
}

// WriteWithMVCC 使用MVCC写入数据
func (ds *MemoryDataSource) WriteWithMVCC(key string, version *TupleVersion) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	
	ds.data[key] = append(ds.data[key], version)
	return nil
}

// DeleteWithMVCC 使用MVCC删除数据
func (ds *MemoryDataSource) DeleteWithMVCC(key string, version *TupleVersion) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	
	versions, exists := ds.data[key]
	if !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	
	// 标记最后一个可见版本为已删除
	for i := len(versions) - 1; i >= 0; i-- {
		if !versions[i].Expired {
			versions[i].MarkDeleted(version.Xmin, version.Cmin)
			return nil
		}
	}
	
	return fmt.Errorf("no visible version to delete: %s", key)
}

// BeginTransaction 开始事务
func (ds *MemoryDataSource) BeginTransaction(xid XID, level IsolationLevel) (TransactionHandle, error) {
	// 由管理器管理事务，这里返回nil
	return nil, nil
}

// CommitTransaction 提交事务
func (ds *MemoryDataSource) CommitTransaction(xid XID) error {
	// 由管理器管理事务，这里不做任何操作
	return nil
}

// RollbackTransaction 回滚事务
func (ds *MemoryDataSource) RollbackTransaction(xid XID) error {
	// 由管理器管理事务，这里不做任何操作
	return nil
}

// ==================== 非MVCC数据源 ====================

// NonMVCCDataSource 非MVCC数据源
type NonMVCCDataSource struct {
	name     string
	data     map[string]interface{}
	features *DataSourceFeatures
	mu       sync.RWMutex
}

// NewNonMVCCDataSource 创建非MVCC数据源
func NewNonMVCCDataSource(name string) *NonMVCCDataSource {
	return &NonMVCCDataSource{
		name:     name,
		data:     make(map[string]interface{}),
		features: NewDataSourceFeatures(name, CapabilityNone),
	}
}

// GetFeatures 获取数据源特性
func (ds *NonMVCCDataSource) GetFeatures() *DataSourceFeatures {
	return ds.features
}

// ReadWithMVCC 使用MVCC读取数据（不支持）
func (ds *NonMVCCDataSource) ReadWithMVCC(key string, snapshot *Snapshot) (*TupleVersion, error) {
	return nil, fmt.Errorf("MVCC not supported")
}

// WriteWithMVCC 使用MVCC写入数据（不支持）
func (ds *NonMVCCDataSource) WriteWithMVCC(key string, version *TupleVersion) error {
	return fmt.Errorf("MVCC not supported")
}

// DeleteWithMVCC 使用MVCC删除数据（不支持）
func (ds *NonMVCCDataSource) DeleteWithMVCC(key string, version *TupleVersion) error {
	return fmt.Errorf("MVCC not supported")
}

// BeginTransaction 开始事务（不支持）
func (ds *NonMVCCDataSource) BeginTransaction(xid XID, level IsolationLevel) (TransactionHandle, error) {
	return nil, fmt.Errorf("MVCC not supported")
}

// CommitTransaction 提交事务（不支持）
func (ds *NonMVCCDataSource) CommitTransaction(xid XID) error {
	return fmt.Errorf("MVCC not supported")
}

// RollbackTransaction 回滚事务（不支持）
func (ds *NonMVCCDataSource) RollbackTransaction(xid XID) error {
	return fmt.Errorf("MVCC not supported")
}

// SimpleRead 简单读取（非MVCC）
func (ds *NonMVCCDataSource) SimpleRead(key string) (interface{}, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	
	value, exists := ds.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return value, nil
}

// SimpleWrite 简单写入（非MVCC）
func (ds *NonMVCCDataSource) SimpleWrite(key string, value interface{}) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	
	ds.data[key] = value
	return nil
}
