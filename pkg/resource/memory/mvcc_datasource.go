package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== MVCC 数据源实现 ====================

// MVCCDataSource 支持多版本并发控制的内存数据源
// 这是所有外部数据源的底层基础，所有数据源都应该映射到这里
type MVCCDataSource struct {
	config    *domain.DataSourceConfig
	connected bool
	mu        sync.RWMutex

	// MVCC 相关
	nextTxID    int64
	currentVer  int64
	snapshots   map[int64]*Snapshot
	activeTxns  map[int64]*Transaction

	// 数据存储（按版本管理）
	tables      map[string]*TableVersions
}

// SupportsMVCC 实现IsMVCCable接口
func (m *MVCCDataSource) SupportsMVCC() bool {
	return true
}

// TableVersions 表的多版本数据
type TableVersions struct {
	mu      sync.RWMutex
	versions map[int64]*TableData  // version -> data
	latest  int64                   // 最新版本号
}

// TableData 单个版本的数据
type TableData struct {
	version   int64
	createdAt time.Time
	schema    *domain.TableInfo
	rows      []domain.Row
}

// Snapshot 事务快照
type Snapshot struct {
	txnID      int64
	startVer   int64
	createdAt  time.Time
}

// Transaction 事务信息
type Transaction struct {
	txnID      int64
	startTime  time.Time
	readOnly   bool
}

// NewMVCCDataSource 创建MVCC内存数据源
func NewMVCCDataSource(config *domain.DataSourceConfig) *MVCCDataSource {
	if config == nil {
		config = &domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		}
	}

	return &MVCCDataSource{
		config:     config,
		connected:  false,
		nextTxID:   1,
		currentVer: 0,
		snapshots:  make(map[int64]*Snapshot),
		activeTxns: make(map[int64]*Transaction),
		tables:     make(map[string]*TableVersions),
	}
}

// ==================== 连接管理 ====================

// Connect 连接数据源
func (m *MVCCDataSource) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	return nil
}

// Close 关闭连接
func (m *MVCCDataSource) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清理所有快照和事务
	m.snapshots = make(map[int64]*Snapshot)
	m.activeTxns = make(map[int64]*Transaction)
	m.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (m *MVCCDataSource) IsConnected() bool {
	return m.connected
}

// IsWritable 检查是否可写
func (m *MVCCDataSource) IsWritable() bool {
	return m.config.Writable
}

// GetConfig 获取数据源配置
func (m *MVCCDataSource) GetConfig() *domain.DataSourceConfig {
	return m.config
}

// ==================== 事务管理 ====================

// BeginTx 开始一个新事务
func (m *MVCCDataSource) BeginTx(ctx context.Context, readOnly bool) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxID
	m.nextTxID++

	snapshot := &Snapshot{
		txnID:     txnID,
		startVer:  m.currentVer,
		createdAt: time.Now(),
	}

	txn := &Transaction{
		txnID:     txnID,
		startTime: time.Now(),
		readOnly:  readOnly,
	}

	m.snapshots[txnID] = snapshot
	m.activeTxns[txnID] = txn

	return txnID, nil
}

// CommitTx 提交事务
func (m *MVCCDataSource) CommitTx(ctx context.Context, txnID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction not found: %d", txnID)
	}

	if txn.readOnly {
		// 只读事务直接结束
		delete(m.activeTxns, txnID)
		delete(m.snapshots, txnID)
		return nil
	}

	// 写事务提交时，创建新版本
	m.currentVer++
	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

	return nil
}

// RollbackTx 回滚事务
func (m *MVCCDataSource) RollbackTx(ctx context.Context, txnID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxns[txnID]; !ok {
		return fmt.Errorf("transaction not found: %d", txnID)
	}

	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

	return nil
}

// ==================== 表管理 ====================

// GetTables 获取所有表
func (m *MVCCDataSource) GetTables(ctx context.Context) ([]string, error) {
	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// GetTableInfo 获取表信息
func (m *MVCCDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	// 获取最新版本的数据
	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	// 深拷贝表信息
	cols := make([]domain.ColumnInfo, len(latest.schema.Columns))
	copy(cols, latest.schema.Columns)

	return &domain.TableInfo{
		Name:    latest.schema.Name,
		Schema:  latest.schema.Schema,
		Columns: cols,
	}, nil
}

// CreateTable 创建表
func (m *MVCCDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[tableInfo.Name]; ok {
		return fmt.Errorf("table %s already exists", tableInfo.Name)
	}

	// 深拷贝表信息
	cols := make([]domain.ColumnInfo, len(tableInfo.Columns))
	copy(cols, tableInfo.Columns)

	// 创建新版本
	m.currentVer++
	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    tableInfo.Name,
			Schema:  tableInfo.Schema,
			Columns: cols,
		},
		rows: []domain.Row{},
	}

	m.tables[tableInfo.Name] = &TableVersions{
		versions: map[int64]*TableData{
			m.currentVer: versionData,
		},
		latest: m.currentVer,
	}

	return nil
}

// DropTable 删除表
func (m *MVCCDataSource) DropTable(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[tableName]; !ok {
		return domain.NewErrTableNotFound(tableName)
	}

	delete(m.tables, tableName)
	return nil
}

// TruncateTable 清空表
func (m *MVCCDataSource) TruncateTable(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.Lock()
	defer tableVer.mu.Unlock()

	// 创建新版本（空数据）
	m.currentVer++
	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema:    tableVer.versions[tableVer.latest].schema,
		rows:      []domain.Row{},
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return nil
}

// ==================== 数据查询 ====================

// Query 查询数据
func (m *MVCCDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	// 获取最新版本的数据
	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	// 应用查询操作（过滤、排序、分页）
	pagedRows := util.ApplyQueryOperations(latest.rows, options, &latest.schema.Columns)

	// Total应该是返回的行数
	total := int64(len(pagedRows))

	return &domain.QueryResult{
		Columns: latest.schema.Columns,
		Rows:    pagedRows,
		Total:   total,
	}, nil
}

// ==================== 数据修改 ====================

// Insert 插入数据
func (m *MVCCDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "insert")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.Lock()
	defer tableVer.mu.Unlock()

	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 创建新版本
	m.currentVer++
	newRows := make([]domain.Row, len(latest.rows)+len(rows))
	copy(newRows, latest.rows)
	copy(newRows[len(latest.rows):], rows)

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema:    latest.schema,
		rows:      newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return int64(len(rows)), nil
}

// Update 更新数据
func (m *MVCCDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "update")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.Lock()
	defer tableVer.mu.Unlock()

	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 创建新版本
	m.currentVer++
	newRows := make([]domain.Row, len(latest.rows))
	copy(newRows, latest.rows)

	updated := int64(0)
	for i, row := range newRows {
		if util.MatchesFilters(row, filters) {
			for k, v := range updates {
				newRows[i][k] = v
			}
			updated++
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema:    latest.schema,
		rows:      newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return updated, nil
}

// Delete 删除数据
func (m *MVCCDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "delete")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.Lock()
	defer tableVer.mu.Unlock()

	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 创建新版本
	m.currentVer++
	newRows := make([]domain.Row, 0, len(latest.rows))

	deleted := int64(0)
	for _, row := range latest.rows {
		if !util.MatchesFilters(row, filters) {
			newRows = append(newRows, row)
		} else {
			deleted++
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema:    latest.schema,
		rows:      newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return deleted, nil
}

// Execute 执行自定义SQL语句
func (m *MVCCDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	// 内存数据源不支持SQL执行
	return nil, domain.NewErrUnsupportedOperation(string(m.config.Type), "execute SQL")
}

// ==================== 适配器接口 ====================

// LoadTable 加载表数据到内存（供外部数据源适配器使用）
func (m *MVCCDataSource) LoadTable(tableName string, schema *domain.TableInfo, rows []domain.Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 创建新版本
	m.currentVer++

	// 深拷贝schema
	cols := make([]domain.ColumnInfo, len(schema.Columns))
	copy(cols, schema.Columns)

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    schema.Name,
			Schema:  schema.Schema,
			Columns: cols,
		},
		rows: rows,
	}

	if existing, ok := m.tables[tableName]; ok {
		existing.mu.Lock()
		existing.versions[m.currentVer] = versionData
		existing.latest = m.currentVer
		existing.mu.Unlock()
	} else {
		m.tables[tableName] = &TableVersions{
			versions: map[int64]*TableData{
				m.currentVer: versionData,
			},
			latest: m.currentVer,
		}
	}

	return nil
}

// GetLatestTableData 获取最新表数据（供外部数据源适配器写回使用）
func (m *MVCCDataSource) GetLatestTableData(tableName string) (*domain.TableInfo, []domain.Row, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}

	return latest.schema, latest.rows, nil
}

// GetCurrentVersion 获取当前版本号
func (m *MVCCDataSource) GetCurrentVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentVer
}
