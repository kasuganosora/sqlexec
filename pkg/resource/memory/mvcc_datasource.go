package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// TransactionIDKey context key for transaction ID
type TransactionIDKey struct{}

// GetTransactionID 从context中获取事务ID
func GetTransactionID(ctx context.Context) (int64, bool) {
	txnID, ok := ctx.Value(TransactionIDKey{}).(int64)
	return txnID, ok
}

// SetTransactionID 设置事务ID到context
func SetTransactionID(ctx context.Context, txnID int64) context.Context {
	return context.WithValue(ctx, TransactionIDKey{}, txnID)
}

// ==================== MVCC 数据源实现 ====================

// MVCCDataSource 支持多版本并发控制的内存数据源
// 这是所有外部数据源的底层基础，所有数据源都应该映射到这里
type MVCCDataSource struct {
	config    *domain.DataSourceConfig
	connected bool
	mu        sync.RWMutex

	// 索引管理
	indexManager *IndexManager
	queryPlanner *QueryPlanner

	// MVCC 相关
	nextTxID    int64
	currentVer  int64
	snapshots   map[int64]*Snapshot
	activeTxns  map[int64]*Transaction

	// 数据存储（按版本管理）
	tables      map[string]*TableVersions

	// 临时表（会话结束时自动删除）
	tempTables  map[string]bool
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

// COWTableSnapshot 写时复制的表快照
type COWTableSnapshot struct {
	tableName    string
	copied       bool               // 是否已创建修改副本
	baseData     *TableData          // 基础数据引用（未修改时）
	modifiedData *TableData         // 修改后的数据
	rowLocks     map[int64]bool     // 行级锁：跟踪哪些行被修改了
	rowCopies    map[int64]domain.Row // 行级拷贝：存储修改后的行
	deletedRows  map[int64]bool     // 行级删除：标记哪些行被删除了
	mu           sync.RWMutex
}

// Snapshot 事务快照（写时复制）
type Snapshot struct {
	txnID        int64
	startVer     int64
	createdAt    time.Time
	// 事务工作区：每张表的写时复制快照
	// 在首次修改表时才拷贝数据
	tableSnapshots map[string]*COWTableSnapshot
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

	indexMgr := NewIndexManager()
	return &MVCCDataSource{
		config:        config,
		connected:     false,
		indexManager:  indexMgr,
		queryPlanner:  NewQueryPlanner(indexMgr),
		nextTxID:       1,
		currentVer:    0,
		snapshots:     make(map[int64]*Snapshot),
		activeTxns:    make(map[int64]*Transaction),
		tables:        make(map[string]*TableVersions),
		tempTables:    make(map[string]bool),
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

	// 删除所有临时表
	for tableName := range m.tempTables {
		delete(m.tables, tableName)
	}
	m.tempTables = make(map[string]bool)

	// 清理所有快照和事务
	m.snapshots = make(map[int64]*Snapshot)
	m.activeTxns = make(map[int64]*Transaction)
	m.connected = false
	return nil
}

// BeginTransaction 实现 TransactionalDataSource 接口
func (m *MVCCDataSource) BeginTransaction(ctx context.Context, options *domain.TransactionOptions) (domain.Transaction, error) {
	readOnly := false
	if options != nil {
		readOnly = options.ReadOnly
	}

	txnID, err := m.BeginTx(ctx, readOnly)
	if err != nil {
		return nil, err
	}

	return &MVCCTransaction{
		ds:    m,
		txnID: txnID,
	}, nil
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

// BeginTx 开始一个新事务（写时复制）
func (m *MVCCDataSource) BeginTx(ctx context.Context, readOnly bool) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxID
	m.nextTxID++

	// 创建写时复制快照结构，不拷贝数据
	tableSnapshots := make(map[string]*COWTableSnapshot)
	for tableName := range m.tables {
		// 只创建快照结构，引用基础数据
		tableSnapshots[tableName] = &COWTableSnapshot{
			tableName:    tableName,
			copied:       false,
			baseData:     nil,  // 访问时延迟加载
			modifiedData: nil,
		}
	}

	snapshot := &Snapshot{
		txnID:        txnID,
		startVer:     m.currentVer,
		createdAt:    time.Now(),
		tableSnapshots: tableSnapshots,
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

// CommitTx 提交事务（COW优化，支持行级COW）
func (m *MVCCDataSource) CommitTx(ctx context.Context, txnID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction not found: %d", txnID)
	}

	snapshot, ok := m.snapshots[txnID]
	if !ok {
		return fmt.Errorf("transaction snapshot not found: %d", txnID)
	}

	if txn.readOnly {
		// 只读事务直接结束
		delete(m.activeTxns, txnID)
		delete(m.snapshots, txnID)
		return nil
	}

	// 写事务提交时，只提交已修改的表
	for tableName, cowSnapshot := range snapshot.tableSnapshots {
		tableVer := m.tables[tableName]
		if tableVer != nil && cowSnapshot.copied {
			cowSnapshot.mu.Lock()

			// 检查是否有行级修改
			if len(cowSnapshot.rowCopies) == 0 && len(cowSnapshot.deletedRows) == 0 {
				// 没有行被修改，无需创建新版本
				cowSnapshot.mu.Unlock()
				continue
			}

			// 行级COW：合并基础数据和修改的行
			tableVer.mu.Lock()
			m.currentVer++

			// 合并基础数据和行级修改
			newRows := make([]domain.Row, 0, len(cowSnapshot.baseData.rows))
			for i, row := range cowSnapshot.baseData.rows {
				rowID := int64(i + 1)

				// 检查此行是否被删除
				if _, deleted := cowSnapshot.deletedRows[rowID]; deleted {
					continue // 跳过已删除的行
				}

				// 检查此行是否被修改
				if modifiedRow, ok := cowSnapshot.rowCopies[rowID]; ok {
					// 使用修改后的行
					newRows = append(newRows, modifiedRow)
				} else {
					// 使用原始行（需要深拷贝）
					rowCopy := make(map[string]interface{}, len(row))
					for k, v := range row {
						rowCopy[k] = v
					}
					newRows = append(newRows, rowCopy)
				}
			}

			// 处理新增的行（rowID超过基础数据行数的行）
			baseRowsCount := len(cowSnapshot.baseData.rows)
			for rowID, row := range cowSnapshot.rowCopies {
				if rowID > int64(baseRowsCount) {
					// 这是新插入的行
					newRows = append(newRows, row)
				}
			}

			// 创建新版本
			cols := make([]domain.ColumnInfo, len(cowSnapshot.modifiedData.schema.Columns))
			copy(cols, cowSnapshot.modifiedData.schema.Columns)

			// 深拷贝表属性
			var atts map[string]interface{}
			if cowSnapshot.modifiedData.schema.Atts != nil {
				atts = make(map[string]interface{}, len(cowSnapshot.modifiedData.schema.Atts))
				for k, v := range cowSnapshot.modifiedData.schema.Atts {
					atts[k] = v
				}
			}

			newVersionData := &TableData{
				version:   m.currentVer,
				createdAt: time.Now(),
				schema: &domain.TableInfo{
					Name:    cowSnapshot.modifiedData.schema.Name,
					Schema:  cowSnapshot.modifiedData.schema.Schema,
					Columns: cols,
					Atts:    atts,
				},
				rows: newRows,
			}

			tableVer.versions[m.currentVer] = newVersionData
			tableVer.latest = m.currentVer
			tableVer.mu.Unlock()

			cowSnapshot.mu.Unlock()
		}
	}

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

	// 写时复制下，回滚只需删除快照，无需释放数据
	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

	return nil
}

// ensureTableCopied 确保表数据已拷贝到事务快照（写时复制）
// 采用行级COW：只创建结构，不立即拷贝所有行
func (s *COWTableSnapshot) ensureCopied(tableVer *TableVersions) error {
	if s.copied {
		return nil // 已创建副本
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 再次检查，避免重复创建
	if s.copied {
		return nil
	}

	// 获取主版本数据
	tableVer.mu.RLock()
	baseData := tableVer.versions[tableVer.latest]
	tableVer.mu.RUnlock()

	if baseData == nil {
		return fmt.Errorf("table %s not found", s.tableName)
	}

	// 拷贝schema
	cols := make([]domain.ColumnInfo, len(baseData.schema.Columns))
	copy(cols, baseData.schema.Columns)

	// 深拷贝表属性
	var atts map[string]interface{}
	if baseData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(baseData.schema.Atts))
		for k, v := range baseData.schema.Atts {
			atts[k] = v
		}
	}

	// 创建修改后的数据结构，但不立即拷贝所有行
	// 采用行级COW：只创建结构，行按需拷贝
	s.modifiedData = &TableData{
		version:   baseData.version,
		createdAt: baseData.createdAt,
		schema: &domain.TableInfo{
			Name:    baseData.schema.Name,
			Schema:  baseData.schema.Schema,
			Columns: cols,
			Atts:    atts,
		},
		rows: nil, // 行数据延迟加载和拷贝
	}

	// 初始化行级跟踪结构
	s.rowLocks = make(map[int64]bool)      // 跟踪修改的行
	s.rowCopies = make(map[int64]domain.Row) // 存储修改后的行
	s.deletedRows = make(map[int64]bool) // 标记删除的行

	s.baseData = baseData
	s.copied = true

	return nil
}

// getTableData 从COW快照获取表数据（行级COW）
func (s *COWTableSnapshot) getTableData(tableVer *TableVersions) *TableData {
	if !s.copied {
		// 未创建副本时，直接读取主版本
		tableVer.mu.RLock()
		data := tableVer.versions[tableVer.latest]
		tableVer.mu.RUnlock()
		return data
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 已创建副本，需要合并基础数据和行级修改
	if len(s.rowCopies) == 0 {
		// 没有行被修改，返回基础数据
		return s.baseData
	}

	// 合并基础数据和修改的行
	mergedRows := make([]domain.Row, 0, len(s.baseData.rows))
	for i, row := range s.baseData.rows {
		rowID := int64(i + 1) // 行ID从1开始
		if modifiedRow, ok := s.rowCopies[rowID]; ok {
			// 使用修改后的行
			mergedRows = append(mergedRows, modifiedRow)
		} else {
			// 使用原始行
			mergedRows = append(mergedRows, row)
		}
	}

	return &TableData{
		version:   s.modifiedData.version,
		createdAt: s.modifiedData.createdAt,
		schema:    s.modifiedData.schema,
		rows:      mergedRows,
	}
}

// ==================== 表管理 ====================

// GetTables 获取所有表（不包括临时表）
func (m *MVCCDataSource) GetTables(ctx context.Context) ([]string, error) {
	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		// 排除临时表
		if !m.tempTables[name] {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// GetAllTables 获取所有表（包括临时表）
func (m *MVCCDataSource) GetAllTables(ctx context.Context) ([]string, error) {
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

// GetTemporaryTables 获取所有临时表
func (m *MVCCDataSource) GetTemporaryTables(ctx context.Context) ([]string, error) {
	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := make([]string, 0, len(m.tempTables))
	for name := range m.tempTables {
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

	// 深拷贝表属性
	var atts map[string]interface{}
	if latest.schema.Atts != nil {
		atts = make(map[string]interface{}, len(latest.schema.Atts))
		for k, v := range latest.schema.Atts {
			atts[k] = v
		}
	}

	return &domain.TableInfo{
		Name:    latest.schema.Name,
		Schema:  latest.schema.Schema,
		Columns: cols,
		Atts:    atts,
	}, nil
}

// CreateTable 创建表
func (m *MVCCDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[tableInfo.Name]; ok {
		return fmt.Errorf("table %s already exists", tableInfo.Name)
	}

	// 验证生成列定义（如果有）
	validator := &generated.GeneratedColumnValidator{}
	if err := validator.ValidateSchema(tableInfo); err != nil {
		return fmt.Errorf("generated column validation failed: %w", err)
	}

	// 深拷贝表信息
	cols := make([]domain.ColumnInfo, len(tableInfo.Columns))
	copy(cols, tableInfo.Columns)

	// 深拷贝表属性
	var atts map[string]interface{}
	if tableInfo.Atts != nil {
		atts = make(map[string]interface{}, len(tableInfo.Atts))
		for k, v := range tableInfo.Atts {
			atts[k] = v
		}
	}

	// 创建新版本
	m.currentVer++
	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:       tableInfo.Name,
			Schema:     tableInfo.Schema,
			Columns:    cols,
			Temporary:  tableInfo.Temporary,
			Atts:       atts,
		},
		rows: []domain.Row{},
	}

	m.tables[tableInfo.Name] = &TableVersions{
		versions: map[int64]*TableData{
			m.currentVer: versionData,
		},
		latest: m.currentVer,
	}

	// 如果是临时表，添加到临时表列表
	if tableInfo.Temporary {
		m.tempTables[tableInfo.Name] = true
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
	// 删除索引
	_ = m.indexManager.DropTableIndexes(tableName)
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

	// 深拷贝表属性
	var atts map[string]interface{}
	if tableVer.versions[tableVer.latest].schema.Atts != nil {
		atts = make(map[string]interface{}, len(tableVer.versions[tableVer.latest].schema.Atts))
		for k, v := range tableVer.versions[tableVer.latest].schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    tableVer.versions[tableVer.latest].schema.Name,
			Schema:  tableVer.versions[tableVer.latest].schema.Schema,
			Columns: tableVer.versions[tableVer.latest].schema.Columns,
			Atts:    atts,
		},
		rows:      []domain.Row{},
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return nil
}

// CreateIndex 创建索引
func (m *MVCCDataSource) CreateIndex(tableName, columnName, indexType string, unique bool) error {
	// 转换索引类型
	var idxType IndexType
	switch indexType {
	case "btree":
		idxType = IndexTypeBTree
	case "hash":
		idxType = IndexTypeHash
	case "fulltext":
		idxType = IndexTypeFullText
	default:
		idxType = IndexTypeBTree // 默认
	}

	// 创建索引
	_, err := m.indexManager.CreateIndex(tableName, columnName, idxType, unique)
	if err != nil {
		return fmt.Errorf("create index failed: %w", err)
	}

	return nil
}

// DropIndex 删除索引
func (m *MVCCDataSource) DropIndex(tableName, indexName string) error {
	err := m.indexManager.DropIndex(tableName, indexName)
	if err != nil {
		return fmt.Errorf("drop index failed: %w", err)
	}

	return nil
}

// ==================== 数据查询 ====================

// Query 查询数据
func (m *MVCCDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.mu.RLock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.RUnlock()
		return nil, domain.NewErrTableNotFound(tableName)
	}

	txnID, hasTxn := GetTransactionID(ctx)
	var tableData *TableData

	if hasTxn {
		// 在事务中，从COW快照读取数据
		snapshot, ok := m.snapshots[txnID]
		if ok {
			cowSnapshot, ok := snapshot.tableSnapshots[tableName]
			if ok {
				tableData = cowSnapshot.getTableData(tableVer)
				m.mu.RUnlock()
			} else {
				m.mu.RUnlock()
				tableVer.mu.RLock()
				tableData = tableVer.versions[tableVer.latest]
				tableVer.mu.RUnlock()
			}
		} else {
			m.mu.RUnlock()
			tableVer.mu.RLock()
			tableData = tableVer.versions[tableVer.latest]
			tableVer.mu.RUnlock()
		}
	} else {
		// 非事务查询，从最新版本读取
		m.mu.RUnlock()
		tableVer.mu.RLock()
		tableData = tableVer.versions[tableVer.latest]
		tableVer.mu.RUnlock()
	}

	if tableData == nil {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	// 使用查询优化器优化查询
	var queryResult *domain.QueryResult
	var err error

	if options != nil && len(options.Filters) > 0 {
		// 有过滤条件，使用查询优化器
		plan, planErr := m.queryPlanner.PlanQuery(tableName, options.Filters, options)
		if planErr != nil {
			// 优化失败，使用全表扫描
			pagedRows := util.ApplyQueryOperations(tableData.rows, options, &tableData.schema.Columns)
			queryResult = &domain.QueryResult{
				Columns: tableData.schema.Columns,
				Rows:    pagedRows,
				Total:   int64(len(pagedRows)),
			}
		} else {
			// 执行优化后的查询计划
			queryResult, err = m.queryPlanner.ExecutePlan(plan, tableData)
			if err != nil {
				// 执行失败，使用全表扫描
				pagedRows := util.ApplyQueryOperations(tableData.rows, options, &tableData.schema.Columns)
				queryResult = &domain.QueryResult{
					Columns: tableData.schema.Columns,
					Rows:    pagedRows,
					Total:   int64(len(pagedRows)),
				}
			} else {
				// 第二阶段：处理 VIRTUAL 列的动态计算
				// 检查表是否包含 VIRTUAL 列
				virtualCalc := generated.NewVirtualCalculator()
				if virtualCalc.HasVirtualColumns(tableData.schema) {
					// 动态计算所有 VIRTUAL 列
					calculatedRows, calcErr := virtualCalc.CalculateBatchVirtuals(queryResult.Rows, tableData.schema)
					if calcErr == nil {
						queryResult.Rows = calculatedRows
					}
					// 如果计算失败，使用原始行数据（VIRTUAL 列为 NULL）
				}

				// 应用排序和分页
				if options != nil {
					if options.OrderBy != "" {
						queryResult.Rows = util.ApplyOrder(queryResult.Rows, options)
						}
						if options.Limit > 0 || options.Offset > 0 {
							queryResult.Rows = util.ApplyPagination(queryResult.Rows, int(options.Limit), int(options.Offset))
						}
					}
					queryResult.Total = int64(len(queryResult.Rows))
				}
			}
	} else {
		// 无过滤条件，使用全表扫描
		pagedRows := util.ApplyQueryOperations(tableData.rows, options, &tableData.schema.Columns)
		queryResult = &domain.QueryResult{
			Columns: tableData.schema.Columns,
			Rows:    pagedRows,
			Total:   int64(len(pagedRows)),
		}
		// 第二阶段：处理 VIRTUAL 列的动态计算
		virtualCalc := generated.NewVirtualCalculator()
		if virtualCalc.HasVirtualColumns(tableData.schema) {
			// 动态计算所有 VIRTUAL 列
			calculatedRows, calcErr := virtualCalc.CalculateBatchVirtuals(queryResult.Rows, tableData.schema)
			if calcErr == nil {
				queryResult.Rows = calculatedRows
			}
			// 如果计算失败，使用原始行数据（VIRTUAL 列为 NULL）
		}
	}

	return queryResult, nil
}

// ==================== 数据修改 ====================

// Insert 插入数据
func (m *MVCCDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "insert")
	}

	txnID, hasTxn := GetTransactionID(ctx)

	// 先获取全局锁
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 获取表schema
	sourceData := tableVer.versions[tableVer.latest]
	schema := sourceData.schema

	// 处理生成列：区分 STORED 和 VIRTUAL 类型
	processedRows := make([]domain.Row, 0, len(rows))
	evaluator := generated.NewGeneratedColumnEvaluator()

	for _, row := range rows {
		// 1. 过滤生成列的显式插入值（不允许显式插入）
		filteredRow := generated.FilterGeneratedColumns(row, schema)

		// 2. 区分 STORED 和 VIRTUAL 列
		// STORED 列：计算并存储
		// VIRTUAL 列：不存储到表数据中
		var storedRow domain.Row

		// 检查表是否包含 VIRTUAL 列
		hasVirtualCols := false
		for _, col := range schema.Columns {
			if col.IsGenerated && col.GeneratedType == "VIRTUAL" {
				hasVirtualCols = true
				break
			}
		}

		if hasVirtualCols {
			// 计算所有 STORED 生成列（不包括 VIRTUAL）
			// 注意：EvaluateAll 会计算所有生成列，但我们只保存 STORED 列
			computedRow, err := evaluator.EvaluateAll(filteredRow, schema)
			if err != nil {
				// 计算失败，将生成列设为 NULL
				computedRow = generated.SetGeneratedColumnsToNULL(filteredRow, schema)
			}

			// 移除 VIRTUAL 列（不存储）
			storedRow = m.removeVirtualColumns(computedRow, schema)
			// 确保只保留基础列和 STORED 生成列
			storedRow = make(map[string]interface{})
			for k, v := range computedRow {
				// 只保留非生成列或 STORED 类型
				keep := true
				for _, col := range schema.Columns {
					if col.Name == k && col.IsGenerated && col.GeneratedType == "VIRTUAL" {
						keep = false
						break
					}
				}
				if keep {
					storedRow[k] = v
				}
			}
		} else {
			// 没有 VIRTUAL 列，保持原有逻辑
			computedRow, err := evaluator.EvaluateAll(filteredRow, schema)
			if err != nil {
				computedRow = generated.SetGeneratedColumnsToNULL(filteredRow, schema)
			}
			storedRow = computedRow
		}

		processedRows = append(processedRows, storedRow)
	}

	// 替换原始行为处理后的行
	rows = processedRows

	if hasTxn {
		// 在事务中，使用COW快照
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, fmt.Errorf("transaction not found: %d", txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// 确保数据已拷贝（写时复制，行级COW）
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// 行级COW：不直接拷贝整个表，只记录新插入的行
		cowSnapshot.mu.Lock()

		// 获取基础数据的行数
		baseRowsCount := int64(len(cowSnapshot.baseData.rows))
		inserted := int64(0)

		for _, row := range rows {
			// 每个新行使用递增的rowID（从基础数据行数+1开始）
			rowID := baseRowsCount + inserted + 1
			cowSnapshot.rowLocks[rowID] = true

			// 深拷贝行数据
			rowCopy := make(map[string]interface{}, len(row))
			for k, v := range row {
				rowCopy[k] = v
			}
			cowSnapshot.rowCopies[rowID] = rowCopy

			inserted++
		}

		cowSnapshot.mu.Unlock()
		return inserted, nil
	}

	// 非事务模式：在持有全局锁时，获取表版本锁
	// 锁顺序：先全局锁，后表级锁（避免死锁）
	tableVer.mu.Lock()

	// 现在可以安全地释放全局锁，因为已经持有表锁
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 非事务插入，创建新版本
	m.currentVer++
	
	// 深拷贝 schema
	cols := make([]domain.ColumnInfo, len(latestData.schema.Columns))
	for i := range latestData.schema.Columns {
		cols[i] = domain.ColumnInfo{
			Name:           latestData.schema.Columns[i].Name,
			Type:           latestData.schema.Columns[i].Type,
			Nullable:       latestData.schema.Columns[i].Nullable,
			Primary:        latestData.schema.Columns[i].Primary,
			Default:        latestData.schema.Columns[i].Default,
			Unique:         latestData.schema.Columns[i].Unique,
			AutoIncrement:   latestData.schema.Columns[i].AutoIncrement,
			ForeignKey:      latestData.schema.Columns[i].ForeignKey,
			IsGenerated:     latestData.schema.Columns[i].IsGenerated,
			GeneratedType:   latestData.schema.Columns[i].GeneratedType,
			GeneratedExpr:   latestData.schema.Columns[i].GeneratedExpr,
			GeneratedDepends: latestData.schema.Columns[i].GeneratedDepends,
		}
	}

	// 深拷贝表属性
	var atts map[string]interface{}
	if latestData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(latestData.schema.Atts))
		for k, v := range latestData.schema.Atts {
			atts[k] = v
		}
	}

	newRows := make([]domain.Row, len(latestData.rows)+len(rows))
	copy(newRows, latestData.rows)
	copy(newRows[len(latestData.rows):], rows)

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    latestData.schema.Name,
			Schema:  latestData.schema.Schema,
			Columns: cols,
			Atts:    atts,
		},
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

	txnID, hasTxn := GetTransactionID(ctx)

	// 先获取全局锁
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 获取表schema
	sourceData := tableVer.versions[tableVer.latest]
	schema := sourceData.schema

	// 过滤生成列的更新值（不允许显式更新）
	filteredUpdates := generated.FilterGeneratedColumns(updates, schema)

	// 获取受影响的生成列（递归）
	updatedCols := make([]string, 0, len(filteredUpdates))
	for k := range filteredUpdates {
		updatedCols = append(updatedCols, k)
	}
	affectedGeneratedCols := generated.GetAffectedGeneratedColumns(updatedCols, schema)

	if hasTxn {
		// 在事务中，使用COW快照
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, fmt.Errorf("transaction not found: %d", txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// 确保数据已拷贝（写时复制，行级COW）
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// 创建求值器
		evaluator := generated.NewGeneratedColumnEvaluator()

		// 行级COW：遍历基础数据，对匹配的行进行拷贝和修改
		cowSnapshot.mu.Lock()
		defer cowSnapshot.mu.Unlock()

		updated := int64(0)
		for i, row := range cowSnapshot.baseData.rows {
			rowID := int64(i + 1) // 行ID从1开始
			if util.MatchesFilters(row, filters) {
				// 行匹配过滤条件，需要进行修改
				if _, alreadyModified := cowSnapshot.rowLocks[rowID]; !alreadyModified {
					// 第一次修改此行，创建深拷贝
					rowCopy := make(map[string]interface{}, len(row))
					for k, v := range row {
						rowCopy[k] = v
					}
					// 应用更新
					for k, v := range filteredUpdates {
						rowCopy[k] = v
					}
					// 计算受影响的生成列
					for _, genColName := range affectedGeneratedCols {
						colInfo := getColumnInfo(genColName, schema)
						if colInfo != nil && colInfo.IsGenerated {
							val, err := evaluator.Evaluate(colInfo.GeneratedExpr, rowCopy, schema)
							if err != nil {
								val = nil // 计算失败设为 NULL
							}
							rowCopy[genColName] = val
						}
					}
					// 存储修改后的行
					cowSnapshot.rowCopies[rowID] = rowCopy
					cowSnapshot.rowLocks[rowID] = true
				} else {
					// 行已经修改过，直接更新已有副本
					if existingRow, ok := cowSnapshot.rowCopies[rowID]; ok {
						for k, v := range filteredUpdates {
							existingRow[k] = v
						}
						// 计算受影响的生成列
						for _, genColName := range affectedGeneratedCols {
							colInfo := getColumnInfo(genColName, schema)
							if colInfo != nil && colInfo.IsGenerated {
								val, err := evaluator.Evaluate(colInfo.GeneratedExpr, existingRow, schema)
								if err != nil {
									val = nil // 计算失败设为 NULL
								}
								existingRow[genColName] = val
							}
						}
					}
				}
				updated++
			}
		}
		return updated, nil
	}

	// 非事务模式：锁顺序：先全局锁，后表级锁
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	sourceData = tableVer.versions[tableVer.latest]
	if sourceData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 创建求值器
	evaluator := generated.NewGeneratedColumnEvaluator()

	// 非事务更新，创建新版本
	m.currentVer++
	newRows := make([]domain.Row, len(sourceData.rows))
	copy(newRows, sourceData.rows)

	updated := int64(0)
	for i, row := range newRows {
		if util.MatchesFilters(row, filters) {
			// 应用更新
			for k, v := range filteredUpdates {
				newRows[i][k] = v
			}
			// 计算受影响的生成列
			for _, genColName := range affectedGeneratedCols {
				colInfo := getColumnInfo(genColName, schema)
				if colInfo != nil && colInfo.IsGenerated {
					val, err := evaluator.Evaluate(colInfo.GeneratedExpr, newRows[i], schema)
					if err != nil {
						val = nil // 计算失败设为 NULL
					}
					newRows[i][genColName] = val
				}
			}
			updated++
		}
	}

	// 深拷贝表属性
	var atts map[string]interface{}
	if sourceData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(sourceData.schema.Atts))
		for k, v := range sourceData.schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    sourceData.schema.Name,
			Schema:  sourceData.schema.Schema,
			Columns: sourceData.schema.Columns,
			Atts:    atts,
		},
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

	txnID, hasTxn := GetTransactionID(ctx)

	// 先获取全局锁
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	var sourceData *TableData
	if hasTxn {
		// 在事务中，使用COW快照
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, fmt.Errorf("transaction not found: %d", txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// 确保数据已拷贝（写时复制，行级COW）
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// 行级COW：标记要删除的行，不立即修改数据
		cowSnapshot.mu.Lock()
		defer cowSnapshot.mu.Unlock()

		deleted := int64(0)
		for i, row := range cowSnapshot.baseData.rows {
			rowID := int64(i + 1) // 行ID从1开始

			// 检查行是否匹配删除条件
			if util.MatchesFilters(row, filters) {
				// 如果此行已经被修改过，需要从rowCopies中移除
				if _, alreadyModified := cowSnapshot.rowLocks[rowID]; alreadyModified {
					delete(cowSnapshot.rowCopies, rowID)
				}
				// 标记为已删除
				cowSnapshot.deletedRows[rowID] = true
				delete(cowSnapshot.rowLocks, rowID)
				deleted++
			}
		}
		return deleted, nil
	}

	// 非事务模式：锁顺序：先全局锁，后表级锁
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	sourceData = tableVer.versions[tableVer.latest]
	if sourceData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// 非事务删除，创建新版本
	m.currentVer++
	newRows := make([]domain.Row, 0, len(sourceData.rows))

	deleted := int64(0)
	for _, row := range sourceData.rows {
		if !util.MatchesFilters(row, filters) {
			newRows = append(newRows, row)
		} else {
			deleted++
		}
	}

	// 深拷贝表属性
	var atts map[string]interface{}
	if sourceData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(sourceData.schema.Atts))
		for k, v := range sourceData.schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    sourceData.schema.Name,
			Schema:  sourceData.schema.Schema,
			Columns: sourceData.schema.Columns,
			Atts:    atts,
		},
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

// removeVirtualColumns 从行中移除 VIRTUAL 列（不存储）
func (m *MVCCDataSource) removeVirtualColumns(row domain.Row, schema *domain.TableInfo) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		// 只保留非 VIRTUAL 列
		if !generated.IsVirtualColumn(k, schema) {
			result[k] = v
		}
	}
	return result
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

	// 深拷贝表属性
	var atts map[string]interface{}
	if schema.Atts != nil {
		atts = make(map[string]interface{}, len(schema.Atts))
		for k, v := range schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    schema.Name,
			Schema:  schema.Schema,
			Columns: cols,
			Atts:    atts,
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

	// 重建索引
	_ = m.indexManager.RebuildIndex(tableName, versionData.schema, rows)

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

// getColumnInfo 获取列信息
func getColumnInfo(name string, schema *domain.TableInfo) *domain.ColumnInfo {
	for i, col := range schema.Columns {
		if col.Name == name {
			return &schema.Columns[i]
		}
	}
	return nil
}
