package resource

import (
	"context"
	"fmt"
	"github.com/kasuganosora/sqlexec/service/mvcc"
	"sync"
)

// MVCCMemorySource 支持MVCC的内存数据源
type MVCCMemorySource struct {
	*MemorySource                  // 嵌入基础内存数据�?
	manager        *mvcc.Manager   // MVCC管理�?
	mvccData       map[string][]*mvcc.TupleVersion // �?-> 版本�?
	txns           map[mvcc.XID]*MVCCContext      // 事务上下�?
	mu             sync.RWMutex
}

// MVCCContext MVCC事务上下�?
type MVCCContext struct {
	xid         mvcc.XID
	snapshot    *mvcc.Snapshot
	level       mvcc.IsolationLevel
	writes      map[string]map[string]*mvcc.TupleVersion // table -> key -> version
	deletes     map[string]map[string]bool              // table -> key -> deleted
	snapshots   map[string][]*mvcc.TupleVersion        // 缓存的表快照（用于可重复读）
	mu          sync.RWMutex
}

// NewMVCCMemorySource 创建支持MVCC的内存数据源
func NewMVCCMemorySource(config *DataSourceConfig) *MVCCMemorySource {
	base := &MemorySource{
		config:   config,
		writable: true,
		tables:   make(map[string]*TableInfo),
		data:     make(map[string][]Row),
		autoID:   make(map[string]int64),
	}

	manager := mvcc.GetGlobalManager()

	return &MVCCMemorySource{
		MemorySource: base,
		manager:      manager,
		mvccData:     make(map[string][]*mvcc.TupleVersion),
		txns:         make(map[mvcc.XID]*MVCCContext),
	}
}

// ==================== 事务管理 ====================

// BeginTransaction 开始事�?
func (s *MVCCMemorySource) BeginTransaction(level mvcc.IsolationLevel) (mvcc.XID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 创建数据源特�?
	features := mvcc.NewDataSourceFeatures("memory-mvcc", mvcc.CapabilityFull)
	features.AddSupport("transaction")
	features.AddSupport("mvcc")
	features.AddSupport("isolation_levels")

	// 注册数据�?
	s.manager.RegisterDataSource(features)

	// 开始事�?
	txn, err := s.manager.Begin(level, features)
	if err != nil {
		return 0, err
	}

	// 创建事务上下�?
	ctx := &MVCCContext{
		xid:       txn.XID(),
		snapshot:  txn.Snapshot(),
		level:     level,
		writes:    make(map[string]map[string]*mvcc.TupleVersion),
		deletes:   make(map[string]map[string]bool),
		snapshots: make(map[string][]*mvcc.TupleVersion),
	}

	s.txns[txn.XID()] = ctx

	return txn.XID(), nil
}

// CommitTransaction 提交事务
func (s *MVCCMemorySource) CommitTransaction(xid mvcc.XID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取事务上下�?
	ctx, exists := s.txns[xid]
	if !exists {
		return fmt.Errorf("transaction not found: %d", xid)
	}

	// 应用所有写�?
	for tableName, writes := range ctx.writes {
		if _, ok := s.mvccData[tableName]; !ok {
			s.mvccData[tableName] = make([]*mvcc.TupleVersion, 0)
		}

		for _, version := range writes {
			s.mvccData[tableName] = append(s.mvccData[tableName], version)
		}
	}

	// 应用所有删�?
	for tableName, deletes := range ctx.deletes {
		if versions, ok := s.mvccData[tableName]; ok {
			for _, version := range versions {
				if !version.Expired {
					if deletes[version.CTID] {
						// 标记为删除（使用事务的XID�?
						version.MarkDeleted(xid, 0)
						break
					}
				}
			}
		}
	}

	// 提交事务
	if ctx.snapshot != nil {
		// 记录事务状态到clog
		if err := s.manager.SetTransactionStatus(xid, mvcc.TxnStatusCommitted); err != nil {
			return err
		}
	}

	// 清理事务上下�?
	delete(s.txns, xid)

	return nil
}

// RollbackTransaction 回滚事务
func (s *MVCCMemorySource) RollbackTransaction(xid mvcc.XID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取事务上下�?
	ctx, exists := s.txns[xid]
	if !exists {
		return fmt.Errorf("transaction not found: %d", xid)
	}

	// 回滚事务
	if ctx.snapshot != nil {
		// 记录事务状态到clog
		if err := s.manager.SetTransactionStatus(xid, mvcc.TxnStatusAborted); err != nil {
			return err
		}
	}

	// 清理事务上下文（不需要应用任何写入，因为它们没有被提交）
	delete(s.txns, xid)

	return nil
}

// ==================== MVCC查询 ====================

// QueryWithTransaction 使用事务查询
func (s *MVCCMemorySource) QueryWithTransaction(ctx context.Context, tableName string, options *QueryOptions, xid mvcc.XID) (*QueryResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 获取事务上下�?
	txnCtx, exists := s.txns[xid]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %d", xid)
	}

	// 检查表是否存在
	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// 获取所有版�?
	versions, ok := s.mvccData[tableName]
	if !ok {
		// 表没有数据，从基础数据源获�?
		return s.MemorySource.Query(ctx, tableName, options)
	}

	// 对于可重复读和串行化，使用缓存的快照
	var visibleVersions []*mvcc.TupleVersion
	if txnCtx.level == mvcc.RepeatableRead || txnCtx.level == mvcc.Serializable {
		// 检查是否已有缓存的快照
		if cached, hasCache := txnCtx.snapshots[tableName]; hasCache {
			visibleVersions = cached
		} else {
			// 使用快照过滤可见版本并缓�?
			checker := s.manager.GetVisibilityChecker()
			visibleVersions = checker.FilterVisible(versions, txnCtx.snapshot)
			txnCtx.snapshots[tableName] = visibleVersions
		}
	} else {
		// 读已提交或读未提交，每次都重新计�?
		checker := s.manager.GetVisibilityChecker()
		visibleVersions = checker.FilterVisible(versions, txnCtx.snapshot)
	}

	// 检查事务中的删�?
	if deletes, ok := txnCtx.deletes[tableName]; ok {
		filtered := make([]*mvcc.TupleVersion, 0)
		for _, version := range visibleVersions {
			key := version.CTID
			if !deletes[key] {
				filtered = append(filtered, version)
			}
		}
		visibleVersions = filtered
	}

	// 检查事务中的写入（未提交的�?
	if writes, ok := txnCtx.writes[tableName]; ok {
		for _, version := range writes {
			visibleVersions = append(visibleVersions, version)
		}
	}

	// 转换为Row格式
	rows := make([]Row, 0)
	for _, version := range visibleVersions {
		if data, ok := version.GetValue().(Row); ok {
			rows = append(rows, data)
		}
	}

	// 应用过滤�?
	filteredRows := s.applyFilters(rows, options)

	// 应用排序
	sortedRows := s.applyOrder(filteredRows, options)

	// 应用分页
	pagedRows := s.applyPagination(sortedRows, options)

	total := int64(len(pagedRows))

	return &QueryResult{
		Columns: table.Columns,
		Rows:    pagedRows,
		Total:   total,
	}, nil
}

// ==================== MVCC写入 ====================

// InsertWithTransaction 使用事务插入
func (s *MVCCMemorySource) InsertWithTransaction(ctx context.Context, tableName string, rows []Row, options *InsertOptions, xid mvcc.XID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取事务上下�?
	txnCtx, exists := s.txns[xid]
	if !exists {
		return 0, fmt.Errorf("transaction not found: %d", xid)
	}

	// 检查表是否存在
	table, ok := s.tables[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	// 初始化写入map
	if txnCtx.writes[tableName] == nil {
		txnCtx.writes[tableName] = make(map[string]*mvcc.TupleVersion)
	}

	inserted := int64(0)
	for _, row := range rows {
		// 处理自增ID
		newRow := make(Row)
		for k, v := range row {
			newRow[k] = v
		}

		// 查找主键列并处理自增
		for _, col := range table.Columns {
			if col.Primary {
				if _, exists := newRow[col.Name]; !exists {
					s.autoID[tableName]++
					newRow[col.Name] = s.autoID[tableName]
				}
			}
		}

		// 创建新的TupleVersion
		ctid := fmt.Sprintf("ctid:%d:%d", xid, len(txnCtx.writes[tableName]))
		version := mvcc.NewTupleVersion(newRow, xid)
		version.CTID = ctid

		// 添加到事务上下文
		txnCtx.writes[tableName][ctid] = version
		inserted++
	}

	return inserted, nil
}

// UpdateWithTransaction 使用事务更新
func (s *MVCCMemorySource) UpdateWithTransaction(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions, xid mvcc.XID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取事务上下�?
	txnCtx, exists := s.txns[xid]
	if !exists {
		return 0, fmt.Errorf("transaction not found: %d", xid)
	}

	// 检查表是否存在
	if _, ok := s.tables[tableName]; !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	// 获取MVCC数据
	versions, ok := s.mvccData[tableName]
	if !ok {
		// 表没有数据，从基础数据源获�?
		return s.MemorySource.Update(ctx, tableName, filters, updates, options)
	}

	// 使用快照过滤可见版本
	checker := s.manager.GetVisibilityChecker()
	visibleVersions := checker.FilterVisible(versions, txnCtx.snapshot)

	// 匹配过滤�?
	matchedVersions := make([]*mvcc.TupleVersion, 0)
	for _, version := range visibleVersions {
		if row, ok := version.GetValue().(Row); ok {
			if s.matchesFilters(row, filters) {
				matchedVersions = append(matchedVersions, version)
			}
		}
	}

	// 初始化写入map
	if txnCtx.writes[tableName] == nil {
		txnCtx.writes[tableName] = make(map[string]*mvcc.TupleVersion)
	}

	updated := int64(0)
	for _, oldVersion := range matchedVersions {
		// 标记旧版本为删除
		oldVersion.MarkDeleted(xid, 0)

		// 创建新版�?
		if oldRow, ok := oldVersion.GetValue().(Row); ok {
			newRow := make(Row)
			for k, v := range oldRow {
				newRow[k] = v
			}
			for k, v := range updates {
				newRow[k] = v
			}

			ctid := fmt.Sprintf("ctid:%d:%d", xid, len(txnCtx.writes[tableName]))
			newVersion := mvcc.NewTupleVersion(newRow, xid)
			newVersion.CTID = ctid

			txnCtx.writes[tableName][ctid] = newVersion
			updated++
		}
	}

	return updated, nil
}

// DeleteWithTransaction 使用事务删除
func (s *MVCCMemorySource) DeleteWithTransaction(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions, xid mvcc.XID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取事务上下�?
	txnCtx, exists := s.txns[xid]
	if !exists {
		return 0, fmt.Errorf("transaction not found: %d", xid)
	}

	// 检查表是否存在
	if _, ok := s.tables[tableName]; !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	// 获取MVCC数据
	versions, ok := s.mvccData[tableName]
	if !ok {
		// 表没有数据，从基础数据源获�?
		return s.MemorySource.Delete(ctx, tableName, filters, options)
	}

	// 使用快照过滤可见版本
	checker := s.manager.GetVisibilityChecker()
	visibleVersions := checker.FilterVisible(versions, txnCtx.snapshot)

	// 初始化删除map
	if txnCtx.deletes[tableName] == nil {
		txnCtx.deletes[tableName] = make(map[string]bool)
	}

	deleted := int64(0)
	for _, version := range visibleVersions {
		if row, ok := version.GetValue().(Row); ok {
			if s.matchesFilters(row, filters) {
				txnCtx.deletes[tableName][version.CTID] = true
				deleted++
			}
		}
	}

	return deleted, nil
}
