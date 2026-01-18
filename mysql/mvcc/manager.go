package mvcc

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== 配置 ====================

// Config MVCC配置
type Config struct {
	EnableWarning      bool          // 是否启用警告
	AutoDowngrade      bool          // 是否自动降级
	GCInterval         time.Duration // GC间隔
	GCAgeThreshold     time.Duration // 版本保留时间
	XIDWrapThreshold   uint32        // XID环绕阈值
	MaxActiveTxns      int           // 最大活跃事务数
	WarningLogger      *log.Logger   // 警告日志器
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		EnableWarning:      true,
		AutoDowngrade:      true,
		GCInterval:         5 * time.Minute,
		GCAgeThreshold:     1 * time.Hour,
		XIDWrapThreshold:   100000,
		MaxActiveTxns:      10000,
		WarningLogger:      log.Default(),
	}
}

// ==================== 事务管理器 ====================

// Manager 事务管理器
type Manager struct {
	config        *Config
	xid           XID               // 当前事务ID
	snapshots     map[XID]*Snapshot  // 快照缓存
	transactions  map[XID]*Transaction // 活跃事务
	clog          *CommitLog         // 提交日志
	checker       *VisibilityChecker // 可见性检查器
	dataSources   map[string]*DataSourceFeatures // 数据源特性缓存
	mu            sync.RWMutex      // 全局互斥锁
	closed        bool              // 是否已关闭
	gcStop        chan struct{}     // GC停止信号
}

var (
	// globalManager 全局管理器
	globalManager *Manager
	globalOnce    sync.Once
)

// NewManager 创建事务管理器
func NewManager(config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	}
	
	m := &Manager{
		config:       config,
		xid:          XIDBootstrap,
		snapshots:    make(map[XID]*Snapshot),
		transactions: make(map[XID]*Transaction),
		clog:         NewCommitLog(),
		checker:      NewVisibilityChecker(),
		dataSources:  make(map[string]*DataSourceFeatures),
		gcStop:       make(chan struct{}),
	}
	
	// 启动GC协程
	go m.gcLoop()
	
	return m
}

// GetGlobalManager 获取全局管理器（单例）
func GetGlobalManager() *Manager {
	globalOnce.Do(func() {
		globalManager = NewManager(DefaultConfig())
	})
	return globalManager
}

// Close 关闭管理器
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return nil
	}
	
	// 停止GC
	close(m.gcStop)
	
	// 清理资源
	m.closed = true
	
	return nil
}

// ==================== 事务管理 ====================

// Begin 开始事务
func (m *Manager) Begin(level IsolationLevel, features *DataSourceFeatures) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return nil, fmt.Errorf("manager is closed")
	}
	
	// 检查活跃事务数
	if len(m.transactions) >= m.config.MaxActiveTxns {
		return nil, fmt.Errorf("too many active transactions")
	}
	
	// 检查数据源是否支持MVCC
	if !m.checkMVCCCapability(features) {
		// 数据源不支持MVCC，降级
		return m.beginNonMVCC(level)
	}
	
	// 生成新XID
	xid := m.nextXID()
	
	// 创建快照（注意：调用者已持有写锁，不能再次获取锁）
	xip := make([]XID, 0, len(m.transactions))
	for txnXID := range m.transactions {
		xip = append(xip, txnXID)
	}
	snapshot := NewSnapshot(m.xid, xid, xip, level)
	m.snapshots[xid] = snapshot
	
	// 创建事务
	txn := &Transaction{
		xid:        xid,
		snapshot:   snapshot,
		status:     TxnStatusInProgress,
		createdAt:  time.Now(),
		startTime:  time.Now(),
		manager:    m,
		level:      level,
		mvcc:       true,
		commands:   make([]Command, 0),
		reads:      make(map[string]bool),
		writes:     make(map[string]*TupleVersion),
		locks:      make(map[string]bool),
	}
	
	// 记录事务
	m.transactions[xid] = txn
	
	return txn, nil
}

// beginNonMVCC 开始非MVCC事务（降级）
func (m *Manager) beginNonMVCC(level IsolationLevel) (*Transaction, error) {
	// 输出警告
	if m.config.EnableWarning {
		m.warning("MVCC not supported, downgrading to non-MVCC transaction")
	}
	
	// 创建非MVCC事务
	txn := &Transaction{
		xid:        0,
		snapshot:   nil,
		status:     TxnStatusInProgress,
		createdAt:  time.Now(),
		startTime:  time.Now(),
		manager:    m,
		level:      level,
		mvcc:       false,
		commands:   make([]Command, 0),
		reads:      make(map[string]bool),
		writes:     make(map[string]*TupleVersion),
		locks:      make(map[string]bool),
	}
	
	return txn, nil
}

// Commit 提交事务
func (m *Manager) Commit(txn *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if txn.status != TxnStatusInProgress {
		return fmt.Errorf("transaction is not in progress")
	}
	
	// 如果是非MVCC事务
	if !txn.mvcc {
		txn.status = TxnStatusCommitted
		txn.endTime = time.Now()
		return nil
	}
	
	// 应用所有命令
	for _, cmd := range txn.commands {
		if err := cmd.Apply(); err != nil {
			// 回滚
			for i := len(txn.commands) - 1; i >= 0; i-- {
				txn.commands[i].Rollback()
			}
			txn.status = TxnStatusAborted
			return err
		}
	}
	
	// 更新事务状态
	txn.status = TxnStatusCommitted
	txn.endTime = time.Now()
	
	// 记录到clog
	m.clog.SetStatus(txn.xid, TxnStatusCommitted)
	
	// 从活跃事务中移除
	delete(m.transactions, txn.xid)
	
	// 清理快照
	delete(m.snapshots, txn.xid)
	
	return nil
}

// Rollback 回滚事务
func (m *Manager) Rollback(txn *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if txn.status != TxnStatusInProgress {
		return fmt.Errorf("transaction is not in progress")
	}
	
	// 回滚所有命令
	for i := len(txn.commands) - 1; i >= 0; i-- {
		txn.commands[i].Rollback()
	}
	
	// 更新事务状态
	txn.status = TxnStatusAborted
	txn.endTime = time.Now()
	
	// 如果是MVCC事务
	if txn.mvcc && txn.xid != 0 {
		// 记录到clog
		m.clog.SetStatus(txn.xid, TxnStatusAborted)
		
		// 从活跃事务中移除
		delete(m.transactions, txn.xid)
		
		// 清理快照
		delete(m.snapshots, txn.xid)
	}
	
	return nil
}


// ==================== 快照管理 ====================

// GetSnapshot 获取快照
func (m *Manager) GetSnapshot(xid XID) (*Snapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	snapshot, exists := m.snapshots[xid]
	return snapshot, exists
}

// ==================== MVCC能力检查 ====================

// checkMVCCCapability 检查数据源是否支持MVCC
func (m *Manager) checkMVCCCapability(features *DataSourceFeatures) bool {
	if features == nil {
		return false
	}
	return features.HasMVCC()
}

// RegisterDataSource 注册数据源特性
func (m *Manager) RegisterDataSource(features *DataSourceFeatures) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.dataSources[features.Name] = features
}

// GetDataSource 获取数据源特性
func (m *Manager) GetDataSource(name string) (*DataSourceFeatures, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	features, exists := m.dataSources[name]
	return features, exists
}

// ==================== XID管理 ====================

// nextXID 生成下一个XID
func (m *Manager) nextXID() XID {
	current := atomic.LoadUint32((*uint32)(&m.xid))
	
	// 处理环绕
	if current >= uint32(XIDMax)-m.config.XIDWrapThreshold {
		m.warning("XID approaching wrap-around, consider vacuum")
	}
	
	next := NextXID(XID(current))
	atomic.StoreUint32((*uint32)(&m.xid), uint32(next))
	
	return next
}

// CurrentXID 返回当前XID
func (m *Manager) CurrentXID() XID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.xid
}

// ==================== GC机制 ====================

// gcLoop GC循环
func (m *Manager) gcLoop() {
	ticker := time.NewTicker(m.config.GCInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			m.GC()
		case <-m.gcStop:
			return
		}
	}
}

// GC 执行垃圾回收
func (m *Manager) GC() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 清理过期快照
	m.gcSnapshots()
	
	// 清理clog
	m.clog.GC(m.xid)
}

// gcSnapshots 清理过期快照
func (m *Manager) gcSnapshots() {
	for xid, snapshot := range m.snapshots {
		if snapshot.Age() > m.config.GCAgeThreshold {
			delete(m.snapshots, xid)
		}
	}
}

// ==================== 工具方法 ====================

// warning 输出警告
func (m *Manager) warning(msg string) {
	if m.config.EnableWarning && m.config.WarningLogger != nil {
		m.config.WarningLogger.Printf("[MVCC-WARN] %s\n", msg)
	}
}

// GetStatistics 获取统计信息
func (m *Manager) GetStatistics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return map[string]interface{}{
		"current_xid":      m.xid,
		"active_txns":      len(m.transactions),
		"cached_snapshots": len(m.snapshots),
		"closed":           m.closed,
	}
}

// ListActiveTransactions 列出活跃事务
func (m *Manager) ListActiveTransactions() []XID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	txns := make([]XID, 0, len(m.transactions))
	for xid := range m.transactions {
		txns = append(txns, xid)
	}
	return txns
}

// IsTransactionActive 检查事务是否活跃
func (m *Manager) IsTransactionActive(xid XID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	_, exists := m.transactions[xid]
	return exists
}

// GetCommitLog 获取提交日志
func (m *Manager) GetCommitLog() *CommitLog {
	return m.clog
}

// GetVisibilityChecker 获取可见性检查器
func (m *Manager) GetVisibilityChecker() *VisibilityChecker {
	return m.checker
}
