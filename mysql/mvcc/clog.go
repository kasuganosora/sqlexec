package mvcc

import (
	"sync"
)

// ==================== 事务日志 ====================

// CommitLog 事务提交日志（类似PostgreSQL的clog）
type CommitLog struct {
	entries  map[XID]TransactionStatus // 事务状态映射
	oldest   XID                        // 最小事务ID
	mu       sync.RWMutex
}

// NewCommitLog 创建提交日志
func NewCommitLog() *CommitLog {
	return &CommitLog{
		entries: make(map[XID]TransactionStatus),
		oldest:  XIDBootstrap,
	}
}

// SetStatus 设置事务状态
func (l *CommitLog) SetStatus(xid XID, status TransactionStatus) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.entries[xid] = status
	
	// 更新最小XID
	if xid < l.oldest {
		l.oldest = xid
	}
}

// GetStatus 获取事务状态
func (l *CommitLog) GetStatus(xid XID) (TransactionStatus, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	status, exists := l.entries[xid]
	return status, exists
}

// IsCommitted 检查事务是否已提交
func (l *CommitLog) IsCommitted(xid XID) bool {
	status, exists := l.GetStatus(xid)
	return exists && status == TxnStatusCommitted
}

// IsAborted 检查事务是否已回滚
func (l *CommitLog) IsAborted(xid XID) bool {
	status, exists := l.GetStatus(xid)
	return exists && status == TxnStatusAborted
}

// IsInProgress 检查事务是否进行中
func (l *CommitLog) IsInProgress(xid XID) bool {
	status, exists := l.GetStatus(xid)
	return !exists || status == TxnStatusInProgress
}

// GetOldestXID 获取最小事务ID
func (l *CommitLog) GetOldestXID() XID {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.oldest
}

// GetEntryCount 获取日志条目数
func (l *CommitLog) GetEntryCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// GC 垃圾回收
func (l *CommitLog) GC(currentXID XID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// 清理旧的事务日志
	// 只保留最近的事务日志
	for xid := range l.entries {
		if xid < currentXID-10000 {
			delete(l.entries, xid)
		}
	}
	
	// 更新oldest
	l.updateOldest()
}

// updateOldest 更新最小XID
func (l *CommitLog) updateOldest() {
	oldest := XIDMax
	for xid := range l.entries {
		if xid < oldest {
			oldest = xid
		}
	}
	
	if len(l.entries) > 0 {
		l.oldest = oldest
	} else {
		l.oldest = XIDBootstrap
	}
}

// Clear 清空日志
func (l *CommitLog) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.entries = make(map[XID]TransactionStatus)
	l.oldest = XIDBootstrap
}

// Size 返回日志大小
func (l *CommitLog) Size() int {
	return l.GetEntryCount()
}

// ==================== SLRU缓存 ====================

// SLRU 简单LRU缓存（用于clog缓存）
type SLRU struct {
	size    int
	entries map[XID]TransactionStatus
	keys    []XID
	mu      sync.RWMutex
}

// NewSLRU 创建SLRU缓存
func NewSLRU(size int) *SLRU {
	return &SLRU{
		size:    size,
		entries: make(map[XID]TransactionStatus),
		keys:    make([]XID, 0, size),
	}
}

// Get 获取事务状态
func (s *SLRU) Get(xid XID) (TransactionStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	status, exists := s.entries[xid]
	return status, exists
}

// Set 设置事务状态
func (s *SLRU) Set(xid XID, status TransactionStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 如果已存在，更新
	if _, exists := s.entries[xid]; exists {
		s.entries[xid] = status
		return
	}
	
	// 如果已满，移除最旧的
	if len(s.keys) >= s.size {
		oldest := s.keys[0]
		delete(s.entries, oldest)
		s.keys = s.keys[1:]
	}
	
	// 添加新条目
	s.entries[xid] = status
	s.keys = append(s.keys, xid)
}

// Clear 清空缓存
func (s *SLRU) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.entries = make(map[XID]TransactionStatus)
	s.keys = make([]XID, 0, s.size)
}

// Len 返回缓存大小
func (s *SLRU) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
