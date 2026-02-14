package mvcc

import (
	"fmt"
	"sync"
	"time"
)

// ==================== 常量定义 ====================

const (
	// XIDMax 最大事务ID（32位）
	XIDMax = XID(4294967295)
)

// ==================== 数据源能力 ====================

// DataSourceCapability 数据源能力等级
type DataSourceCapability int

const (
	CapabilityNone          DataSourceCapability = 0 // 不支持MVCC
	CapabilityReadSnapshot  DataSourceCapability = 1 // 支持读快照
	CapabilityWriteVersion  DataSourceCapability = 2 // 支持写多版本
	CapabilityFull          DataSourceCapability = 3 // 完全支持MVCC
)

// String 返回能力等级的字符串表示
func (c DataSourceCapability) String() string {
	switch c {
	case CapabilityNone:
		return "None"
	case CapabilityReadSnapshot:
		return "ReadSnapshot"
	case CapabilityWriteVersion:
		return "WriteVersion"
	case CapabilityFull:
		return "Full"
	default:
		return "Unknown"
	}
}

// DataSourceFeatures 数据源特性
type DataSourceFeatures struct {
	Name        string                // 数据源名称
	Capability  DataSourceCapability  // MVCC能力
	Supports    []string              // 支持的特性列表
	Config      map[string]interface{} // 配置项
	ReadOnly    bool                  // 是否只读
	Version     string                // 版本号
	CreatedAt   time.Time             // 创建时间
	mu          sync.RWMutex          // 互斥锁
}

// NewDataSourceFeatures 创建数据源特性
func NewDataSourceFeatures(name string, capability DataSourceCapability) *DataSourceFeatures {
	return &DataSourceFeatures{
		Name:       name,
		Capability: capability,
		Supports:   make([]string, 0),
		Config:     make(map[string]interface{}),
		ReadOnly:   false,
		Version:    "1.0.0",
		CreatedAt:  time.Now(),
	}
}

// Support 是否支持指定特性
func (f *DataSourceFeatures) Support(feature string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, s := range f.Supports {
		if s == feature {
			return true
		}
	}
	return false
}

// AddSupport 添加支持特性
func (f *DataSourceFeatures) AddSupport(feature string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Supports = append(f.Supports, feature)
}

// HasMVCC 是否支持MVCC
func (f *DataSourceFeatures) HasMVCC() bool {
	return f.Capability >= CapabilityReadSnapshot
}

// IsReadOnly 是否只读
func (f *DataSourceFeatures) IsReadOnly() bool {
	return f.ReadOnly
}

// ==================== 事务ID (XID) ====================

// XID 事务ID (PostgreSQL风格：32位)
type XID uint32

// XIDNone 空事务ID
const XIDNone XID = 0

// XIDBootstrap 引导事务ID
const XIDBootstrap XID = 1

// IsBefore 是否在另一个XID之前
func (x XID) IsBefore(other XID) bool {
	// 处理环绕情况
	if x < XIDBootstrap || other < XIDBootstrap {
		return x < other
	}
	
	// 正常比较
	if x > other {
		// 可能是环绕情况：x是新的，other是旧的
		return (XIDMax - x) < (other - XIDBootstrap)
	}
	return x < other
}

// IsAfter 是否在另一个XID之后
func (x XID) IsAfter(other XID) bool {
	return other.IsBefore(x)
}

// String 返回XID的字符串表示
func (x XID) String() string {
	return fmt.Sprintf("%d", x)
}

// NextXID 生成下一个事务ID
func NextXID(current XID) XID {
	if current == XID(XIDMax) {
		return XIDBootstrap // 环绕
	}
	return current + 1
}

// ==================== 事务状态 ====================

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TxnStatusInProgress TransactionStatus = 0 // 进行中
	TxnStatusCommitted TransactionStatus = 1  // 已提交
	TxnStatusAborted   TransactionStatus = 2  // 已回滚
)

// String 返回事务状态的字符串表示
func (s TransactionStatus) String() string {
	switch s {
	case TxnStatusInProgress:
		return "InProgress"
	case TxnStatusCommitted:
		return "Committed"
	case TxnStatusAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// ==================== 事务快照 ====================

// Snapshot 事务快照 (PostgreSQL风格)
type Snapshot struct {
	xmin     XID              // 最小的活跃事务ID
	xmax     XID              // 最大的已分配事务ID
	xip      []XID            // 活跃事务列表
	level    IsolationLevel   // 隔离级别
	created  time.Time        // 创建时间
	mu       sync.RWMutex     // 互斥锁
}

// NewSnapshot 创建新快照
func NewSnapshot(xmin, xmax XID, xip []XID, level IsolationLevel) *Snapshot {
	// Defensive copy of xip to prevent external mutation
	xipCopy := make([]XID, len(xip))
	copy(xipCopy, xip)
	return &Snapshot{
		xmin:    xmin,
		xmax:    xmax,
		xip:     xipCopy,
		level:   level,
		created: time.Now(),
	}
}

// Xmin 返回xmin
func (s *Snapshot) Xmin() XID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.xmin
}

// Xmax 返回xmax
func (s *Snapshot) Xmax() XID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.xmax
}

// Xip 返回活跃事务列表
func (s *Snapshot) Xip() []XID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.xip
}

// Level 返回隔离级别
func (s *Snapshot) Level() IsolationLevel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.level
}

// IsActive 检查事务是否在快照中是活跃的
func (s *Snapshot) IsActive(xid XID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, activeXID := range s.xip {
		if activeXID == xid {
			return true
		}
	}
	return false
}

// Age 返回快照的年龄
func (s *Snapshot) Age() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.created)
}

// String 返回快照的字符串表示
func (s *Snapshot) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Snapshot{xmin=%d, xmax=%d, xip=%v, level=%s}",
		s.xmin, s.xmax, s.xip, s.level)
}

// ==================== 行版本 ====================

// TupleVersion 行版本 (PostgreSQL风格)
type TupleVersion struct {
	Data      interface{} // 数据
	Xmin      XID         // 创建事务ID
	Xmax      XID         // 删除事务ID（0表示未删除）
	Cmin      uint32      // 命令序号（创建）
	Cmax      uint32      // 命令序号（删除）
	CTID      string      // 行标识
	Expired   bool        // 是否已过期
	CreatedAt time.Time   // 创建时间
	mu        sync.RWMutex
}

// NewTupleVersion 创建新版本
func NewTupleVersion(data interface{}, xmin XID) *TupleVersion {
	return &TupleVersion{
		Data:      data,
		Xmin:      xmin,
		Xmax:      0,
		Cmin:      0,
		Cmax:      0,
		CTID:      fmt.Sprintf("ctid:%d", time.Now().UnixNano()),
		Expired:   false,
		CreatedAt: time.Now(),
	}
}

// IsVisibleTo 检查版本对快照是否可见 (PostgreSQL风格)
func (v *TupleVersion) IsVisibleTo(snapshot *Snapshot) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// 如果已过期，不可见
	if v.Expired {
		return false
	}

	// 规则1: xmin必须在快照可见范围内
	// xmin <= snapshot.xmin 或者 xmin不在活跃事务列表中
	if v.Xmin > snapshot.Xmin() {
		// xmin > snapshot.xmin，检查是否在活跃列表中
		if snapshot.IsActive(v.Xmin) {
			return false // xmin仍然是活跃的，不可见
		}
	}

	// 规则2: xmax必须为0（未删除）或者xmax > snapshot.xmin
	if v.Xmax != 0 {
		// 行已被删除
		if v.Xmax <= snapshot.Xmin() {
			return false // 删除事务已提交，不可见
		}
		if snapshot.IsActive(v.Xmax) {
			return false // 删除事务仍然活跃，不可见
		}
	}

	return true
}

// MarkDeleted 标记为已删除
func (v *TupleVersion) MarkDeleted(xmax XID, cmax uint32) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.Xmax = xmax
	v.Cmax = cmax
	v.Expired = true
}

// MarkExpired 标记为过期
func (v *TupleVersion) MarkExpired() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.Expired = true
}

// IsDeleted 检查是否已删除
func (v *TupleVersion) IsDeleted() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.Xmax != 0
}

// XminValue 返回xmin
func (v *TupleVersion) XminValue() XID {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.Xmin
}

// XmaxValue 返回xmax
func (v *TupleVersion) XmaxValue() XID {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.Xmax
}

// GetValue 返回数据
func (v *TupleVersion) GetValue() interface{} {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.Data
}

// String 返回版本的字符串表示
func (v *TupleVersion) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return fmt.Sprintf("TupleVersion{data=%v, xmin=%d, xmax=%d, ctid=%s}",
		v.Data, v.Xmin, v.Xmax, v.CTID)
}

// ==================== 隔离级别 ====================

// IsolationLevel 隔离级别
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = 0
	ReadCommitted   IsolationLevel = 1
	RepeatableRead  IsolationLevel = 2
	Serializable    IsolationLevel = 3
)

// String 返回隔离级别的字符串表示
func (l IsolationLevel) String() string {
	switch l {
	case ReadUncommitted:
		return "ReadUncommitted"
	case ReadCommitted:
		return "ReadCommitted"
	case RepeatableRead:
		return "RepeatableRead"
	case Serializable:
		return "Serializable"
	default:
		return "Unknown"
	}
}

// IsolationLevelFromString 从字符串解析隔离级别
func IsolationLevelFromString(s string) IsolationLevel {
	switch s {
	case "READ UNCOMMITTED", "ReadUncommitted":
		return ReadUncommitted
	case "READ COMMITTED", "ReadCommitted":
		return ReadCommitted
	case "REPEATABLE READ", "RepeatableRead":
		return RepeatableRead
	case "SERIALIZABLE", "Serializable":
		return Serializable
	default:
		return RepeatableRead // 默认
	}
}

// ==================== 可见性检查器 ====================

// VisibilityChecker 可见性检查器
type VisibilityChecker struct {
	mu sync.RWMutex
}

// NewVisibilityChecker 创建可见性检查器
func NewVisibilityChecker() *VisibilityChecker {
	return &VisibilityChecker{}
}

// Check 检查版本对快照是否可见
func (vc *VisibilityChecker) Check(version *TupleVersion, snapshot *Snapshot) bool {
	return version.IsVisibleTo(snapshot)
}

// CheckBatch 批量检查多个版本
func (vc *VisibilityChecker) CheckBatch(versions []*TupleVersion, snapshot *Snapshot) []bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	result := make([]bool, len(versions))
	for i, version := range versions {
		result[i] = version.IsVisibleTo(snapshot)
	}
	return result
}

// FilterVisible 过滤可见版本
func (vc *VisibilityChecker) FilterVisible(versions []*TupleVersion, snapshot *Snapshot) []*TupleVersion {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	
	visible := make([]*TupleVersion, 0)
	for _, version := range versions {
		if version.IsVisibleTo(snapshot) {
			visible = append(visible, version)
		}
	}
	return visible
}
