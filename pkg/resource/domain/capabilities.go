package domain

import "context"

// ==================== 数据源能力接口 ====================

// IsMVCCable 判断数据源是否支持MVCC的接口
// 实现此接口的数据源支持多版本并发控制
type IsMVCCable interface {
	// SupportsMVCC 是否支持MVCC
	SupportsMVCC() bool

	// BeginTx 开始事务
	BeginTx(ctx context.Context, readOnly bool) (int64, error)

	// CommitTx 提交事务
	CommitTx(ctx context.Context, txnID int64) error

	// RollbackTx 回滚事务
	RollbackTx(ctx context.Context, txnID int64) error
}

// IsWritableSource 判断数据源是否可写的接口
// 注意：DataSource接口已经包含了IsWritable()方法
// 这个接口用于标记数据源明确支持写操作
type IsWritableSource interface {
	DataSource

	// SupportsWrite 明确声明支持写操作
	SupportsWrite() bool
}

// ==================== 辅助函数 ====================

// HasMVCCSupport 检查数据源是否支持MVCC
// 如果实现了IsMVCCable接口，使用其返回值
// 否则返回false（默认不支持）
func HasMVCCSupport(ds DataSource) bool {
	if mvccable, ok := ds.(IsMVCCable); ok {
		return mvccable.SupportsMVCC()
	}
	return false
}

// HasWriteSupport 检查数据源是否可写
// 如果实现了IsWritableSource接口，使用其返回值
// 否则使用DataSource的IsWritable()方法
func HasWriteSupport(ds DataSource) bool {
	if writable, ok := ds.(IsWritableSource); ok {
		return writable.SupportsWrite()
	}
	// 默认使用DataSource接口的IsWritable()方法
	return ds.IsWritable()
}

// GetMVCCDataSource 获取支持MVCC的数据源
// 返回IsMVCCable接口和是否支持
func GetMVCCDataSource(ds DataSource) (IsMVCCable, bool) {
	mvccable, ok := ds.(IsMVCCable)
	return mvccable, ok
}
