package domain

import "context"

// FilterableDataSource 支持原生过滤的数据源接口
// 实现此接口的数据源可以高效处理过滤和分页操作，
// 避免将所有数据加载到内存后再进行过滤。
//
// 使用场景：
// - 数据库数据源：可以利用 WHERE 子句在数据库层面过滤数据
// - 外部 API 数据源：可以将过滤条件转换为 API 参数传递给后端
// - 索引优化数据源：可以利用索引快速定位符合条件的数据
//
// 设计原理：
// - 统一接口：Filter 方法同时处理过滤和分页，简化调用
// - 嵌套支持：Filter.Value 可以是 []Filter，支持复杂的 AND/OR 逻辑
// - 返回原始数据：直接返回 []Row，避免额外的包装开销
// - 支持取消：通过 context.Context 支持超时和取消操作
//
// 示例：
//
//	type MVCCDataSource struct {
//	    // ... 字段 ...
//	}
//
//	func (m *MVCCDataSource) SupportsFiltering(tableName string) bool {
//	    // 检查表是否存在
//	    m.mu.RLock()
//	    defer m.mu.RUnlock()
//	    _, ok := m.tables[tableName]
//	    return ok
//	}
//
//	func (m *MVCCDataSource) Filter(
//	    ctx context.Context,
//	    tableName string,
//	    filter Filter,
//	    offset, limit int,
//	) ([]Row, int64, error) {
//	    // 1. 获取表数据
//	    // 2. 应用过滤
//	    // 3. 应用分页
//	    // 4. 返回结果
//	}
type FilterableDataSource interface {
	DataSource

	// SupportsFiltering 检查数据源是否支持对指定表进行过滤
	//
	// 参数:
	//   - tableName: 表名
	//
	// 返回:
	//   - true: 支持该表的数据源层面过滤
	//   - false: 不支持该表的数据源层面过滤，需要在内存中过滤
	//
	// 注意:
	//   - 这是一个能力声明方法，不涉及具体的过滤实现
	//   - 实际的过滤逻辑通过 Filter 方法执行
	//   - 数据源可以根据表类型、数据量等因素决定是否支持过滤
	//   - 对于不支持的场景，返回 false，由上层在内存中过滤
	SupportsFiltering(tableName string) bool

	// Filter 执行过滤和分页操作
	//
	// 参数:
	//   - ctx: 上下文（支持取消和超时）
	//   - tableName: 表名
	//   - filter: 过滤条件（支持嵌套）
	//     - 如果 filter.Value 是 []Filter，表示嵌套逻辑（AND/OR）
	//     - filter.Logic: "AND" 表示所有子条件必须满足
	//     - filter.Logic: "OR" 表示任一子条件满足即可
	//   - offset: 偏移量（跳过的行数）
	//   - limit: 限制行数（0 表示不限制）
	//
	// 返回:
	//   - []Row: 符合条件的行数据（受 limit 影响）
	//   - int64: 满足条件的总行数（不受 limit 影响）
	//   - error: 错误信息
	//
	// 注意:
	//   - total 是满足条件的总行数，不受 limit 限制
	//   - rows 的长度应 <= limit（当 limit > 0 时）
	//   - 支持嵌套过滤：递归处理 filter.Value 为 []Filter 的情况
	//   - offset 必须是有效的非负整数
	//   - limit 必须是非负整数（0 表示无限制）
	//   - 应该返回符合条件的行，而不是在数据源中直接分页（由调用方决定是否分页）
	//
	// 示例：
	//
	//	// 简单过滤
	//	rows, total, err := ds.Filter(ctx, "users",
	//	    Filter{Field: "age", Operator: ">", Value: 30},
	//	    0, 10) // 不跳过，返回前10条
	//
	//	// 嵌套过滤 (AND)
	//	rows, total, err := ds.Filter(ctx, "users",
	//	    Filter{
	//	        Logic: "AND",
	//	        Value: []Filter{
	//	            {Field: "age", Operator: ">", Value: 30},
	//	            {Field: "status", Operator: "=", Value: "active"},
	//	        },
	//	    },
	//	    0, 0)
	//
	//	// 分页查询
	//	rows, total, err := ds.Filter(ctx, "users", Filter{}, 20, 10)
	Filter(ctx context.Context, tableName string, filter Filter, offset, limit int) ([]Row, int64, error)
}
