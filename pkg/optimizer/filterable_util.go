package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IsFilterable 检查数据源是否支持过滤能力
//
// 该函数通过类型断言检测数据源是否实现了 FilterableDataSource 接口。
// 如果数据源支持过滤，优化器可以将过滤条件下推到数据源层面，
// 避免将所有数据加载到内存后再过滤。
//
// 参数:
//   - ds: 数据源实例
//
// 返回:
//   - true: 数据源支持过滤能力
//   - false: 数据源不支持过滤能力，需要在内存中过滤
//
// 使用示例:
//
//	if optimizer.IsFilterable(dataSource) {
//	    // 将过滤器下推到数据源
//	    rows, total, err := filterableDS.Filter(ctx, tableName, filter, offset, limit)
//	} else {
//	    // 在内存中过滤
//	    result, err := dataSource.Query(ctx, tableName, &domain.QueryOptions{})
//	    rows = applyFilters(result.Rows, filter)
//	}
func IsFilterable(ds domain.DataSource) bool {
	_, ok := ds.(domain.FilterableDataSource)
	return ok
}
