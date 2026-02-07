package plan

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"
import "github.com/kasuganosora/sqlexec/pkg/types"

// TableScanConfig 表扫描配置
type TableScanConfig struct {
	TableName       string
	Columns         []types.ColumnInfo
	Filters         []domain.Filter
	LimitInfo       *types.LimitInfo
	EnableParallel  bool
	MinParallelRows int64
}
