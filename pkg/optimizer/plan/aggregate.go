package plan

import "github.com/kasuganosora/sqlexec/pkg/types"

// AggregateConfig 聚合配置
type AggregateConfig struct {
	AggFuncs    []*types.AggregationItem
	GroupByCols []string
}
