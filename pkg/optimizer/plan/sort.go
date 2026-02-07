package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// SortConfig 排序配置
type SortConfig struct {
	OrderByItems []*parser.OrderItem
}
