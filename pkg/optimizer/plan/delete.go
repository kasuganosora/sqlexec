package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// DeleteConfig DELETE配置
type DeleteConfig struct {
	TableName string
	Where     *parser.Expression
	OrderBy   []*parser.OrderItem
	Limit     *int64
}
