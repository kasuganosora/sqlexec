package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// UpdateConfig UPDATE配置
type UpdateConfig struct {
	TableName string
	Set       map[string]parser.Expression
	Where     *parser.Expression
	OrderBy   []*parser.OrderItem
	Limit     *int64
}
