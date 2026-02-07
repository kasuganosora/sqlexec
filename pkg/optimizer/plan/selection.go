package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// SelectionConfig 选择配置
type SelectionConfig struct {
	Condition *parser.Expression
}
