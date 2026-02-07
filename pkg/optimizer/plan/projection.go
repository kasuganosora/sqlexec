package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// ProjectionConfig 投影配置
type ProjectionConfig struct {
	Expressions []*parser.Expression
	Aliases     []string
}
