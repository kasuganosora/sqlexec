package plan

import "github.com/kasuganosora/sqlexec/pkg/parser"

// InsertConfig INSERT配置
type InsertConfig struct {
	TableName    string
	Columns      []string
	Values       [][]parser.Expression
	OnDuplicate  *map[string]parser.Expression // ON DUPLICATE KEY UPDATE
}
