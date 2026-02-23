package optimizer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalProjection 逻辑投影
type LogicalProjection struct {
	Exprs         []*parser.Expression
	columnAliases []string
	Columns       []ColumnInfo
	children      []LogicalPlan
}

// NewLogicalProjection 创建逻辑投影
func NewLogicalProjection(exprs []*parser.Expression, aliases []string, child LogicalPlan) *LogicalProjection {
	columns := make([]ColumnInfo, len(exprs))
	for i, expr := range exprs {
		var name string
		if i < len(aliases) {
			name = aliases[i]
		}
		if name == "" {
			if expr.Type == parser.ExprTypeColumn {
				name = expr.Column
			} else {
				name = "expr_" + strconv.Itoa(i)
			}
		}
		columns[i] = ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		}
	}

	return &LogicalProjection{
		Exprs:         exprs,
		columnAliases: aliases,
		Columns:       columns,
		children:      []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalProjection) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalProjection) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalProjection) Schema() []ColumnInfo {
	return p.Columns
}

// GetExprs 返回投影表达式
func (p *LogicalProjection) GetExprs() []*parser.Expression {
	return p.Exprs
}

// GetAliases 返回别名列表
func (p *LogicalProjection) GetAliases() []string {
	return p.columnAliases
}

// Explain 返回计划说明
func (p *LogicalProjection) Explain() string {
	var exprs strings.Builder
	for i, expr := range p.Exprs {
		if i > 0 {
			exprs.WriteString(", ")
		}
		if expr.Type == parser.ExprTypeColumn {
			exprs.WriteString(expr.Column)
		} else {
			exprs.WriteString(fmt.Sprintf("%v", expr))
		}
	}
	return "Projection(" + exprs.String() + ")"
}
