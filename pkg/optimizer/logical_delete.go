package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalDelete 逻辑删除计划
type LogicalDelete struct {
	TableName string
	Where     *parser.Expression  // WHERE 条件
	OrderBy   []*parser.OrderItem // ORDER BY
	Limit     *int64              // LIMIT
	children  []LogicalPlan       // 子节点（目前无子节点）
}

// NewLogicalDelete 创建逻辑删除计划
func NewLogicalDelete(tableName string) *LogicalDelete {
	return &LogicalDelete{
		TableName: tableName,
		children:  []LogicalPlan{},
	}
}

// Children 获取子节点
func (p *LogicalDelete) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalDelete) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列（DELETE 返回影响的行数）
func (p *LogicalDelete) Schema() []ColumnInfo {
	return []ColumnInfo{
		{
			Name:     "rows_affected",
			Type:     "int",
			Nullable: false,
		},
	}
}

// Explain 返回计划说明
func (p *LogicalDelete) Explain() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Delete(%s", p.TableName))

	// WHERE 条件
	if p.Where != nil {
		sb.WriteString(fmt.Sprintf(" WHERE %v", *p.Where))
	}

	// ORDER BY
	if len(p.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, item := range p.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(item.Expr.Column)
			if item.Direction == "DESC" {
				sb.WriteString(" DESC")
			} else {
				sb.WriteString(" ASC")
			}
		}
	}

	// LIMIT
	if p.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *p.Limit))
	}

	sb.WriteString(")")
	return sb.String()
}

// GetTableName 获取表名
func (p *LogicalDelete) GetTableName() string {
	return p.TableName
}

// GetWhere 获取 WHERE 条件
func (p *LogicalDelete) GetWhere() *parser.Expression {
	return p.Where
}

// SetWhere 设置 WHERE 条件
func (p *LogicalDelete) SetWhere(where *parser.Expression) {
	p.Where = where
}

// GetOrderBy 获取 ORDER BY 列表
func (p *LogicalDelete) GetOrderBy() []*parser.OrderItem {
	return p.OrderBy
}

// SetOrderBy 设置 ORDER BY
func (p *LogicalDelete) SetOrderBy(orderBy []*parser.OrderItem) {
	p.OrderBy = orderBy
}

// GetLimit 获取 LIMIT
func (p *LogicalDelete) GetLimit() *int64 {
	return p.Limit
}

// SetLimit 设置 LIMIT
func (p *LogicalDelete) SetLimit(limit *int64) {
	p.Limit = limit
}
