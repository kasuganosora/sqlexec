package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalUpdate 逻辑更新计划
type LogicalUpdate struct {
	TableName string
	Set       map[string]parser.Expression // 列名 -> 表达式
	Where     *parser.Expression           // WHERE 条件
	OrderBy   []*parser.OrderItem          // ORDER BY
	Limit     *int64                       // LIMIT
	children  []LogicalPlan                // 子节点（目前无子节点）
}

// NewLogicalUpdate 创建逻辑更新计划
func NewLogicalUpdate(tableName string, set map[string]parser.Expression) *LogicalUpdate {
	return &LogicalUpdate{
		TableName: tableName,
		Set:       set,
		children:  []LogicalPlan{},
	}
}

// Children 获取子节点
func (p *LogicalUpdate) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalUpdate) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列（UPDATE 返回影响的行数）
func (p *LogicalUpdate) Schema() []ColumnInfo {
	return []ColumnInfo{
		{
			Name:     "rows_affected",
			Type:     "int",
			Nullable: false,
		},
	}
}

// Explain 返回计划说明
func (p *LogicalUpdate) Explain() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Update(%s SET", p.TableName))

	// SET 子句
	first := true
	for col := range p.Set {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(col)
		sb.WriteString(" = ?")
		first = false
	}

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
func (p *LogicalUpdate) GetTableName() string {
	return p.TableName
}

// GetSet 获取 SET 映射
func (p *LogicalUpdate) GetSet() map[string]parser.Expression {
	return p.Set
}

// GetWhere 获取 WHERE 条件
func (p *LogicalUpdate) GetWhere() *parser.Expression {
	return p.Where
}

// SetWhere 设置 WHERE 条件
func (p *LogicalUpdate) SetWhere(where *parser.Expression) {
	p.Where = where
}

// GetOrderBy 获取 ORDER BY 列表
func (p *LogicalUpdate) GetOrderBy() []*parser.OrderItem {
	return p.OrderBy
}

// SetOrderBy 设置 ORDER BY
func (p *LogicalUpdate) SetOrderBy(orderBy []*parser.OrderItem) {
	p.OrderBy = orderBy
}

// GetLimit 获取 LIMIT
func (p *LogicalUpdate) GetLimit() *int64 {
	return p.Limit
}

// SetLimit 设置 LIMIT
func (p *LogicalUpdate) SetLimit(limit *int64) {
	p.Limit = limit
}
