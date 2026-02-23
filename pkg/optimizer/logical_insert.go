package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalInsert 逻辑插入计划
type LogicalInsert struct {
	TableName   string
	Columns     []string              // 指定列（为空时使用表的所有列）
	Values      [][]parser.Expression // 要插入的值（表达式）
	OnDuplicate *LogicalUpdate        // ON DUPLICATE KEY UPDATE
	children    []LogicalPlan         // 子节点（INSERT ... SELECT）
}

// NewLogicalInsert 创建逻辑插入计划
func NewLogicalInsert(tableName string, columns []string, values [][]parser.Expression) *LogicalInsert {
	return &LogicalInsert{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
		children:  []LogicalPlan{},
	}
}

// NewLogicalInsertWithSelect 创建带有 SELECT 的插入计划
func NewLogicalInsertWithSelect(tableName string, columns []string, selectPlan LogicalPlan) *LogicalInsert {
	return &LogicalInsert{
		TableName: tableName,
		Columns:   columns,
		Values:    [][]parser.Expression{},
		children:  []LogicalPlan{selectPlan},
	}
}

// Children 获取子节点
func (p *LogicalInsert) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalInsert) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列（INSERT 返回影响的行数等元信息）
func (p *LogicalInsert) Schema() []ColumnInfo {
	return []ColumnInfo{
		{
			Name:     "rows_affected",
			Type:     "int",
			Nullable: false,
		},
		{
			Name:     "last_insert_id",
			Type:     "int",
			Nullable: true,
		},
	}
}

// Explain 返回计划说明
func (p *LogicalInsert) Explain() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Insert(%s", p.TableName))

	if len(p.Columns) > 0 {
		sb.WriteString("(")
		for i, col := range p.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}

	if len(p.Values) > 0 {
		sb.WriteString(fmt.Sprintf(" - %d rows", len(p.Values)))
	}

	if len(p.children) > 0 {
		sb.WriteString(" - SELECT")
	}

	if p.OnDuplicate != nil {
		sb.WriteString(" ON DUPLICATE KEY UPDATE")
	}

	sb.WriteString(")")
	return sb.String()
}

// GetTableName 获取表名
func (p *LogicalInsert) GetTableName() string {
	return p.TableName
}

// GetColumns 获取列列表
func (p *LogicalInsert) GetColumns() []string {
	return p.Columns
}

// GetValues 获取值列表
func (p *LogicalInsert) GetValues() [][]parser.Expression {
	return p.Values
}

// HasSelect 是否包含 SELECT 子查询
func (p *LogicalInsert) HasSelect() bool {
	return len(p.children) > 0
}

// GetSelectPlan 获取 SELECT 子查询计划
func (p *LogicalInsert) GetSelectPlan() LogicalPlan {
	if len(p.children) > 0 {
		return p.children[0]
	}
	return nil
}

// SetOnDuplicate 设置 ON DUPLICATE KEY UPDATE
func (p *LogicalInsert) SetOnDuplicate(update *LogicalUpdate) {
	p.OnDuplicate = update
}
