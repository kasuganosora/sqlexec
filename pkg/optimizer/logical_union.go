package optimizer

import (
	"fmt"
	"strings"
)

// LogicalUnion 逻辑UNION操作符
// 用于合并多个子查询的结果集
type LogicalUnion struct {
	children []LogicalPlan
	isAll    bool // 是否为UNION ALL（不去重）
}

// NewLogicalUnion 创建逻辑UNION
func NewLogicalUnion(children []LogicalPlan) *LogicalUnion {
	return &LogicalUnion{
		children: children,
		isAll:    true, // 默认使用UNION ALL以提升性能
	}
}

// Children 获取子节点
func (p *LogicalUnion) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalUnion) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalUnion) Schema() []ColumnInfo {
	if len(p.children) == 0 {
		return []ColumnInfo{}
	}
	// 返回第一个子节点的schema（假设所有子节点schema一致）
	return p.children[0].Schema()
}

// IsAll 返回是否为UNION ALL
func (p *LogicalUnion) IsAll() bool {
	return p.isAll
}

// SetAll 设置是否为UNION ALL
func (p *LogicalUnion) SetAll(isAll bool) {
	p.isAll = isAll
}

// Explain 返回计划说明
func (p *LogicalUnion) Explain() string {
	unionType := "UNION"
	if p.isAll {
		unionType = "UNION ALL"
	}

	childSchemas := make([]string, 0, len(p.children))
	for _, child := range p.children {
		childSchemas = append(childSchemas, child.Explain())
	}

	return fmt.Sprintf("%s(%s)", unionType, strings.Join(childSchemas, ", "))
}
