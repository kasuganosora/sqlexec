package optimizer

import (
	"strconv"
)

// LogicalLimit 逻辑限制
type LogicalLimit struct {
	limitVal  int64
	offsetVal int64
	children  []LogicalPlan
}

// NewLogicalLimit 创建逻辑限制
func NewLogicalLimit(limit, offset int64, child LogicalPlan) *LogicalLimit {
	return &LogicalLimit{
		limitVal:  limit,
		offsetVal: offset,
		children:  []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalLimit) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalLimit) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalLimit) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// GetLimit 返回LIMIT值
func (p *LogicalLimit) GetLimit() int64 {
	return p.limitVal
}

// GetOffset 返回OFFSET值
func (p *LogicalLimit) GetOffset() int64 {
	return p.offsetVal
}

// Explain 返回计划说明
func (p *LogicalLimit) Explain() string {
	return "Limit(offset=" + strconv.FormatInt(p.GetOffset(), 10) + ", limit=" + strconv.FormatInt(p.GetLimit(), 10) + ")"
}
