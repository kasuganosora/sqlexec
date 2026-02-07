package physical

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
)

// PhysicalLimit 物理限制算子
type PhysicalLimit struct {
	Limit    int64
	Offset   int64
	cost     float64
	children []PhysicalOperator
}

// NewPhysicalLimit 创建物理限制算子
func NewPhysicalLimit(limit, offset int64, child PhysicalOperator) *PhysicalLimit {
	inputCost := child.Cost()
	cost := inputCost + float64(limit)*0.01 // 限制操作成本很低

	return &PhysicalLimit{
		Limit:    limit,
		Offset:   offset,
		cost:     cost,
		children: []PhysicalOperator{child},
	}
}

// Children 获取子节点
func (p *PhysicalLimit) Children() []PhysicalOperator {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalLimit) SetChildren(children ...PhysicalOperator) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalLimit) Schema() []optimizer.ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []optimizer.ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalLimit) Cost() float64 {
	return p.cost
}

// Explain 返回计划说明
func (p *PhysicalLimit) Explain() string {
	return fmt.Sprintf("Limit(offset=%d, limit=%d, cost=%.2f)", p.Offset, p.Limit, p.cost)
}

// GetLimit 获取限制数（用于测试）
func (p *PhysicalLimit) GetLimit() int64 {
	return p.Limit
}

// GetOffset 获取偏移量（用于测试）
func (p *PhysicalLimit) GetOffset() int64 {
	return p.Offset
}

// LimitInfo 限制信息
type LimitInfo struct {
	Limit  int64
	Offset int64
}

// NewLimitInfo 创建限制信息
func NewLimitInfo(limit, offset int64) *LimitInfo {
	return &LimitInfo{
		Limit:  limit,
		Offset: offset,
	}
}
