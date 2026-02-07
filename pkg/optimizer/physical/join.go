package physical

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
)

// PhysicalHashJoin 物理哈希连接算子
type PhysicalHashJoin struct {
	JoinType   optimizer.JoinType
	Conditions []*optimizer.JoinCondition
	cost       float64
	children   []PhysicalOperator
}

// NewPhysicalHashJoin 创建物理哈希连接算子
func NewPhysicalHashJoin(joinType optimizer.JoinType, left, right PhysicalOperator, conditions []*optimizer.JoinCondition) *PhysicalHashJoin {
	leftRows := int64(1000) // 假设
	rightRows := int64(1000) // 假设

	// Hash Join 成本 = 构建哈希表 + 探测
	buildCost := float64(leftRows) * 0.1
	probeCost := float64(rightRows) * 0.1
	cost := left.Cost() + right.Cost() + buildCost + probeCost

	return &PhysicalHashJoin{
		JoinType:   joinType,
		Conditions: conditions,
		cost:       cost,
		children:   []PhysicalOperator{left, right},
	}
}

// Children 获取子节点
func (p *PhysicalHashJoin) Children() []PhysicalOperator {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashJoin) SetChildren(children ...PhysicalOperator) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashJoin) Schema() []optimizer.ColumnInfo {
	columns := []optimizer.ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// Cost 返回执行成本
func (p *PhysicalHashJoin) Cost() float64 {
	return p.cost
}

// Explain 返回计划说明
func (p *PhysicalHashJoin) Explain() string {
	return fmt.Sprintf("HashJoin(type=%s, cost=%.2f)", p.JoinType, p.cost)
}

// GetJoinType 获取连接类型（用于测试）
func (p *PhysicalHashJoin) GetJoinType() optimizer.JoinType {
	return p.JoinType
}

// GetConditions 获取连接条件（用于测试）
func (p *PhysicalHashJoin) GetConditions() []*optimizer.JoinCondition {
	return p.Conditions
}
