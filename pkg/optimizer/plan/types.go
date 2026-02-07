package plan

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/types"
)

// PlanType 算子类型
type PlanType string

const (
	TypeTableScan  PlanType = "TableScan"
	TypeHashJoin   PlanType = "HashJoin"
	TypeSort       PlanType = "Sort"
	TypeAggregate  PlanType = "Aggregate"
	TypeProjection PlanType = "Projection"
	TypeSelection  PlanType = "Selection"
	TypeLimit      PlanType = "Limit"
	TypeInsert     PlanType = "Insert"
	TypeUpdate     PlanType = "Update"
	TypeDelete     PlanType = "Delete"
	TypeUnion      PlanType = "Union"
)

// Plan 可序列化的执行计划（不含数据源引用）
type Plan struct {
	ID           string
	Type         PlanType
	OutputSchema []types.ColumnInfo
	Children     []*Plan
	Config       interface{}
	EstimatedCost float64
}

// Explain 返回计划的说明
func (p *Plan) Explain() string {
	return fmt.Sprintf("%s[%s]", p.Type, p.ID)
}

// Cost 返回计划的成本
func (p *Plan) Cost() float64 {
	return p.EstimatedCost
}


