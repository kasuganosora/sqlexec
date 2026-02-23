package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalApply represents a correlated subquery execution
// It executes the inner query for each row from the outer query
type LogicalApply struct {
	joinType   JoinType
	conditions []*parser.Expression
	correlated []CorrelatedColumn
	children   []LogicalPlan
}

// CorrelatedColumn represents a column that references outer query
type CorrelatedColumn struct {
	Table      string
	Column     string
	OuterLevel int
}

// NewLogicalApply creates a new LogicalApply node
func NewLogicalApply(joinType JoinType, outerPlan, innerPlan LogicalPlan, conditions []*parser.Expression) *LogicalApply {
	return &LogicalApply{
		joinType:   joinType,
		conditions: conditions,
		correlated: []CorrelatedColumn{},
		children:   []LogicalPlan{outerPlan, innerPlan},
	}
}

// Children returns the child plans
func (p *LogicalApply) Children() []LogicalPlan {
	return p.children
}

// SetChildren sets the child plans
func (p *LogicalApply) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema returns the output columns
// For Apply, schema is the combination of outer and inner schemas
func (p *LogicalApply) Schema() []ColumnInfo {
	cols := []ColumnInfo{}
	if len(p.children) > 0 {
		cols = append(cols, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		cols = append(cols, p.children[1].Schema()...)
	}
	return cols
}

// GetJoinType returns the join type
func (p *LogicalApply) GetJoinType() JoinType {
	return p.joinType
}

// GetConditions returns the join conditions
func (p *LogicalApply) GetConditions() []*parser.Expression {
	return p.conditions
}

// CorrelatedColumns returns the correlated columns
func (p *LogicalApply) CorrelatedColumns() []CorrelatedColumn {
	return p.correlated
}

// SetCorrelatedColumns sets the correlated columns
func (p *LogicalApply) SetCorrelatedColumns(cols []CorrelatedColumn) {
	p.correlated = cols
}

// Explain returns the plan description
func (p *LogicalApply) Explain() string {
	return "Apply(" + p.GetJoinType().String() + ")"
}
