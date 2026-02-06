package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalJoin 逻辑连接
type LogicalJoin struct {
	joinType       JoinType
	LeftTable      string
	RightTable     string
	joinConditions []*JoinCondition
	exprConditions []*parser.Expression // Alternative representation using expressions
	children       []LogicalPlan
}

// NewLogicalJoin 创建逻辑连接
func NewLogicalJoin(joinType JoinType, left, right LogicalPlan, conditions []*JoinCondition) *LogicalJoin {
	leftTable := ""
	if left != nil {
		if ds, ok := left.(*LogicalDataSource); ok {
			leftTable = ds.TableName
		}
	}

	rightTable := ""
	if right != nil {
		if ds, ok := right.(*LogicalDataSource); ok {
			rightTable = ds.TableName
		}
	}

	return &LogicalJoin{
		joinType:       joinType,
		LeftTable:      leftTable,
		RightTable:     rightTable,
		joinConditions: conditions,
		children:       []LogicalPlan{left, right},
	}
}

// NewLogicalJoinWithExprs creates a join with expression conditions
func NewLogicalJoinWithExprs(joinType JoinType, left, right LogicalPlan, conditions []*parser.Expression) *LogicalJoin {
	leftTable := ""
	if left != nil {
		if ds, ok := left.(*LogicalDataSource); ok {
			leftTable = ds.TableName
		}
	}

	rightTable := ""
	if right != nil {
		if ds, ok := right.(*LogicalDataSource); ok {
			rightTable = ds.TableName
		}
	}

	// Convert expressions to JoinConditions (simplified)
	joinConditions := make([]*JoinCondition, len(conditions))
	for i, cond := range conditions {
		joinConditions[i] = &JoinCondition{
			Left:     cond.Left,
			Right:    cond.Right,
			Operator: cond.Operator,
		}
	}

	return &LogicalJoin{
		joinType:       joinType,
		LeftTable:      leftTable,
		RightTable:     rightTable,
		joinConditions: joinConditions,
		exprConditions: conditions,
		children:       []LogicalPlan{left, right},
	}
}

// Children 获取子节点
func (p *LogicalJoin) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalJoin) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalJoin) Schema() []ColumnInfo {
	columns := []ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// GetJoinType 返回连接类型
func (p *LogicalJoin) GetJoinType() JoinType {
	return p.joinType
}

// GetJoinConditions 返回连接条件
func (p *LogicalJoin) GetJoinConditions() []*JoinCondition {
	return p.joinConditions
}

// GetConditions returns conditions as expressions
func (p *LogicalJoin) GetConditions() []*parser.Expression {
	if len(p.exprConditions) > 0 {
		return p.exprConditions
	}

	// Convert join conditions to expressions (simplified)
	exprs := make([]*parser.Expression, len(p.joinConditions))
	for i, jc := range p.joinConditions {
		exprs[i] = &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: jc.Operator,
			Left:     jc.Left,
			Right:    jc.Right,
		}
	}
	return exprs
}

// Explain 返回计划说明
func (p *LogicalJoin) Explain() string {
	return "Join(" + p.LeftTable + ", " + p.RightTable + ", type=" + p.GetJoinType().String() + ")"
}

// hintApplied 标记已应用的 hint
type hintApplied struct {
	hintType string
	applied  bool
}

// SetHintApplied 标记已应用的 hint
func (p *LogicalJoin) SetHintApplied(hintType string) {
	// Placeholder for hint tracking
	// In full implementation, would track which hints were applied
}
