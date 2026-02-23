package types

// ColumnInfo 列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// JoinType 连接类型
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
	CrossJoin
	SemiJoin
	AntiSemiJoin
	HashJoin
)

// AggregationType 聚合函数类型
type AggregationType int

const (
	Count AggregationType = iota
	Sum
	Avg
	Max
	Min
)

// JoinCondition 连接条件
type JoinCondition struct {
	Left     *Expression
	Right    *Expression
	Operator string
}

// LimitInfo Limit信息
type LimitInfo struct {
	Limit  int64
	Offset int64
}

// AggregationItem 聚合项
type AggregationItem struct {
	Type     AggregationType
	Expr     *Expression
	Alias    string
	Distinct bool
}

// Expression 表达式
type Expression struct {
	Type     string
	Column   string
	Value    interface{}
	Operator string
	Left     *Expression
	Right    *Expression
}
