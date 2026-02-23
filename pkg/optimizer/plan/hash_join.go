package plan

import "github.com/kasuganosora/sqlexec/pkg/types"

// HashJoinConfig Hash Join配置
type HashJoinConfig struct {
	JoinType  types.JoinType
	LeftCond  *types.JoinCondition
	RightCond *types.JoinCondition
	// LeftConds/RightConds support multi-column JOIN conditions
	LeftConds  []*types.JoinCondition
	RightConds []*types.JoinCondition
	BuildSide  string
}
