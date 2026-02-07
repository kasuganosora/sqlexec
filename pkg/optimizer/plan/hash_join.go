package plan

import "github.com/kasuganosora/sqlexec/pkg/types"

// HashJoinConfig Hash Join配置
type HashJoinConfig struct {
	JoinType  types.JoinType
	LeftCond  *types.JoinCondition
	RightCond *types.JoinCondition
	BuildSide string
}
