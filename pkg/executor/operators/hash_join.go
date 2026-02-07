package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// HashJoinOperator Hash Join算子
type HashJoinOperator struct {
	*BaseOperator
	config *plan.HashJoinConfig
}

// NewHashJoinOperator 创建Hash Join算子
func NewHashJoinOperator(p *plan.Plan, das dataaccess.Service) (*HashJoinOperator, error) {
	config, ok := p.Config.(*plan.HashJoinConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for HashJoin: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	
	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &HashJoinOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行Hash Join
func (op *HashJoinOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	joinTypeStr := ""
	switch op.config.JoinType {
	case 0:
		joinTypeStr = "INNER"
	case 1:
		joinTypeStr = "LEFT OUTER"
	case 2:
		joinTypeStr = "RIGHT OUTER"
	case 3:
		joinTypeStr = "FULL OUTER"
	case 4:
		joinTypeStr = "CROSS"
	default:
		joinTypeStr = "UNKNOWN"
	}
	fmt.Printf("  [EXECUTOR] HashJoin: 执行JOIN, 类型: %s\n", joinTypeStr)

	// 获取左右子算子
	if len(op.children) < 2 {
		return nil, fmt.Errorf("HashJoin requires 2 children, got %d", len(op.children))
	}

	leftChild := op.children[0]
	rightChild := op.children[1]

	// 执行子算子
	leftResult, err := leftChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute left child failed: %w", err)
	}

	rightResult, err := rightChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute right child failed: %w", err)
	}

	// 简化版本：Cross Join所有行
	// 实际实现应该根据条件进行hash join
	joinedRows := make([]domain.Row, 0)
	for _, leftRow := range leftResult.Rows {
		for _, rightRow := range rightResult.Rows {
			merged := make(domain.Row)
			for k, v := range leftRow {
				merged[k] = v
			}
			for k, v := range rightRow {
				merged[k] = v
			}
			joinedRows = append(joinedRows, merged)
		}
	}

	return &domain.QueryResult{
		Columns: leftResult.Columns,
		Rows:    joinedRows,
	}, nil
}

// buildOperator 构建算子的辅助函数
func buildOperator(p *plan.Plan, das dataaccess.Service) (Operator, error) {
	switch p.Type {
	case plan.TypeTableScan:
		return NewTableScanOperator(p, das)
	case plan.TypeSelection:
		return NewSelectionOperator(p, das)
	case plan.TypeProjection:
		return NewProjectionOperator(p, das)
	case plan.TypeLimit:
		return NewLimitOperator(p, das)
	case plan.TypeAggregate:
		return NewAggregateOperator(p, das)
	case plan.TypeHashJoin:
		return NewHashJoinOperator(p, das)
	default:
		return nil, fmt.Errorf("unsupported plan type: %s", p.Type)
	}
}
