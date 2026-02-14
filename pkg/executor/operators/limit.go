package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// LimitOperator Limit算子
type LimitOperator struct {
	*BaseOperator
	config *plan.LimitConfig
}

// NewLimitOperator 创建Limit算子
func NewLimitOperator(p *plan.Plan, das dataaccess.Service) (*LimitOperator, error) {
	config, ok := p.Config.(*plan.LimitConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Limit: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	
	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &LimitOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行Limit
func (op *LimitOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 执行子算子
	if len(op.children) == 0 {
		return nil, fmt.Errorf("LimitOperator requires at least 1 child")
	}

	childResult, err := op.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute child failed: %w", err)
	}

	// 应用Offset
	offset := op.config.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(childResult.Rows)) {
		offset = int64(len(childResult.Rows))
	}

	// 应用Limit
	limit := op.config.Limit
	if limit < 0 {
		limit = int64(len(childResult.Rows)) - offset
	}
	end := offset + limit
	if end > int64(len(childResult.Rows)) {
		end = int64(len(childResult.Rows))
	}

	// 切片
	resultRows := childResult.Rows[int(offset):int(end)]

	return &domain.QueryResult{
		Columns: childResult.Columns,
		Rows:    resultRows,
	}, nil
}
