package executor

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/executor/operators"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Executor 执行器接口
type Executor interface {
	// Execute 执行计划
	Execute(ctx context.Context, plan *plan.Plan) (*domain.QueryResult, error)
}

// BaseExecutor 基础执行器
type BaseExecutor struct {
	dataAccessService dataaccess.Service
	runtime           *Runtime
}

// NewExecutor 创建执行器
func NewExecutor(dataAccessService dataaccess.Service) Executor {
	return &BaseExecutor{
		dataAccessService: dataAccessService,
		runtime:           NewRuntime(),
	}
}

// Execute 执行计划
func (e *BaseExecutor) Execute(ctx context.Context, plan *plan.Plan) (*domain.QueryResult, error) {
	// 1. 构建算子树
	operator, err := e.buildOperator(plan)
	if err != nil {
		return nil, fmt.Errorf("build operator failed: %w", err)
	}

	// 2. 执行算子
	result, err := operator.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute operator failed: %w", err)
	}

	return result, nil
}

// buildOperator 构建算子树
func (e *BaseExecutor) buildOperator(p *plan.Plan) (operators.Operator, error) {
	switch p.Type {
	case plan.TypeTableScan:
		return operators.NewTableScanOperator(p, e.dataAccessService)
	case plan.TypeSelection:
		return operators.NewSelectionOperator(p, e.dataAccessService)
	case plan.TypeProjection:
		return operators.NewProjectionOperator(p, e.dataAccessService)
	case plan.TypeLimit:
		return operators.NewLimitOperator(p, e.dataAccessService)
	case plan.TypeAggregate:
		return operators.NewAggregateOperator(p, e.dataAccessService)
	case plan.TypeHashJoin:
		return operators.NewHashJoinOperator(p, e.dataAccessService)
	case plan.TypeInsert:
		return operators.NewInsertOperator(p, e.dataAccessService)
	case plan.TypeUpdate:
		return operators.NewUpdateOperator(p, e.dataAccessService)
	case plan.TypeDelete:
		return operators.NewDeleteOperator(p, e.dataAccessService)
	case plan.TypeSort:
		return operators.NewSortOperator(p, e.dataAccessService)
	case plan.TypeUnion:
		return operators.NewUnionOperator(p, e.dataAccessService)
	default:
		return nil, fmt.Errorf("unsupported plan type: %s", p.Type)
	}
}
