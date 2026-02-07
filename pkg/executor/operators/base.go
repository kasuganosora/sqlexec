package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Operator 算子接口
type Operator interface {
	// Execute 执行算子
	Execute(ctx context.Context) (*domain.QueryResult, error)
	// GetChildren 获取子算子
	GetChildren() []Operator
	// GetSchema 获取输出Schema
	GetSchema() []domain.ColumnInfo
}

// BaseOperator 算子基类
type BaseOperator struct {
	plan              *plan.Plan
	dataAccessService dataaccess.Service
	children          []Operator
}

// NewBaseOperator 创建算子基类
func NewBaseOperator(p *plan.Plan, das dataaccess.Service) *BaseOperator {
	return &BaseOperator{
		plan:              p,
		dataAccessService: das,
		children:          make([]Operator, 0),
	}
}

// GetChildren 获取子算子
func (op *BaseOperator) GetChildren() []Operator {
	return op.children
}

// GetSchema 获取输出Schema
func (op *BaseOperator) GetSchema() []domain.ColumnInfo {
	schema := make([]domain.ColumnInfo, 0, len(op.plan.OutputSchema))
	for _, col := range op.plan.OutputSchema {
		schema = append(schema, domain.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}
	return schema
}

// BuildChildOperators 构建子算子
func (op *BaseOperator) BuildChildOperators(buildFn func(p *plan.Plan) (Operator, error)) error {
	if len(op.plan.Children) == 0 {
		return nil
	}

	op.children = make([]Operator, 0, len(op.plan.Children))
	for _, childPlan := range op.plan.Children {
		child, err := buildFn(childPlan)
		if err != nil {
			return fmt.Errorf("build child operator failed: %w", err)
		}
		op.children = append(op.children, child)
	}
	return nil
}
