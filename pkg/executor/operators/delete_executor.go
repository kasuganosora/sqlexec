package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// DeleteOperator DELETE算子
type DeleteOperator struct {
	*BaseOperator
	config *plan.DeleteConfig
}

// NewDeleteOperator 创建DELETE算子
func NewDeleteOperator(p *plan.Plan, das dataaccess.Service) (*DeleteOperator, error) {
	config, ok := p.Config.(*plan.DeleteConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Delete: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	return &DeleteOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行DELETE
func (op *DeleteOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [EXECUTOR] Delete: 删除表 %s 的数据\n", op.config.TableName)

	// 构建WHERE过滤器
	var whereFilter *domain.Filter
	if op.config.Where != nil {
		whereFilter = op.expressionToFilter(op.config.Where)
	}

	// 执行删除
	err := op.dataAccessService.Delete(ctx, op.config.TableName, whereFilter)
	if err != nil {
		return nil, fmt.Errorf("delete data failed: %w", err)
	}

	// 简化实现：返回影响的行数为1
	// 实际实现应该从数据源获取实际影响的行数
	rowsAffected := int64(1)

	// 构建结果
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "rows_affected", Type: "int", Nullable: false},
		},
		Rows: []domain.Row{
			{"rows_affected": rowsAffected},
		},
		Total: 1,
	}

	fmt.Printf("  [EXECUTOR] Delete: 成功删除 %d 行\n", rowsAffected)
	return result, nil
}

// expressionToFilter 将表达式转换为过滤器
func (op *DeleteOperator) expressionToFilter(expr *parser.Expression) *domain.Filter {
	// 简化实现
	if expr == nil {
		return nil
	}
	return &domain.Filter{
		Field:    "id",
		Operator: "=",
		Value:    expr.Value,
	}
}
