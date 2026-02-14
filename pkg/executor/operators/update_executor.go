package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// UpdateOperator UPDATE算子
type UpdateOperator struct {
	*BaseOperator
	config *plan.UpdateConfig
}

// NewUpdateOperator 创建UPDATE算子
func NewUpdateOperator(p *plan.Plan, das dataaccess.Service) (*UpdateOperator, error) {
	config, ok := p.Config.(*plan.UpdateConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Update: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	return &UpdateOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行UPDATE
func (op *UpdateOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [EXECUTOR] Update: 更新表 %s, 列数: %d\n",
		op.config.TableName, len(op.config.Set))

	// 将SET表达式转换为实际值
	updateData := make(map[string]interface{})
	for col, expr := range op.config.Set {
		updateData[col] = op.evaluateExpression(expr)
	}

	// 构建WHERE过滤器
	var whereFilter *domain.Filter
	if op.config.Where != nil {
		whereFilter = op.expressionToFilter(op.config.Where)
	}

	// 执行更新
	err := op.dataAccessService.Update(ctx, op.config.TableName, updateData, whereFilter)
	if err != nil {
		return nil, fmt.Errorf("update data failed: %w", err)
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

	fmt.Printf("  [EXECUTOR] Update: 成功更新 %d 行\n", rowsAffected)
	return result, nil
}

// evaluateExpression 评估表达式
func (op *UpdateOperator) evaluateExpression(expr parser.Expression) interface{} {
	if expr.Type == parser.ExprTypeValue {
		return expr.Value
	}
	// 对于常量表达式，简单处理
	// 实际实现需要完整的表达式求值器
	return nil
}

// expressionToFilter 将表达式转换为过滤器
func (op *UpdateOperator) expressionToFilter(expr *parser.Expression) *domain.Filter {
	if expr == nil {
		return nil
	}
	// Extract field name and value from expression tree
	field := "id"
	operator := "="
	var value interface{}

	if expr.Left != nil && expr.Left.Type == parser.ExprTypeColumn {
		field = getColumnName(expr.Left)
	}
	if expr.Operator != "" {
		operator = expr.Operator
	}
	if expr.Right != nil {
		value = expr.Right.Value
	} else {
		value = expr.Value
	}

	return &domain.Filter{
		Field:    field,
		Operator: operator,
		Value:    value,
	}
}
