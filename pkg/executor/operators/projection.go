package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ProjectionOperator 投影算子
type ProjectionOperator struct {
	*BaseOperator
	config *plan.ProjectionConfig
}

// NewProjectionOperator 创建投影算子
func NewProjectionOperator(p *plan.Plan, das dataaccess.Service) (*ProjectionOperator, error) {
	config, ok := p.Config.(*plan.ProjectionConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Projection: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	
	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &ProjectionOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行投影
func (op *ProjectionOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [EXECUTOR] Projection: 表达式数: %d\n", len(op.config.Expressions))

	// 执行子算子
	if len(op.children) == 0 {
		return nil, fmt.Errorf("ProjectionOperator requires at least 1 child")
	}

	childResult, err := op.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute child failed: %w", err)
	}

	// 投影列
	resultRows := make([]domain.Row, len(childResult.Rows))
	outputColumns := make([]domain.ColumnInfo, 0)

	// 计算输出列
	for i, expr := range op.config.Expressions {
		colName := ""
		if i < len(op.config.Aliases) && op.config.Aliases[i] != "" {
			colName = op.config.Aliases[i]
		} else if expr.Type == parser.ExprTypeColumn {
			colName = expr.Column
		} else {
			colName = fmt.Sprintf("col_%d", i)
		}
		outputColumns = append(outputColumns, domain.ColumnInfo{
			Name: colName,
			Type: "TEXT",
		})
	}

	// 应用投影
	for i, row := range childResult.Rows {
		newRow := make(domain.Row)
		for j, expr := range op.config.Expressions {
			colName := ""
			if j < len(op.config.Aliases) && op.config.Aliases[j] != "" {
				colName = op.config.Aliases[j]
			} else if expr.Type == parser.ExprTypeColumn {
				colName = expr.Column
			} else {
				colName = fmt.Sprintf("col_%d", j)
			}

			// 简化处理：只处理列引用
			if expr.Type == parser.ExprTypeColumn {
				if val, ok := row[expr.Column]; ok {
					newRow[colName] = val
				}
			} else {
				newRow[colName] = nil
			}
		}
		resultRows[i] = newRow
	}

	return &domain.QueryResult{
		Columns: outputColumns,
		Rows:    resultRows,
	}, nil
}
