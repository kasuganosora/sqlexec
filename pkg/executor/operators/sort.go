package operators

import (
	"context"
	"fmt"
	"sort"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// SortOperator 排序算子
type SortOperator struct {
	*BaseOperator
	config *plan.SortConfig
}

// NewSortOperator 创建排序算子
func NewSortOperator(p *plan.Plan, das dataaccess.Service) (*SortOperator, error) {
	config, ok := p.Config.(*plan.SortConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Sort: %T", p.Config)
	}

	base := NewBaseOperator(p, das)

	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &SortOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行排序
func (op *SortOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 执行子算子
	if len(op.children) == 0 {
		return nil, fmt.Errorf("SortOperator requires at least 1 child")
	}

	childResult, err := op.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute child failed: %w", err)
	}

	// 创建排序后的行切片
	sortedRows := make([]domain.Row, len(childResult.Rows))
	copy(sortedRows, childResult.Rows)

	// 排序：支持多列排序
	sort.SliceStable(sortedRows, func(i, j int) bool {
		for _, item := range op.config.OrderByItems {
			if item.Expr.Type != parser.ExprTypeColumn {
				continue
			}
			colName := item.Expr.Column
			valI, okI := sortedRows[i][colName]
			valJ, okJ := sortedRows[j][colName]

			if !okI || !okJ {
				continue
			}

			var cmp int
			if item.Collation != "" {
				cmp = utils.CompareValuesForSortWithCollation(valI, valJ, item.Collation)
			} else {
				cmp = utils.CompareValuesForSort(valI, valJ)
			}
			if cmp == 0 {
				continue
			}
			if item.Direction == "DESC" {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return &domain.QueryResult{
		Columns: childResult.Columns,
		Rows:    sortedRows,
	}, nil
}
