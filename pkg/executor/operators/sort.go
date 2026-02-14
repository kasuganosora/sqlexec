package operators

import (
	"context"
	"fmt"
	"sort"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
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

			cmp := compareValues(valI, valJ)
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

// compareValues 比较两个值
func compareValues(a, b interface{}) int {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	aInt, aOk := a.(int)
	bInt, bOk := b.(int)
	if aOk && bOk {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	aInt64, aOk := a.(int64)
	bInt64, bOk := b.(int64)
	if aOk && bOk {
		if aInt64 < bInt64 {
			return -1
		} else if aInt64 > bInt64 {
			return 1
		}
		return 0
	}

	aFloat, aOk := a.(float64)
	bFloat, bOk := b.(float64)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	return 0
}
