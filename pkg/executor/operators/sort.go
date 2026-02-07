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
	fmt.Printf("  [EXECUTOR] Sort: 排序字段数: %d\n", len(op.config.OrderByItems))

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

	// 排序
	sort.Slice(sortedRows, func(i, j int) bool {
		// 简化版本：只处理第一个排序字段
		if len(op.config.OrderByItems) > 0 {
			item := op.config.OrderByItems[0]
			if item.Expr.Type == parser.ExprTypeColumn {
				colName := item.Expr.Column
				valI, okI := sortedRows[i][colName]
				valJ, okJ := sortedRows[j][colName]
				
				if !okI || !okJ {
					return false
				}

				// 简单比较（实际需要处理不同类型）
				cmp := compareValues(valI, valJ)
				if cmp == 0 {
					return false
				}
				if item.Direction == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
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
		return aInt - bInt
	}

	return 0
}
