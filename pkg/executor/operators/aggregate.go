package operators

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// toFloat64 safely converts a numeric value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}

// AggregateOperator 聚合算子
type AggregateOperator struct {
	*BaseOperator
	config *plan.AggregateConfig
}

// NewAggregateOperator 创建聚合算子
func NewAggregateOperator(p *plan.Plan, das dataaccess.Service) (*AggregateOperator, error) {
	config, ok := p.Config.(*plan.AggregateConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Aggregate: %T", p.Config)
	}

	base := NewBaseOperator(p, das)

	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &AggregateOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行聚合
func (op *AggregateOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 执行子算子
	if len(op.children) == 0 {
		return nil, fmt.Errorf("AggregateOperator requires at least 1 child")
	}

	childResult, err := op.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute child failed: %w", err)
	}

	// 分组聚合
	groups := make(map[string]map[string]interface{})

	var keyBuilder strings.Builder
	for _, row := range childResult.Rows {
		// 构建分组键
		keyBuilder.Reset()
		for _, col := range op.config.GroupByCols {
			if val, ok := row[col]; ok {
				fmt.Fprintf(&keyBuilder, "%v|", val)
			}
		}
		groupKey := keyBuilder.String()

		// 初始化分组
		if _, exists := groups[groupKey]; !exists {
			groups[groupKey] = make(map[string]interface{})
			for _, col := range op.config.GroupByCols {
				groups[groupKey][col] = row[col]
			}
		}

		// 执行聚合函数
		for aggIdx, agg := range op.config.AggFuncs {
			alias := agg.Alias
			if alias == "" && agg.Expr != nil {
				alias = fmt.Sprintf("agg_%d", aggIdx)
			}

			switch agg.Type {
			case types.Count:
				if _, ok := groups[groupKey][alias]; !ok {
					groups[groupKey][alias] = 0
				}
				if count, ok := groups[groupKey][alias].(int); ok {
					groups[groupKey][alias] = count + 1
				}
			case types.Sum:
				if agg.Expr != nil && agg.Expr.Column != "" {
					if val, ok := row[agg.Expr.Column]; ok {
						if num, ok := toFloat64(val); ok {
							if _, exists := groups[groupKey][alias]; !exists {
								groups[groupKey][alias] = float64(0)
							}
							if sum, ok := toFloat64(groups[groupKey][alias]); ok {
								groups[groupKey][alias] = sum + num
							}
						}
					}
				}
			case types.Avg:
				sumKey := alias + "_sum"
				countKey := alias + "_count"
				if _, ok := groups[groupKey][sumKey]; !ok {
					groups[groupKey][sumKey] = float64(0)
					groups[groupKey][countKey] = 0
				}
				if agg.Expr != nil && agg.Expr.Column != "" {
					if val, ok := row[agg.Expr.Column]; ok {
						if num, ok := toFloat64(val); ok {
							if sum, ok := toFloat64(groups[groupKey][sumKey]); ok {
								groups[groupKey][sumKey] = sum + num
							}
							if count, ok := groups[groupKey][countKey].(int); ok {
								groups[groupKey][countKey] = count + 1
							}
						}
					}
				}
			case types.Min:
				if agg.Expr != nil && agg.Expr.Column != "" {
					if val, ok := row[agg.Expr.Column]; ok && val != nil {
						cur, exists := groups[groupKey][alias]
						if !exists || cur == nil {
							groups[groupKey][alias] = val
						} else if utils.CompareValuesForSort(val, cur) < 0 {
							groups[groupKey][alias] = val
						}
					}
				}
			case types.Max:
				if agg.Expr != nil && agg.Expr.Column != "" {
					if val, ok := row[agg.Expr.Column]; ok && val != nil {
						cur, exists := groups[groupKey][alias]
						if !exists || cur == nil {
							groups[groupKey][alias] = val
						} else if utils.CompareValuesForSort(val, cur) > 0 {
							groups[groupKey][alias] = val
						}
					}
				}
			}
		}
	}

	// 构建结果行
	resultRows := make([]domain.Row, 0, len(groups))
	for _, group := range groups {
		// 计算AVG
		for aggIdx, agg := range op.config.AggFuncs {
			if agg.Type == types.Avg {
				alias := agg.Alias
				if alias == "" {
					alias = fmt.Sprintf("agg_%d", aggIdx)
				}
				sumKey := alias + "_sum"
				countKey := alias + "_count"

				if sum, ok := toFloat64(group[sumKey]); ok {
					if count, ok := group[countKey].(int); ok && count > 0 {
						group[alias] = sum / float64(count)
					}
				}
				delete(group, sumKey)
				delete(group, countKey)
			}
		}
		resultRows = append(resultRows, group)
	}

	// 构建输出列
	outputColumns := make([]domain.ColumnInfo, 0, len(op.config.GroupByCols)+len(op.config.AggFuncs))
	for _, col := range op.config.GroupByCols {
		outputColumns = append(outputColumns, domain.ColumnInfo{
			Name: col,
			Type: "TEXT",
		})
	}
	for aggIdx, agg := range op.config.AggFuncs {
		alias := agg.Alias
		if alias == "" {
			alias = fmt.Sprintf("agg_%d", aggIdx)
		}
		colType := "INTEGER"
		switch agg.Type {
		case types.Sum, types.Avg:
			colType = "DOUBLE"
		case types.Min, types.Max:
			// Preserve input column type if available
			if agg.Expr != nil && agg.Expr.Column != "" {
				colType = "TEXT" // default; overridden below if child provides type
				if childResult != nil {
					for _, ci := range childResult.Columns {
						if ci.Name == agg.Expr.Column {
							colType = ci.Type
							break
						}
					}
				}
			}
		}
		outputColumns = append(outputColumns, domain.ColumnInfo{
			Name: alias,
			Type: colType,
		})
	}

	return &domain.QueryResult{
		Columns: outputColumns,
		Rows:    resultRows,
	}, nil
}
