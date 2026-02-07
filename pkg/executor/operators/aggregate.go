package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

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
	fmt.Printf("  [EXECUTOR] Aggregate: 聚合函数数: %d, 分组字段数: %d\n", 
		len(op.config.AggFuncs), len(op.config.GroupByCols))

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

	for _, row := range childResult.Rows {
		// 构建分组键
		groupKey := ""
		for _, col := range op.config.GroupByCols {
			if val, ok := row[col]; ok {
				groupKey += fmt.Sprintf("%v|", val)
			}
		}

		// 初始化分组
		if _, exists := groups[groupKey]; !exists {
			groups[groupKey] = make(map[string]interface{})
			for _, col := range op.config.GroupByCols {
				groups[groupKey][col] = row[col]
			}
		}

		// 执行聚合函数
		for _, agg := range op.config.AggFuncs {
			alias := agg.Alias
			if alias == "" && agg.Expr != nil {
				alias = fmt.Sprintf("agg_%d", len(groups[groupKey]))
			}

			switch agg.Type {
			case types.Count:
				if _, ok := groups[groupKey][alias]; !ok {
					groups[groupKey][alias] = 0
				}
				count := groups[groupKey][alias].(int)
				groups[groupKey][alias] = count + 1
			case types.Sum:
				// 简化处理：假设第一列是数值
				for _, val := range row {
					if num, ok := val.(int); ok {
						if _, ok := groups[groupKey][alias]; !ok {
							groups[groupKey][alias] = 0
						}
						sum := groups[groupKey][alias].(int)
						groups[groupKey][alias] = sum + num
						break
					}
				}
			case types.Avg:
				// 简化：存储sum和count
				sumKey := alias + "_sum"
				countKey := alias + "_count"
				if _, ok := groups[groupKey][sumKey]; !ok {
					groups[groupKey][sumKey] = 0
					groups[groupKey][countKey] = 0
				}
				for _, val := range row {
					if num, ok := val.(int); ok {
						sum := groups[groupKey][sumKey].(int)
						count := groups[groupKey][countKey].(int)
						groups[groupKey][sumKey] = sum + num
						groups[groupKey][countKey] = count + 1
						break
					}
				}
			}
		}
	}

	// 构建结果行
	resultRows := make([]domain.Row, 0, len(groups))
	for _, group := range groups {
		// 计算AVG
		for _, agg := range op.config.AggFuncs {
			if agg.Type == types.Avg {
				alias := agg.Alias
				if alias == "" {
					alias = fmt.Sprintf("agg_%d", len(group))
				}
				sumKey := alias + "_sum"
				countKey := alias + "_count"
				
				if sum, ok := group[sumKey].(int); ok {
					if count, ok := group[countKey].(int); ok && count > 0 {
						group[alias] = sum / count
					}
				}
				delete(group, sumKey)
				delete(group, countKey)
			}
		}
		resultRows = append(resultRows, group)
	}

	// 构建输出列
	outputColumns := make([]domain.ColumnInfo, 0)
	for _, col := range op.config.GroupByCols {
		outputColumns = append(outputColumns, domain.ColumnInfo{
			Name: col,
			Type: "TEXT",
		})
	}
	for _, agg := range op.config.AggFuncs {
		alias := agg.Alias
		if alias == "" {
			alias = fmt.Sprintf("agg_%d", len(outputColumns))
		}
		outputColumns = append(outputColumns, domain.ColumnInfo{
			Name: alias,
			Type: "INTEGER",
		})
	}

	return &domain.QueryResult{
		Columns: outputColumns,
		Rows:    resultRows,
	}, nil
}
