package operators

import (
	"context"
	"fmt"
	"sort"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// UnionOperator UNION算子
type UnionOperator struct {
	*BaseOperator
	config *plan.UnionConfig
}

// NewUnionOperator 创建UNION算子
func NewUnionOperator(p *plan.Plan, das dataaccess.Service) (*UnionOperator, error) {
	config, ok := p.Config.(*plan.UnionConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Union: %T", p.Config)
	}

	base := NewBaseOperator(p, das)

	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &UnionOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行UNION
func (op *UnionOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [EXECUTOR] Union: 合并 %d 个子结果集, Distinct=%v\n",
		len(op.children), op.config.Distinct)

	// 执行所有子算子
	allRows := make([]domain.Row, 0)
	var columns []domain.ColumnInfo

	for i, child := range op.children {
		result, err := child.Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("execute child %d failed: %w", i, err)
		}

		// 使用第一个子节点的列信息
		if i == 0 {
			columns = result.Columns
		}

		// 合并行
		allRows = append(allRows, result.Rows...)
	}

	// 如果是UNION DISTINCT，去重
	if op.config.Distinct {
		allRows = op.distinctRows(allRows)
		fmt.Printf("  [EXECUTOR] Union: 去重后行数: %d\n", len(allRows))
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    allRows,
		Total:   int64(len(allRows)),
	}, nil
}

// distinctRows 对行去重
func (op *UnionOperator) distinctRows(rows []domain.Row) []domain.Row {
	if len(rows) == 0 {
		return rows
	}

	// 使用map去重
	seen := make(map[string]bool)
	distinct := make([]domain.Row, 0, len(rows))

	for _, row := range rows {
		key := op.rowToKey(row)
		if !seen[key] {
			seen[key] = true
			distinct = append(distinct, row)
		}
	}

	return distinct
}

// rowToKey 将行转换为字符串key用于去重
func (op *UnionOperator) rowToKey(row domain.Row) string {
	// Sort column names for deterministic key generation
	cols := make([]string, 0, len(row))
	for colName := range row {
		cols = append(cols, colName)
	}
	sort.Strings(cols)

	key := ""
	for _, colName := range cols {
		key += fmt.Sprintf("%v:%v|", colName, row[colName])
	}
	return key
}
