package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TableScanOperator 表扫描算子
type TableScanOperator struct {
	*BaseOperator
	config *plan.TableScanConfig
}

// NewTableScanOperator 创建表扫描算子
func NewTableScanOperator(p *plan.Plan, das dataaccess.Service) (*TableScanOperator, error) {
	config, ok := p.Config.(*plan.TableScanConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for TableScan: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	return &TableScanOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行表扫描
func (op *TableScanOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 使用数据访问服务查询数据
	options := &dataaccess.QueryOptions{
		SelectColumns: make([]string, 0),
		Filters:       op.config.Filters,
	}

	for _, col := range op.config.Columns {
		options.SelectColumns = append(options.SelectColumns, col.Name)
	}

	if op.config.LimitInfo != nil {
		options.Limit = int(op.config.LimitInfo.Limit)
		options.Offset = int(op.config.LimitInfo.Offset)
	}

	result, err := op.dataAccessService.Query(ctx, op.config.TableName, options)
	if err != nil {
		return nil, fmt.Errorf("query table failed: %w", err)
	}

	return result, nil
}
