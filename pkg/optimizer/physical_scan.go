package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PhysicalTableScan 物理表扫描（已重构到pkg/optimizer/physical包）
// 在重构完成前，暂时保留原始实现
type PhysicalTableScan struct {
	TableName             string
	Columns               []ColumnInfo
	TableInfo             *domain.TableInfo
	cost                  float64
	children              []PhysicalPlan
	dataSource            domain.DataSource
	filters               []domain.Filter // 下推的过滤条件
	limitInfo             *LimitInfo      // 下推的Limit信息
	parallelScanner       *OptimizedParallelScanner // 并行扫描器
	enableParallelScan    bool           // 是否启用并行扫描
	minParallelScanRows   int64          // 启用并行扫描的最小行数
}

// NewPhysicalTableScan 创建物理表扫描
func NewPhysicalTableScan(tableName string, tableInfo *domain.TableInfo, dataSource domain.DataSource, filters []domain.Filter, limitInfo *LimitInfo) *PhysicalTableScan {
	columns := make([]ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	// 假设表有1000行
	rowCount := int64(1000)

	// 如果有Limit，调整成本估计
	if limitInfo != nil && limitInfo.Limit > 0 {
		rowCount = limitInfo.Limit
	}

	// 创建并行扫描器（自动选择最优并行度：min(CPU核心数, 8)，范围 [4, 8]）
	parallelScanner := NewOptimizedParallelScanner(dataSource, 0)

	// 启用并行扫描的最小行数（100行，根据性能基准测试优化）
	minParallelScanRows := int64(100)

	// 如果数据量足够大且没有过滤条件，启用并行扫描
	// <100行使用串行扫描，避免并行开销
	enableParallelScan := rowCount >= minParallelScanRows && len(filters) == 0

	return &PhysicalTableScan{
		TableName:           tableName,
		Columns:             columns,
		TableInfo:           tableInfo,
		cost:                float64(rowCount),
		children:            []PhysicalPlan{},
		dataSource:          dataSource,
		filters:             filters,
		limitInfo:           limitInfo,
		parallelScanner:     parallelScanner,
		enableParallelScan:  enableParallelScan,
		minParallelScanRows: minParallelScanRows,
	}
}

// PhysicalSelection 物理过滤
// 在重构完成前，暂时保留原始实现
type PhysicalSelection struct {
	Conditions []*parser.Expression
	Filters    []domain.Filter
	cost       float64
	children   []PhysicalPlan
	dataSource domain.DataSource
}

// NewPhysicalSelection 创建物理过滤
func NewPhysicalSelection(conditions []*parser.Expression, filters []domain.Filter, child PhysicalPlan, dataSource domain.DataSource) *PhysicalSelection {
	inputCost := child.Cost()
	cost := inputCost*1.2 + 10 // 过滤成本

	return &PhysicalSelection{
		Conditions: conditions,
		Filters:    filters,
		cost:       cost,
		children:   []PhysicalPlan{child},
		dataSource: dataSource,
	}
}

// PhysicalProjection 物理投影
// 在重构完成前，暂时保留原始实现
type PhysicalProjection struct {
	Exprs    []*parser.Expression
	Aliases  []string
	Columns  []ColumnInfo
	cost     float64
	children []PhysicalPlan
}

// NewPhysicalProjection 创建物理投影
func NewPhysicalProjection(exprs []*parser.Expression, aliases []string, child PhysicalPlan) *PhysicalProjection {
	inputCost := child.Cost()
	cost := inputCost*1.1 + float64(len(exprs))*5 // 投影成本

	columns := make([]ColumnInfo, len(exprs))
	for i, expr := range exprs {
		name := aliases[i]
		if name == "" {
			if expr.Type == parser.ExprTypeColumn {
				name = expr.Column
			} else {
				name = fmt.Sprintf("expr_%d", i)
			}
		}
		columns[i] = ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		}
	}

	return &PhysicalProjection{
		Exprs:    exprs,
		Aliases:  aliases,
		Columns:  columns,
		cost:     cost,
		children: []PhysicalPlan{child},
	}
}

// PhysicalLimit 物理限制
// 在重构完成前，暂时保留原始实现
type PhysicalLimit struct {
	Limit    int64
	Offset   int64
	cost     float64
	children []PhysicalPlan
}

// NewPhysicalLimit 创建物理限制
func NewPhysicalLimit(limit, offset int64, child PhysicalPlan) *PhysicalLimit {
	inputCost := child.Cost()
	cost := inputCost + float64(limit)*0.01 // 限制操作成本很低

	return &PhysicalLimit{
		Limit:    limit,
		Offset:   offset,
		cost:     cost,
		children: []PhysicalPlan{child},
	}
}

// PhysicalHashJoin 物理哈希连接
// 在重构完成前，暂时保留原始实现
type PhysicalHashJoin struct {
	JoinType   JoinType
	Conditions []*JoinCondition
	cost       float64
	children   []PhysicalPlan
}

// NewPhysicalHashJoin 创建物理哈希连接
func NewPhysicalHashJoin(joinType JoinType, left, right PhysicalPlan, conditions []*JoinCondition) *PhysicalHashJoin {
	leftRows := int64(1000) // 假设
	rightRows := int64(1000) // 假设

	// Hash Join 成本 = 构建哈希表 + 探测
	buildCost := float64(leftRows) * 0.1
	probeCost := float64(rightRows) * 0.1
	cost := left.Cost() + right.Cost() + buildCost + probeCost

	return &PhysicalHashJoin{
		JoinType:   joinType,
		Conditions: conditions,
		cost:       cost,
		children:   []PhysicalPlan{left, right},
	}
}

// Execute 执行扫描
func (p *PhysicalTableScan) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 计算偏移量和限制量
	offset := int64(0)
	limit := int64(0)
	if p.limitInfo != nil {
		offset = p.limitInfo.Offset
		limit = p.limitInfo.Limit
	}

	// 如果没有过滤条件且启用了并行扫描，使用 OptimizedParallelScanner
	if p.enableParallelScan && len(p.filters) == 0 {
		// 使用并行扫描器执行查询
		scanRange := ScanRange{
			TableName: p.TableName,
			Offset:    offset,
			Limit:     limit,
		}

		options := &domain.QueryOptions{}
		if limit > 0 {
			options.Limit = int(limit)
		}
		if offset > 0 {
			options.Offset = int(offset)
		}

		// 如果应用了列裁剪，只选择需要的列
		if len(p.Columns) < len(p.TableInfo.Columns) {
			options.SelectColumns = make([]string, len(p.Columns))
			for i, col := range p.Columns {
				options.SelectColumns[i] = col.Name
			}
		}

		result, err := p.parallelScanner.Execute(ctx, scanRange, options)
		if err != nil {
			// 回退到串行扫描
			return p.executeSerialScan(ctx)
		}

		// 如果应用了列裁剪，调整结果的Columns
		if len(p.Columns) < len(p.TableInfo.Columns) {
			filteredRows := make([]domain.Row, len(result.Rows))
			for i, row := range result.Rows {
				filteredRow := make(domain.Row)
				for _, col := range p.Columns {
					if val, exists := row[col.Name]; exists {
						filteredRow[col.Name] = val
					}
				}
				filteredRows[i] = filteredRow
			}

			// 更新结果的Columns
			filteredColumns := make([]domain.ColumnInfo, len(p.Columns))
			for i, col := range p.Columns {
				filteredColumns[i] = domain.ColumnInfo{
					Name:     col.Name,
					Type:     col.Type,
					Nullable: col.Nullable,
				}
			}

			result.Columns = filteredColumns
			result.Rows = filteredRows
		}

		return result, nil
	}

	// 否则使用串行扫描
	return p.executeSerialScan(ctx)
}

// executeSerialScan 执行串行扫描
func (p *PhysicalTableScan) executeSerialScan(ctx context.Context) (*domain.QueryResult, error) {
	// 检查数据源是否支持 FilterableDataSource
	filterableDS, isFilterable := p.dataSource.(domain.FilterableDataSource)

	// 计算偏移量和限制量
	offset := int64(0)
	limit := int64(0)
	if p.limitInfo != nil {
		offset = p.limitInfo.Offset
		limit = p.limitInfo.Limit
	}

	var result *domain.QueryResult
	var err error

	if isFilterable && len(p.filters) > 0 {
		// 数据源支持过滤，调用 Filter 方法
		// 构建过滤条件
		var filter domain.Filter
		if len(p.filters) == 1 {
			// 单个条件，直接使用
			filter = p.filters[0]
		} else {
			// 多个条件，使用 AND 逻辑组合
			filter = domain.Filter{
				Logic: "AND",
				Value: p.filters,
			}
		}

		// 调用 Filter 方法
		rows, total, filterErr := filterableDS.Filter(ctx, p.TableName, filter, int(offset), int(limit))
		if filterErr != nil {
			return nil, filterErr
		}

		// 构建结果
		result = &domain.QueryResult{
			Rows:  rows,
			Total: total,
		}
	} else {
		// 数据源不支持过滤或无过滤条件，使用 Query 方法
		options := &domain.QueryOptions{}
		if len(p.filters) > 0 {
			options.Filters = p.filters
		}
		if limit > 0 {
			options.Limit = int(limit)
		}
		if offset > 0 {
			options.Offset = int(offset)
		}

		// 如果应用了列裁剪，只选择需要的列
		if len(p.Columns) < len(p.TableInfo.Columns) {
			options.SelectColumns = make([]string, len(p.Columns))
			for i, col := range p.Columns {
				options.SelectColumns[i] = col.Name
			}
		}

		// 调用 Query 方法
		result, err = p.dataSource.Query(ctx, p.TableName, options)
		if err != nil {
			return nil, err
		}
	}

	// 如果应用了列裁剪，调整结果的Columns
	if len(p.Columns) < len(p.TableInfo.Columns) {
		filteredRows := make([]domain.Row, len(result.Rows))
		for i, row := range result.Rows {
			filteredRow := make(domain.Row)
			for _, col := range p.Columns {
				if val, exists := row[col.Name]; exists {
					filteredRow[col.Name] = val
				}
			}
			filteredRows[i] = filteredRow
		}

		// 更新结果的Columns
		filteredColumns := make([]domain.ColumnInfo, len(p.Columns))
		for i, col := range p.Columns {
			filteredColumns[i] = domain.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			}
		}

		result.Columns = filteredColumns
		result.Rows = filteredRows
	}

	return result, nil
}

// Cost 返回执行成本
func (p *PhysicalTableScan) Cost() float64 {
	return p.cost
}

// PhysicalHashAggregate 物理哈希聚合
// 在重构完成前，暂时保留原始实现
type PhysicalHashAggregate struct {
	AggFuncs   []*AggregationItem
	GroupByCols []string
	cost        float64
	children    []PhysicalPlan
}

// NewPhysicalHashAggregate 创建物理哈希聚合
func NewPhysicalHashAggregate(aggFuncs []*AggregationItem, groupByCols []string, child PhysicalPlan) *PhysicalHashAggregate {
	inputRows := int64(1000) // 假设

	// Hash Agg 成本 = 分组 + 聚合
	groupCost := float64(inputRows) * float64(len(groupByCols)) * 0.05
	aggCost := float64(inputRows) * float64(len(aggFuncs)) * 0.05
	cost := child.Cost() + groupCost + aggCost

	return &PhysicalHashAggregate{
		AggFuncs:   aggFuncs,
		GroupByCols: groupByCols,
		cost:        cost,
		children:    []PhysicalPlan{child},
	}
}
