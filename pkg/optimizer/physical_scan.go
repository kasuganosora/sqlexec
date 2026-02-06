package optimizer

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// PhysicalTableScan 物理表扫描
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

// Children 获取子节点
func (p *PhysicalTableScan) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalTableScan) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalTableScan) Schema() []ColumnInfo {
	return p.Columns
}

// Cost 返回执行成本
func (p *PhysicalTableScan) Cost() float64 {
	return p.cost
}

// Execute 执行扫描
func (p *PhysicalTableScan) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 开始查询表 %s, 过滤器数: %d, Limit: %v\n", p.TableName, len(p.filters), p.limitInfo)

	// 计算偏移量和限制量
	offset := int64(0)
	limit := int64(0)
	if p.limitInfo != nil {
		offset = p.limitInfo.Offset
		limit = p.limitInfo.Limit
	}

	// 如果没有过滤条件且启用了并行扫描，使用 OptimizedParallelScanner
	if p.enableParallelScan && len(p.filters) == 0 {
		fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 使用 OptimizedParallelScanner 进行并行扫描\n")
		
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
				fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 选择列 %s\n", col.Name)
			}
		}

		result, err := p.parallelScanner.Execute(ctx, scanRange, options)
		if err != nil {
			fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 并行扫描失败 %v，回退到串行扫描\n", err)
			// 回退到串行扫描
			return p.executeSerialScan(ctx)
		}

		fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 并行扫描完成，返回 %d 行\n", len(result.Rows))
		
		// 如果应用了列裁剪，调整结果的Columns
		if len(p.Columns) < len(p.TableInfo.Columns) {
			fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 应用列裁剪到结果，原列数=%d，裁剪后=%d\n", len(result.Columns), len(p.Columns))
			// 只保留需要的列
			columnMap := make(map[string]int)
			for i, col := range p.Columns {
				columnMap[col.Name] = i
			}
			
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
			fmt.Printf("  [DEBUG] PhysicalTableScan.Execute: 列裁剪完成\n")
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
		fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 数据源支持过滤，调用 Filter 方法\n")
		for i, filter := range p.filters {
			fmt.Printf("  [DEBUG]   过滤器%d: Field=%s, Operator=%s, Value=%v\n", i, filter.Field, filter.Operator, filter.Value)
		}

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
			fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: Filter 方法失败 %v\n", filterErr)
			return nil, filterErr
		}

		// 构建结果
		result = &domain.QueryResult{
			Rows:  rows,
			Total: total,
		}
		fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: Filter 完成，返回 %d 行（total=%d）\n", len(rows), total)
		} else {
			// 数据源不支持过滤或无过滤条件，使用 Query 方法
			fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 数据源不支持过滤或无过滤条件，使用 Query 方法\n")
			
			// 使用 QueryOptions 传递过滤和分页
			options := &domain.QueryOptions{}
			if len(p.filters) > 0 {
				options.Filters = p.filters
				fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 应用过滤条件到 QueryOptions\n")
				for i, filter := range p.filters {
					fmt.Printf("  [DEBUG]   过滤器%d: Field=%s, Operator=%s, Value=%v\n", i, filter.Field, filter.Operator, filter.Value)
				}
			}
			if limit > 0 {
				options.Limit = int(limit)
			}
			if offset > 0 {
				options.Offset = int(offset)
			}
			fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 应用分页参数: limit=%d, offset=%d\n", options.Limit, options.Offset)
			
			// 如果应用了列裁剪，只选择需要的列
			if len(p.Columns) < len(p.TableInfo.Columns) {
				options.SelectColumns = make([]string, len(p.Columns))
				for i, col := range p.Columns {
					options.SelectColumns[i] = col.Name
					fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 选择列 %s\n", col.Name)
				}
			}

		// 调用 Query 方法
		result, err = p.dataSource.Query(ctx, p.TableName, options)
		if err != nil {
			fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: Query 方法失败 %v\n", err)
			return nil, err
		}
		fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: Query 完成，返回 %d 行\n", len(result.Rows))
	}

	// 如果应用了列裁剪，调整结果的Columns
	if len(p.Columns) < len(p.TableInfo.Columns) {
		fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 应用列裁剪到结果，原列数=%d，裁剪后=%d\n", len(result.Columns), len(p.Columns))
		
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
		fmt.Printf("  [DEBUG] PhysicalTableScan.executeSerialScan: 列裁剪完成\n")
	}

	return result, nil
}

// Explain 返回计划说明
func (p *PhysicalTableScan) Explain() string {
	return fmt.Sprintf("TableScan(%s, cost=%.2f)", p.TableName, p.cost)
}

// PhysicalSelection 物理过滤
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

// Children 获取子节点
func (p *PhysicalSelection) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalSelection) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalSelection) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalSelection) Cost() float64 {
	return p.cost
}

// Execute 执行过滤
func (p *PhysicalSelection) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalSelection has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	// 手动应用过滤（简化实现）
	filtered := []domain.Row{}
	for _, row := range input.Rows {
		match := true
		for _, filter := range p.Filters {
			if !matchesFilter(row, filter) {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, row)
		}
	}

	return &domain.QueryResult{
		Columns: input.Columns,
		Rows:    filtered,
		Total:    int64(len(filtered)),
	}, nil
}

// matchesFilter 检查行是否匹配过滤器（简化实现）
func matchesFilter(row domain.Row, filter domain.Filter) bool {
	match, _ := utils.MatchesFilter(row, filter)
	return match
}

// Explain 返回计划说明
func (p *PhysicalSelection) Explain() string {
	return fmt.Sprintf("Selection(cost=%.2f)", p.cost)
}

// PhysicalProjection 物理投影
type PhysicalProjection struct {
	Exprs   []*parser.Expression
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
		Aliases:   aliases,
		Columns:   columns,
		cost:      cost,
		children:  []PhysicalPlan{child},
	}
}

// Children 获取子节点
func (p *PhysicalProjection) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalProjection) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalProjection) Schema() []ColumnInfo {
	return p.Columns
}

// Cost 返回执行成本
func (p *PhysicalProjection) Cost() float64 {
	return p.cost
}

// Execute 执行投影
func (p *PhysicalProjection) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalProjection has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 输入行数: %d, 输入列数: %d\n", len(input.Rows), len(input.Columns))

	// 应用投影（简化实现，只支持列选择）
	output := []domain.Row{}
	for rowIdx, row := range input.Rows {
		newRow := make(domain.Row)
		fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 处理行 %d, 原始keys: %v\n", rowIdx, getMapKeys(row))
		for i, expr := range p.Exprs {
			if expr.Type == parser.ExprTypeColumn {
				fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 尝试提取列 %s (别名: %s)\n", expr.Column, p.Aliases[i])
				if val, exists := row[expr.Column]; exists {
					newRow[p.Aliases[i]] = val
					fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 提取成功, 值: %v\n", val)
				} else {
					fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 列 %s 不存在于行中\n", expr.Column)
					// 简化：不支持表达式计算
					newRow[p.Aliases[i]] = nil
				}
			} else {
				// 简化：不支持表达式计算
				newRow[p.Aliases[i]] = nil
			}
		}
		fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 新行keys: %v\n", getMapKeys(newRow))
		output = append(output, newRow)
	}

	// 更新列信息
	columns := make([]domain.ColumnInfo, len(p.Columns))
	for i, col := range p.Columns {
		columns[i] = domain.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	fmt.Printf("  [DEBUG] PhysicalProjection.Execute: 输出行数: %d, 输出列: %v\n", len(output), p.Aliases)
	return &domain.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:    int64(len(output)),
	}, nil
}

// getMapKeys 获取map的所有key
func getMapKeys(m domain.Row) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Explain 返回计划说明
func (p *PhysicalProjection) Explain() string {
	return fmt.Sprintf("Projection(cost=%.2f)", p.cost)
}

// PhysicalLimit 物理限制
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

// Children 获取子节点
func (p *PhysicalLimit) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalLimit) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalLimit) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalLimit) Cost() float64 {
	return p.cost
}

// Execute 执行限制
func (p *PhysicalLimit) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalLimit has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	// 应用 OFFSET 和 LIMIT
	start := p.Offset
	if start < 0 {
		start = 0
	}
	if start >= int64(len(input.Rows)) {
		return &domain.QueryResult{
			Columns: input.Columns,
			Rows:    []domain.Row{},
			Total:    0,
		}, nil
	}

	end := start + p.Limit
	if end > int64(len(input.Rows)) {
		end = int64(len(input.Rows))
	}

	output := input.Rows[start:end]

	return &domain.QueryResult{
		Columns: input.Columns,
		Rows:    output,
		Total:    int64(len(output)),
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalLimit) Explain() string {
	return fmt.Sprintf("Limit(offset=%d, limit=%d, cost=%.2f)", p.Offset, p.Limit, p.cost)
}

// PhysicalHashJoin 物理哈希连接
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

// Children 获取子节点
func (p *PhysicalHashJoin) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashJoin) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashJoin) Schema() []ColumnInfo {
	columns := []ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// Cost 返回执行成本
func (p *PhysicalHashJoin) Cost() float64 {
	return p.cost
}

// Execute 执行哈希连接
func (p *PhysicalHashJoin) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) != 2 {
		return nil, fmt.Errorf("HashJoin requires exactly 2 children")
	}

	// 获取连接条件（简化：只支持单列等值连接）
	leftJoinCol := ""
	rightJoinCol := ""
	if len(p.Conditions) > 0 && p.Conditions[0].Left != nil {
		leftJoinCol = fmt.Sprintf("%v", p.Conditions[0].Left)
	}
	if len(p.Conditions) > 0 && p.Conditions[0].Right != nil {
		rightJoinCol = fmt.Sprintf("%v", p.Conditions[0].Right)
	}

	// 1. 执行左表（构建端）
	leftResult, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("left table execute error: %w", err)
	}

	// 2. 执行右表（探测端）
	rightResult, err := p.children[1].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("right table execute error: %w", err)
	}

	// 3. 构建哈希表（从左表）
	hashTable := make(map[interface{}][]domain.Row)
	for _, row := range leftResult.Rows {
		key := row[leftJoinCol]
		hashTable[key] = append(hashTable[key], row)
	}

	// 4. 探测右表并产生结果
	output := []domain.Row{}

	// 根据连接类型处理
	switch p.JoinType {
	case InnerJoin:
		// INNER JOIN：两边都有匹配
		for _, rightRow := range rightResult.Rows {
			key := rightRow[rightJoinCol]
			if leftRows, exists := hashTable[key]; exists {
				for _, leftRow := range leftRows {
					// 合并左右行
					merged := make(domain.Row)
					maps.Copy(merged, leftRow)
					for k, v := range rightRow {
						// 如果列名冲突，添加前缀
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			}
		}
	case LeftOuterJoin:
		// LEFT JOIN：左边所有行，右边没有匹配的用NULL填充
		// 跟踪右边已匹配的行
		rightMatched := make(map[int]bool)
		for _, rightRow := range rightResult.Rows {
			key := rightRow[rightJoinCol]
			if leftRows, exists := hashTable[key]; exists {
// 有匹配：连接
			for _, leftRow := range leftRows {
				merged := make(domain.Row)
				maps.Copy(merged, leftRow)
				for k, v := range rightRow {
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			// 标记右边已匹配的行 - 简化：不比较行内容
			// 由于 map 不能直接比较，使用索引方式
			rightMatched[len(rightResult.Rows)-1] = true
			}
		}
		// 添加左边没有匹配的行
		for _, leftRow := range leftResult.Rows {
			leftKey := leftRow[leftJoinCol]
			matched := false
			for _, rightRow := range rightResult.Rows {
				if rightRow[rightJoinCol] == leftKey {
					matched = true
					break
				}
			}
if !matched {
			merged := make(domain.Row)
			maps.Copy(merged, leftRow)
			for _, col := range rightResult.Columns {
					newKey := col.Name
					if _, exists := merged[newKey]; exists {
						newKey = "right_" + col.Name
					}
					merged[newKey] = nil
				}
				output = append(output, merged)
			}
		}
case RightOuterJoin:
		// RIGHT JOIN：右边所有行，左边没有匹配的用NULL填充
		// 重新构建左表的哈希表用于RIGHT JOIN
		leftHashTable := make(map[interface{}][]domain.Row)
		for _, row := range leftResult.Rows {
			key := row[leftJoinCol]
			leftHashTable[key] = append(leftHashTable[key], row)
		}

		for _, rightRow := range rightResult.Rows {
			key := rightRow[rightJoinCol]
			if leftRows, exists := leftHashTable[key]; exists {
				// 有匹配：连接
				for _, leftRow := range leftRows {
					merged := make(domain.Row)
					maps.Copy(merged, leftRow)
					for k, v := range rightRow {
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			} else {
				// 无匹配：左边NULL + 右边行
				merged := make(domain.Row)
				for _, col := range leftResult.Columns {
					merged[col.Name] = nil
				}
				for k, v := range rightRow {
					newKey := k
					if _, exists := merged[newKey]; exists {
						newKey = "right_" + k
					}
					merged[newKey] = v
				}
				output = append(output, merged)
			}
		}

default:
		return nil, fmt.Errorf("unsupported join type: %s", p.JoinType)
	}

	// 合并列信息
	columns := []domain.ColumnInfo{}
	columns = append(columns, leftResult.Columns...)
	for _, col := range rightResult.Columns {
		// 检查列名是否冲突
		conflict := false
		for _, leftCol := range leftResult.Columns {
			if leftCol.Name == col.Name {
				conflict = true
				break
			}
		}
		if conflict {
			columns = append(columns, domain.ColumnInfo{
				Name:     "right_" + col.Name,
				Type:     col.Type,
				Nullable: true,
			})
		} else {
			columns = append(columns, col)
		}
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalHashJoin) Explain() string {
	return fmt.Sprintf("HashJoin(type=%s, cost=%.2f)", p.JoinType, p.cost)
}

// PhysicalHashAggregate 物理哈希聚合
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

// Children 获取子节点
func (p *PhysicalHashAggregate) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashAggregate) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashAggregate) Schema() []ColumnInfo {
	columns := []ColumnInfo{}

	// 添加 GROUP BY 列
	for _, col := range p.GroupByCols {
		columns = append(columns, ColumnInfo{
			Name:     col,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 添加聚合函数列
	for _, agg := range p.AggFuncs {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return columns
}

// Cost 返回执行成本
func (p *PhysicalHashAggregate) Cost() float64 {
	return p.cost
}

// Execute 执行哈希聚合
func (p *PhysicalHashAggregate) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("HashAggregate has no child")
	}

	// 执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	if len(p.AggFuncs) == 0 && len(p.GroupByCols) == 0 {
		// 没有聚合函数也没有分组，直接返回
		return input, nil
	}

	// 用于存储分组结果的哈希表
	type groupKey struct {
		values []interface{}
	}
	groups := make(map[interface{}]*aggregateGroup)

	// 遍历所有行，进行分组和聚合
	for _, row := range input.Rows {
		// 构建分组键
		key := make([]interface{}, len(p.GroupByCols))
		for i, colName := range p.GroupByCols {
			key[i] = row[colName]
		}

		// 将key转换为字符串作为map的key
		keyStr := fmt.Sprintf("%v", key)

		// 获取或创建分组
		group, exists := groups[keyStr]
		if !exists {
			group = &aggregateGroup{
				key:    key,
				rows:   []domain.Row{},
				values: make(map[string]interface{}),
			}
			groups[keyStr] = group
		}

		group.rows = append(group.rows, row)
	}

	// 为每个分组计算聚合函数
	output := []domain.Row{}
	for _, group := range groups {
		row := make(domain.Row)

		// 添加 GROUP BY 列
		for i, colName := range p.GroupByCols {
			if i < len(group.key) {
				row[colName] = group.key[i]
			}
		}

		// 计算聚合函数
		for _, agg := range p.AggFuncs {
			result := p.calculateAggregation(agg, group.rows)
			colName := agg.Alias
			if colName == "" {
				colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
			}
			row[colName] = result
		}

		output = append(output, row)
	}

	// 构建列信息
	columns := []domain.ColumnInfo{}

	// GROUP BY 列
	for _, colName := range p.GroupByCols {
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 聚合函数列
	for _, agg := range p.AggFuncs {
		colName := agg.Alias
		if colName == "" {
			colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// aggregateGroup 表示一个分组
type aggregateGroup struct {
	key    []interface{}
	rows   []domain.Row
	values map[string]interface{}
}

// calculateAggregation 计算聚合函数
func (p *PhysicalHashAggregate) calculateAggregation(agg *AggregationItem, rows []domain.Row) interface{} {
	if len(rows) == 0 {
		switch agg.Type {
		case Count:
			return int64(0)
		case Sum, Avg, Max, Min:
			return nil
		}
	}

	// 获取聚合列名
	colName := agg.Expr.Column
	if colName == "" && agg.Expr.Function != "" {
		colName = fmt.Sprintf("%s(%v)", agg.Expr.Function, agg.Expr.Args)
	}

	switch agg.Type {
	case Count:
		return int64(len(rows))
	case Sum:
		sum := 0.0
		for _, row := range rows {
			val := row[colName]
			if val != nil {
				fval, _ := utils.ToFloat64(val)
				sum += fval
			}
		}
		return sum
	case Avg:
		if len(rows) == 0 {
			return nil
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			val := row[colName]
			if val != nil {
				fval, _ := utils.ToFloat64(val)
				sum += fval
				count++
			}
		}
		if count > 0 {
			return sum / float64(count)
		}
		return nil
	case Max:
		var max interface{}
		for _, row := range rows {
			val := row[colName]
			if val != nil && max == nil {
				max = val
			} else if val != nil && max != nil {
				cmp := compareValues(val, max)
				if cmp > 0 {
					max = val
				}
			}
		}
		return max
	case Min:
		var min interface{}
		for _, row := range rows {
			val := row[colName]
			if val != nil && min == nil {
				min = val
			} else if val != nil && min != nil {
				cmp := compareValues(val, min)
				if cmp < 0 {
					min = val
				}
			}
		}
		return min
	}
	return nil
}

// Explain 返回计划说明
func (p *PhysicalHashAggregate) Explain() string {
	var aggFuncsBuilder strings.Builder
	for i, agg := range p.AggFuncs {
		if i > 0 {
			aggFuncsBuilder.WriteString(", ")
		}
		aggFuncsBuilder.WriteString(agg.Type.String())
	}
	aggFuncs := aggFuncsBuilder.String()
	
	groupBy := ""
	if len(p.GroupByCols) > 0 {
		groupBy = fmt.Sprintf(", GROUP BY(%s)", fmt.Sprintf("%v", p.GroupByCols))
	}
	
	return fmt.Sprintf("HashAggregate(funcs=[%s]%s, cost=%.2f)", aggFuncs, groupBy, p.cost)
}
