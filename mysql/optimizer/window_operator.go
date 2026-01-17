package optimizer

import (
	"context"
	"fmt"
	"mysql-proxy/mysql/parser"
	"sort"
)

// WindowOperator 窗口函数执行算子
type WindowOperator struct {
	// 子算子
	child PhysicalOperator
	
	// 窗口函数定义
	windowFuncs []*WindowFunctionDef
	
	// 执行上下文
	ctx context.Context
}

// WindowFunctionDef 窗口函数定义
type WindowFunctionDef struct {
	Expr     *parser.WindowExpression
	ResultCol string // 结果列名
}

// NewWindowOperator 创建窗口函数算子
func NewWindowOperator(child PhysicalOperator, windowFuncs []*parser.WindowExpression) *WindowOperator {
	funcDefs := make([]*WindowFunctionDef, len(windowFuncs))
	for i, wf := range windowFuncs {
		funcDefs[i] = &WindowFunctionDef{
			Expr:     wf,
			ResultCol: fmt.Sprintf("window_%d", i),
		}
	}
	
	return &WindowOperator{
		child:        child,
		windowFuncs: funcDefs,
	}
}

// Execute 执行窗口函数
func (op *WindowOperator) Execute(ctx context.Context) ([]map[string]interface{}, error) {
	op.ctx = ctx
	
	// 1. 从子算子获取数据
	rows, err := op.child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute child: %w", err)
	}
	
	if len(rows) == 0 {
		return rows, nil
	}
	
	// 2. 处理每个窗口函数
	for _, wfDef := range op.windowFuncs {
		rows, err = op.executeWindowFunction(rows, wfDef)
		if err != nil {
			return nil, fmt.Errorf("failed to execute window function %s: %w", wfDef.Expr.FuncName, err)
		}
	}
	
	return rows, nil
}

// executeWindowFunction 执行单个窗口函数
func (op *WindowOperator) executeWindowFunction(rows []map[string]interface{}, wfDef *WindowFunctionDef) ([]map[string]interface{}, error) {
	wf := wfDef.Expr
	
	// 3. 分区
	partitions := op.partitionRows(rows, wf.Spec.PartitionBy)
	
	// 4. 在每个分区内执行窗口函数
	result := make([]map[string]interface{}, 0, len(rows))
	for _, partition := range partitions {
		// 5. 排序
		sortedPartition := op.sortRows(partition, wf.Spec.OrderBy)
		
		// 6. 计算窗口函数值
		for i, row := range sortedPartition {
			// 克隆行
			newRow := make(map[string]interface{})
			for k, v := range row {
				newRow[k] = v
			}
			
			// 计算窗口函数
			value, err := op.computeWindowValue(sortedPartition, i, wf)
			if err != nil {
				return nil, err
			}
			
			newRow[wfDef.ResultCol] = value
			result = append(result, newRow)
		}
	}
	
	return result, nil
}

// partitionRows 根据分区表达式分割行
func (op *WindowOperator) partitionRows(rows []map[string]interface{}, partitionBy []parser.Expression) [][]map[string]interface{} {
	if len(partitionBy) == 0 {
		// 无分区,所有行为一个分区
		return [][]map[string]interface{}{rows}
	}
	
	// 使用map分组
	partitions := make(map[string][]map[string]interface{})
	
	for _, row := range rows {
		// 计算分区键
		key := op.computePartitionKey(row, partitionBy)
		
		// 添加到对应分区
		partitions[key] = append(partitions[key], row)
	}
	
	// 转换为slice
	result := make([][]map[string]interface{}, 0, len(partitions))
	for _, partition := range partitions {
		result = append(result, partition)
	}
	
	return result
}

// computePartitionKey 计算分区键
func (op *WindowOperator) computePartitionKey(row map[string]interface{}, partitionBy []parser.Expression) string {
	keyParts := make([]interface{}, len(partitionBy))
	
	for i, expr := range partitionBy {
		value, err := EvaluateExpression(expr, row)
		if err != nil {
			keyParts[i] = fmt.Sprintf("ERROR:%v", err)
		} else {
			keyParts[i] = value
		}
	}
	
	return fmt.Sprintf("%v", keyParts)
}

// sortRows 根据排序表达式排序行
func (op *WindowOperator) sortRows(rows []map[string]interface{}, orderBy []parser.OrderItem) []map[string]interface{} {
	if len(orderBy) == 0 {
		return rows
	}
	
	// 克隆行,避免修改原始数据
	sorted := make([]map[string]interface{}, len(rows))
	copy(sorted, rows)
	
	// 排序
	sort.Slice(sorted, func(i, j int) bool {
		return op.compareRows(sorted[i], sorted[j], orderBy)
	})
	
	return sorted
}

// compareRows 比较两行
func (op *WindowOperator) compareRows(row1, row2 map[string]interface{}, orderBy []parser.OrderItem) bool {
	for _, orderItem := range orderBy {
		val1, err1 := EvaluateExpression(orderItem.Expr, row1)
		val2, err2 := EvaluateExpression(orderItem.Expr, row2)
		
		if err1 != nil || err2 != nil {
			continue
		}
		
		cmp := compareValues(val1, val2)
		
		if cmp != 0 {
			if orderItem.Direction == parser.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	
	return false
}

// computeWindowValue 计算窗口函数值
func (op *WindowOperator) computeWindowValue(rows []map[string]interface{}, rowIndex int, wf *parser.WindowExpression) (interface{}, error) {
	switch wf.FuncName {
	case "ROW_NUMBER":
		return op.computeRowNumber(rowIndex), nil
		
	case "RANK":
		return op.computeRank(rows, rowIndex), nil
		
	case "DENSE_RANK":
		return op.computeDenseRank(rows, rowIndex), nil
		
	case "LAG":
		return op.computeLag(rows, rowIndex, wf.Args)
		
	case "LEAD":
		return op.computeLead(rows, rowIndex, wf.Args)
		
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return op.computeAggregateWindow(rows, rowIndex, wf)
		
	default:
		return nil, fmt.Errorf("unsupported window function: %s", wf.FuncName)
	}
}

// ROW_NUMBER实现
func (op *WindowOperator) computeRowNumber(rowIndex int) int64 {
	return int64(rowIndex + 1)
}

// RANK实现
func (op *WindowOperator) computeRank(rows []map[string]interface{}, rowIndex int) int64 {
	if rowIndex == 0 {
		return 1
	}
	
	currentRow := rows[rowIndex]
	prevRow := rows[rowIndex-1]
	
	// 如果当前行与前一行相同,则排名相同
	if op.isEqual(currentRow, prevRow) {
		return op.computeRank(rows, rowIndex-1)
	}
	
	// 否则排名为当前行号
	return int64(rowIndex + 1)
}

// DENSE_RANK实现
func (op *WindowOperator) computeDenseRank(rows []map[string]interface{}, rowIndex int) int64 {
	if rowIndex == 0 {
		return 1
	}
	
	currentRow := rows[rowIndex]
	prevRow := rows[rowIndex-1]
	
	// 如果当前行与前一行相同,则密集排名相同
	if op.isEqual(currentRow, prevRow) {
		return op.computeDenseRank(rows, rowIndex-1)
	}
	
	// 否则寻找不同的行数
	rank := int64(1)
	for i := 1; i <= rowIndex; i++ {
		if !op.isEqual(rows[i], rows[i-1]) {
			rank++
		}
	}
	
	return rank
}

// LAG实现
func (op *WindowOperator) computeLag(rows []map[string]interface{}, rowIndex int, args []parser.Expression) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LAG() requires 1 argument")
	}
	
	// 获取偏移量(默认为1)
	offset := 1
	if len(args) > 0 {
		val, err := EvaluateExpression(args[0], nil)
		if err == nil {
			if offsetInt, ok := val.(int64); ok {
				offset = int(offsetInt)
			}
		}
	}
	
	// 计算目标行索引
	targetIndex := rowIndex - offset
	
	if targetIndex < 0 {
		// 超出范围,返回NULL
		return nil, nil
	}
	
	// 返回目标行的值
	return op.getRowValue(rows[targetIndex], args[0])
}

// LEAD实现
func (op *WindowOperator) computeLead(rows []map[string]interface{}, rowIndex int, args []parser.Expression) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LEAD() requires 1 argument")
	}
	
	// 获取偏移量(默认为1)
	offset := 1
	if len(args) > 0 {
		val, err := EvaluateExpression(args[0], nil)
		if err == nil {
			if offsetInt, ok := val.(int64); ok {
				offset = int(offsetInt)
			}
		}
	}
	
	// 计算目标行索引
	targetIndex := rowIndex + offset
	
	if targetIndex >= len(rows) {
		// 超出范围,返回NULL
		return nil, nil
	}
	
	// 返回目标行的值
	return op.getRowValue(rows[targetIndex], args[0])
}

// 聚合窗口函数实现
func (op *WindowOperator) computeAggregateWindow(rows []map[string]interface{}, rowIndex int, wf *parser.WindowExpression) (interface{}, error) {
	// 确定窗口范围
	start, end := op.getWindowBounds(rows, rowIndex, wf.Spec.Frame)
	
	// 计算聚合值
	switch wf.FuncName {
	case "COUNT":
		return op.computeCount(rows, start, end), nil
	case "SUM":
		return op.computeSum(rows, start, end, wf.Args[0])
	case "AVG":
		return op.computeAvg(rows, start, end, wf.Args[0])
	case "MIN":
		return op.computeMin(rows, start, end, wf.Args[0])
	case "MAX":
		return op.computeMax(rows, start, end, wf.Args[0])
	default:
		return nil, fmt.Errorf("unsupported aggregate window function: %s", wf.FuncName)
	}
}

// getWindowBounds 获取窗口边界
func (op *WindowOperator) getWindowBounds(rows []map[string]interface{}, rowIndex int, frame *parser.WindowFrame) (int, int) {
	if frame == nil {
		// 无帧定义,默认从第一行到当前行
		return 0, rowIndex + 1
	}
	
	start := rowIndex
	end := rowIndex + 1
	
	// 处理起始边界
	switch frame.Start.Type {
	case parser.BoundUnboundedPreceding:
		start = 0
	case parser.BoundPreceding:
		if offsetVal, ok := op.getFrameOffset(rows[rowIndex], frame.Start.Value); ok {
			start = rowIndex - offsetVal
			if start < 0 {
				start = 0
			}
		}
	case parser.BoundCurrentRow:
		start = rowIndex
	case parser.BoundFollowing:
		if offsetVal, ok := op.getFrameOffset(rows[rowIndex], frame.Start.Value); ok {
			start = rowIndex + offsetVal
			if start >= len(rows) {
				start = len(rows) - 1
			}
		}
	}
	
	// 处理结束边界
	if frame.End != nil {
		switch frame.End.Type {
		case parser.BoundCurrentRow:
			end = rowIndex + 1
		case parser.BoundFollowing:
			if offsetVal, ok := op.getFrameOffset(rows[rowIndex], frame.End.Value); ok {
				end = rowIndex + offsetVal + 1
				if end > len(rows) {
					end = len(rows)
				}
			}
		case parser.BoundUnboundedFollowing:
			end = len(rows)
		}
	}
	
	return start, end
}

// getFrameOffset 获取帧偏移
func (op *WindowOperator) getFrameOffset(row map[string]interface{}, expr parser.Expression) (int, bool) {
	val, err := EvaluateExpression(expr, row)
	if err != nil {
		return 0, false
	}
	
	if offsetInt, ok := val.(int64); ok {
		return int(offsetInt), true
	}
	
	return 0, false
}

// 辅助函数

// isEqual 比较两行是否相等(根据ORDER BY列)
func (op *WindowOperator) isEqual(row1, row2 map[string]interface{}) bool {
	// 简化实现:比较所有列
	// 实际应该只比较ORDER BY的列
	for k, v1 := range row1 {
		v2, ok := row2[k]
		if !ok {
			return false
		}
		if !compareValuesEqual(v1, v2) {
			return false
		}
	}
	return true
}

// getRowValue 获取行的指定列值
func (op *WindowOperator) getRowValue(row map[string]interface{}, expr parser.Expression) (interface{}, error) {
	return EvaluateExpression(expr, row)
}

// 聚合函数

// computeCount 计数
func (op *WindowOperator) computeCount(rows []map[string]interface{}, start, end int) int64 {
	count := int64(0)
	for i := start; i < end; i++ {
		if rows[i] != nil {
			count++
		}
	}
	return count
}

// computeSum 求和
func (op *WindowOperator) computeSum(rows []map[string]interface{}, start, end int, expr parser.Expression) interface{} {
	var sum float64
	for i := start; i < end; i++ {
		val, err := EvaluateExpression(expr, rows[i])
		if err != nil {
			continue
		}
		if num, ok := toFloat64(val); ok {
			sum += num
		}
	}
	return sum
}

// computeAvg 平均值
func (op *WindowOperator) computeAvg(rows []map[string]interface{}, start, end int, expr parser.Expression) interface{} {
	sum, count := 0.0, 0
	
	for i := start; i < end; i++ {
		val, err := EvaluateExpression(expr, rows[i])
		if err != nil {
			continue
		}
		if num, ok := toFloat64(val); ok {
			sum += num
			count++
		}
	}
	
	if count == 0 {
		return nil
	}
	
	return sum / float64(count)
}

// computeMin 最小值
func (op *WindowOperator) computeMin(rows []map[string]interface{}, start, end int, expr parser.Expression) interface{} {
	minVal := interface{}(nil)
	
	for i := start; i < end; i++ {
		val, err := EvaluateExpression(expr, rows[i])
		if err != nil {
			continue
		}
		if minVal == nil || compareValues(val, minVal) < 0 {
			minVal = val
		}
	}
	
	return minVal
}

// computeMax 最大值
func (op *WindowOperator) computeMax(rows []map[string]interface{}, start, end int, expr parser.Expression) interface{} {
	maxVal := interface{}(nil)
	
	for i := start; i < end; i++ {
		val, err := EvaluateExpression(expr, rows[i])
		if err != nil {
			continue
		}
		if maxVal == nil || compareValues(val, maxVal) > 0 {
			maxVal = val
		}
	}
	
	return maxVal
}

// 值比较函数

func compareValues(v1, v2 interface{}) int {
	n1, ok1 := toFloat64(v1)
	n2, ok2 := toFloat64(v2)
	
	if ok1 && ok2 {
		if n1 < n2 {
			return -1
		} else if n1 > n2 {
			return 1
		}
		return 0
	}
	
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	
	if s1 < s2 {
		return -1
	} else if s1 > s2 {
		return 1
	}
	return 0
}

func compareValuesEqual(v1, v2 interface{}) bool {
	return compareValues(v1, v2) == 0
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}
