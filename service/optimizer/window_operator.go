package optimizer

import (
	"context"
	"fmt"
	"github.com/kasuganosora/sqlexec/service/parser"
	"github.com/kasuganosora/sqlexec/service/resource"
	"sort"
)

// WindowOperator 窗口函数执行算子
type WindowOperator struct {
	// 子算�?
	child PhysicalPlan
	
	// 窗口函数定义
	windowFuncs []*WindowFunctionDef
	
	// 执行上下�?
	ctx context.Context
	
	// 表达式求值器
	evaluator *ExpressionEvaluator
}

// WindowFunctionDef 窗口函数定义
type WindowFunctionDef struct {
	Expr     *parser.WindowExpression
	ResultCol string // 结果列名
}

// NewWindowOperator 创建窗口函数算子
func NewWindowOperator(child PhysicalPlan, windowFuncs []*parser.WindowExpression) *WindowOperator {
	funcDefs := make([]*WindowFunctionDef, len(windowFuncs))
	for i, wf := range windowFuncs {
		funcDefs[i] = &WindowFunctionDef{
			Expr:     wf,
			ResultCol: fmt.Sprintf("window_%d", i),
		}
	}
	
	return &WindowOperator{
		child:        child,
		windowFuncs:  funcDefs,
		evaluator:    NewExpressionEvaluatorWithoutAPI(),
	}
}

// Execute 执行窗口函数
func (op *WindowOperator) Execute(ctx context.Context) (*resource.QueryResult, error) {
	op.ctx = ctx
	
	// 1. 从子算子获取数据
	result, err := op.child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute child: %w", err)
	}
	
	if len(result.Rows) == 0 {
		return result, nil
	}
	
	// 2. 处理每个窗口函数
	for _, wfDef := range op.windowFuncs {
		result.Rows, err = op.executeWindowFunction(result.Rows, wfDef)
		if err != nil {
			return nil, fmt.Errorf("failed to execute window function %s: %w", wfDef.Expr.FuncName, err)
		}
	}
	
	return result, nil
}

// executeWindowFunction 执行单个窗口函数
func (op *WindowOperator) executeWindowFunction(rows []resource.Row, wfDef *WindowFunctionDef) ([]resource.Row, error) {
	wf := wfDef.Expr
	
	// 3. 分区
	partitions := op.partitionRows(rows, wf.Spec.PartitionBy)
	
	// 4. 在每个分区内执行窗口函数
	result := make([]resource.Row, 0, len(rows))
	for _, partition := range partitions {
		// 5. 排序
		sortedPartition := op.sortRows(partition, wf.Spec.OrderBy)
		
		// 6. 计算窗口函数�?
		for i, row := range sortedPartition {
			// 克隆�?
			newRow := make(resource.Row)
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
func (op *WindowOperator) partitionRows(rows []resource.Row, partitionBy []parser.Expression) [][]resource.Row {
	if len(partitionBy) == 0 {
		// 无分�?所有行为一个分�?
		return [][]resource.Row{rows}
	}
	
	// 使用map分组
	partitions := make(map[string][]resource.Row)
	
	for _, row := range rows {
		// 计算分区�?
		key := op.computePartitionKey(row, partitionBy)
		
		// 添加到对应分�?
		partitions[key] = append(partitions[key], row)
	}
	
	// 转换为slice
	result := make([][]resource.Row, 0, len(partitions))
	for _, partition := range partitions {
		result = append(result, partition)
	}
	
	return result
}

// computePartitionKey 计算分区�?
func (op *WindowOperator) computePartitionKey(row resource.Row, partitionBy []parser.Expression) string {
	keyParts := make([]interface{}, len(partitionBy))
	
	for i, expr := range partitionBy {
		value, err := op.evaluator.Evaluate(&expr, parser.Row(row))
		if err != nil {
			keyParts[i] = fmt.Sprintf("ERROR:%v", err)
		} else {
			keyParts[i] = value
		}
	}
	
	return fmt.Sprintf("%v", keyParts)
}

// sortRows 根据排序表达式排序行
func (op *WindowOperator) sortRows(rows []resource.Row, orderBy []parser.OrderItem) []resource.Row {
	if len(orderBy) == 0 {
		return rows
	}
	
	// 克隆�?避免修改原始数据
	sorted := make([]resource.Row, len(rows))
	copy(sorted, rows)
	
	// 排序
	sort.Slice(sorted, func(i, j int) bool {
		return op.compareRows(sorted[i], sorted[j], orderBy)
	})
	
	return sorted
}

// compareRows 比较两行
func (op *WindowOperator) compareRows(row1, row2 resource.Row, orderBy []parser.OrderItem) bool {
	for _, orderItem := range orderBy {
		val1, err1 := op.evaluator.Evaluate(&orderItem.Expr, parser.Row(row1))
		val2, err2 := op.evaluator.Evaluate(&orderItem.Expr, parser.Row(row2))
		
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

// computeWindowValue 计算窗口函数�?
func (op *WindowOperator) computeWindowValue(rows []resource.Row, rowIndex int, wf *parser.WindowExpression) (interface{}, error) {
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
func (op *WindowOperator) computeRank(rows []resource.Row, rowIndex int) int64 {
	if rowIndex == 0 {
		return 1
	}
	
	currentRow := rows[rowIndex]
	prevRow := rows[rowIndex-1]
	
	// 如果当前行与前一行相�?则排名相�?
	if op.isEqual(currentRow, prevRow) {
		return op.computeRank(rows, rowIndex-1)
	}
	
	// 否则排名为当前行�?
	return int64(rowIndex + 1)
}

// DENSE_RANK实现
func (op *WindowOperator) computeDenseRank(rows []resource.Row, rowIndex int) int64 {
	if rowIndex == 0 {
		return 1
	}
	
	currentRow := rows[rowIndex]
	prevRow := rows[rowIndex-1]
	
	// 如果当前行与前一行相�?则密集排名相�?
	if op.isEqual(currentRow, prevRow) {
		return op.computeDenseRank(rows, rowIndex-1)
	}
	
	// 否则寻找不同的行�?
	rank := int64(1)
	for i := 1; i <= rowIndex; i++ {
		if !op.isEqual(rows[i], rows[i-1]) {
			rank++
		}
	}
	
	return rank
}

// LAG实现
func (op *WindowOperator) computeLag(rows []resource.Row, rowIndex int, args []parser.Expression) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LAG() requires 1 argument")
	}
	
	// 获取偏移�?默认�?)
	offset := 1
	if len(args) > 0 {
		val, err := op.evaluator.Evaluate(&args[0], nil)
		if err == nil {
			if offsetInt, ok := val.(int64); ok {
				offset = int(offsetInt)
			}
		}
	}
	
	// 计算目标行索�?
	targetIndex := rowIndex - offset
	
	if targetIndex < 0 {
		// 超出范围,返回NULL
		return nil, nil
	}
	
	// 返回目标行的�?
	return op.getRowValue(rows[targetIndex], args[0])
}

// LEAD实现
func (op *WindowOperator) computeLead(rows []resource.Row, rowIndex int, args []parser.Expression) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LEAD() requires 1 argument")
	}
	
	// 获取偏移�?默认�?)
	offset := 1
	if len(args) > 0 {
		val, err := op.evaluator.Evaluate(&args[0], nil)
		if err == nil {
			if offsetInt, ok := val.(int64); ok {
				offset = int(offsetInt)
			}
		}
	}
	
	// 计算目标行索�?
	targetIndex := rowIndex + offset
	
	if targetIndex >= len(rows) {
		// 超出范围,返回NULL
		return nil, nil
	}
	
	// 返回目标行的�?
	return op.getRowValue(rows[targetIndex], args[0])
}

// 聚合窗口函数实现
func (op *WindowOperator) computeAggregateWindow(rows []resource.Row, rowIndex int, wf *parser.WindowExpression) (interface{}, error) {
	// 确定窗口范围
	start, end := op.getWindowBounds(rows, rowIndex, wf.Spec.Frame)
	
	// 计算聚合�?
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
func (op *WindowOperator) getWindowBounds(rows []resource.Row, rowIndex int, frame *parser.WindowFrame) (int, int) {
	if frame == nil {
		// 无帧定义,默认从第一行到当前�?
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

// getFrameOffset 获取帧偏�?
func (op *WindowOperator) getFrameOffset(row resource.Row, expr parser.Expression) (int, bool) {
	val, err := op.evaluator.Evaluate(&expr, parser.Row(row))
	if err != nil {
		return 0, false
	}
	
	if offsetInt, ok := val.(int64); ok {
		return int(offsetInt), true
	}
	
	return 0, false
}

// 辅助函数

// isEqual 比较两行是否相等(根据ORDER BY�?
func (op *WindowOperator) isEqual(row1, row2 resource.Row) bool {
	// 简化实�?比较所有列
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
	return len(row1) == len(row2)
}

// getRowValue 获取行的指定列�?
func (op *WindowOperator) getRowValue(row resource.Row, expr parser.Expression) (interface{}, error) {
	return op.evaluator.Evaluate(&expr, parser.Row(row))
}

// 聚合函数

// computeCount 计数
func (op *WindowOperator) computeCount(rows []resource.Row, start, end int) int64 {
	count := int64(0)
	for i := start; i < end; i++ {
		if rows[i] != nil {
			count++
		}
	}
	return count
}

// computeSum 求和
func (op *WindowOperator) computeSum(rows []resource.Row, start, end int, expr parser.Expression) (interface{}, error) {
	var sum float64
	for i := start; i < end; i++ {
		val, err := op.evaluator.Evaluate(&expr, parser.Row(rows[i]))
		if err != nil {
			continue
		}
		fval, ok := toFloat64(val)
		if ok {
			sum += fval
		}
	}
	return sum, nil
}

// computeAvg 平均�?
func (op *WindowOperator) computeAvg(rows []resource.Row, start, end int, expr parser.Expression) (interface{}, error) {
	sum, count := 0.0, 0
	
	for i := start; i < end; i++ {
		val, err := op.evaluator.Evaluate(&expr, parser.Row(rows[i]))
		if err != nil {
			continue
		}
		fval, ok := toFloat64(val)
		if ok {
			sum += fval
			count++
		}
	}
	
	if count == 0 {
		return nil, nil
	}
	
	return sum / float64(count), nil
}

// computeMin 最小�?
func (op *WindowOperator) computeMin(rows []resource.Row, start, end int, expr parser.Expression) (interface{}, error) {
	minVal := interface{}(nil)
	
	for i := start; i < end; i++ {
		val, err := op.evaluator.Evaluate(&expr, parser.Row(rows[i]))
		if err != nil {
			continue
		}
		if minVal == nil || compareValues(val, minVal) < 0 {
			minVal = val
		}
	}
	
	return minVal, nil
}

// computeMax 最大�?
func (op *WindowOperator) computeMax(rows []resource.Row, start, end int, expr parser.Expression) (interface{}, error) {
	maxVal := interface{}(nil)
	
	for i := start; i < end; i++ {
		val, err := op.evaluator.Evaluate(&expr, parser.Row(rows[i]))
		if err != nil {
			continue
		}
		if maxVal == nil || compareValues(val, maxVal) > 0 {
			maxVal = val
		}
	}
	
	return maxVal, nil
}
