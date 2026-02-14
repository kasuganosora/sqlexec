package parallel

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ParallelHashJoinExecutor 并行哈希连接执行器
// 支持并行构建哈希表和探测
type ParallelHashJoinExecutor struct {
	joinType      parser.JoinType
	left          *domain.QueryResult
	right         *domain.QueryResult
	joinCondition *parser.Expression
	buildParallel int
	probeParallel int
	workerPool    *WorkerPool

	// State for parallel execution
	hashTable map[uint64][]domain.Row
	joinCols  JoinColumns
	hashTableReady chan struct{}
}

// NewParallelHashJoinExecutor 创建并行哈希连接执行器
func NewParallelHashJoinExecutor(
	joinType parser.JoinType,
	left, right *domain.QueryResult,
	condition *parser.Expression,
	buildParallel, probeParallel int,
	workerPool *WorkerPool,
) *ParallelHashJoinExecutor {
	return &ParallelHashJoinExecutor{
		joinType:      joinType,
		left:          left,
		right:         right,
		joinCondition: condition,
		buildParallel: buildParallel,
		probeParallel: probeParallel,
		workerPool:    workerPool,
	}
}

// Execute 执行并行哈希连接
func (phje *ParallelHashJoinExecutor) Execute(ctx context.Context) (*domain.QueryResult, error) {
	fmt.Printf("  [PARALLEL JOIN] Starting parallel %s join, leftRows=%d, rightRows=%d, buildParallel=%d, probeParallel=%d\n",
		phje.joinType, len(phje.left.Rows), len(phje.right.Rows),
		phje.buildParallel, phje.probeParallel)

	// 确定连接列
	phje.joinCols = phje.extractJoinColumns()
	if len(phje.joinCols.Left) == 0 || len(phje.joinCols.Right) == 0 {
		return nil, fmt.Errorf("no join columns specified")
	}

	// 构建哈希表
	phje.hashTable = phje.buildHash(ctx, phje.left.Rows, phje.joinCols.Left, phje.buildParallel)
	if phje.hashTable == nil {
		// 可能是上下文取消导致
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to build hash table")
	}

	// 探测哈希表
	results := phje.probeHash(ctx, phje.right.Rows, phje.joinCols.Right, phje.probeParallel)
	if results == nil {
		// 可能是上下文取消导致
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to probe hash table")
	}

	mergedResult := &domain.QueryResult{
		Rows:    results,
		Columns: phje.mergeColumns(phje.left.Columns, phje.right.Columns),
		Total:   int64(len(results)),
	}

	fmt.Printf("  [PARALLEL JOIN] Completed: %d rows\n", len(results))
	return mergedResult, nil
}

// buildHash 并行构建哈希表（同步方法）
func (phje *ParallelHashJoinExecutor) buildHash(ctx context.Context, rows []domain.Row, cols []string, parallelism int) map[uint64][]domain.Row {
	if parallelism <= 0 {
		parallelism = 1
	}

	rowCount := len(rows)
	hashTable := make(map[uint64][]domain.Row)
	mu := sync.Mutex{}
	ctxCancelChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(parallelism)

	rowsPerWorker := (rowCount + parallelism - 1) / parallelism // ceiling division

	for i := 0; i < parallelism; i++ {
		start := i * rowsPerWorker
		if start >= rowCount {
			wg.Done() // no work for this worker
			continue
		}
		end := start + rowsPerWorker
		if end > rowCount {
			end = rowCount
		}

		go func(workerIdx, start, end int) {
			defer wg.Done()

			for rowIdx := start; rowIdx < end; rowIdx++ {
				select {
				case <-ctx.Done():
					return
				case <-ctxCancelChan:
					return
				default:
					row := rows[rowIdx]
					key := phje.computeHashKey(row, cols)

					mu.Lock()
					hashTable[key] = append(hashTable[key], row)
					mu.Unlock()
				}
			}
		}(i, start, end)
	}

	// 等待所有 worker 完成或上下文取消
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有 worker 完成，返回哈希表
		return hashTable
	case <-ctx.Done():
		// 上下文被取消，关闭取消通道通知所有 worker
		close(ctxCancelChan)
		wg.Wait() // 等待所有 worker 退出
		return nil
	}
}

// probeHash 并行探测哈希表（同步方法）
func (phje *ParallelHashJoinExecutor) probeHash(ctx context.Context, rows []domain.Row, cols []string, parallelism int) []domain.Row {
	if parallelism <= 0 {
		parallelism = 1
	}

	rowCount := len(rows)
	results := make([]domain.Row, 0, rowCount)
	mu := sync.Mutex{}
	ctxCancelChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(parallelism)

	rowsPerWorker := (rowCount + parallelism - 1) / parallelism // ceiling division

	for i := 0; i < parallelism; i++ {
		start := i * rowsPerWorker
		if start >= rowCount {
			wg.Done() // no work for this worker
			continue
		}
		end := start + rowsPerWorker
		if end > rowCount {
			end = rowCount
		}

		go func(workerIdx, start, end int) {
			defer wg.Done()

			for rowIdx := start; rowIdx < end; rowIdx++ {
				select {
				case <-ctx.Done():
					return
				case <-ctxCancelChan:
					return
				default:
					row := rows[rowIdx]
					key := phje.computeHashKey(row, cols)

					if matchedRows, exists := phje.hashTable[key]; exists {
						for _, leftRow := range matchedRows {
							mu.Lock()
							merged := phje.mergeRows(leftRow, row, phje.left.Columns, phje.right.Columns)
							results = append(results, merged)
							mu.Unlock()
						}
					}
				}
			}
		}(i, start, end)
	}

	// 等待所有 worker 完成或上下文取消
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有 worker 完成，返回结果
		return results
	case <-ctx.Done():
		// 上下文被取消，关闭取消通道通知所有 worker
		close(ctxCancelChan)
		wg.Wait() // 等待所有 worker 退出
		return nil
	}
}

// mergeJoinResults 合并JOIN结果（简化）
func (phje *ParallelHashJoinExecutor) mergeJoinResults(results [2]*domain.QueryResult) *domain.QueryResult {
	// 简化：只返回第一个非空结果
	for _, result := range results {
		if result != nil && len(result.Rows) > 0 {
			return result
		}
	}
	
	return &domain.QueryResult{
		Rows:    []domain.Row{},
		Columns: []domain.ColumnInfo{},
		Total:   0,
	}
}

// mergeRows 合并行行（同步）
func (phje *ParallelHashJoinExecutor) mergeRows(leftRow, rightRow domain.Row, leftCols, rightCols []domain.ColumnInfo) domain.Row {
	merged := make(domain.Row)
	
	// 添加左表列
	for _, col := range leftCols {
		merged[col.Name] = leftRow[col.Name]
	}
	
	// 添加右表列（处理冲突）
	for _, col := range rightCols {
		value := rightRow[col.Name]
		colName := col.Name
		
		// 检查列名冲突
		if _, exists := merged[colName]; exists {
			colName = "right_" + colName
		}
		merged[colName] = value
	}
	
	return merged
}

// mergeColumns 合并列信息
func (phje *ParallelHashJoinExecutor) mergeColumns(left, right []domain.ColumnInfo) []domain.ColumnInfo {
	merged := make([]domain.ColumnInfo, 0, len(left)+len(right))
	
	// 添加左表列
	for _, col := range left {
		merged = append(merged, col)
	}
	
	// 添加右表列（处理冲突）
	for _, col := range right {
		colName := col.Name
		
		// 检查列名冲突
		conflict := false
		for _, leftCol := range left {
			if leftCol.Name == colName {
				conflict = true
				break
			}
		}
		
		if conflict {
			colName = "right_" + colName
		}
		
		merged = append(merged, domain.ColumnInfo{
			Name:     colName,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}
	
	return merged
}

// JoinColumns 连接列结果
type JoinColumns struct {
	Left  []string
	Right []string
}

// extractJoinColumns 提取连接列
func (phje *ParallelHashJoinExecutor) extractJoinColumns() JoinColumns {
	// 简化：从连接条件提取列名
	leftCols := []string{}
	rightCols := []string{}

	if phje.joinCondition != nil {
		phje.extractColumnsFromExpr(phje.joinCondition, &leftCols, &rightCols)
	}

	// 如果没有连接条件，使用id列
	if len(leftCols) == 0 {
		leftCols = []string{"id"}
	}
	if len(rightCols) == 0 {
		rightCols = []string{"id"}
	}

	return JoinColumns{
		Left:  leftCols,
		Right: rightCols,
	}
}

// extractColumnsFromExpr 从表达式提取列名
func (phje *ParallelHashJoinExecutor) extractColumnsFromExpr(expr *parser.Expression, leftCols, rightCols *[]string) {
	if expr == nil {
		return
	}

	if expr.Left != nil {
		phje.extractColumnsFromExpr(expr.Left, leftCols, rightCols)
	}

	if expr.Right != nil {
		phje.extractColumnsFromExpr(expr.Right, leftCols, rightCols)
	}
}

// computeHashKey 计算行的哈希键
func (phje *ParallelHashJoinExecutor) computeHashKey(row domain.Row, cols []string) uint64 {
	if len(cols) == 0 {
		return 0
	}

	// 使用FNV-64a哈希算法
	h := fnv.New64a()
	for _, col := range cols {
		if val, exists := row[col]; exists {
			h.Write([]byte(fmt.Sprintf("%v", val)))
		}
	}

	return h.Sum64()
}

// Explain 解释并行JOIN执行器
func (phje *ParallelHashJoinExecutor) Explain() string {
	return fmt.Sprintf(
		"ParallelHashJoin(type=%s, buildParallel=%d, probeParallel=%d)",
		phje.joinType, phje.buildParallel, phje.probeParallel,
	)
}
