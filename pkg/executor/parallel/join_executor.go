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
	hashTable      map[uint64][]domain.Row
	joinCols       JoinColumns
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
		joinType:       joinType,
		left:           left,
		right:          right,
		joinCondition:  condition,
		buildParallel:  buildParallel,
		probeParallel:  probeParallel,
		workerPool:     workerPool,
		hashTableReady: make(chan struct{}),
	}
}

// Execute 执行并行哈希连接
func (phje *ParallelHashJoinExecutor) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 并行构建和探测哈希表
	// Use buffered channels large enough to avoid goroutine leaks
	resultChan := make(chan *domain.QueryResult, 2)
	errChan := make(chan error, 2)

	// 并行构建哈希表
	go phje.buildHashTable(ctx, resultChan, errChan)
	// 并行探测哈希表
	go phje.probeHashTable(ctx, resultChan, errChan)

	// 等待结果 - must collect exactly 2 signals (one from build, one from probe)
	var results [2]*domain.QueryResult
	var firstErr error
	for i := 0; i < 2; i++ {
		select {
		case result := <-resultChan:
			if result != nil {
				results[i] = result
			}
		case err := <-errChan:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = ctx.Err()
			}
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	// 合并结果
	merged := phje.mergeJoinResults(results)

	return merged, nil
}

// buildHashTable 构建哈希表（可并行）
func (phje *ParallelHashJoinExecutor) buildHashTable(ctx context.Context, resultChan chan<- *domain.QueryResult, errChan chan<- error) {
	// 确定连接列
	phje.joinCols = phje.extractJoinColumns()
	if len(phje.joinCols.Left) == 0 || len(phje.joinCols.Right) == 0 {
		errChan <- fmt.Errorf("no join columns specified")
		return
	}

	// 构建哈希表
	phje.hashTable = make(map[uint64][]domain.Row)

	// 使用parallelism
	parallelism := phje.buildParallel
	if parallelism <= 0 {
		parallelism = 1
	}

	rowCount := len(phje.left.Rows)
	rowsPerWorker := rowCount / parallelism

	// 并行构建
	var wg sync.WaitGroup
	var mu sync.Mutex
	workerErrs := make(chan error, parallelism)

	for i := 0; i < parallelism; i++ {
		wg.Add(1)

		start := i * rowsPerWorker
		end := start + rowsPerWorker
		if end > rowCount {
			end = rowCount
		}

		go func(workerIdx int, start, end int) {
			defer wg.Done()

			for rowIdx := start; rowIdx < end; rowIdx++ {
				row := phje.left.Rows[rowIdx]

				// 计算哈希键
				key := phje.computeHashKey(row, phje.joinCols.Left)

				mu.Lock()
				phje.hashTable[key] = append(phje.hashTable[key], row)
				mu.Unlock()
			}
		}(i, start, end)
	}

	// 等待构建完成
	go func() {
		wg.Wait()
		close(workerErrs)

		// 检查错误
		for err := range workerErrs {
			if err != nil {
				errChan <- err
				return
			}
		}

		// 构建完成，通知探测阶段可以开始
		select {
		case <-phje.hashTableReady:
			// already closed (should not happen in normal flow)
		default:
			close(phje.hashTableReady)
		}

		// 发送构建完成信号到resultChan
		resultChan <- nil
	}()
}

// probeHashTable 探测哈希表（可并行）
func (phje *ParallelHashJoinExecutor) probeHashTable(ctx context.Context, resultChan chan<- *domain.QueryResult, errChan chan<- error) {
	// 等待哈希表构建完成
	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		return
	case <-phje.hashTableReady:
		// 哈希表已构建完成，开始探测
	}

	// 探测阶段
	parallelism := phje.probeParallel
	if parallelism <= 0 {
		parallelism = 1
	}

	// 并行探测
	rowCount := len(phje.right.Rows)
	rowsPerWorker := rowCount / parallelism
	results := make([]domain.Row, 0, rowCount)
	mu := sync.Mutex{}

	var wg sync.WaitGroup
	workerErrs := make(chan error, parallelism)

	for i := 0; i < parallelism; i++ {
		wg.Add(1)

		start := i * rowsPerWorker
		end := start + rowsPerWorker
		if end > rowCount {
			end = rowCount
		}

		go func(workerIdx int, start, end int) {
			defer wg.Done()

			for rowIdx := start; rowIdx < end; rowIdx++ {
				row := phje.right.Rows[rowIdx]

				// 计算哈希键
				key := phje.computeHashKey(row, phje.joinCols.Right)

				// 查找匹配的行
				if matchedRows, exists := phje.hashTable[key]; exists {
					for _, leftRow := range matchedRows {
						mu.Lock()
						merged := phje.mergeRows(leftRow, row, phje.left.Columns, phje.right.Columns)
						results = append(results, merged)
						mu.Unlock()
					}
				}
			}
		}(i, start, end)
	}

	// 等待探测完成
	go func() {
		wg.Wait()
		close(workerErrs)

		// 检查错误
		for err := range workerErrs {
			if err != nil {
				errChan <- err
				return
			}
		}

		// 探测完成
		mu.Lock()
		finalResults := make([]domain.Row, len(results))
		copy(finalResults, results)
		mu.Unlock()

		resultChan <- &domain.QueryResult{
			Rows:    finalResults,
			Columns: phje.mergeColumns(phje.left.Columns, phje.right.Columns),
			Total:   int64(len(finalResults)),
		}
	}()
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

	// For comparison expressions (e.g. left.id = right.id),
	// the left child is the left join column and the right child is the right join column
	if expr.Operator == "=" || expr.Operator == "==" {
		if expr.Left != nil && expr.Left.Type == parser.ExprTypeColumn {
			colName := expr.Left.Column
			if colName == "" {
				if s, ok := expr.Left.Value.(string); ok {
					colName = s
				}
			}
			if colName != "" {
				*leftCols = append(*leftCols, colName)
			}
		}
		if expr.Right != nil && expr.Right.Type == parser.ExprTypeColumn {
			colName := expr.Right.Column
			if colName == "" {
				if s, ok := expr.Right.Value.(string); ok {
					colName = s
				}
			}
			if colName != "" {
				*rightCols = append(*rightCols, colName)
			}
		}
		return
	}

	// Recurse for AND/OR compound expressions
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
