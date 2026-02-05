package optimizer

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// OptimizedParallelScanner 优化的并行扫描器
// 基于 TiDB 多线程并发模型，使用 Worker Pool 和批量处理
type OptimizedParallelScanner struct {
	dataSource  domain.DataSource
	parallelism int
	batchSize   int // 批量大小
}

// NewOptimizedParallelScanner 创建优化的并行扫描器
func NewOptimizedParallelScanner(dataSource domain.DataSource, parallelism int) *OptimizedParallelScanner {
	// 默认使用 CPU 核心数
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	// 限制最大并行度
	if parallelism > 64 {
		parallelism = 64
	}

	// 根据数据大小设置批量大小
	batchSize := 1000 // 默认批量大小
	if parallelism > 8 {
		batchSize = 500 // 高并行度时减小批量大小
	}

	return &OptimizedParallelScanner{
		dataSource:  dataSource,
		parallelism: parallelism,
		batchSize:   batchSize,
	}
}

// ScanRange 扫描范围
type ScanRange struct {
	TableName string
	Offset    int64
	Limit     int64
}

// Execute 优化的并行执行扫描
func (ops *OptimizedParallelScanner) Execute(ctx context.Context, scanRange ScanRange, options *domain.QueryOptions) (*domain.QueryResult, error) {
	limit := scanRange.Limit
	if limit <= 0 {
		limit = 10000 // 默认
	}

	offset := scanRange.Offset

	// Only print debug info if not in benchmark mode
	if os.Getenv("PARALLEL_SCAN_DEBUG") == "1" {
		fmt.Printf("  [OPTIMIZED PARALLEL SCAN] Table: %s, Offset: %d, Limit: %d, Parallelism: %d, BatchSize: %d\n",
			scanRange.TableName, offset, limit, ops.parallelism, ops.batchSize)
	}

	// 小数据集直接串行扫描
	if limit < int64(ops.batchSize) {
		// 如果有 offset 或 limit，需要应用到 options
		scanOptions := &domain.QueryOptions{}
		if options != nil {
			scanOptions.Limit = options.Limit
			scanOptions.Offset = options.Offset
		}
		// 使用 scanRange 的 offset 和 limit
		scanOptions.Offset = int(offset)
		scanOptions.Limit = int(limit)
		return ops.dataSource.Query(ctx, scanRange.TableName, scanOptions)
	}

	// 计算每个 worker 的任务
	tasks := ops.divideIntoBatches(scanRange.TableName, offset, limit, ops.parallelism)

	if len(tasks) == 0 {
		// 如果没有任务，返回空结果
		return &domain.QueryResult{
			Rows:  []domain.Row{},
			Total: 0,
		}, nil
	}

	// 使用 worker pool 并行执行
	results := ops.executeWithWorkerPool(ctx, tasks, options)

	// 合并结果
	mergedResult := ops.mergeResults(results, scanRange.TableName)

	// Only print debug info if not in benchmark mode
	if os.Getenv("PARALLEL_SCAN_DEBUG") == "1" {
		fmt.Printf("  [OPTIMIZED PARALLEL SCAN] Completed: %d rows from %d workers\n", len(mergedResult.Rows), len(tasks))
	}

	return mergedResult, nil
}

// divideIntoBatches 将扫描范围划分为批次
func (ops *OptimizedParallelScanner) divideIntoBatches(tableName string, offset, limit int64, parallelism int) []ScanRange {
	// 计算每个 worker 的行数
	rowsPerWorker := int64(float64(limit) / float64(parallelism))

	// 确保每批至少有一行
	if rowsPerWorker < 1 {
		rowsPerWorker = 1
	}

	tasks := make([]ScanRange, 0, parallelism)

	for i := 0; i < parallelism; i++ {
		taskOffset := offset + int64(i)*rowsPerWorker
		taskLimit := rowsPerWorker

		// 最后一个 worker 处理剩余的行
		if i == parallelism-1 {
			taskLimit = limit - int64(i)*rowsPerWorker
		}

		if taskLimit > 0 {
			tasks = append(tasks, ScanRange{
				TableName: tableName,
				Offset:    taskOffset,
				Limit:     taskLimit,
			})
		}
	}

	return tasks
}

// executeWithWorkerPool 使用 worker pool 执行任务
func (ops *OptimizedParallelScanner) executeWithWorkerPool(ctx context.Context, tasks []ScanRange, options *domain.QueryOptions) []*ScanResult {
	results := make([]*ScanResult, len(tasks))
	var wg sync.WaitGroup
	var workerPool = make(chan struct{}, ops.parallelism) // 限制并发度

	for i, task := range tasks {
		wg.Add(1)

		go func(idx int, t ScanRange) {
			defer wg.Done()

			// 获取 worker 信号量
			workerPool <- struct{}{}
			defer func() { <-workerPool }()

			// 执行扫描
			result, err := ops.executeSingleRange(ctx, t, options)
			if err != nil {
				if os.Getenv("PARALLEL_SCAN_DEBUG") == "1" {
					fmt.Printf("  [WARN] Worker %d failed: %v\n", idx, err)
				}
				results[idx] = &ScanResult{Error: err}
				return
			}

			results[idx] = &ScanResult{Result: result}
		}(i, task)
	}

	wg.Wait()
	return results
}

// executeSingleRange 执行单个扫描范围
func (ops *OptimizedParallelScanner) executeSingleRange(ctx context.Context, t ScanRange, options *domain.QueryOptions) (*domain.QueryResult, error) {
	scanOptions := &domain.QueryOptions{}
	// 直接使用分区的 offset 和 limit
	scanOptions.Offset = int(t.Offset)
	scanOptions.Limit = int(t.Limit)

	return ops.dataSource.Query(ctx, t.TableName, scanOptions)
}

// mergeResults 合并扫描结果
func (ops *OptimizedParallelScanner) mergeResults(results []*ScanResult, tableName string) *domain.QueryResult {
	if len(results) == 0 {
		return &domain.QueryResult{
			Rows:  []domain.Row{},
			Total: 0,
		}
	}

	// 预分配总容量
	totalRows := int64(0)
	for _, result := range results {
		if result != nil && result.Result != nil {
			totalRows += result.Result.Total
		}
	}

	mergedRows := make([]domain.Row, 0, totalRows)
	var columns []domain.ColumnInfo

	for i, result := range results {
		if result == nil || result.Result == nil {
			continue
		}

		if result.Error != nil {
			if os.Getenv("PARALLEL_SCAN_DEBUG") == "1" {
				fmt.Printf("  [WARN] Skipping result %d due to error: %v\n", i, result.Error)
			}
			continue
		}

		// 从第一个有效结果获取列信息
		if len(columns) == 0 {
			columns = result.Result.Columns
		}

		mergedRows = append(mergedRows, result.Result.Rows...)
	}

	return &domain.QueryResult{
		Rows:    mergedRows,
		Columns: columns,
		Total:   totalRows,
	}
}

// ScanResult 扫描结果
type ScanResult struct {
	Result *domain.QueryResult
	Error  error
}

// GetParallelism 获取并行度
func (ops *OptimizedParallelScanner) GetParallelism() int {
	return ops.parallelism
}

// SetParallelism 设置并行度
func (ops *OptimizedParallelScanner) SetParallelism(parallelism int) {
	if parallelism > 0 && parallelism <= 64 {
		ops.parallelism = parallelism
		if parallelism > 8 {
			ops.batchSize = 500
		} else {
			ops.batchSize = 1000
		}
	}
}

// Explain 解释优化的并行扫描器
func (ops *OptimizedParallelScanner) Explain() string {
	return fmt.Sprintf(
		"OptimizedParallelScanner(parallelism=%d, batchSize=%d)",
		ops.parallelism,
		ops.batchSize,
	)
}
