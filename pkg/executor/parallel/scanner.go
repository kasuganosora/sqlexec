package parallel

import (
	"context"
	"fmt"
	"runtime"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ParallelScanner 并行表扫描器
// 根据CPU核心数自动划分ScanRange，实现并行扫描
type ParallelScanner struct {
	dataSource   domain.DataSource
	parallelism int       // 并行度
	workerPool   *WorkerPool
}

// ScanRange 扫描范围
type ScanRange struct {
	TableName string
	Offset    int64
	Limit     int64
}

// NewParallelScanner 创建并行扫描器
func NewParallelScanner(dataSource domain.DataSource, parallelism int) *ParallelScanner {
	// 默认使用CPU核心数
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}
	
	return &ParallelScanner{
		dataSource:   dataSource,
		parallelism: parallelism,
		workerPool:   NewWorkerPool(parallelism),
	}
}

// Execute 并行执行扫描
func (ps *ParallelScanner) Execute(ctx context.Context, scanRange ScanRange, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// 计算扫描参数
	limit := scanRange.Limit
	if limit <= 0 {
		limit = 10000 // 默认
	}
	
	offset := scanRange.Offset

	// 计算每个worker的ScanRange
	ranges := ps.divideScanRange(scanRange.TableName, offset, limit, ps.parallelism)

	if len(ranges) == 0 {
		// 单个范围，直接扫描
		return ps.dataSource.Query(ctx, scanRange.TableName, options)
	}

	// 并行执行扫描
	resultChan := make(chan *ScanResult, len(ranges))
	errChan := make(chan error, len(ranges))

	for i, r := range ranges {
		go func(idx int, r ScanRange) {
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("worker %d panic: %v", idx, r)
				}
			}()

			// 执行扫描
			result, err := ps.executeScanRange(ctx, r, options)
			if err != nil {
				errChan <- err
				return
			}
			resultChan <- &ScanResult{
				WorkerIndex: idx,
				Result:      result,
			}
		}(i, r)
	}

	// 收集结果（使用 WorkerIndex 保持顺序）
	results := make([]*ScanResult, len(ranges))
	completed := 0

	for completed < len(ranges) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-resultChan:
			if result != nil {
				results[result.WorkerIndex] = result
				completed++
			}
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		}
	}

	// 合并结果
	mergedResult := ps.mergeScanResults(results, scanRange.TableName)
	return mergedResult, nil
}

// divideScanRange 划分扫描范围
func (ps *ParallelScanner) divideScanRange(tableName string, offset, limit int64, parallelism int) []ScanRange {
	ranges := make([]ScanRange, parallelism)

	// 每个worker的行数
	rowsPerWorker := int64(float64(limit) / float64(parallelism))

	for i := 0; i < parallelism; i++ {
		ranges[i] = ScanRange{
			TableName: tableName,
			Offset:    offset + int64(i)*rowsPerWorker,
			Limit:     rowsPerWorker,
		}
	}

	return ranges
}

// executeScanRange 执行单个扫描范围
func (ps *ParallelScanner) executeScanRange(ctx context.Context, r ScanRange, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// 构建查询选项（必须创建副本，因为多个goroutine并行调用）
	scanOptions := &domain.QueryOptions{}
	if options != nil {
		*scanOptions = *options
	}
	scanOptions.Offset = int(r.Offset)
	scanOptions.Limit = int(r.Limit)

	return ps.dataSource.Query(ctx, r.TableName, scanOptions)
}

// mergeScanResults 合并扫描结果
func (ps *ParallelScanner) mergeScanResults(results []*ScanResult, tableName string) *domain.QueryResult {
	if len(results) == 0 {
		return &domain.QueryResult{
			Rows:  []domain.Row{},
			Total: 0,
		}
	}

	// 合并行行
	totalRows := int64(0)
	for _, result := range results {
		totalRows += result.Result.Total
	}

	mergedRows := make([]domain.Row, 0, totalRows)
	for _, result := range results {
		mergedRows = append(mergedRows, result.Result.Rows...)
	}

	// 获取列信息（从第一个结果）
	var columns []domain.ColumnInfo
	if len(results) > 0 && results[0] != nil {
		columns = results[0].Result.Columns
	}

	return &domain.QueryResult{
		Rows:    mergedRows,
		Columns: columns,
		Total:   totalRows,
	}
}

// ScanResult 扫描结果
type ScanResult struct {
	WorkerIndex int
	Result      *domain.QueryResult
}

// GetParallelism 获取并行度
func (ps *ParallelScanner) GetParallelism() int {
	return ps.parallelism
}

// SetParallelism 设置并行度
func (ps *ParallelScanner) SetParallelism(parallelism int) {
	if parallelism > 0 && parallelism <= 64 {
		ps.parallelism = parallelism
		ps.workerPool = NewWorkerPool(parallelism)
	}
}

// Explain 解释并行扫描器
func (ps *ParallelScanner) Explain() string {
	return fmt.Sprintf(
		"ParallelScanner(parallelism=%d, workerPool=%s)",
		ps.parallelism,
		ps.workerPool.Explain(),
	)
}
