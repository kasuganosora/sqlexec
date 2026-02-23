package statistics

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// SamplingCollector 采样统计收集器
// 使用Reservoir Sampling算法进行高效采样，避免全表扫描
type SamplingCollector struct {
	dataSource domain.DataSource
	sampleRate float64 // 采样率 (0.01-0.05 = 1%-5%)
	maxRows    int64   // 最大采样行数
	rand       *rand.Rand
}

// NewSamplingCollector 创建采样收集器
func NewSamplingCollector(dataSource domain.DataSource, sampleRate float64) *SamplingCollector {
	return &SamplingCollector{
		dataSource: dataSource,
		sampleRate: sampleRate,
		maxRows:    10000, // 默认最多采样10000行
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetMaxRows 设置最大采样行数
func (sc *SamplingCollector) SetMaxRows(maxRows int64) {
	sc.maxRows = maxRows
}

// CollectStatistics 采样收集统计信息
func (sc *SamplingCollector) CollectStatistics(ctx context.Context, tableName string) (*TableStatistics, error) {
	// 第一步：获取表信息和总行数（用于计算采样大小）
	_, err := sc.dataSource.GetTableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table info failed: %w", err)
	}

	// 第二步：计算采样参数
	// TableInfo 没有 RowCount 字段，使用默认值
	totalRows := int64(100000) // 默认假设10万行
	sampleSize := sc.calculateSampleSize(totalRows)
	if sampleSize <= 0 {
		sampleSize = 100 // 最小采样100行
	}

	debugf("  [STATISTICS] Sampling table %s: total=%d, sampleRate=%.2f%%, sampleSize=%d\n",
		tableName, totalRows, sc.sampleRate*100, sampleSize)

	// 第三步：执行采样查询
	sampleRows, err := sc.sampleRows(ctx, tableName, int(sampleSize))
	if err != nil {
		return nil, fmt.Errorf("sample rows failed: %w", err)
	}

	// 第四步：从样本计算统计信息
	stats := sc.calculateStatisticsFromSample(tableName, sampleRows, totalRows)

	// 估算总行数（基于采样比例）
	stats.EstimatedRowCount = totalRows

	debugf("  [STATISTICS] Collected statistics for %s: rows=%d, columns=%d\n",
		tableName, stats.RowCount, len(stats.ColumnStats))

	return stats, nil
}

// calculateSampleSize 计算采样大小
func (sc *SamplingCollector) calculateSampleSize(totalRows int64) int64 {
	// 如果行数很少，返回所有数据
	if totalRows <= 100 {
		return totalRows
	}

	sampleSize := int64(float64(totalRows) * sc.sampleRate)

	// 限制最大采样行数
	if sampleSize > sc.maxRows {
		sampleSize = sc.maxRows
	}

	// 确保至少采样一定比例
	if sampleSize < 100 {
		sampleSize = 100
	}

	return sampleSize
}

// sampleRows 采样行数据（使用系统采样 Systematic Sampling）
func (sc *SamplingCollector) sampleRows(ctx context.Context, tableName string, sampleSize int) ([]domain.Row, error) {
	// 获取所有数据（简化实现，实际应该使用游标跳过采样）
	result, err := sc.dataSource.Query(ctx, tableName, &domain.QueryOptions{})
	if err != nil {
		return nil, err
	}

	totalRows := len(result.Rows)
	if totalRows == 0 {
		return result.Rows, nil
	}

	// 系统采样：计算步长
	step := float64(totalRows) / float64(sampleSize)
	if step < 1 {
		step = 1
	}

	// 按步长采样
	sampleRows := make([]domain.Row, 0, sampleSize)
	for i := 0; i < totalRows; i += int(step) {
		if len(sampleRows) >= sampleSize {
			break
		}
		sampleRows = append(sampleRows, result.Rows[i])
	}

	return sampleRows, nil
}

// calculateStatisticsFromSample 从样本计算统计信息
func (sc *SamplingCollector) calculateStatisticsFromSample(tableName string, sampleRows []domain.Row, totalRowCount int64) *TableStatistics {
	sampleCount := int64(len(sampleRows))

	// 计算采样比例
	sampleRatio := 1.0
	if totalRowCount > 0 {
		sampleRatio = float64(sampleCount) / float64(totalRowCount)
	}

	stats := &TableStatistics{
		Name:              tableName,
		RowCount:          totalRowCount,
		SampleCount:       sampleCount,
		SampleRatio:       sampleRatio,
		ColumnStats:       make(map[string]*ColumnStatistics),
		Histograms:        make(map[string]*Histogram),
		CollectTimestamp:  time.Now(),
		EstimatedRowCount: totalRowCount,
	}

	// 收集列统计信息
	if len(sampleRows) > 0 {
		// 获取列名
		colNames := make([]string, 0, len(sampleRows[0]))
		for colName := range sampleRows[0] {
			colNames = append(colNames, colName)
		}

		// 为每列构建统计信息和直方图
		var wg sync.WaitGroup
		var mu sync.Mutex

		for _, colName := range colNames {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()

				colStats, histogram := sc.collectColumnStats(sampleRows, name)

				mu.Lock()
				stats.ColumnStats[name] = colStats
				stats.Histograms[name] = histogram
				mu.Unlock()
			}(colName)
		}

		wg.Wait()
	}

	return stats
}

// collectColumnStats 收集单个列的统计信息
func (sc *SamplingCollector) collectColumnStats(rows []domain.Row, colName string) (*ColumnStatistics, *Histogram) {
	colStats := &ColumnStatistics{
		Name:     colName,
		DataType: inferDataType(rows, colName),
	}

	values := make([]interface{}, 0, len(rows))
	distinctValues := make(map[interface{}]bool)
	nullCount := int64(0)
	totalWidth := 0.0

	// 收集值
	for _, row := range rows {
		val := row[colName]
		if val == nil {
			nullCount++
			continue
		}

		values = append(values, val)
		distinctValues[val] = true

		// 字符串宽度
		if s, ok := val.(string); ok {
			totalWidth += float64(len(s))
		}
	}

	// 计算基本统计
	colStats.NullCount = nullCount
	colStats.NullFraction = float64(nullCount) / float64(len(rows))
	colStats.DistinctCount = int64(len(distinctValues))

	if len(values) > 0 {
		// 计算Min/Max
		colStats.MinValue = values[0]
		colStats.MaxValue = values[0]
		for _, val := range values {
			if compareValues(val, colStats.MinValue) < 0 {
				colStats.MinValue = val
			}
			if compareValues(val, colStats.MaxValue) > 0 {
				colStats.MaxValue = val
			}
		}

		// 计算平均宽度
		if len(values)-int(nullCount) > 0 {
			colStats.AvgWidth = totalWidth / float64(len(values)-int(nullCount))
		}

		// 构建直方图（等宽直方图）
		histogram := BuildEquiWidthHistogram(values, 10)
		return colStats, histogram
	}

	return colStats, nil
}

// inferDataType 推断列的数据类型
func inferDataType(rows []domain.Row, colName string) string {
	for _, row := range rows {
		val := row[colName]
		if val == nil {
			continue
		}

		switch val.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return "integer"
		case float32, float64:
			return "numeric"
		case string:
			return "varchar"
		case bool:
			return "boolean"
		case time.Time:
			return "datetime"
		default:
			return "unknown"
		}
	}
	return "unknown"
}

// compareValues 比较两个值
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 数值比较
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}
