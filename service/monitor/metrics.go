package monitor

import (
	"sync"
	"time"
)

// MetricsCollector 监控指标收集�?
type MetricsCollector struct {
	mu               sync.RWMutex
	queryCount       int64
	querySuccess     int64
	queryError       int64
	totalDuration    time.Duration
	slowQueryCount   int64
	activeQueries    int64
	errorCount       map[string]int64
	tableAccessCount map[string]int64
	startTime        time.Time
}

// NewMetricsCollector 创建监控指标收集�?
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		errorCount:       make(map[string]int64),
		tableAccessCount: make(map[string]int64),
		startTime:        time.Now(),
	}
}

// RecordQuery 记录查询
func (m *MetricsCollector) RecordQuery(duration time.Duration, success bool, tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queryCount++
	m.totalDuration += duration

	if success {
		m.querySuccess++
	} else {
		m.queryError++
	}

	if tableName != "" {
		m.tableAccessCount[tableName]++
	}
}

// RecordError 记录错误
func (m *MetricsCollector) RecordError(errType string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errorCount[errType]++
	m.queryError++
}

// RecordSlowQuery 记录慢查�?
func (m *MetricsCollector) RecordSlowQuery() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.slowQueryCount++
}

// StartQuery 开始查�?
func (m *MetricsCollector) StartQuery() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeQueries++
}

// EndQuery 结束查询
func (m *MetricsCollector) EndQuery() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeQueries > 0 {
		m.activeQueries--
	}
}

// GetQueryCount 获取查询总数
func (m *MetricsCollector) GetQueryCount() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queryCount
}

// GetQuerySuccess 获取成功查询�?
func (m *MetricsCollector) GetQuerySuccess() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.querySuccess
}

// GetQueryError 获取错误查询�?
func (m *MetricsCollector) GetQueryError() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queryError
}

// GetSuccessRate 获取成功�?
func (m *MetricsCollector) GetSuccessRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.queryCount == 0 {
		return 0
	}
	return float64(m.querySuccess) / float64(m.queryCount) * 100
}

// GetAvgDuration 获取平均查询时长
func (m *MetricsCollector) GetAvgDuration() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.queryCount == 0 {
		return 0
	}
	return m.totalDuration / time.Duration(m.queryCount)
}

// GetSlowQueryCount 获取慢查询数�?
func (m *MetricsCollector) GetSlowQueryCount() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.slowQueryCount
}

// GetActiveQueries 获取当前活跃查询�?
func (m *MetricsCollector) GetActiveQueries() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeQueries
}

// GetErrorCount 获取错误统计
func (m *MetricsCollector) GetErrorCount(errType string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.errorCount[errType]
}

// GetAllErrors 获取所有错误统�?
func (m *MetricsCollector) GetAllErrors() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]int64)
	for k, v := range m.errorCount {
		result[k] = v
	}
	return result
}

// GetTableAccessCount 获取表访问统�?
func (m *MetricsCollector) GetTableAccessCount(tableName string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tableAccessCount[tableName]
}

// GetAllTableAccessCount 获取所有表访问统计
func (m *MetricsCollector) GetAllTableAccessCount() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]int64)
	for k, v := range m.tableAccessCount {
		result[k] = v
	}
	return result
}

// GetUptime 获取运行时间
func (m *MetricsCollector) GetUptime() time.Duration {
	return time.Since(m.startTime)
}

// Reset 重置所有指�?
func (m *MetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queryCount = 0
	m.querySuccess = 0
	m.queryError = 0
	m.totalDuration = 0
	m.slowQueryCount = 0
	m.activeQueries = 0
	m.errorCount = make(map[string]int64)
	m.tableAccessCount = make(map[string]int64)
	m.startTime = time.Now()
}

// QueryMetrics 查询指标快照
type QueryMetrics struct {
	QueryCount       int64
	QuerySuccess     int64
	QueryError       int64
	SuccessRate      float64
	AvgDuration      time.Duration
	SlowQueryCount   int64
	ActiveQueries    int64
	ErrorCount       map[string]int64
	TableAccessCount map[string]int64
	Uptime           time.Duration
}

// GetSnapshot 获取指标快照
func (m *MetricsCollector) GetSnapshot() *QueryMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &QueryMetrics{
		QueryCount:       m.queryCount,
		QuerySuccess:     m.querySuccess,
		QueryError:       m.queryError,
		SuccessRate:      m.GetSuccessRate(),
		AvgDuration:      m.GetAvgDuration(),
		SlowQueryCount:   m.slowQueryCount,
		ActiveQueries:    m.activeQueries,
		ErrorCount:       m.GetAllErrors(),
		TableAccessCount: m.GetAllTableAccessCount(),
		Uptime:           m.GetUptime(),
	}
}
