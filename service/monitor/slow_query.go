package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SlowQueryLog 慢查询日志项
type SlowQueryLog struct {
	ID          int64
	SQL         string
	Duration    time.Duration
	Timestamp   time.Time
	TableName   string
	RowCount    int64
	ExecutedBy  string
	Error       string
	ExplainPlan string
}

// SlowQueryAnalyzer 慢查询分析器
type SlowQueryAnalyzer struct {
	mu           sync.RWMutex
	slowQueries  []*SlowQueryLog
	slowQueryMap map[int64]*SlowQueryLog
	threshold    time.Duration
	maxEntries   int
	nextID       int64
}

// NewSlowQueryAnalyzer 创建慢查询分析器
func NewSlowQueryAnalyzer(threshold time.Duration, maxEntries int) *SlowQueryAnalyzer {
	return &SlowQueryAnalyzer{
		slowQueries:  make([]*SlowQueryLog, 0, maxEntries),
		slowQueryMap: make(map[int64]*SlowQueryLog),
		threshold:    threshold,
		maxEntries:   maxEntries,
		nextID:       1,
	}
}

// IsSlowQuery 检查是否为慢查�?
func (s *SlowQueryAnalyzer) IsSlowQuery(duration time.Duration) bool {
	return duration >= s.threshold
}

// RecordSlowQuery 记录慢查�?
func (s *SlowQueryAnalyzer) RecordSlowQuery(sql string, duration time.Duration, tableName string, rowCount int64) int64 {
	if !s.IsSlowQuery(duration) {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log := &SlowQueryLog{
		ID:         s.nextID,
		SQL:        sql,
		Duration:   duration,
		Timestamp:  time.Now(),
		TableName:  tableName,
		RowCount:   rowCount,
		ExecutedBy: "system",
	}

	s.slowQueryMap[log.ID] = log
	s.slowQueries = append(s.slowQueries, log)
	s.nextID++

	// 如果超出最大条目数，移除最旧的记录
	if len(s.slowQueries) > s.maxEntries {
		oldest := s.slowQueries[0]
		delete(s.slowQueryMap, oldest.ID)
		s.slowQueries = s.slowQueries[1:]
	}

	return log.ID
}

// RecordSlowQueryWithError 记录带错误的慢查�?
func (s *SlowQueryAnalyzer) RecordSlowQueryWithError(sql string, duration time.Duration, tableName string, rowCount int64, err string) int64 {
	id := s.RecordSlowQuery(sql, duration, tableName, rowCount)
	if id > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		if log, ok := s.slowQueryMap[id]; ok {
			log.Error = err
		}
	}
	return id
}

// GetSlowQuery 获取慢查询记�?
func (s *SlowQueryAnalyzer) GetSlowQuery(id int64) (*SlowQueryLog, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	log, ok := s.slowQueryMap[id]
	return log, ok
}

// GetAllSlowQueries 获取所有慢查询
func (s *SlowQueryAnalyzer) GetAllSlowQueries() []*SlowQueryLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*SlowQueryLog, len(s.slowQueries))
	copy(result, s.slowQueries)
	return result
}

// GetSlowQueriesByTable 获取指定表的慢查�?
func (s *SlowQueryAnalyzer) GetSlowQueriesByTable(tableName string) []*SlowQueryLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := []*SlowQueryLog{}
	for _, log := range s.slowQueries {
		if log.TableName == tableName {
			result = append(result, log)
		}
	}
	return result
}

// GetSlowQueriesByTimeRange 获取指定时间范围的慢查询
func (s *SlowQueryAnalyzer) GetSlowQueriesByTimeRange(start, end time.Time) []*SlowQueryLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := []*SlowQueryLog{}
	for _, log := range s.slowQueries {
		if log.Timestamp.After(start) && log.Timestamp.Before(end) {
			result = append(result, log)
		}
	}
	return result
}

// GetSlowQueriesAfter 获取指定时间之后的慢查询
func (s *SlowQueryAnalyzer) GetSlowQueriesAfter(start time.Time) []*SlowQueryLog {
	return s.GetSlowQueriesByTimeRange(start, time.Now())
}

// GetSlowQueryCount 获取慢查询总数
func (s *SlowQueryAnalyzer) GetSlowQueryCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.slowQueries)
}

// SetExplainPlan 设置执行计划
func (s *SlowQueryAnalyzer) SetExplainPlan(id int64, explainPlan string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if log, ok := s.slowQueryMap[id]; ok {
		log.ExplainPlan = explainPlan
	}
}

// DeleteSlowQuery 删除慢查询记�?
func (s *SlowQueryAnalyzer) DeleteSlowQuery(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.slowQueryMap[id]; !ok {
		return false
	}

	delete(s.slowQueryMap, id)
	for i, log := range s.slowQueries {
		if log.ID == id {
			s.slowQueries = append(s.slowQueries[:i], s.slowQueries[i+1:]...)
			break
		}
	}
	return true
}

// Clear 清空所有慢查询
func (s *SlowQueryAnalyzer) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.slowQueries = make([]*SlowQueryLog, 0, s.maxEntries)
	s.slowQueryMap = make(map[int64]*SlowQueryLog)
	s.nextID = 1
}

// SetThreshold 设置慢查询阈�?
func (s *SlowQueryAnalyzer) SetThreshold(threshold time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threshold = threshold
}

// GetThreshold 获取慢查询阈�?
func (s *SlowQueryAnalyzer) GetThreshold() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threshold
}

// AnalyzeSlowQueries 分析慢查�?
func (s *SlowQueryAnalyzer) AnalyzeSlowQueries() *SlowQueryAnalysis {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.slowQueries) == 0 {
		return &SlowQueryAnalysis{}
	}

	analysis := &SlowQueryAnalysis{
		TotalQueries:      len(s.slowQueries),
		TableStats:        make(map[string]*TableSlowQueryStats),
		ErrorCount:        0,
		AvgDuration:       0,
		MaxDuration:       s.slowQueries[0].Duration,
		MinDuration:       s.slowQueries[0].Duration,
		TotalDuration:     0,
		AvgRowCount:       0,
		TotalRowCount:     0,
	}

	totalDuration := time.Duration(0)
	totalRowCount := int64(0)

	for _, log := range s.slowQueries {
		totalDuration += log.Duration
		totalRowCount += log.RowCount

		if log.Duration > analysis.MaxDuration {
			analysis.MaxDuration = log.Duration
		}
		if log.Duration < analysis.MinDuration {
			analysis.MinDuration = log.Duration
		}

		if log.Error != "" {
			analysis.ErrorCount++
		}

		// 表级别统�?
		if stats, ok := analysis.TableStats[log.TableName]; ok {
			stats.QueryCount++
			stats.TotalDuration += log.Duration
			stats.TotalRowCount += log.RowCount
			if log.Duration > stats.MaxDuration {
				stats.MaxDuration = log.Duration
			}
		} else {
			analysis.TableStats[log.TableName] = &TableSlowQueryStats{
				TableName:     log.TableName,
				QueryCount:    1,
				TotalDuration: log.Duration,
				MaxDuration:   log.Duration,
				TotalRowCount: log.RowCount,
			}
		}
	}

	analysis.TotalDuration = totalDuration
	analysis.AvgDuration = totalDuration / time.Duration(len(s.slowQueries))
	analysis.TotalRowCount = totalRowCount
	if len(s.slowQueries) > 0 {
		analysis.AvgRowCount = totalRowCount / int64(len(s.slowQueries))
	}

	return analysis
}

// SlowQueryAnalysis 慢查询分析结�?
type SlowQueryAnalysis struct {
	TotalQueries   int
	AvgDuration    time.Duration
	MaxDuration    time.Duration
	MinDuration    time.Duration
	TotalDuration  time.Duration
	AvgRowCount    int64
	TotalRowCount  int64
	ErrorCount     int
	TableStats     map[string]*TableSlowQueryStats
}

// TableSlowQueryStats 表级别慢查询统计
type TableSlowQueryStats struct {
	TableName     string
	QueryCount    int
	TotalDuration time.Duration
	MaxDuration   time.Duration
	AvgDuration   time.Duration
	TotalRowCount int64
	AvgRowCount   int64
}

// GetRecommendations 获取优化建议
func (s *SlowQueryAnalyzer) GetRecommendations() []string {
	analysis := s.AnalyzeSlowQueries()
	recommendations := []string{}

	// 基于慢查询总数的建�?
	if analysis.TotalQueries > 100 {
		recommendations = append(recommendations, fmt.Sprintf("慢查询数量过�?%d)，建议检查查询优化策�?, analysis.TotalQueries))
	}

	// 基于平均时长的建�?
	if analysis.AvgDuration > time.Second {
		recommendations = append(recommendations, fmt.Sprintf("平均查询时长较长(%v)，建议添加索引或优化查询语句", analysis.AvgDuration))
	}

	// 基于错误率的建议
	if analysis.TotalQueries > 0 {
		errorRate := float64(analysis.ErrorCount) / float64(analysis.TotalQueries)
		if errorRate > 0.1 {
			recommendations = append(recommendations, fmt.Sprintf("慢查询错误率过高(%.2f%%)，建议检查错误原�?, errorRate*100))
		}
	}

	// 表级别建�?
	for tableName, stats := range analysis.TableStats {
		if stats.QueryCount > 10 {
			recommendations = append(recommendations, fmt.Sprintf("�?%s �?%d 条慢查询，建议优化该表的查询", tableName, stats.QueryCount))
		}
		if stats.AvgDuration > time.Second*2 {
			recommendations = append(recommendations, fmt.Sprintf("�?%s 的平均查询时长较�?%v)，建议添加索�?, tableName, stats.AvgDuration))
		}
	}

	return recommendations
}

// MonitorContext 监控上下�?
type MonitorContext struct {
	Metrics      *MetricsCollector
	SlowQuery    *SlowQueryAnalyzer
	Ctx          context.Context
	QueryID      int64
	StartTime    time.Time
	TableName    string
}

// NewMonitorContext 创建监控上下�?
func NewMonitorContext(ctx context.Context, metrics *MetricsCollector, slowQuery *SlowQueryAnalyzer, sql string) *MonitorContext {
	return &MonitorContext{
		Metrics:   metrics,
		SlowQuery: slowQuery,
		Ctx:       ctx,
		StartTime: time.Now(),
	}
}

// Start 开始监�?
func (mc *MonitorContext) Start() {
	mc.Metrics.StartQuery()
}

// End 结束监控
func (mc *MonitorContext) End(success bool, rowCount int64, err error) {
	duration := time.Since(mc.StartTime)
	mc.Metrics.RecordQuery(duration, success, mc.TableName)
	mc.Metrics.EndQuery()

	if mc.SlowQuery.IsSlowQuery(duration) {
		var errMsg string
		if err != nil {
			errMsg = err.Error()
		}
		mc.SlowQuery.RecordSlowQueryWithError("", duration, mc.TableName, rowCount, errMsg)
	}
}
