package monitor

import (
	"testing"
	"time"
)

func TestNewMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()
	if collector == nil {
		t.Fatal("NewMetricsCollector returned nil")
	}
	if collector.errorCount == nil {
		t.Error("errorCount map should be initialized")
	}
	if collector.tableAccessCount == nil {
		t.Error("tableAccessCount map should be initialized")
	}
	if collector.startTime.IsZero() {
		t.Error("startTime should be set")
	}
}

func TestRecordQuery(t *testing.T) {
	collector := NewMetricsCollector()

	// 记录成功查询
	collector.RecordQuery(100*time.Millisecond, true, "users")
	if collector.GetQueryCount() != 1 {
		t.Errorf("QueryCount = %d, want 1", collector.GetQueryCount())
	}
	if collector.GetQuerySuccess() != 1 {
		t.Errorf("QuerySuccess = %d, want 1", collector.GetQuerySuccess())
	}

	// 记录失败查询
	collector.RecordQuery(200*time.Millisecond, false, "orders")
	if collector.GetQueryCount() != 2 {
		t.Errorf("QueryCount = %d, want 2", collector.GetQueryCount())
	}
	if collector.GetQueryError() != 1 {
		t.Errorf("QueryError = %d, want 1", collector.GetQueryError())
	}
}

func TestGetQueryCount(t *testing.T) {
	collector := NewMetricsCollector()

	tests := []struct {
		name          string
		recordCount   int
		expectedCount int64
	}{
		{"Zero queries", 0, 0},
		{"One query", 1, 1},
		{"Multiple queries", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector.Reset()
			for i := 0; i < tt.recordCount; i++ {
				collector.RecordQuery(100*time.Millisecond, true, "test")
			}
			if count := collector.GetQueryCount(); count != tt.expectedCount {
				t.Errorf("GetQueryCount() = %d, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestGetQuerySuccess(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(200*time.Millisecond, true, "orders")
	collector.RecordQuery(150*time.Millisecond, false, "products")

	successCount := collector.GetQuerySuccess()
	if successCount != 2 {
		t.Errorf("GetQuerySuccess() = %d, want 2", successCount)
	}
}

func TestGetQueryError(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(200*time.Millisecond, false, "orders")
	collector.RecordQuery(150*time.Millisecond, false, "products")

	errorCount := collector.GetQueryError()
	if errorCount != 2 {
		t.Errorf("GetQueryError() = %d, want 2", errorCount)
	}
}

func TestGetSuccessRate(t *testing.T) {
	collector := NewMetricsCollector()

	// 初始状态
	rate := collector.GetSuccessRate()
	if rate != 0 {
		t.Errorf("Initial success rate = %f, want 0", rate)
	}

	// 记录100%成功率
	collector.RecordQuery(100*time.Millisecond, true, "users")
	rate = collector.GetSuccessRate()
	if rate != 100.0 {
		t.Errorf("Success rate = %f, want 100.0", rate)
	}

	// 50%成功率
	collector.RecordQuery(200*time.Millisecond, false, "orders")
	rate = collector.GetSuccessRate()
	if rate != 50.0 {
		t.Errorf("Success rate = %f, want 50.0", rate)
	}

	// 33.33%成功率
	collector.RecordQuery(150*time.Millisecond, false, "products")
	rate = collector.GetSuccessRate()
	if rate != 33.33333333333333 {
		t.Errorf("Success rate = %f, want 33.33", rate)
	}
}

func TestGetAvgDuration(t *testing.T) {
	collector := NewMetricsCollector()

	// 初始状态
	avg := collector.GetAvgDuration()
	if avg != 0 {
		t.Errorf("Initial avg duration = %v, want 0", avg)
	}

	// 单个查询
	collector.RecordQuery(100*time.Millisecond, true, "users")
	avg = collector.GetAvgDuration()
	if avg != 100*time.Millisecond {
		t.Errorf("Avg duration = %v, want 100ms", avg)
	}

	// 多个查询 (100ms + 200ms + 300ms) / 3 = 200ms
	collector.RecordQuery(200*time.Millisecond, true, "orders")
	collector.RecordQuery(300*time.Millisecond, true, "products")
	avg = collector.GetAvgDuration()
	if avg != 200*time.Millisecond {
		t.Errorf("Avg duration = %v, want 200ms", avg)
	}
}

func TestMetricsCollectorRecordSlowQuery(t *testing.T) {
	collector := NewMetricsCollector()

	// 记录慢查询
	collector.RecordSlowQuery()
	if collector.GetSlowQueryCount() != 1 {
		t.Errorf("SlowQueryCount = %d, want 1", collector.GetSlowQueryCount())
	}

	// 记录更多慢查询
	for i := 0; i < 5; i++ {
		collector.RecordSlowQuery()
	}
	if collector.GetSlowQueryCount() != 6 {
		t.Errorf("SlowQueryCount = %d, want 6", collector.GetSlowQueryCount())
	}
}

func TestMetricsCollectorGetSlowQueryCount(t *testing.T) {
	collector := NewMetricsCollector()

	tests := []struct {
		name     string
		expected int64
	}{
		{"Initial", 0},
		{"After one record", 1},
		{"After multiple records", 2},  // 只记录两次
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if i > 0 {
				collector.RecordSlowQuery()
			}
			count := collector.GetSlowQueryCount()
			if count != tt.expected {
				t.Errorf("MetricsGetSlowQueryCount() = %d, want %d", count, tt.expected)
			}
		})
	}
}

func TestMetricsStartQueryEndQuery(t *testing.T) {
	collector := NewMetricsCollector()

	// 开始查询
	collector.StartQuery()
	active := collector.GetActiveQueries()
	if active != 1 {
		t.Errorf("Active queries = %d, want 1", active)
	}

	// 开始更多查询
	collector.StartQuery()
	collector.StartQuery()
	active = collector.GetActiveQueries()
	if active != 3 {
		t.Errorf("Active queries = %d, want 3", active)
	}

	// 结束查询
	collector.EndQuery()
	active = collector.GetActiveQueries()
	if active != 2 {
		t.Errorf("Active queries = %d, want 2", active)
	}

	// 结束所有查询
	collector.EndQuery()
	collector.EndQuery()
	active = collector.GetActiveQueries()
	if active != 0 {
		t.Errorf("Active queries = %d, want 0", active)
	}
}

func TestGetActiveQueries(t *testing.T) {
	collector := NewMetricsCollector()

	active := collector.GetActiveQueries()
	if active != 0 {
		t.Errorf("Initial active queries = %d, want 0", active)
	}

	collector.StartQuery()
	active = collector.GetActiveQueries()
	if active != 1 {
		t.Errorf("Active queries = %d, want 1", active)
	}
}

func TestRecordError(t *testing.T) {
	collector := NewMetricsCollector()

	// 记录错误
	collector.RecordError("connection_error")
	collector.RecordError("timeout_error")
	collector.RecordError("connection_error")

	// 检查特定错误计数
	count := collector.GetErrorCount("connection_error")
	if count != 2 {
		t.Errorf("connection_error count = %d, want 2", count)
	}

	count = collector.GetErrorCount("timeout_error")
	if count != 1 {
		t.Errorf("timeout_error count = %d, want 1", count)
	}

	// 检查不存在的错误
	count = collector.GetErrorCount("nonexistent")
	if count != 0 {
		t.Errorf("nonexistent error count = %d, want 0", count)
	}

	// RecordError 应该增加 queryError
	if collector.GetQueryError() != 3 {
		t.Errorf("QueryError = %d, want 3", collector.GetQueryError())
	}
}

func TestGetAllErrors(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordError("error1")
	collector.RecordError("error2")
	collector.RecordError("error1")

	errors := collector.GetAllErrors()
	if len(errors) != 2 {
		t.Errorf("Error types count = %d, want 2", len(errors))
	}

	if errors["error1"] != 2 {
		t.Errorf("error1 count = %d, want 2", errors["error1"])
	}
	if errors["error2"] != 1 {
		t.Errorf("error2 count = %d, want 1", errors["error2"])
	}
}

func TestTableAccessCount(t *testing.T) {
	collector := NewMetricsCollector()

	// 记录表访问
	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(100*time.Millisecond, true, "orders")
	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(100*time.Millisecond, true, "products")

	// 检查特定表的访问计数
	count := collector.GetTableAccessCount("users")
	if count != 2 {
		t.Errorf("users access count = %d, want 2", count)
	}

	count = collector.GetTableAccessCount("orders")
	if count != 1 {
		t.Errorf("orders access count = %d, want 1", count)
	}

	count = collector.GetTableAccessCount("products")
	if count != 1 {
		t.Errorf("products access count = %d, want 1", count)
	}

	// 检查不存在的表
	count = collector.GetTableAccessCount("nonexistent")
	if count != 0 {
		t.Errorf("nonexistent table count = %d, want 0", count)
	}
}

func TestGetAllTableAccessCount(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(100*time.Millisecond, true, "orders")
	collector.RecordQuery(100*time.Millisecond, true, "users")

	counts := collector.GetAllTableAccessCount()
	if len(counts) != 2 {
		t.Errorf("Table count = %d, want 2", len(counts))
	}

	if counts["users"] != 2 {
		t.Errorf("users count = %d, want 2", counts["users"])
	}
	if counts["orders"] != 1 {
		t.Errorf("orders count = %d, want 1", counts["orders"])
	}
}

func TestGetUptime(t *testing.T) {
	collector := NewMetricsCollector()

	uptime := collector.GetUptime()
	if uptime < 0 {
		t.Error("Uptime should be positive")
	}

	// 等待一段时间
	time.Sleep(10 * time.Millisecond)

	uptime2 := collector.GetUptime()
	if uptime2 <= uptime {
		t.Error("Uptime should increase")
	}
}

func TestReset(t *testing.T) {
	collector := NewMetricsCollector()

	// 添加一些数据
	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(200*time.Millisecond, false, "orders")
	collector.RecordError("test_error")
	collector.RecordSlowQuery()
	collector.StartQuery()

	// 重置
	collector.Reset()

	// 验证所有数据已重置
	if collector.GetQueryCount() != 0 {
		t.Error("QueryCount should be 0 after reset")
	}
	if collector.GetQuerySuccess() != 0 {
		t.Error("QuerySuccess should be 0 after reset")
	}
	if collector.GetQueryError() != 0 {
		t.Error("QueryError should be 0 after reset")
	}
	if collector.GetSlowQueryCount() != 0 {
		t.Error("SlowQueryCount should be 0 after reset")
	}
	if collector.GetActiveQueries() != 0 {
		t.Error("ActiveQueries should be 0 after reset")
	}
	if len(collector.GetAllErrors()) != 0 {
		t.Error("Errors should be empty after reset")
	}
	if len(collector.GetAllTableAccessCount()) != 0 {
		t.Error("TableAccessCount should be empty after reset")
	}
}

func TestGetSnapshot(t *testing.T) {
	collector := NewMetricsCollector()

	// 添加一些数据
	collector.RecordQuery(100*time.Millisecond, true, "users")
	collector.RecordQuery(200*time.Millisecond, false, "orders")
	collector.RecordError("test_error")

	snapshot := collector.GetSnapshot()
	if snapshot == nil {
		t.Fatal("GetSnapshot() returned nil")
	}

	// 验证快照数据
	// RecordQuery会递增queryCount，但RecordError不会
	if snapshot.QueryCount != 2 {
		t.Errorf("Snapshot QueryCount = %d, want 2", snapshot.QueryCount)
	}
	if snapshot.QuerySuccess != 1 {
		t.Errorf("Snapshot QuerySuccess = %d, want 1", snapshot.QuerySuccess)
	}
	// RecordQuery失败 + RecordError = 2个错误
	if snapshot.QueryError != 2 {
		t.Errorf("Snapshot QueryError = %d, want 2", snapshot.QueryError)
	}
	if snapshot.SuccessRate != 50.0 {
		t.Errorf("Snapshot SuccessRate = %f, want 50.0", snapshot.SuccessRate)
	}
	if snapshot.AvgDuration != 150*time.Millisecond {
		t.Errorf("Snapshot AvgDuration = %v, want 150ms", snapshot.AvgDuration)
	}
	if len(snapshot.ErrorCount) != 1 {
		t.Errorf("Snapshot ErrorCount length = %d, want 1", len(snapshot.ErrorCount))
	}
	if len(snapshot.TableAccessCount) != 2 {
		t.Errorf("Snapshot TableAccessCount length = %d, want 2", len(snapshot.TableAccessCount))
	}
	if snapshot.Uptime < 0 {
		t.Error("Snapshot Uptime should be positive")
	}
}

func TestQueryWithNoTable(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordQuery(100*time.Millisecond, true, "")

	// 不应该记录表访问
	counts := collector.GetAllTableAccessCount()
	if len(counts) != 0 {
		t.Error("No table should be recorded when table is empty")
	}
}

func TestConcurrentAccess(t *testing.T) {
	collector := NewMetricsCollector()
	done := make(chan bool)

	// 并发记录查询
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				collector.RecordQuery(100*time.Millisecond, true, "test")
				collector.RecordError("test_error")
			}
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证数据
	if collector.GetQueryCount() != 1000 {
		t.Errorf("QueryCount = %d, want 1000", collector.GetQueryCount())
	}
	if collector.GetErrorCount("test_error") != 1000 {
		t.Errorf("test_error count = %d, want 1000", collector.GetErrorCount("test_error"))
	}
}

func TestZeroDuration(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordQuery(0, true, "users")
	
	avg := collector.GetAvgDuration()
	if avg != 0 {
		t.Errorf("AvgDuration with zero duration = %v, want 0", avg)
	}
}

func TestLongRunningMetrics(t *testing.T) {
	collector := NewMetricsCollector()

	// 记录大量查询
	for i := 0; i < 10000; i++ {
		collector.RecordQuery(time.Duration(i%100+1)*time.Millisecond, i%2 == 0, "table")
	}

	if collector.GetQueryCount() != 10000 {
		t.Error("Should record 10000 queries")
	}

	successRate := collector.GetSuccessRate()
	if successRate < 49 || successRate > 51 {
		t.Errorf("Success rate should be around 50%%, got %f", successRate)
	}
}

func TestMetricsAccuracy(t *testing.T) {
	collector := NewMetricsCollector()

	durations := []time.Duration{50, 100, 150, 200, 250}
	total := time.Duration(0)
	for _, d := range durations {
		collector.RecordQuery(d, true, "test")
		total += d
	}

	expectedAvg := total / time.Duration(len(durations))
	actualAvg := collector.GetAvgDuration()

	if actualAvg != expectedAvg {
		t.Errorf("AvgDuration = %v, want %v", actualAvg, expectedAvg)
	}
}
