package monitor

import (
	"fmt"
	"testing"
	"time"
)

func TestNewSlowQueryAnalyzer(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)
	if analyzer == nil {
		t.Fatal("NewSlowQueryAnalyzer returned nil")
	}
	if analyzer.slowQueries == nil {
		t.Error("slowQueries should be initialized")
	}
	if analyzer.slowQueryMap == nil {
		t.Error("slowQueryMap should be initialized")
	}
	if analyzer.threshold != 1*time.Second {
		t.Errorf("threshold = %v, want 1s", analyzer.threshold)
	}
	if analyzer.maxEntries != 100 {
		t.Errorf("maxEntries = %d, want 100", analyzer.maxEntries)
	}
	if analyzer.nextID != 1 {
		t.Errorf("nextID = %d, want 1", analyzer.nextID)
	}
}

func TestIsSlowQuery(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	tests := []struct {
		name     string
		duration time.Duration
		expected bool
	}{
		{"Fast query", 500 * time.Millisecond, false},
		{"At threshold", 1 * time.Second, true},
		{"Slow query", 2 * time.Second, true},
		{"Very slow", 10 * time.Second, true},
		{"Zero duration", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.IsSlowQuery(tt.duration)
			if result != tt.expected {
				t.Errorf("IsSlowQuery(%v) = %v, want %v", tt.duration, result, tt.expected)
			}
		})
	}
}

func TestRecordSlowQuery(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录慢查询
	sql := "SELECT * FROM users"
	duration := 2 * time.Second
	tableName := "users"
	rowCount := int64(100)

	id := analyzer.RecordSlowQuery(sql, duration, tableName, rowCount)
	if id != 1 {
		t.Errorf("First query ID = %d, want 1", id)
	}

	// 验证查询已记录
	count := analyzer.GetSlowQueryCount()
	if count != 1 {
		t.Errorf("Slow query count = %d, want 1", count)
	}

	log, ok := analyzer.GetSlowQuery(id)
	if !ok {
		t.Fatal("Should retrieve slow query by ID")
	}

	if log.SQL != sql {
		t.Errorf("SQL = %s, want %s", log.SQL, sql)
	}
	if log.Duration != duration {
		t.Errorf("Duration = %v, want %v", log.Duration, duration)
	}
	if log.TableName != tableName {
		t.Errorf("TableName = %s, want %s", log.TableName, tableName)
	}
	if log.RowCount != rowCount {
		t.Errorf("RowCount = %d, want %d", log.RowCount, rowCount)
	}
}

func TestRecordSlowQueryBelowThreshold(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录快速查询（不应该被记录）
	id := analyzer.RecordSlowQuery("SELECT * FROM fast", 500*time.Millisecond, "table", 10)
	if id != 0 {
		t.Errorf("Fast query should not be recorded, got ID %d", id)
	}

	if analyzer.GetSlowQueryCount() != 0 {
		t.Error("No slow queries should be recorded")
	}
}

func TestRecordSlowQueryWithError(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	id := analyzer.RecordSlowQueryWithError(
		"SELECT * FROM users",
		2*time.Second,
		"users",
		100,
		"connection timeout",
	)

	if id == 0 {
		t.Fatal("Should record slow query with error")
	}

	log, _ := analyzer.GetSlowQuery(id)
	if log.Error != "connection timeout" {
		t.Errorf("Error = %s, want 'connection timeout'", log.Error)
	}
}

func TestGetSlowQuery(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 测试获取不存在的查询
	_, ok := analyzer.GetSlowQuery(999)
	if ok {
		t.Error("Should not find nonexistent slow query")
	}

	// 记录慢查询
	id := analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)

	// 获取存在的查询
	log, ok := analyzer.GetSlowQuery(id)
	if !ok {
		t.Fatal("Should find slow query")
	}
	if log.ID != id {
		t.Errorf("Log ID = %d, want %d", log.ID, id)
	}
}

func TestGetAllSlowQueries(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录多个慢查询
	queries := []string{
		"SELECT * FROM users",
		"SELECT * FROM orders",
		"SELECT * FROM products",
	}

	for _, sql := range queries {
		analyzer.RecordSlowQuery(sql, 2*time.Second, "test", 100)
	}

	allQueries := analyzer.GetAllSlowQueries()
	if len(allQueries) != len(queries) {
		t.Errorf("Got %d queries, want %d", len(allQueries), len(queries))
	}
}

func TestGetSlowQueriesByTable(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录不同表的查询
	analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)
	analyzer.RecordSlowQuery("SELECT * FROM orders", 2*time.Second, "orders", 50)
	analyzer.RecordSlowQuery("SELECT * FROM users", 3*time.Second, "users", 200)

	userQueries := analyzer.GetSlowQueriesByTable("users")
	if len(userQueries) != 2 {
		t.Errorf("users table queries = %d, want 2", len(userQueries))
	}

	orderQueries := analyzer.GetSlowQueriesByTable("orders")
	if len(orderQueries) != 1 {
		t.Errorf("orders table queries = %d, want 1", len(orderQueries))
	}

	nonexistentQueries := analyzer.GetSlowQueriesByTable("nonexistent")
	if len(nonexistentQueries) != 0 {
		t.Error("Nonexistent table should have 0 queries")
	}
}

func TestGetSlowQueriesByTimeRange(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录不同时间的查询（实际记录时间是自动的）
	analyzer.RecordSlowQuery("SELECT 1", 2*time.Second, "test", 100)
	analyzer.RecordSlowQuery("SELECT 2", 2*time.Second, "test", 100)
	analyzer.RecordSlowQuery("SELECT 3", 2*time.Second, "test", 100)

	// 记录后获取当前时间
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	// 获取最近一小时内的查询
	queries := analyzer.GetSlowQueriesByTimeRange(oneHourAgo, now)
	if len(queries) == 0 {
		t.Error("Should find queries in time range")
	}
}

func TestGetSlowQueriesAfter(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 先记录一个很早的时间戳
	beforeTime := time.Now().Add(-24 * time.Hour)

	analyzer.RecordSlowQuery("SELECT 1", 2*time.Second, "test", 100)
	analyzer.RecordSlowQuery("SELECT 2", 2*time.Second, "test", 100)

	// 获取所有查询来验证记录成功
	allQueries := analyzer.GetAllSlowQueries()
	if len(allQueries) != 2 {
		t.Errorf("Expected 2 recorded queries, got %d", len(allQueries))
	}

	// 使用很早的时间点来查询，确保能查到所有记录
	queries := analyzer.GetSlowQueriesAfter(beforeTime)
	if len(queries) != 2 {
		t.Errorf("Got %d queries, want 2", len(queries))
	}
}

func TestGetSlowQueryCount(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	count := analyzer.GetSlowQueryCount()
	if count != 0 {
		t.Errorf("Initial count = %d, want 0", count)
	}

	analyzer.RecordSlowQuery("SELECT 1", 2*time.Second, "test", 100)
	count = analyzer.GetSlowQueryCount()
	if count != 1 {
		t.Errorf("After one record count = %d, want 1", count)
	}

	analyzer.RecordSlowQuery("SELECT 2", 2*time.Second, "test", 100)
	analyzer.RecordSlowQuery("SELECT 3", 2*time.Second, "test", 100)
	count = analyzer.GetSlowQueryCount()
	if count != 3 {
		t.Errorf("After three records count = %d, want 3", count)
	}
}

func TestSetExplainPlan(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	id := analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)
	explainPlan := "Table Scan on users"

	analyzer.SetExplainPlan(id, explainPlan)

	log, _ := analyzer.GetSlowQuery(id)
	if log.ExplainPlan != explainPlan {
		t.Errorf("ExplainPlan = %s, want %s", log.ExplainPlan, explainPlan)
	}
}

func TestDeleteSlowQuery(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	id := analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)

	// 删除查询
	deleted := analyzer.DeleteSlowQuery(id)
	if !deleted {
		t.Error("Should successfully delete slow query")
	}

	// 验证已删除
	_, ok := analyzer.GetSlowQuery(id)
	if ok {
		t.Error("Query should be deleted")
	}

	// 尝试删除不存在的查询
	deleted = analyzer.DeleteSlowQuery(999)
	if deleted {
		t.Error("Should not delete nonexistent query")
	}
}

func TestClear(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 添加多个慢查询
	for i := 0; i < 10; i++ {
		analyzer.RecordSlowQuery("SELECT * FROM test", 2*time.Second, "test", 100)
	}

	// 清空所有
	analyzer.Clear()

	// 验证已清空
	if analyzer.GetSlowQueryCount() != 0 {
		t.Error("Should have 0 slow queries after clear")
	}

	// 验证可以重新添加新记录（ID应该从1开始）
	analyzer.RecordSlowQuery("SELECT 1", 2*time.Second, "test", 100)
	if analyzer.GetSlowQueryCount() != 1 {
		t.Error("Should have 1 slow query after adding new record")
	}
}

func TestSetThreshold(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录一个慢查询（2秒）
	analyzer.RecordSlowQuery("SELECT * FROM slow", 2*time.Second, "test", 100)

	// 修改阈值为5秒
	analyzer.SetThreshold(5 * time.Second)

	if analyzer.GetThreshold() != 5*time.Second {
		t.Errorf("Threshold = %v, want 5s", analyzer.GetThreshold())
	}

	// 现在记录2秒的查询不应该被记录
	id := analyzer.RecordSlowQuery("SELECT * FROM fast", 2*time.Second, "test", 100)
	if id != 0 {
		t.Error("Query below new threshold should not be recorded")
	}
}

func TestGetThreshold(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(500*time.Millisecond, 100)

	threshold := analyzer.GetThreshold()
	if threshold != 500*time.Millisecond {
		t.Errorf("Threshold = %v, want 500ms", threshold)
	}
}

func TestMaxEntriesLimit(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 3)

	// 记录超过最大条目数
	for i := 0; i < 10; i++ {
		analyzer.RecordSlowQuery("SELECT * FROM test", 2*time.Second, "test", 100)
	}

	// 验证不超过最大条目数
	count := analyzer.GetSlowQueryCount()
	if count != 3 {
		t.Errorf("Count = %d, want 3 (maxEntries)", count)
	}

	// 验证最旧的被移除
	queries := analyzer.GetAllSlowQueries()
	if queries[0].ID != 8 { // 应该是第8、9、10条查询
		t.Errorf("Oldest query ID = %d, want 8", queries[0].ID)
	}
}

func TestAnalyzeSlowQueries(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 记录多个慢查询
	analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)
	analyzer.RecordSlowQuery("SELECT * FROM orders", 3*time.Second, "orders", 50)
	analyzer.RecordSlowQuery("SELECT * FROM users", 4*time.Second, "users", 200)
	analyzer.RecordSlowQueryWithError("SELECT * FROM products", 5*time.Second, "products", 150, "error")

	analysis := analyzer.AnalyzeSlowQueries()
	if analysis == nil {
		t.Fatal("Analysis should not be nil")
	}

	if analysis.TotalQueries != 4 {
		t.Errorf("TotalQueries = %d, want 4", analysis.TotalQueries)
	}

	if analysis.ErrorCount != 1 {
		t.Errorf("ErrorCount = %d, want 1", analysis.ErrorCount)
	}

	expectedAvgDuration := (2 + 3 + 4 + 5) * time.Second / 4
	if analysis.AvgDuration != expectedAvgDuration {
		t.Errorf("AvgDuration = %v, want %v", analysis.AvgDuration, expectedAvgDuration)
	}

	if analysis.MaxDuration != 5*time.Second {
		t.Errorf("MaxDuration = %v, want 5s", analysis.MaxDuration)
	}

	if analysis.MinDuration != 2*time.Second {
		t.Errorf("MinDuration = %v, want 2s", analysis.MinDuration)
	}

	// 检查表级别统计
	if len(analysis.TableStats) != 3 {
		t.Errorf("TableStats count = %d, want 3", len(analysis.TableStats))
	}

	usersStats := analysis.TableStats["users"]
	if usersStats.QueryCount != 2 {
		t.Errorf("users query count = %d, want 2", usersStats.QueryCount)
	}
}

func TestAnalyzeEmptyQueries(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	analysis := analyzer.AnalyzeSlowQueries()
	if analysis == nil {
		t.Fatal("Analysis should not be nil")
	}

	if analysis.TotalQueries != 0 {
		t.Error("Empty analysis should have 0 total queries")
	}
}

func TestGetRecommendations(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	// 添加大量慢查询
	for i := 0; i < 110; i++ {
		analyzer.RecordSlowQuery("SELECT * FROM large_table", 2*time.Second, "large_table", 1000)
	}

	recommendations := analyzer.GetRecommendations()
	if len(recommendations) == 0 {
		t.Error("Should have recommendations for many slow queries")
	}

	// 添加高平均时长
	analyzer.Clear()
	for i := 0; i < 10; i++ {
		analyzer.RecordSlowQuery("SELECT * FROM slow_table", 3*time.Second, "slow_table", 100)
	}

	recommendations = analyzer.GetRecommendations()
	if len(recommendations) == 0 {
		t.Error("Should have recommendations for high avg duration")
	}
}

func TestMonitorContext(t *testing.T) {
	metrics := NewMetricsCollector()
	slowQuery := NewSlowQueryAnalyzer(1*time.Second, 100)

	ctx := NewMonitorContext(nil, metrics, slowQuery, "SELECT * FROM users")
	if ctx == nil {
		t.Fatal("NewMonitorContext returned nil")
	}

	ctx.Start()
	if metrics.GetActiveQueries() != 1 {
		t.Error("Should increment active queries")
	}

	ctx.TableName = "users"
	ctx.End(true, 100, nil)

	if metrics.GetActiveQueries() != 0 {
		t.Error("Should decrement active queries")
	}
}

func TestMonitorContextWithSlowQuery(t *testing.T) {
	metrics := NewMetricsCollector()
	slowQuery := NewSlowQueryAnalyzer(100*time.Millisecond, 100)

	ctx := NewMonitorContext(nil, metrics, slowQuery, "SELECT * FROM users")
	ctx.Start()

	// 等待使其成为慢查询
	time.Sleep(150 * time.Millisecond)

	ctx.End(true, 100, nil)

	// 验证慢查询被记录
	if slowQuery.GetSlowQueryCount() != 1 {
		t.Error("Slow query should be recorded")
	}
}

func TestMonitorContextWithError(t *testing.T) {
	metrics := NewMetricsCollector()
	slowQuery := NewSlowQueryAnalyzer(100*time.Millisecond, 100)

	ctx := NewMonitorContext(nil, metrics, slowQuery, "SELECT * FROM users")
	ctx.Start()
	time.Sleep(150 * time.Millisecond)

	err := fmt.Errorf("connection failed")
	ctx.End(false, 0, err)

	// 验证错误被记录
	if metrics.GetQueryError() != 1 {
		t.Error("Error should be recorded")
	}

	// 验证慢查询带有错误
	queries := slowQuery.GetAllSlowQueries()
	if len(queries) != 1 {
		t.Error("Should have one slow query")
	}
	if queries[0].Error == "" {
		t.Error("Slow query should have error message")
	}
}

func TestSlowQueryLogFields(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	sql := "SELECT * FROM users WHERE id = 1"
	duration := 2 * time.Second
	tableName := "users"
	rowCount := int64(100)

	id := analyzer.RecordSlowQuery(sql, duration, tableName, rowCount)
	log, _ := analyzer.GetSlowQuery(id)

	if log.ID != id {
		t.Errorf("ID = %d, want %d", log.ID, id)
	}
	if log.SQL != sql {
		t.Errorf("SQL = %s, want %s", log.SQL, sql)
	}
	if log.Duration != duration {
		t.Errorf("Duration = %v, want %v", log.Duration, duration)
	}
	if log.TableName != tableName {
		t.Errorf("TableName = %s, want %s", log.TableName, tableName)
	}
	if log.RowCount != rowCount {
		t.Errorf("RowCount = %d, want %d", log.RowCount, rowCount)
	}
	if log.ExecutedBy != "system" {
		t.Errorf("ExecutedBy = %s, want system", log.ExecutedBy)
	}
	if log.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestTableSlowQueryStats(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 100)

	analyzer.RecordSlowQuery("SELECT * FROM users", 2*time.Second, "users", 100)
	analyzer.RecordSlowQuery("SELECT * FROM users", 3*time.Second, "users", 200)
	analyzer.RecordSlowQuery("SELECT * FROM users", 4*time.Second, "users", 300)

	analysis := analyzer.AnalyzeSlowQueries()
	stats := analysis.TableStats["users"]

	if stats.TableName != "users" {
		t.Errorf("TableName = %s, want users", stats.TableName)
	}
	if stats.QueryCount != 3 {
		t.Errorf("QueryCount = %d, want 3", stats.QueryCount)
	}
	if stats.TotalRowCount != 600 {
		t.Errorf("TotalRowCount = %d, want 600", stats.TotalRowCount)
	}
	if stats.AvgRowCount != 200 {
		t.Errorf("AvgRowCount = %d, want 200", stats.AvgRowCount)
	}
	if stats.MaxDuration != 4*time.Second {
		t.Errorf("MaxDuration = %v, want 4s", stats.MaxDuration)
	}
}

func TestConcurrentSlowQueries(t *testing.T) {
	analyzer := NewSlowQueryAnalyzer(1*time.Second, 1000)
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				analyzer.RecordSlowQuery(
					"SELECT * FROM test",
					2*time.Second,
					"test",
					100,
				)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	count := analyzer.GetSlowQueryCount()
	if count != 1000 {
		t.Errorf("Expected 1000 slow queries, got %d", count)
	}
}
