package optimizer

import (
	"context"
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockDataSource 用于测试的 mock 数据源
type MockDataSource struct {
	data      []domain.Row
	columns   []domain.ColumnInfo
	callCount int
	mu        sync.Mutex
}

// NewMockDataSource 创建 mock 数据源
func NewMockDataSource(rows int) *MockDataSource {
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int64"},
		{Name: "value", Type: "int64"},
	}

	data := make([]domain.Row, rows)
	for i := 0; i < rows; i++ {
		data[i] = domain.Row{
			"id":    int64(i + 1),
			"value": int64((i + 1) * 10),
		}
	}

	return &MockDataSource{
		data:    data,
		columns: columns,
	}
}

// Connect 实现 DataSource 接口
func (m *MockDataSource) Connect(ctx context.Context) error {
	return nil
}

// IsConnected 实现 DataSource 接口
func (m *MockDataSource) IsConnected() bool {
	return true
}

// IsWritable 实现 DataSource 接口
func (m *MockDataSource) IsWritable() bool {
	return false
}

// GetConfig 实现 DataSource 接口
func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{}
}

// GetTables 实现 DataSource 接口
func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"test_table"}, nil
}

// Query 实现 DataSource 接口
func (m *MockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	offset := 0
	limit := len(m.data)

	if options != nil {
		if options.Offset > 0 {
			offset = options.Offset
		}
		if options.Limit > 0 {
			limit = options.Limit
		}
	}

	// Apply offset and limit
	end := offset + limit
	if end > len(m.data) {
		end = len(m.data)
	}

	if offset >= len(m.data) {
		return &domain.QueryResult{
			Columns: m.columns,
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}

	return &domain.QueryResult{
		Columns: m.columns,
		Rows:    m.data[offset:end],
		Total:   int64(end - offset),
	}, nil
}

// GetTableInfo 实现 DataSource 接口
func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name:    tableName,
		Columns: m.columns,
	}, nil
}

// Insert 实现 DataSource 接口
func (m *MockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

// Update 实现 DataSource 接口
func (m *MockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

// Delete 实现 DataSource 接口
func (m *MockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

// CreateTable 实现 DataSource 接口
func (m *MockDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return nil
}

// DropTable 实现 DataSource 接口
func (m *MockDataSource) DropTable(ctx context.Context, tableName string) error {
	return nil
}

// TruncateTable 实现 DataSource 接口
func (m *MockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

// Execute 实现 DataSource 接口
func (m *MockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

// Close 实现 DataSource 接口
func (m *MockDataSource) Close(ctx context.Context) error {
	return nil
}

// GetCallCount 获取 Query 调用次数
func (m *MockDataSource) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// TestOptimizedParallelScannerBasic 测试基本并行扫描功能
func TestOptimizedParallelScannerBasic(t *testing.T) {
	// 创建 mock 数据源
	dataSource := NewMockDataSource(100)

	// 创建并行扫描器（4 个 worker）
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// 执行扫描
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     100,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if result.Total != 100 {
		t.Errorf("Expected Total=100, got %d", result.Total)
	}

	if len(result.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(result.Rows))
	}

	// 验证数据正确性
	for i, row := range result.Rows {
		expectedID := int64(i + 1)
		actualID, ok := row["id"].(int64)
		if !ok || actualID != expectedID {
			t.Errorf("Row %d: Expected id=%d, got %v", i, expectedID, actualID)
		}
	}
}

// TestOptimizedParallelScannerLargeDataset 测试大数据集的并行扫描
func TestOptimizedParallelScannerLargeDataset(t *testing.T) {
	// 创建大数据集（10000 行）
	dataSource := NewMockDataSource(10000)

	// 创建并行扫描器（8 个 worker）
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	// 执行扫描
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if result.Total != 10000 {
		t.Errorf("Expected Total=10000, got %d", result.Total)
	}

	if len(result.Rows) != 10000 {
		t.Errorf("Expected 10000 rows, got %d", len(result.Rows))
	}

	// 验证调用了多次 Query（因为使用了并行扫描）
	callCount := dataSource.GetCallCount()
	if callCount < 2 {
		t.Errorf("Expected at least 2 Query calls (parallel), got %d", callCount)
	}
}

// TestOptimizedParallelScannerSmallDataset 测试小数据集（应该使用串行扫描）
func TestOptimizedParallelScannerSmallDataset(t *testing.T) {
	// 创建小数据集（500 行 < batchSize）
	dataSource := NewMockDataSource(500)

	// 创建并行扫描器
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// 执行扫描
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     500,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if result.Total != 500 {
		t.Errorf("Expected Total=500, got %d", result.Total)
	}

	// 验证只调用了一次 Query（串行扫描）
	callCount := dataSource.GetCallCount()
	if callCount != 1 {
		t.Errorf("Expected 1 Query call (serial), got %d", callCount)
	}
}

// TestOptimizedParallelScannerWithOffsetAndLimit 测试带 offset 和 limit 的扫描
func TestOptimizedParallelScannerWithOffsetAndLimit(t *testing.T) {
	// 创建数据源
	dataSource := NewMockDataSource(1000)

	// 创建并行扫描器
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// 执行扫描：offset=100, limit=200
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    100,
		Limit:     200,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 验证结果
	if result.Total != 200 {
		t.Errorf("Expected Total=200, got %d", result.Total)
	}

	if len(result.Rows) != 200 {
		t.Errorf("Expected 200 rows, got %d", len(result.Rows))
	}

	// 验证 offset 正确：第一行的 id 应该是 101
	firstRowID, ok := result.Rows[0]["id"].(int64)
	if !ok || firstRowID != 101 {
		t.Errorf("Expected first row id=101, got %v", firstRowID)
	}
}

// TestOptimizedParallelScannerParallelism 测试不同并行度
func TestOptimizedParallelScannerParallelism(t *testing.T) {
	// 测试不同的并行度
	parallelisms := []int{1, 2, 4, 8, 16}

	for _, parallelism := range parallelisms {
		t.Run(parallelismName(parallelism), func(t *testing.T) {
			// 创建新的数据源
			dataSource := NewMockDataSource(5000)

			// 创建并行扫描器
			scanner := NewOptimizedParallelScanner(dataSource, parallelism)

			// 执行扫描
			scanRange := ScanRange{
				TableName: "test_table",
				Offset:    0,
				Limit:     5000,
			}

			result, err := scanner.Execute(context.Background(), scanRange, nil)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// 验证结果
			if result.Total != 5000 {
				t.Errorf("Expected Total=5000, got %d", result.Total)
			}

			// 验证并行度
			if scanner.GetParallelism() != parallelism {
				t.Errorf("Expected parallelism=%d, got %d", parallelism, scanner.GetParallelism())
			}

			// 验证 Query 调用次数应该等于并行度（或更少）
			callCount := dataSource.GetCallCount()
			if callCount > parallelism {
				t.Errorf("Expected callCount <= parallelism (%d), got %d", parallelism, callCount)
			}
		})
	}
}

// TestOptimizedParallelScannerSetParallelism 测试设置并行度
func TestOptimizedParallelScannerSetParallelism(t *testing.T) {
	dataSource := NewMockDataSource(1000)

	// 创建并行扫描器
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// 修改并行度
	scanner.SetParallelism(8)

	// 验证并行度已修改
	if scanner.GetParallelism() != 8 {
		t.Errorf("Expected parallelism=8, got %d", scanner.GetParallelism())
	}

	// 测试设置有效并行度（在范围内）
	scanner.SetParallelism(32)
	if scanner.GetParallelism() != 32 {
		t.Errorf("Expected parallelism=32, got %d", scanner.GetParallelism())
	}

	// 测试设置最大并行度
	scanner.SetParallelism(64)
	if scanner.GetParallelism() != 64 {
		t.Errorf("Expected parallelism=64 (max), got %d", scanner.GetParallelism())
	}

	// 测试设置无效并行度（超过最大值 64，应该保持不变）
	scanner.SetParallelism(100)
	// SetParallelism 不会更新并行度，因为 100 > 64
	if scanner.GetParallelism() != 64 {
		t.Errorf("Expected parallelism=64 (unchanged), got %d", scanner.GetParallelism())
	}
}

// TestOptimizedParallelScannerExplain 测试 Explain 方法
func TestOptimizedParallelScannerExplain(t *testing.T) {
	dataSource := NewMockDataSource(1000)
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	explain := scanner.Explain()
	if explain == "" {
		t.Error("Expected non-empty Explain output")
	}

	// 验证 Explain 包含关键信息
	expectedContains := []string{
		"OptimizedParallelScanner",
		"parallelism",
		"batchSize",
	}

	for _, expected := range expectedContains {
		if !contains(explain, expected) {
			t.Errorf("Explain output should contain '%s', got: %s", expected, explain)
		}
	}
}

// parallelismName 生成测试名称
func parallelismName(p int) string {
	switch p {
	case 1:
		return "Parallelism1"
	case 2:
		return "Parallelism2"
	case 4:
		return "Parallelism4"
	case 8:
		return "Parallelism8"
	case 16:
		return "Parallelism16"
	default:
		return "ParallelismUnknown"
	}
}

// contains 检查字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================================
// BenchmarkParallelScan 并行扫描性能基准测试
// ============================================================================

// BenchmarkParallelScan_Small 并行扫描 - 小数据集
func BenchmarkParallelScan_Small(b *testing.B) {
	dataSource := NewMockDataSource(100)
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Medium 并行扫描 - 中等数据集
func BenchmarkParallelScan_Medium(b *testing.B) {
	dataSource := NewMockDataSource(1000)
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     1000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Large 并行扫描 - 大数据集
func BenchmarkParallelScan_Large(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_VeryLarge 并行扫描 - 超大数据集
func BenchmarkParallelScan_VeryLarge(b *testing.B) {
	dataSource := NewMockDataSource(50000)
	scanner := NewOptimizedParallelScanner(dataSource, 12)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     50000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Parallelism2 并行扫描 - 2 个 worker
func BenchmarkParallelScan_Parallelism2(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 2)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Parallelism4 并行扫描 - 4 个 worker
func BenchmarkParallelScan_Parallelism4(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Parallelism8 并行扫描 - 8 个 worker
func BenchmarkParallelScan_Parallelism8(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Parallelism16 并行扫描 - 16 个 worker
func BenchmarkParallelScan_Parallelism16(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 16)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_WithOffsetAndLimit 并行扫描 - 带偏移量和限制
func BenchmarkParallelScan_WithOffsetAndLimit(b *testing.B) {
	dataSource := NewMockDataSource(10000)
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    1000,
		Limit:     5000,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := scanner.Execute(context.Background(), scanRange, nil)
		if err != nil {
			b.Fatalf("Execute failed: %v", err)
		}
	}
}

// BenchmarkParallelScan_Compare 并行扫描 vs 串行扫描性能对比
func BenchmarkParallelScan_Compare(b *testing.B) {
	// 小数据集对比
	b.Run("Small/Parallel", func(b *testing.B) {
		dataSource := NewMockDataSource(100)
		scanner := NewOptimizedParallelScanner(dataSource, 4)

		scanRange := ScanRange{
			TableName: "test_table",
			Offset:    0,
			Limit:     100,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = scanner.Execute(context.Background(), scanRange, nil)
		}
	})

	// 中等数据集对比
	b.Run("Medium/Parallel", func(b *testing.B) {
		dataSource := NewMockDataSource(1000)
		scanner := NewOptimizedParallelScanner(dataSource, 8)

		scanRange := ScanRange{
			TableName: "test_table",
			Offset:    0,
			Limit:     1000,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = scanner.Execute(context.Background(), scanRange, nil)
		}
	})

	// 大数据集对比
	b.Run("Large/Parallel", func(b *testing.B) {
		dataSource := NewMockDataSource(10000)
		scanner := NewOptimizedParallelScanner(dataSource, 8)

		scanRange := ScanRange{
			TableName: "test_table",
			Offset:    0,
			Limit:     10000,
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = scanner.Execute(context.Background(), scanRange, nil)
		}
	})
}
