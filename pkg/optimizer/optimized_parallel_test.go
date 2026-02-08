package optimizer

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockDataSource is a mock data source for testing
type MockDataSource struct {
	data      []domain.Row
	columns   []domain.ColumnInfo
	callCount int
	mu        sync.Mutex
}

// NewMockDataSource creates a new mock data source
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

// Connect implements DataSource interface
func (m *MockDataSource) Connect(ctx context.Context) error {
	return nil
}

// IsConnected implements DataSource interface
func (m *MockDataSource) IsConnected() bool {
	return true
}

// IsWritable implements DataSource interface
func (m *MockDataSource) IsWritable() bool {
	return false
}

// GetConfig implements DataSource interface
func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{}
}

// GetTables implements DataSource interface
func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"test_table"}, nil
}

// Query implements DataSource interface
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

// GetTableInfo implements DataSource interface
func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name:    tableName,
		Columns: m.columns,
	}, nil
}

// Insert implements DataSource interface
func (m *MockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

// Update implements DataSource interface
func (m *MockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

// Delete implements DataSource interface
func (m *MockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

// CreateTable implements DataSource interface
func (m *MockDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return nil
}

// DropTable implements DataSource interface
func (m *MockDataSource) DropTable(ctx context.Context, tableName string) error {
	return nil
}

// TruncateTable implements DataSource interface
func (m *MockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

// Execute implements DataSource interface
func (m *MockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

// Close implements DataSource interface
func (m *MockDataSource) Close(ctx context.Context) error {
	return nil
}

// GetCallCount returns the number of Query calls
func (m *MockDataSource) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// TestOptimizedParallelScannerBasic tests basic parallel scanning functionality
func TestOptimizedParallelScannerBasic(t *testing.T) {
	// Create mock data source
	dataSource := NewMockDataSource(100)

	// Create parallel scanner (4 workers)
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// Execute scan
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     100,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify results
	if result.Total != 100 {
		t.Errorf("Expected Total=100, got %d", result.Total)
	}

	if len(result.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(result.Rows))
	}

	// Verify data correctness
	for i, row := range result.Rows {
		expectedID := int64(i + 1)
		actualID, ok := row["id"].(int64)
		if !ok || actualID != expectedID {
			t.Errorf("Row %d: Expected id=%d, got %v", i, expectedID, actualID)
		}
	}
}

// TestOptimizedParallelScannerLargeDataset tests parallel scanning of large datasets
func TestOptimizedParallelScannerLargeDataset(t *testing.T) {
	// Create large dataset (10000 rows)
	dataSource := NewMockDataSource(10000)

	// Create parallel scanner (8 workers)
	scanner := NewOptimizedParallelScanner(dataSource, 8)

	// Execute scan
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify results
	if result.Total != 10000 {
		t.Errorf("Expected Total=10000, got %d", result.Total)
	}

	if len(result.Rows) != 10000 {
		t.Errorf("Expected 10000 rows, got %d", len(result.Rows))
	}

	// Verify multiple Query calls (parallel scanning)
	callCount := dataSource.GetCallCount()
	if callCount < 2 {
		t.Errorf("Expected at least 2 Query calls (parallel), got %d", callCount)
	}
}

// TestOptimizedParallelScannerSmallDataset tests small dataset (should use serial scanning)
func TestOptimizedParallelScannerSmallDataset(t *testing.T) {
	// Create small dataset (500 rows < batchSize)
	dataSource := NewMockDataSource(500)

	// Create parallel scanner
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// Execute scan
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     500,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify results
	if result.Total != 500 {
		t.Errorf("Expected Total=500, got %d", result.Total)
	}

	// Verify only one Query call (serial scanning)
	callCount := dataSource.GetCallCount()
	if callCount != 1 {
		t.Errorf("Expected 1 Query call (serial), got %d", callCount)
	}
}

// TestOptimizedParallelScannerWithOffsetAndLimit tests scanning with offset and limit
func TestOptimizedParallelScannerWithOffsetAndLimit(t *testing.T) {
	// Create data source
	dataSource := NewMockDataSource(1000)

	// Create parallel scanner
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// Execute scan: offset=100, limit=200
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    100,
		Limit:     200,
	}

	result, err := scanner.Execute(context.Background(), scanRange, nil)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify results
	if result.Total != 200 {
		t.Errorf("Expected Total=200, got %d", result.Total)
	}

	if len(result.Rows) != 200 {
		t.Errorf("Expected 200 rows, got %d", len(result.Rows))
	}

	// Verify offset correctness: first row id should be 101
	firstRowID, ok := result.Rows[0]["id"].(int64)
	if !ok || firstRowID != 101 {
		t.Errorf("Expected first row id=101, got %v", firstRowID)
	}
}

// TestOptimizedParallelScannerParallelism tests different parallelism levels
func TestOptimizedParallelScannerParallelism(t *testing.T) {
	// Test different parallelism levels (max is 8)
	parallelisms := []int{1, 2, 4, 8}

	for _, parallelism := range parallelisms {
		t.Run(parallelismName(parallelism), func(t *testing.T) {
			// Create new data source
			dataSource := NewMockDataSource(5000)

			// Create parallel scanner
			scanner := NewOptimizedParallelScanner(dataSource, parallelism)

			// Execute scan
			scanRange := ScanRange{
				TableName: "test_table",
				Offset:    0,
				Limit:     5000,
			}

			result, err := scanner.Execute(context.Background(), scanRange, nil)
			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// Verify results
			if result.Total != 5000 {
				t.Errorf("Expected Total=5000, got %d", result.Total)
			}

		// Verify parallelism (note: values > 8 are capped to 8)
		expectedParallelism := parallelism
		if parallelism > 8 {
			expectedParallelism = 8
		}
		if scanner.GetParallelism() != expectedParallelism {
			t.Errorf("Expected parallelism=%d, got %d", expectedParallelism, scanner.GetParallelism())
		}

			// Verify Query call count should equal parallelism (or less)
			callCount := dataSource.GetCallCount()
			if callCount > parallelism {
				t.Errorf("Expected callCount <= parallelism (%d), got %d", parallelism, callCount)
			}
		})
	}
}

// TestOptimizedParallelScannerSetParallelism tests setting parallelism
func TestOptimizedParallelScannerSetParallelism(t *testing.T) {
	dataSource := NewMockDataSource(1000)

	// Create parallel scanner
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	// Modify parallelism
	scanner.SetParallelism(8)

	// Verify parallelism modified
	if scanner.GetParallelism() != 8 {
		t.Errorf("Expected parallelism=8, got %d", scanner.GetParallelism())
	}

	// Test setting valid parallelism (within range)
	scanner.SetParallelism(6)
	if scanner.GetParallelism() != 6 {
		t.Errorf("Expected parallelism=6, got %d", scanner.GetParallelism())
	}

	// Test setting max parallelism (capped to 8)
	scanner.SetParallelism(32)
	if scanner.GetParallelism() != 8 {
		t.Errorf("Expected parallelism=8 (max), got %d", scanner.GetParallelism())
	}

	// Test setting parallelism beyond max (should be capped to 8)
	scanner.SetParallelism(100)
	if scanner.GetParallelism() != 8 {
		t.Errorf("Expected parallelism=8 (capped), got %d", scanner.GetParallelism())
	}

	// Test auto-select parallelism (pass 0 or negative)
	scanner.SetParallelism(0)
	if scanner.GetParallelism() < 1 {
		t.Errorf("Expected auto-selected parallelism >= 1, got %d", scanner.GetParallelism())
	}
	if scanner.GetParallelism() > 8 {
		t.Errorf("Expected auto-selected parallelism <= 8, got %d", scanner.GetParallelism())
	}
}

// TestOptimizedParallelScannerExplain tests Explain method
func TestOptimizedParallelScannerExplain(t *testing.T) {
	dataSource := NewMockDataSource(1000)
	scanner := NewOptimizedParallelScanner(dataSource, 4)

	explain := scanner.Explain()
	if explain == "" {
		t.Error("Expected non-empty Explain output")
	}

	// Verify Explain contains key information
	expectedContains := []string{
		"OptimizedParallelScanner",
		"parallelism",
		"batchSize",
	}

	for _, expected := range expectedContains {
		if !strings.Contains(explain, expected) {
			t.Errorf("Explain output should contain '%s', got: %s", expected, explain)
		}
	}
}

// parallelismName generates test names
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

// ============================================================================
// BenchmarkParallelScan Parallel Scan Performance Benchmark
// ============================================================================


// BenchmarkParallelScan_Small Parallel Scan - Small Dataset
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

// BenchmarkParallelScan_Medium Parallel Scan - Medium Dataset
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

// BenchmarkParallelScan_Large Parallel Scan - Large Dataset
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

// BenchmarkParallelScan_VeryLarge Parallel Scan - Very Large Dataset
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

// BenchmarkParallelScan_Parallelism2 Parallel Scan - 2 Workers
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

// BenchmarkParallelScan_Parallelism4 Parallel Scan - 4 Workers
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

// BenchmarkParallelScan_Parallelism8 Parallel Scan - 8 Workers
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

// BenchmarkParallelScan_Parallelism16 Parallel Scan - 16 Workers
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

// BenchmarkParallelScan_WithOffsetAndLimit Parallel Scan - With Offset and Limit
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

// BenchmarkParallelScan_Compare Parallel Scan vs Serial Scan Performance Comparison
func BenchmarkParallelScan_Compare(b *testing.B) {
	// Small dataset comparison
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

	// Medium dataset comparison
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

	// Large dataset comparison
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
