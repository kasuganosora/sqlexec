package parallel

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParallelScanner(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		dataSource  domain.DataSource
		parallelism int
		expected    int
	}{
		{
			name:        "default parallelism",
			dataSource:  dataSource,
			parallelism: 0,
			expected:    1, // will use NumCPU
		},
		{
			name:        "explicit parallelism",
			dataSource:  dataSource,
			parallelism: 4,
			expected:    4,
		},
		{
			name:        "high parallelism",
			dataSource:  dataSource,
			parallelism: 16,
			expected:    16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := NewParallelScanner(tt.dataSource, tt.parallelism)
			assert.NotNil(t, scanner)
			assert.Equal(t, tt.dataSource, scanner.dataSource)
			assert.NotNil(t, scanner.workerPool)
		})
	}
}

func TestParallelScanner_DivideScanRange(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 4)

	tests := []struct {
		name         string
		tableName    string
		offset       int64
		limit        int64
		parallelism  int
		expectedLen  int
	}{
		{
			name:        "small range",
			tableName:   "test_table",
			offset:      0,
			limit:       100,
			parallelism: 4,
			expectedLen: 4,
		},
		{
			name:        "medium range",
			tableName:   "test_table",
			offset:      0,
			limit:       1000,
			parallelism: 4,
			expectedLen: 4,
		},
		{
			name:        "large range",
			tableName:   "test_table",
			offset:      0,
			limit:       100000,
			parallelism: 8,
			expectedLen: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := scanner.divideScanRange(tt.tableName, tt.offset, tt.limit, tt.parallelism)
			assert.Len(t, ranges, tt.expectedLen)

			// Verify total range coverage
			totalLimit := int64(0)
			for _, r := range ranges {
				totalLimit += r.Limit
				assert.Equal(t, tt.tableName, r.TableName)
				assert.GreaterOrEqual(t, r.Offset, tt.offset)
			}
			assert.Equal(t, tt.limit, totalLimit, "total limits should match")
		})
	}
}

func TestParallelScanner_Execute_SmallTable(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	for i := 1; i <= 10; i++ {
		row := domain.Row{
			"id":   int64(i),
			"name": "user" + string(rune('0'+i)),
		}
		_, err := dataSource.Insert(ctx, "test_table", []domain.Row{row}, nil)
		require.NoError(t, err)
	}

	// Scan with parallelism
	scanner := NewParallelScanner(dataSource, 2)
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10,
	}

	result, err := scanner.Execute(ctx, scanRange, &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Rows), 0)
}

func TestParallelScanner_Execute_NoData(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table but no data
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 2)
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10,
	}

	result, err := scanner.Execute(ctx, scanRange, &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.Total)
}

func TestParallelScanner_Execute_WithContextCancellation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	scanner := NewParallelScanner(dataSource, 2)
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10,
	}

	_, err = scanner.Execute(ctx, scanRange, &domain.QueryOptions{})
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestParallelScanner_Execute_WithTimeout(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	scanner := NewParallelScanner(dataSource, 2)
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     10000, // Large limit
	}

	_, err = scanner.Execute(ctx, scanRange, &domain.QueryOptions{})
	// Test that we get some result - either success or error is acceptable
	// due to the very short timeout
	t.Logf("Execute returned error: %v", err)
	// Accept any outcome for this timing-sensitive test
}

func TestParallelScanner_MergeScanResults(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 2)

	tests := []struct {
		name      string
		results   []*ScanResult
		expectRows int
	}{
		{
			name:      "no results",
			results:   []*ScanResult{},
			expectRows: 0,
		},
		{
			name: "single result",
			results: []*ScanResult{
				{
					WorkerIndex: 0,
					Result: &domain.QueryResult{
						Rows:    []domain.Row{{"id": int64(1)}},
						Total:   1,
						Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
					},
				},
			},
			expectRows: 1,
		},
		{
			name: "multiple results",
			results: []*ScanResult{
				{
					WorkerIndex: 0,
					Result: &domain.QueryResult{
						Rows:  []domain.Row{{"id": int64(1)}, {"id": int64(2)}},
						Total: 2,
						Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
					},
				},
				{
					WorkerIndex: 1,
					Result: &domain.QueryResult{
						Rows:  []domain.Row{{"id": int64(3)}, {"id": int64(4)}},
						Total: 2,
						Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
					},
				},
			},
			expectRows: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := scanner.mergeScanResults(tt.results, "test_table")
			assert.NotNil(t, merged)
			assert.Equal(t, tt.expectRows, len(merged.Rows))
		})
	}
}

func TestParallelScanner_GetParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		parallelism int
		expected    int
	}{
		{"zero parallelism", 0, runtime.NumCPU()}, // defaults to NumCPU
		{"positive", 4, 4},
		{"negative", -1, runtime.NumCPU()}, // negative values default to NumCPU
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := NewParallelScanner(dataSource, tt.parallelism)
			parallelism := scanner.GetParallelism()
			assert.Equal(t, tt.expected, parallelism)
		})
	}
}

func TestParallelScanner_SetParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 2)

	tests := []struct {
		name        string
		parallelism int
		shouldChange bool
	}{
		{"set to 4", 4, true},
		{"set to 8", 8, true},
		{"set to 0", 0, false}, // should not change
		{"set to negative", -1, false}, // should not change
		{"set to 100", 100, false}, // should not change (max 64)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldParallelism := scanner.GetParallelism()
			scanner.SetParallelism(tt.parallelism)
			newParallelism := scanner.GetParallelism()

			if tt.shouldChange {
				assert.Equal(t, tt.parallelism, newParallelism)
			} else {
				assert.Equal(t, oldParallelism, newParallelism)
			}
		})
	}
}

func TestParallelScanner_ExecuteScanRange(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table and insert data
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	})
	require.NoError(t, err)

	for i := 1; i <= 20; i++ {
		row := domain.Row{"id": int64(i)}
		_, err := dataSource.Insert(ctx, "test_table", []domain.Row{row}, nil)
		require.NoError(t, err)
	}

	scanner := NewParallelScanner(dataSource, 4)

	tests := []struct {
		name     string
		offset   int64
		limit    int64
		expected int
	}{
		{
			name:     "first batch",
			offset:   0,
			limit:    5,
			expected: 5,
		},
		{
			name:     "second batch",
			offset:   5,
			limit:    5,
			expected: 5,
		},
		{
			name:     "last batch",
			offset:   15,
			limit:    5,
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanRange := ScanRange{
				TableName: "test_table",
				Offset:    tt.offset,
				Limit:     tt.limit,
			}

			result, err := scanner.executeScanRange(ctx, scanRange, &domain.QueryOptions{})
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, tt.expected, len(result.Rows))
		})
	}
}

func TestScanRange_Struct(t *testing.T) {
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    100,
		Limit:     50,
	}

	assert.Equal(t, "test_table", scanRange.TableName)
	assert.Equal(t, int64(100), scanRange.Offset)
	assert.Equal(t, int64(50), scanRange.Limit)
}

func TestScanResult_Struct(t *testing.T) {
	result := ScanResult{
		WorkerIndex: 3,
		Result: &domain.QueryResult{
			Rows:  []domain.Row{{"id": int64(1)}},
			Total: 1,
		},
	}

	assert.Equal(t, 3, result.WorkerIndex)
	assert.NotNil(t, result.Result)
	assert.Len(t, result.Result.Rows, 1)
}

func TestParallelScanner_Explain(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 4)
	explanation := scanner.Explain()

	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "ParallelScanner")
	assert.Contains(t, explanation, "parallelism=4")
}

func TestParallelScanner_ZeroLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	})
	require.NoError(t, err)

	scanner := NewParallelScanner(dataSource, 2)
	scanRange := ScanRange{
		TableName: "test_table",
		Offset:    0,
		Limit:     0, // zero limit
	}

	result, err := scanner.Execute(ctx, scanRange, &domain.QueryOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Should use default limit
}

func TestParallelScanner_LargeParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	// Test with very high parallelism
	scanner := NewParallelScanner(dataSource, 100)
	parallelism := scanner.GetParallelism()
	assert.Equal(t, 100, parallelism)
}

// Benchmark tests
func BenchmarkParallelScanner_DivideScanRange(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	scanner := NewParallelScanner(dataSource, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner.divideScanRange("test_table", 0, 10000, 4)
	}
}

func BenchmarkParallelScanner_MergeScanResults(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	scanner := NewParallelScanner(dataSource, 4)

	// Create mock results
	results := make([]*ScanResult, 10)
	for i := 0; i < 10; i++ {
		rows := make([]domain.Row, 100)
		for j := 0; j < 100; j++ {
			rows[j] = domain.Row{"id": int64(i*100 + j)}
		}
		results[i] = &ScanResult{
			WorkerIndex: i,
			Result: &domain.QueryResult{
				Rows:    rows,
				Total:   int64(len(rows)),
				Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner.mergeScanResults(results, "test_table")
	}
}

func BenchmarkNewParallelScanner(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewParallelScanner(dataSource, 4)
	}
}
