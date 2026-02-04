package statistics

import (
	"context"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDataSource is a mock for domain.DataSource
type MockDataSource struct {
	mock.Mock
}

func (m *MockDataSource) CreateDatabase(ctx context.Context, dbName string) error {
	args := m.Called(ctx, dbName)
	return args.Error(0)
}

func (m *MockDataSource) DropDatabase(ctx context.Context, dbName string) error {
	args := m.Called(ctx, dbName)
	return args.Error(0)
}

func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	args := m.Called(ctx, tableName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.TableInfo), args.Error(1)
}

func (m *MockDataSource) Connect(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDataSource) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDataSource) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockDataSource) IsWritable() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*domain.DataSourceConfig)
}

func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	args := m.Called(ctx, tableName, rows, options)
	return int64(args.Int(0)), args.Error(1)
}

func (m *MockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	args := m.Called(ctx, tableName, options)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.QueryResult), args.Error(1)
}

func (m *MockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	args := m.Called(ctx, tableName, filters, updates, options)
	return int64(args.Int(0)), args.Error(1)
}

func (m *MockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	args := m.Called(ctx, tableName, filters, options)
	return int64(args.Int(0)), args.Error(1)
}

func (m *MockDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	args := m.Called(ctx, tableInfo)
	return args.Error(0)
}

func (m *MockDataSource) DropTable(ctx context.Context, tableName string) error {
	args := m.Called(ctx, tableName)
	return args.Error(0)
}

func (m *MockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	args := m.Called(ctx, tableName)
	return args.Error(0)
}

func (m *MockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	args := m.Called(ctx, sql)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.QueryResult), args.Error(1)
}

func TestNewSamplingCollector(t *testing.T) {
	mockDS := new(MockDataSource)
	collector := NewSamplingCollector(mockDS, 0.05)

	assert.NotNil(t, collector)
	assert.Equal(t, mockDS, collector.dataSource)
	assert.Equal(t, 0.05, collector.sampleRate)
	assert.Equal(t, int64(10000), collector.maxRows)
	assert.NotNil(t, collector.rand)
}

func TestNewSamplingCollector_DefaultSampleRate(t *testing.T) {
	mockDS := new(MockDataSource)
	collector := NewSamplingCollector(mockDS, 0)

	// Sample rate should be preserved even if zero
	assert.NotNil(t, collector)
	assert.Equal(t, mockDS, collector.dataSource)
	assert.Equal(t, float64(0), collector.sampleRate)
}

func TestSamplingCollector_SetMaxRows(t *testing.T) {
	mockDS := new(MockDataSource)
	collector := NewSamplingCollector(mockDS, 0.05)

	tests := []struct {
		name     string
		maxRows  int64
		expected int64
	}{
		{"default", 0, 0},
		{"small", 100, 100},
		{"large", 100000, 100000},
		{"zero", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector.SetMaxRows(tt.maxRows)
			assert.Equal(t, tt.expected, collector.maxRows)
		})
	}
}

func TestSamplingCollector_CalculateSampleSize(t *testing.T) {
	mockDS := new(MockDataSource)
	collector := NewSamplingCollector(mockDS, 0.05) // 5%

	tests := []struct {
		name         string
		totalRows    int64
		sampleRate   float64
		expectedMin  int64
		expectedMax  int64
	}{
		{
			name:        "small table",
			totalRows:   1000,
			sampleRate:  0.05,
			expectedMin: 100, // minimum
			expectedMax: 100, // totalRows * 0.05
		},
		{
			name:        "medium table",
			totalRows:   10000,
			sampleRate:  0.05,
			expectedMin: 100,
			expectedMax: 500,
		},
		{
			name:        "large table",
			totalRows:   1000000,
			sampleRate:  0.05,
			expectedMin: 100,
			expectedMax: 10000, // capped at maxRows
		},
		{
			name:        "zero rows",
			totalRows:   0,
			sampleRate:  0.05,
			expectedMin: 0,
			expectedMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector.sampleRate = tt.sampleRate
			sampleSize := collector.calculateSampleSize(tt.totalRows)
			assert.GreaterOrEqual(t, sampleSize, tt.expectedMin, "sample size should be >= min")
			assert.LessOrEqual(t, sampleSize, tt.expectedMax, "sample size should be <= max")
		})
	}
}

func TestSamplingCollector_CollectStatistics(t *testing.T) {
	// Use memory data source for integration test
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	assert.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
	})
	assert.NoError(t, err)

	// Insert test data
	for i := 1; i <= 100; i++ {
		row := domain.Row{
			"id":   int64(i),
			"name": "user",
			"age":  int64(20 + i%50),
		}
		rows := []domain.Row{row}
		_, err := dataSource.Insert(ctx, "test_table", rows, nil)
		assert.NoError(t, err)
	}

	// Collect statistics
	collector := NewSamplingCollector(dataSource, 0.5) // 50% sample rate
	stats, err := collector.CollectStatistics(ctx, "test_table")

	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, "test_table", stats.Name)
	assert.Greater(t, stats.RowCount, int64(0))
	assert.Greater(t, stats.SampleCount, int64(0))
	assert.Greater(t, stats.SampleRatio, 0.0)
	assert.Greater(t, stats.SampleRatio, 0.0)
	assert.NotEmpty(t, stats.ColumnStats)
	assert.NotNil(t, stats.Histograms)
	assert.NotEmpty(t, stats.CollectTimestamp.String())
}

func TestSamplingCollector_CollectStatistics_TableNotFound(t *testing.T) {
	mockDS := new(MockDataSource)
	mockDS.On("GetTableInfo", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	ctx := context.Background()
	collector := NewSamplingCollector(mockDS, 0.05)

	stats, err := collector.CollectStatistics(ctx, "non_existent_table")

	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "get table info failed")
}

func TestSamplingCollector_SampleRows(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	assert.NoError(t, err)

	ctx := context.Background()

	// Create table and insert data
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	})
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		row := domain.Row{"id": int64(i)}
		rows := []domain.Row{row}
		_, err := dataSource.Insert(ctx, "test_table", rows, nil)
		assert.NoError(t, err)
	}

	collector := NewSamplingCollector(dataSource, 0.5)

	tests := []struct {
		name        string
		sampleSize  int
		expectedMin int
		expectedMax int
	}{
		{
			name:        "small sample",
			sampleSize:  10,
			expectedMin: 0,
			expectedMax: 10,
		},
		{
			name:        "medium sample",
			sampleSize:  30,
			expectedMin: 0,
			expectedMax: 30,
		},
		{
			name:        "large sample",
			sampleSize:  100,
			expectedMin: 0,
			expectedMax: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampleRows, err := collector.sampleRows(ctx, "test_table", tt.sampleSize)
			assert.NoError(t, err)
			assert.NotNil(t, sampleRows)
			assert.GreaterOrEqual(t, len(sampleRows), tt.expectedMin, "sample count should be >= min")
			assert.LessOrEqual(t, len(sampleRows), tt.expectedMax, "sample count should be <= max")
		})
	}
}

func TestInferDataType(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		colName  string
		expected string
	}{
		{
			name: "integer column",
			rows: []domain.Row{
				{"id": int64(1)},
				{"id": int64(2)},
				{"id": int64(3)},
			},
			colName:  "id",
			expected: "integer",
		},
		{
			name: "string column",
			rows: []domain.Row{
				{"name": "Alice"},
				{"name": "Bob"},
			},
			colName:  "name",
			expected: "varchar",
		},
		{
			name: "numeric column",
			rows: []domain.Row{
				{"score": float64(95.5)},
				{"score": float64(88.0)},
			},
			colName:  "score",
			expected: "numeric",
		},
		{
			name: "boolean column",
			rows: []domain.Row{
				{"active": true},
				{"active": false},
			},
			colName:  "active",
			expected: "boolean",
		},
		{
			name: "datetime column",
			rows: []domain.Row{
				{"created_at": time.Now()},
			},
			colName:  "created_at",
			expected: "datetime",
		},
		{
			name: "mixed with nil",
			rows: []domain.Row{
				{"age": int64(25)},
				{"age": nil},
				{"age": int64(30)},
			},
			colName:  "age",
			expected: "integer",
		},
		{
			name:     "all nil",
			rows:     []domain.Row{{"val": nil}, {"val": nil}},
			colName:  "val",
			expected: "unknown",
		},
		{
			name:     "empty rows",
			rows:     []domain.Row{},
			colName:  "test",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataType := inferDataType(tt.rows, tt.colName)
			assert.Equal(t, tt.expected, dataType)
		})
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int // -1, 0, or 1
	}{
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: 0,
		},
		{
			name:     "a nil",
			a:        nil,
			b:        int64(5),
			expected: -1,
		},
		{
			name:     "b nil",
			a:        int64(5),
			b:        nil,
			expected: 1,
		},
		{
			name:     "int less",
			a:        int64(3),
			b:        int64(5),
			expected: -1,
		},
		{
			name:     "int equal",
			a:        int64(5),
			b:        int64(5),
			expected: 0,
		},
		{
			name:     "int greater",
			a:        int64(7),
			b:        int64(5),
			expected: 1,
		},
		{
			name:     "float less",
			a:        float64(3.5),
			b:        float64(5.0),
			expected: -1,
		},
		{
			name:     "string less",
			a:        "apple",
			b:        "banana",
			expected: -1,
		},
		{
			name:     "string greater",
			a:        "zebra",
			b:        "apple",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSamplingCollector_CollectColumnStats(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	assert.NoError(t, err)

	ctx := context.Background()

	// Create table and insert data
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "age", Type: "int"},
			{Name: "name", Type: "string"},
		},
	})
	assert.NoError(t, err)

	// Insert test data
	testRows := []domain.Row{
		{"id": int64(1), "age": int64(25), "name": "Alice"},
		{"id": int64(2), "age": int64(30), "name": "Bob"},
		{"id": int64(3), "age": int64(25), "name": "Charlie"},
		{"id": int64(4), "age": nil, "name": "David"},
		{"id": int64(5), "age": int64(35), "name": "Eve"},
	}

	for _, row := range testRows {
		rows := []domain.Row{row}
		_, err := dataSource.Insert(ctx, "test_table", rows, nil)
		assert.NoError(t, err)
	}

	// Collect column stats
	collector := NewSamplingCollector(dataSource, 1.0) // 100% sample
	stats, err := collector.CollectStatistics(ctx, "test_table")
	assert.NoError(t, err)

	// Check age column stats
	ageStats, exists := stats.ColumnStats["age"]
	assert.True(t, exists, "age stats should exist")
	assert.Equal(t, "age", ageStats.Name)
	assert.Equal(t, "integer", ageStats.DataType)
	assert.Equal(t, int64(1), ageStats.NullCount) // one null value
	assert.Equal(t, int64(3), ageStats.DistinctCount) // 25, 30, 35
	assert.Equal(t, int64(25), ageStats.MinValue.(int64))
	assert.Equal(t, int64(35), ageStats.MaxValue.(int64))
}

func TestTableStatistics_Completeness(t *testing.T) {
	stats := &TableStatistics{
		Name:             "test_table",
		RowCount:         1000,
		SampleCount:      100,
		SampleRatio:      0.1,
		ColumnStats:      make(map[string]*ColumnStatistics),
		Histograms:       make(map[string]*Histogram),
		CollectTimestamp: time.Now(),
		EstimatedRowCount: 1000,
	}

	assert.Equal(t, "test_table", stats.Name)
	assert.Equal(t, int64(1000), stats.RowCount)
	assert.Equal(t, int64(100), stats.SampleCount)
	assert.Equal(t, 0.1, stats.SampleRatio)
	assert.NotNil(t, stats.ColumnStats)
	assert.NotNil(t, stats.Histograms)
	assert.False(t, stats.CollectTimestamp.IsZero())
	assert.Equal(t, int64(1000), stats.EstimatedRowCount)
}

func TestColumnStatistics_DefaultValues(t *testing.T) {
	colStats := &ColumnStatistics{
		Name:          "test_col",
		DataType:      "integer",
		DistinctCount: 100,
		NullCount:     10,
		MinValue:      int64(1),
		MaxValue:      int64(100),
		NullFraction:  0.1,
		AvgWidth:      4.0,
	}

	assert.Equal(t, "test_col", colStats.Name)
	assert.Equal(t, "integer", colStats.DataType)
	assert.Equal(t, int64(100), colStats.DistinctCount)
	assert.Equal(t, int64(10), colStats.NullCount)
	assert.Equal(t, int64(1), colStats.MinValue)
	assert.Equal(t, int64(100), colStats.MaxValue)
	assert.Equal(t, 0.1, colStats.NullFraction)
	assert.Equal(t, 4.0, colStats.AvgWidth)
}

// Benchmark tests
func BenchmarkSamplingCollector_CalculateSampleSize(b *testing.B) {
	mockDS := new(MockDataSource)
	collector := NewSamplingCollector(mockDS, 0.05)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.calculateSampleSize(100000)
	}
}

func BenchmarkInferDataType(b *testing.B) {
	rows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Charlie"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inferDataType(rows, "id")
	}
}

func BenchmarkCompareValues(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compareValues(int64(i), int64(i+1))
	}
}
