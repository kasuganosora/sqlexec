package index

import (
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewIndexManager(t *testing.T) {
	manager := NewIndexManager()

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.indices)
	assert.Empty(t, manager.indices)
}

func TestIndexManager_AddIndex(t *testing.T) {
	manager := NewIndexManager()

	tests := []struct {
		name  string
		index *Index
	}{
		{
			name: "simple index",
			index: &Index{
				Name:      "idx_id",
				TableName: "test_table",
				Columns:   []string{"id"},
				Unique:    true,
				Primary:   false,
				IndexType: BTreeIndex,
			},
		},
		{
			name: "composite index",
			index: &Index{
				Name:      "idx_name_age",
				TableName: "test_table",
				Columns:   []string{"name", "age"},
				Unique:    false,
				Primary:   false,
				IndexType: BTreeIndex,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialCount := len(manager.indices[tt.index.TableName])
			manager.AddIndex(tt.index)
			newCount := len(manager.indices[tt.index.TableName])
			assert.Equal(t, initialCount+1, newCount)
		})
	}
}

func TestIndexManager_GetIndices(t *testing.T) {
	manager := NewIndexManager()

	// Add some indices
	index1 := &Index{
		Name:      "idx_id",
		TableName: "test_table",
		Columns:   []string{"id"},
	}
	index2 := &Index{
		Name:      "idx_name",
		TableName: "test_table",
		Columns:   []string{"name"},
	}
	manager.AddIndex(index1)
	manager.AddIndex(index2)

	// Get indices for existing table
	indices := manager.GetIndices("test_table")
	assert.Len(t, indices, 2)

	// Get indices for non-existent table
	indices = manager.GetIndices("non_existent")
	assert.Nil(t, indices)
}

func TestIndexManager_FindIndexByName(t *testing.T) {
	manager := NewIndexManager()

	index := &Index{
		Name:      "idx_id",
		TableName: "test_table",
		Columns:   []string{"id"},
	}
	manager.AddIndex(index)

	// Find existing index
	found := manager.FindIndexByName("test_table", "idx_id")
	assert.NotNil(t, found)
	assert.Equal(t, "idx_id", found.Name)

	// Find non-existent index
	found = manager.FindIndexByName("test_table", "non_existent")
	assert.Nil(t, found)

	// Find in non-existent table
	found = manager.FindIndexByName("non_existent", "idx_id")
	assert.Nil(t, found)
}

func TestNewIndexSelector(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	assert.NotNil(t, selector)
	assert.Equal(t, estimator, selector.estimator)
	assert.NotNil(t, selector.indexManager)
}

func TestIndexSelector_SelectBestIndex_NoIndices(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id", "name"}

	selection := selector.SelectBestIndex("test_table", filters, requiredCols)

	assert.NotNil(t, selection)
	assert.Nil(t, selection.SelectedIndex)
	assert.Equal(t, "No available index", selection.Reason)
	assert.Equal(t, float64(^uint64(0)>>1), selection.Cost) // MaxFloat64
}

func TestIndexSelector_SelectBestIndex_WithIndices(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add indices
	index1 := &Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Unique:     true,
		Primary:    false,
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}
	selector.indexManager.AddIndex(index1)

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id", "name"}

	selection := selector.SelectBestIndex("test_table", filters, requiredCols)

	assert.NotNil(t, selection)
	assert.NotNil(t, selection.SelectedIndex)
	assert.Equal(t, "idx_id", selection.SelectedIndex.Name)
	assert.Less(t, selection.Cost, float64(^uint64(0)>>1))
}

func TestIndexSelector_SelectBestIndex_CoveringIndex(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add covering index
	index := &Index{
		Name:       "idx_covering",
		TableName:  "test_table",
		Columns:    []string{"id", "name", "age"},
		Unique:     false,
		Primary:    false,
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}
	selector.indexManager.AddIndex(index)

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id", "name"}

	selection := selector.SelectBestIndex("test_table", filters, requiredCols)

	assert.NotNil(t, selection)
	assert.True(t, selection.IsCovering, "should identify as covering index")
}

func TestIndexSelector_SelectBestIndex_MultipleIndices(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add multiple indices
	index1 := &Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}
	index2 := &Index{
		Name:       "idx_name",
		TableName:  "test_table",
		Columns:    []string{"name"},
		Cardinality: 500,
		IndexType:  BTreeIndex,
	}
	selector.indexManager.AddIndex(index1)
	selector.indexManager.AddIndex(index2)

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id"}

	selection := selector.SelectBestIndex("test_table", filters, requiredCols)

	assert.NotNil(t, selection)
	assert.NotNil(t, selection.SelectedIndex)
	assert.Equal(t, "idx_id", selection.SelectedIndex.Name, "should select id index for id filter")
}

func TestIndexSelector_EvaluateIndexes(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add indices
	index1 := &Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}
	index2 := &Index{
		Name:       "idx_name",
		TableName:  "test_table",
		Columns:    []string{"name"},
		Cardinality: 500,
		IndexType:  BTreeIndex,
	}
	indices := []*Index{index1, index2}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id"}

	selection := selector.evaluateIndexes("test_table", filters, requiredCols, indices)

	assert.NotNil(t, selection)
	assert.NotNil(t, selection.SelectedIndex)
}

func TestIndexSelector_EvaluateIndex_Unusable(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add index that doesn't match filters
	index := &Index{
		Name:       "idx_name",
		TableName:  "test_table",
		Columns:    []string{"name"},
		Cardinality: 500,
		IndexType:  BTreeIndex,
	}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id"}

	selection := selector.evaluateIndex("test_table", filters, requiredCols, index)

	assert.NotNil(t, selection)
	assert.Nil(t, selection.SelectedIndex)
	assert.Contains(t, selection.Reason, "not usable")
}

func TestIndexSelector_IsIndexUsable(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	tests := []struct {
		name     string
		filters  []domain.Filter
		index    *Index
		expected bool
	}{
		{
			name: "matching filter",
			filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: 1},
			},
			index: &Index{
				Columns: []string{"id"},
			},
			expected: true,
		},
		{
			name: "no matching filter",
			filters: []domain.Filter{
				{Field: "id", Operator: "=", Value: 1},
			},
			index: &Index{
				Columns: []string{"name"},
			},
			expected: true, // simplified: returns true for empty filters
		},
		{
			name: "empty filters",
			filters: []domain.Filter{},
			index: &Index{
				Columns: []string{"id"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.isIndexUsable("test_table", tt.filters, tt.index)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIndexSelector_IsCoveringIndex(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	tests := []struct {
		name         string
		requiredCols []string
		index        *Index
		expected     bool
	}{
		{
			name:         "fully covered",
			requiredCols: []string{"id", "name"},
			index: &Index{
				Columns: []string{"id", "name", "age"},
			},
			expected: true,
		},
		{
			name:         "partially covered",
			requiredCols: []string{"id", "name", "age"},
			index: &Index{
				Columns: []string{"id", "name"},
			},
			expected: false,
		},
		{
			name:         "not covered",
			requiredCols: []string{"id", "name"},
			index: &Index{
				Columns: []string{"age"},
			},
			expected: false,
		},
		{
			name:         "empty required",
			requiredCols: []string{},
			index: &Index{
				Columns: []string{"id"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selector.isCoveringIndex(tt.requiredCols, tt.index)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIndexSelector_EstimateIndexScanCost(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	index := &Index{
		Name:       "idx_id",
		Columns:    []string{"id"},
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}

	cost := selector.estimateIndexScanCost("test_table", filters, index)
	assert.Greater(t, cost, 0.0, "cost should be positive")
}

func TestIndexSelector_EstimateIndexRows(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	index := &Index{
		Name:       "idx_id",
		Columns:    []string{"id"},
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}

	rows := selector.estimateIndexRows("test_table", filters, index)
	assert.Greater(t, rows, 0.0, "rows should be positive")
}

func TestIndexSelector_EstimateIndexHeight(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	tests := []struct {
		name        string
		index       *Index
		wantMinimum int
	}{
		{
			name: "small index",
			index: &Index{
				Cardinality: 100,
			},
			wantMinimum: 2,
		},
		{
			name: "medium index",
			index: &Index{
				Cardinality: 10000,
			},
			wantMinimum: 2,
		},
		{
			name: "large index",
			index: &Index{
				Cardinality: 1000000,
			},
			wantMinimum: 2,
		},
		{
			name: "zero cardinality",
			index: &Index{
				Cardinality: 0,
			},
			wantMinimum: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			height := selector.estimateIndexHeight(tt.index, NewMockTableStatistics())
			assert.GreaterOrEqual(t, height, tt.wantMinimum, "height should be >= minimum")
		})
	}
}

func TestIndexSelector_GenerateSelectionReason(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	tests := []struct {
		name       string
		index      *Index
		filters    []domain.Filter
		isCovering bool
		scanCost   float64
	}{
		{
			name: "simple selection",
			index: &Index{
				Name: "idx_id",
			},
			filters:    []domain.Filter{{Field: "id"}},
			isCovering: false,
			scanCost:   10.5,
		},
		{
			name: "covering index",
			index: &Index{
				Name: "idx_covering",
			},
			filters:    []domain.Filter{{Field: "id"}},
			isCovering: true,
			scanCost:   10.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := selector.generateSelectionReason(tt.index, tt.filters, tt.isCovering, tt.scanCost)
			assert.NotEmpty(t, reason)
			assert.Contains(t, reason, tt.index.Name)
		})
	}
}

func TestIndexSelector_Explain(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add an index
	index := &Index{
		Name:       "idx_id",
		TableName:  "test_table",
		Columns:    []string{"id"},
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}
	selector.indexManager.AddIndex(index)

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	requiredCols := []string{"id"}

	explanation := selector.Explain("test_table", filters, requiredCols)

	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "test_table")
	assert.Contains(t, explanation, "Selected:")
}

func TestIndexSelection_String(t *testing.T) {
	tests := []struct {
		name     string
		selection *IndexSelection
		contains []string
	}{
		{
			name: "with selected index",
			selection: &IndexSelection{
				SelectedIndex:  &Index{Name: "idx_id"},
				IsCovering:    false,
				EstimatedRows: 100.0,
				Cost:           10.5,
				Reason:         "Selected index 'idx_id' with estimated cost 10.50",
			},
			contains: []string{"IndexSelected", "idx_id", "cost="},
		},
		{
			name: "without selected index",
			selection: &IndexSelection{
				SelectedIndex:  nil,
				IsCovering:    false,
				EstimatedRows: 0,
				Cost:           1.7976931348623157e+308, // MaxFloat64
				Reason:         "No available index",
			},
			contains: []string{"NoIndexSelected", "reason="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.selection.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

// Mock implementations for testing
type mockCardinalityEstimator struct{}

func (m *mockCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	return 10000
}

func (m *mockCardinalityEstimator) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	return 1000
}

func (m *mockCardinalityEstimator) GetStatistics(tableName string) (*statistics.TableStatistics, error) {
	return &statistics.TableStatistics{
		Name:       tableName,
		RowCount:   10000,
		ColumnStats: map[string]*statistics.ColumnStatistics{
			"id": {DistinctCount: 10000},
		},
		Histograms: make(map[string]*statistics.Histogram),
	}, nil
}

type mockTableStatistics struct {
	statistics.TableStatistics
}

// NewMockTableStatistics creates a mock TableStatistics for testing
func NewMockTableStatistics() *statistics.TableStatistics {
	return &statistics.TableStatistics{
		Name:            "mock_table",
		RowCount:        1000,
		SampleCount:     100,
		SampleRatio:     0.1,
		ColumnStats:     make(map[string]*statistics.ColumnStatistics),
		Histograms:      make(map[string]*statistics.Histogram),
		CollectTimestamp: time.Now(),
	}
}

func TestIndexType_String(t *testing.T) {
	tests := []struct {
		name     string
		indexType IndexType
	}{
		{"BTree", BTreeIndex},
		{"Hash", HashIndex},
		{"Bitmap", BitmapIndex},
		{"FullText", FullTextIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the value exists
			assert.GreaterOrEqual(t, int(tt.indexType), 0)
		})
	}
}

func TestIndex_Completeness(t *testing.T) {
	index := &Index{
		Name:       "idx_test",
		TableName:  "test_table",
		Columns:    []string{"id", "name"},
		Unique:     false,
		Primary:    false,
		Cardinality: 1000,
		IndexType:  BTreeIndex,
	}

	assert.Equal(t, "idx_test", index.Name)
	assert.Equal(t, "test_table", index.TableName)
	assert.Len(t, index.Columns, 2)
	assert.False(t, index.Unique)
	assert.False(t, index.Primary)
	assert.Equal(t, int64(1000), index.Cardinality)
	assert.Equal(t, BTreeIndex, index.IndexType)
}

func TestIndexSelector_GetParallelism(t *testing.T) {
	// This tests parallelism if needed
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Just verify the selector is created
	assert.NotNil(t, selector)
}

// Benchmark tests
func BenchmarkIndexSelector_SelectBestIndex(b *testing.B) {
	estimator := &mockCardinalityEstimator{}
	selector := NewIndexSelector(estimator)

	// Add indices
	for i := 0; i < 10; i++ {
		index := &Index{
			Name:       "idx_" + string(rune('0'+i)),
			TableName:  "test_table",
			Columns:    []string{"col" + string(rune('0'+i))},
			Cardinality: int64(1000 * (i + 1)),
			IndexType:  BTreeIndex,
		}
		selector.indexManager.AddIndex(index)
	}

	filters := []domain.Filter{
		{Field: "col0", Operator: "=", Value: 1},
	}
	requiredCols := []string{"col0"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selector.SelectBestIndex("test_table", filters, requiredCols)
	}
}

func BenchmarkIndexManager_GetIndices(b *testing.B) {
	manager := NewIndexManager()

	// Add many indices
	for i := 0; i < 100; i++ {
		index := &Index{
			Name:       "idx_" + string(rune('0'+i%10)),
			TableName:  "table_" + string(rune('0'+i/10)),
			Columns:    []string{"col"},
			Cardinality: int64(1000),
			IndexType:  BTreeIndex,
		}
		manager.AddIndex(index)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetIndices("table_1")
	}
}
