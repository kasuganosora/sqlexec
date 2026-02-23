package executor

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAggregator(t *testing.T) {
	agg := NewAggregator()
	assert.NotNil(t, agg)
	assert.NotNil(t, agg.results)
	assert.Equal(t, 0, agg.Count())
}

func TestAggregator_AddResult(t *testing.T) {
	agg := NewAggregator()

	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}

	agg.AddResult(result1)
	assert.Equal(t, 1, agg.Count())
}

func TestAggregator_Aggregate_SingleResult(t *testing.T) {
	agg := NewAggregator()

	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}

	agg.AddResult(result1)

	merged, err := agg.Aggregate()
	require.NoError(t, err)
	assert.Equal(t, 1, len(merged.Rows))
	assert.Equal(t, "Alice", merged.Rows[0]["name"])
}

func TestAggregator_Aggregate_MultipleResults(t *testing.T) {
	agg := NewAggregator()

	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice"},
		},
	}

	result2 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 2, "name": "Bob"},
		},
	}

	agg.AddResult(result1)
	agg.AddResult(result2)

	merged, err := agg.Aggregate()
	require.NoError(t, err)
	assert.Equal(t, 2, len(merged.Rows))
}

func TestAggregator_Aggregate_NoResults(t *testing.T) {
	agg := NewAggregator()

	_, err := agg.Aggregate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no results to aggregate")
}

func TestAggregator_Clear(t *testing.T) {
	agg := NewAggregator()

	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}

	agg.AddResult(result1)
	assert.Equal(t, 1, agg.Count())

	agg.Clear()
	assert.Equal(t, 0, agg.Count())
}

func TestAggregator_Count(t *testing.T) {
	agg := NewAggregator()

	assert.Equal(t, 0, agg.Count())

	result1 := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{},
	}

	agg.AddResult(result1)
	assert.Equal(t, 1, agg.Count())
}
