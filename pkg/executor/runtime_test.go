package executor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRuntime(t *testing.T) {
	runtime := NewRuntime()
	assert.NotNil(t, runtime)
	assert.NotNil(t, runtime.activeQueries)
}

func TestRuntime_RegisterQuery(t *testing.T) {
	runtime := NewRuntime()
	_, cancel := context.WithCancel(context.Background())

	runtime.RegisterQuery("query1", cancel)

	query, err := runtime.GetQueryStatus("query1")
	require.NoError(t, err)
	assert.Equal(t, "query1", query.QueryID)
	assert.Equal(t, "running", query.Status)
	assert.Equal(t, 0.0, query.Progress)
}

func TestRuntime_UnregisterQuery(t *testing.T) {
	runtime := NewRuntime()
	_, cancel := context.WithCancel(context.Background())

	runtime.RegisterQuery("query1", cancel)
	runtime.UnregisterQuery("query1")

	_, err := runtime.GetQueryStatus("query1")
	assert.Error(t, err)
}

func TestRuntime_UpdateProgress(t *testing.T) {
	runtime := NewRuntime()
	_, cancel := context.WithCancel(context.Background())

	runtime.RegisterQuery("query1", cancel)
	runtime.UpdateProgress("query1", 0.5, "processing")

	query, err := runtime.GetQueryStatus("query1")
	require.NoError(t, err)
	assert.Equal(t, 0.5, query.Progress)
	assert.Equal(t, "processing", query.Status)
}

func TestRuntime_CancelQuery(t *testing.T) {
	runtime := NewRuntime()
	_, cancel := context.WithCancel(context.Background())

	runtime.RegisterQuery("query1", cancel)

	err := runtime.CancelQuery("query1")
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	_, err = runtime.GetQueryStatus("query1")
	require.NoError(t, err)
}

func TestRuntime_CancelQuery_NotFound(t *testing.T) {
	runtime := NewRuntime()

	err := runtime.CancelQuery("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query not found")
}

func TestRuntime_GetQueryStatus_NotFound(t *testing.T) {
	runtime := NewRuntime()

	_, err := runtime.GetQueryStatus("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query not found")
}

func TestRuntime_GetAllQueries(t *testing.T) {
	runtime := NewRuntime()
	_, cancel1 := context.WithCancel(context.Background())
	_, cancel2 := context.WithCancel(context.Background())

	runtime.RegisterQuery("query1", cancel1)
	runtime.RegisterQuery("query2", cancel2)

	queries := runtime.GetAllQueries()
	assert.Len(t, queries, 2)
}

func TestRuntime_GetAllQueries_Empty(t *testing.T) {
	runtime := NewRuntime()

	queries := runtime.GetAllQueries()
	assert.Len(t, queries, 0)
}
