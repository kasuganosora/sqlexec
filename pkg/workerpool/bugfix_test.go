package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 9 (P2): ExecuteParallelWithPool double-writes results
// When scanFunc returns an error, the result is written at line 147 inside
// the pool task, then SubmitWait returns the same error and the result is
// overwritten again at line 164. The second write may use a different
// (wrapped) error, losing the original scanFunc error.
// ==========================================================================

func TestBug9_ExecuteParallelWithPool_NoDoubleWrite(t *testing.T) {
	scanErr := errors.New("scan failed")

	sp, err := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		return ScanResult{
			TaskID: task.ID,
			Items:  []interface{}{"partial-data"},
			Error:  scanErr,
		}, scanErr
	})
	require.NoError(t, err)
	require.NoError(t, sp.Start())
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 1, StartIndex: 0, EndIndex: 10},
	}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	assert.Error(t, err)
	require.Len(t, results, 1)

	// The error in result should be the scanFunc error
	assert.Equal(t, scanErr, results[0].Error,
		"result error should be from scanFunc")
}

func TestBug9_ExecuteParallelWithPool_MixedResults(t *testing.T) {
	var callCount int64

	sp, err := NewScanPool(2, func(ctx context.Context, task ScanTask) (ScanResult, error) {
		atomic.AddInt64(&callCount, 1)
		if task.ID == 2 {
			return ScanResult{}, errors.New("task 2 failed")
		}
		return ScanResult{TaskID: task.ID, Items: []interface{}{"ok"}}, nil
	})
	require.NoError(t, err)
	require.NoError(t, sp.Start())
	defer sp.Close()

	tasks := []ScanTask{
		{ID: 1, StartIndex: 0, EndIndex: 10},
		{ID: 2, StartIndex: 10, EndIndex: 20},
		{ID: 3, StartIndex: 20, EndIndex: 30},
	}

	results, err := sp.ExecuteParallelWithPool(context.Background(), tasks)
	assert.Error(t, err)

	errorCount := 0
	successCount := 0
	for _, r := range results {
		if r.Error != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	assert.Equal(t, 1, errorCount, "only 1 task should have failed")
	assert.Equal(t, 2, successCount, "2 tasks should have succeeded")
}
