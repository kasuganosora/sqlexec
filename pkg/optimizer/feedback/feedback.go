package feedback

import (
	"sync"
)

// ExecutionFeedback collects runtime execution statistics to calibrate
// the cost model, inspired by the DQ paper's reward feedback mechanism.
// As queries execute, actual row counts and selectivities are recorded
// and used to improve future cost estimates.
type ExecutionFeedback struct {
	mu          sync.RWMutex
	tableSizes  map[string]int64   // table → actual row count
	selectivity map[string]float64 // column → observed selectivity
	joinFactors map[string]float64 // "left|right" → observed join factor
	sampleCount map[string]int64   // key → number of observations
}

var globalFeedback *ExecutionFeedback
var feedbackOnce sync.Once

// GetGlobalFeedback returns the singleton ExecutionFeedback instance.
func GetGlobalFeedback() *ExecutionFeedback {
	feedbackOnce.Do(func() {
		globalFeedback = &ExecutionFeedback{
			tableSizes:  make(map[string]int64),
			selectivity: make(map[string]float64),
			joinFactors: make(map[string]float64),
			sampleCount: make(map[string]int64),
		}
	})
	return globalFeedback
}

// RecordTableSize records the actual row count for a table.
// Uses exponential moving average to smooth over fluctuations.
func (ef *ExecutionFeedback) RecordTableSize(table string, rows int64) {
	ef.mu.Lock()
	defer ef.mu.Unlock()

	key := "table:" + table
	count := ef.sampleCount[key]

	if count == 0 {
		ef.tableSizes[table] = rows
	} else {
		// EMA: new = old * 0.7 + observed * 0.3
		old := ef.tableSizes[table]
		ef.tableSizes[table] = int64(float64(old)*0.7 + float64(rows)*0.3)
	}

	ef.sampleCount[key] = count + 1
}

// RecordSelectivity records the observed selectivity for a filter column.
func (ef *ExecutionFeedback) RecordSelectivity(column string, inputRows, outputRows int64) {
	if inputRows <= 0 {
		return
	}

	observed := float64(outputRows) / float64(inputRows)

	ef.mu.Lock()
	defer ef.mu.Unlock()

	key := "sel:" + column
	count := ef.sampleCount[key]

	if count == 0 {
		ef.selectivity[column] = observed
	} else {
		old := ef.selectivity[column]
		ef.selectivity[column] = old*0.7 + observed*0.3
	}

	ef.sampleCount[key] = count + 1
}

// RecordJoinFactor records the observed join output factor.
func (ef *ExecutionFeedback) RecordJoinFactor(leftTable, rightTable string, leftRows, rightRows, outputRows int64) {
	if leftRows <= 0 || rightRows <= 0 {
		return
	}

	observed := float64(outputRows) / (float64(leftRows) * float64(rightRows))
	joinKey := leftTable + "|" + rightTable

	ef.mu.Lock()
	defer ef.mu.Unlock()

	key := "join:" + joinKey
	count := ef.sampleCount[key]

	if count == 0 {
		ef.joinFactors[joinKey] = observed
	} else {
		old := ef.joinFactors[joinKey]
		ef.joinFactors[joinKey] = old*0.7 + observed*0.3
	}

	ef.sampleCount[key] = count + 1
}

// GetTableSize returns the learned table size, if available.
func (ef *ExecutionFeedback) GetTableSize(table string) (int64, bool) {
	ef.mu.RLock()
	defer ef.mu.RUnlock()

	size, ok := ef.tableSizes[table]
	return size, ok
}

// GetSelectivity returns the learned selectivity for a column.
// Returns the default value (0.1) if no data is available.
func (ef *ExecutionFeedback) GetSelectivity(column string) float64 {
	ef.mu.RLock()
	defer ef.mu.RUnlock()

	if sel, ok := ef.selectivity[column]; ok {
		return sel
	}
	return 0.1 // default selectivity
}

// GetJoinFactor returns the learned join factor for a table pair.
// Returns the default value (0.1) if no data is available.
func (ef *ExecutionFeedback) GetJoinFactor(leftTable, rightTable string) float64 {
	ef.mu.RLock()
	defer ef.mu.RUnlock()

	joinKey := leftTable + "|" + rightTable
	if factor, ok := ef.joinFactors[joinKey]; ok {
		return factor
	}

	// Try reverse order
	reverseKey := rightTable + "|" + leftTable
	if factor, ok := ef.joinFactors[reverseKey]; ok {
		return factor
	}

	return 0.1 // default join factor
}

// Reset clears all collected feedback data.
func (ef *ExecutionFeedback) Reset() {
	ef.mu.Lock()
	defer ef.mu.Unlock()

	ef.tableSizes = make(map[string]int64)
	ef.selectivity = make(map[string]float64)
	ef.joinFactors = make(map[string]float64)
	ef.sampleCount = make(map[string]int64)
}
