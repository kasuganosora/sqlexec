package cost

import (
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CostModel is the interface for cost estimation models.
// It provides methods to estimate the cost of different physical operators.
type CostModel interface {
	// ScanCost estimates the cost of scanning a table.
	// tableName: the name of the table to scan
	// rowCount: estimated number of rows to scan
	// useIndex: whether to use an index for the scan
	// Returns the estimated cost.
	ScanCost(tableName string, rowCount int64, useIndex bool) float64

	// FilterCost estimates the cost of applying filters.
	// inputRows: number of input rows
	// selectivity: fraction of rows that pass the filter (0-1)
	// filters: list of filter conditions
	// Returns the estimated cost.
	FilterCost(inputRows int64, selectivity float64, filters []domain.Filter) float64

	// JoinCost estimates the cost of a join operation.
	// left: left input (can be row count or plan)
	// right: right input (can be row count or plan)
	// joinType: type of join (inner, left, right, etc.)
	// conditions: join conditions
	// Returns the estimated cost.
	JoinCost(left, right interface{}, joinType JoinType, conditions []*parser.Expression) float64

	// AggregateCost estimates the cost of aggregation.
	// inputRows: number of input rows
	// groupByCols: number of GROUP BY columns
	// aggFuncs: number of aggregate functions
	// Returns the estimated cost.
	AggregateCost(inputRows int64, groupByCols, aggFuncs int) float64

	// ProjectCost estimates the cost of projection.
	// inputRows: number of input rows
	// projCols: number of projected columns
	// Returns the estimated cost.
	ProjectCost(inputRows int64, projCols int) float64

	// SortCost estimates the cost of sorting.
	// inputRows: number of input rows
	// Returns the estimated cost.
	SortCost(inputRows int64) float64

	// GetCostFactors returns the cost factors used by the model.
	// This is useful for debugging and analysis.
	// Returns a pointer to AdaptiveCostFactor containing the current cost factors.
	GetCostFactors() *AdaptiveCostFactor

	// VectorSearchCost estimates the cost of a vector search operation.
	// indexType: the type of vector index (hnsw, flat, etc.)
	// rowCount: estimated number of vectors in the index
	// k: number of nearest neighbors to search
	// Returns the estimated cost.
	VectorSearchCost(indexType string, rowCount int64, k int) float64
}

// ExtendedCardinalityEstimator is an extended interface for cardinality estimation.
// It extends the base CardinalityEstimator with additional methods.
type ExtendedCardinalityEstimator interface {
	CardinalityEstimator

	// EstimateJoin estimates the number of rows after a join operation.
	// leftRows: estimated rows from left input
	// rightRows: estimated rows from right input
	// joinType: type of join
	// Returns the estimated row count.
	EstimateJoin(leftRows, rightRows int64, joinType JoinType) int64

	// EstimateDistinct estimates the number of distinct values in columns.
	// tableName: name of the table
	// columns: list of column names
	// Returns the estimated distinct count.
	EstimateDistinct(tableName string, columns []string) int64

	// UpdateStatistics updates statistics for a table.
	// This is called when new statistics are available.
	UpdateStatistics(tableName string, stats interface{})
}
