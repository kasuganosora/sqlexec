package index

import (
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// FilterAnalysis contains analysis results for a filter condition.
type FilterAnalysis struct {
	// Filter is the original filter condition.
	Filter domain.Filter

	// IsIndexable indicates whether this filter can use an index.
	IsIndexable bool

	// Selectivity estimates the fraction of rows that will pass this filter (0-1).
	Selectivity float64

	// ApplicableIndexes lists indexes that could potentially be used for this filter.
	ApplicableIndexes []string

	// BestIndexUsage describes how the best index would be used for this filter.
	BestIndexUsage string
}

// IndexAdvisor provides index recommendations based on query patterns.
type IndexAdvisor interface {
	// AnalyzeQueries analyzes a set of queries and recommends indexes.
	// queries: list of queries to analyze
	// Returns a list of index recommendations.
	AnalyzeQueries(queries []QueryInfo) []IndexRecommendation

	// GetIndexBenefit estimates the benefit of creating a new index.
	// tableName: name of the table
	// columns: columns in the proposed index
	// Returns the estimated benefit score.
	GetIndexBenefit(tableName string, columns []string) float64

	// SimulateIndex simulates the effect of creating an index without actually creating it.
	// This is useful for what-if analysis.
	// tableName: name of the table
	// columns: columns in the simulated index
	// Returns statistics about the simulated index.
	SimulateIndex(tableName string, columns []string) *SimulatedIndexStats
}

// QueryInfo contains information about a query for index analysis.
type QueryInfo struct {
	// SQL is the query SQL text.
	SQL string

	// Frequency is how often this query is executed.
	Frequency int

	// ExecutionTime is the average execution time.
	ExecutionTime time.Duration

	// Filters are the filter conditions in the query.
	Filters []domain.Filter

	// SortColumns are columns used in ORDER BY.
	SortColumns []string

	// GroupColumns are columns used in GROUP BY.
	GroupColumns []string

	// JoinColumns are columns used in JOIN conditions.
	JoinColumns []string
}

// IndexRecommendation is a recommendation to create an index.
type IndexRecommendation struct {
	// TableName is the name of the table.
	TableName string

	// Columns are the recommended index columns.
	Columns []string

	// Benefit is the estimated performance improvement.
	Benefit float64

	// Cost is the estimated cost of maintaining the index.
	Cost float64

	// Priority indicates the recommendation priority (high, medium, low).
	Priority string

	// Reason explains why this index is recommended.
	Reason string
}

// SimulatedIndexStats contains statistics about a simulated index.
type SimulatedIndexStats struct {
	// SelectivityEstimate is the estimated selectivity of the index.
	SelectivityEstimate float64

	// CardinalityEstimate is the estimated cardinality.
	CardinalityEstimate int64

	// SizeEstimate is the estimated size of the index.
	SizeEstimate int64

	// BenefitScore is the estimated benefit score.
	BenefitScore float64
}
