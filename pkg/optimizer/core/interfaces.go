package core

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// Optimizer is the interface for query optimizers.
// It takes a SQL statement and produces an optimized execution plan.
type Optimizer interface {
	// Optimize optimizes the given SQL statement and returns an execution plan.
	// Returns an error if optimization fails.
	Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error)

	// Explain returns a string representation of the optimizer's configuration.
	Explain() string
}

// OptimizerFactory is the interface for creating optimizer instances.
// It provides factory methods for different types of optimizers.
type OptimizerFactory interface {
	// CreateOptimizer creates a basic optimizer for the given data source.
	CreateOptimizer(dataSource interface{}) Optimizer

	// CreateEnhancedOptimizer creates an enhanced optimizer with advanced features
	// such as cost-based optimization, index selection, and join reordering.
	CreateEnhancedOptimizer(dataSource interface{}, parallelism int) Optimizer
}
