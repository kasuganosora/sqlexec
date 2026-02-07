package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/container"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// EnhancedOptimizerWrapper wraps the original EnhancedOptimizer to provide DI capabilities.
// It uses the original implementation but gets dependencies from the container.
type EnhancedOptimizerWrapper struct {
	original      *EnhancedOptimizer
	container     container.Container
	dataSource    domain.DataSource
	parallelism   int
}

// NewEnhancedOptimizerWrapper creates a wrapper that uses DI container.
func NewEnhancedOptimizerWrapper(dataSource domain.DataSource, parallelism int, ctr container.Container) *EnhancedOptimizerWrapper {
	// Create original enhanced optimizer (temporary)
	// We'll replace its fields with DI versions
	original := NewEnhancedOptimizer(dataSource, parallelism)

	// Get services from container and update the original optimizer
	if costModel, exists := ctr.Get("cost.model.adaptive"); exists {
		if cm, ok := costModel.(cost.CostModel); ok {
			original.costModelV2 = cm
		}
	}

	if statsCache, exists := ctr.Get("stats.cache.auto_refresh"); exists {
		if sc, ok := statsCache.(*statistics.AutoRefreshStatisticsCache); ok {
			original.statsCache = sc
		}
	}

	return &EnhancedOptimizerWrapper{
		original:    original,
		container:   ctr,
		dataSource:  dataSource,
		parallelism: parallelism,
	}
}

// Optimize delegates to the original optimizer but with DI-managed dependencies.
func (w *EnhancedOptimizerWrapper) Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error) {
	fmt.Println("=== Enhanced Optimizer Wrapper (DI) Started ===")

	// The original optimizer's Optimize method will use the DI-managed dependencies
	// that we injected in the constructor
	return w.original.Optimize(ctx, stmt)
}

// GetOriginal returns the original EnhancedOptimizer (for testing/comparison).
func (w *EnhancedOptimizerWrapper) GetOriginal() *EnhancedOptimizer {
	return w.original
}

// GetContainer returns the DI container.
func (w *EnhancedOptimizerWrapper) GetContainer() container.Container {
	return w.container
}

// Explain returns explanation of the optimizer configuration.
func (w *EnhancedOptimizerWrapper) Explain() string {
	return w.original.Explain()
}

// SetParallelism sets the parallelism.
func (w *EnhancedOptimizerWrapper) SetParallelism(parallelism int) {
	w.parallelism = parallelism
	w.original.SetParallelism(parallelism)
}

// GetParallelism gets the parallelism.
func (w *EnhancedOptimizerWrapper) GetParallelism() int {
	return w.original.GetParallelism()
}

// GetStatisticsCache gets the statistics cache.
func (w *EnhancedOptimizerWrapper) GetStatisticsCache() *statistics.AutoRefreshStatisticsCache {
	return w.original.GetStatisticsCache()
}

// Builder pattern for EnhancedOptimizerWrapper

// EnhancedOptimizerWrapperBuilder builds EnhancedOptimizerWrapper instances.
type EnhancedOptimizerWrapperBuilder struct {
	dataSource  domain.DataSource
	parallelism int
	services    map[string]interface{}
}

// NewEnhancedOptimizerWrapperBuilder creates a new builder.
func NewEnhancedOptimizerWrapperBuilder(dataSource domain.DataSource) *EnhancedOptimizerWrapperBuilder {
	return &EnhancedOptimizerWrapperBuilder{
		dataSource:  dataSource,
		parallelism: 0,
		services:    make(map[string]interface{}),
	}
}

// WithParallelism sets the parallelism.
func (b *EnhancedOptimizerWrapperBuilder) WithParallelism(parallelism int) *EnhancedOptimizerWrapperBuilder {
	b.parallelism = parallelism
	return b
}

// WithService adds a custom service.
func (b *EnhancedOptimizerWrapperBuilder) WithService(name string, service interface{}) *EnhancedOptimizerWrapperBuilder {
	b.services[name] = service
	return b
}

// Build builds the EnhancedOptimizerWrapper.
func (b *EnhancedOptimizerWrapperBuilder) Build(ctr container.Container) *EnhancedOptimizerWrapper {
	return NewEnhancedOptimizerWrapper(b.dataSource, b.parallelism, ctr)
}
