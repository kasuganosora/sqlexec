package container

import (
	"fmt"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/feedback"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/index"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/join"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// defaultContainer is the default implementation of Container.
// It uses a simple map to store services and provides thread-safe access.
type defaultContainer struct {
	mu         sync.RWMutex
	services   map[string]interface{}
	dataSource domain.DataSource
}

// NewContainer creates a new DI container with the given data source.
// It automatically registers default services like statistics cache,
// cardinality estimator, cost model, etc.
func NewContainer(dataSource domain.DataSource) Container {
	c := &defaultContainer{
		services:   make(map[string]interface{}),
		dataSource: dataSource,
	}
	c.registerDefaults()
	return c
}

// registerDefaults registers default services that are commonly used.
// This includes statistics cache, estimators, cost models, etc.
func (c *defaultContainer) registerDefaults() {
	// Register statistics cache
	autoRefreshStatsCache := statistics.NewAutoRefreshStatisticsCache(
		statistics.NewSamplingCollector(c.dataSource, 0.02), // 2% sampling
		c.dataSource,
		24*time.Hour, // 24h TTL
	)
	c.Register("stats.cache.auto_refresh", autoRefreshStatsCache)

	// Register base statistics cache
	statsCache := statistics.NewStatisticsCache(24 * time.Hour)
	c.Register("stats.cache.base", statsCache)

	// Register cardinality estimator
	estimator := statistics.NewEnhancedCardinalityEstimator(statsCache)
	c.Register("estimator.enhanced", estimator)

	// Register cost model with adapter
	costModel := cost.NewAdaptiveCostModel(&costCardinalityAdapter{estimator: estimator})
	// Wire DQ feedback for cost calibration
	costModel.SetFeedback(feedback.GetGlobalFeedback())
	c.Register("cost.model.adaptive", costModel)

	// Register index selector
	indexSelector := index.NewIndexSelector(estimator)
	c.Register("index.selector", indexSelector)

	// Register cost model adapter for join operations
	costModelAdapter := &joinCostAdapter{costModel: costModel}
	c.Register("adapter.cost_model.join", costModelAdapter)

	// Register cardinality adapter for join operations
	cardinalityAdapter := &joinCardinalityAdapter{estimator: estimator}
	c.Register("adapter.cardinality.join", cardinalityAdapter)

	// Register DP join reorder
	dpJoinReorder := join.NewDPJoinReorder(costModelAdapter, cardinalityAdapter, 10)
	c.Register("join.reorder.dp", dpJoinReorder)

	// Register Bushy join tree builder
	bushyTree := join.NewBushyJoinTreeBuilder(costModel, cardinalityAdapter, 3)
	c.Register("join.bushy_tree", bushyTree)

	// Register hints parser
	hintsParser := parser.NewHintsParser()
	c.Register("parser.hints", hintsParser)
}

// Register registers a service with the given name.
// If a service with the same name already exists, it will be overwritten.
func (c *defaultContainer) Register(name string, service interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.services[name] = service
}

// Get retrieves a service by name.
// Returns the service and true if found, or nil and false if not found.
func (c *defaultContainer) Get(name string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	service, exists := c.services[name]
	return service, exists
}

// MustGet retrieves a service by name.
// Returns the service, or panics if not found.
func (c *defaultContainer) MustGet(name string) interface{} {
	if service, exists := c.Get(name); exists {
		return service
	}
	panic(fmt.Sprintf("service '%s' not found in container", name))
}

// Has checks if a service with the given name exists.
func (c *defaultContainer) Has(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.services[name]
	return exists
}

// BuildOptimizer builds a basic optimizer using services from the container.
func (c *defaultContainer) BuildOptimizer() interface{} {
	// This will be implemented in phase 3 when we refactor the optimizer
	// For now, return nil to indicate it's not yet implemented
	return nil
}

// BuildEnhancedOptimizer builds an enhanced optimizer with advanced features.
func (c *defaultContainer) BuildEnhancedOptimizer(parallelism int) interface{} {
	// Use the builder pattern from optimizer package
	// This creates an EnhancedOptimizer with dependencies from the container
	
	// Get required dependencies
	costModel, ok := c.Get("cost.model.adaptive")
	if !ok {
		panic("cost.model.adaptive not found in container")
	}
	
	indexSelector, ok := c.Get("index.selector")
	if !ok {
		panic("index.selector not found in container")
	}
	
	statsCache, ok := c.Get("stats.cache.auto_refresh")
	if !ok {
		panic("stats.cache.auto_refresh not found in container")
	}
	
	estimator, ok := c.Get("estimator.enhanced")
	if !ok {
		panic("estimator.enhanced not found in container")
	}
	
	// Get optional dependencies
	var dpJoinReorder interface{}
	var bushyTree interface{}
	var hintsParser interface{}
	
	if jr, ok := c.Get("join.reorder.dp"); ok {
		dpJoinReorder = jr
	}
	if bt, ok := c.Get("join.bushy_tree"); ok {
		bushyTree = bt
	}
	if hp, ok := c.Get("parser.hints"); ok {
		hintsParser = hp
	} else {
		hintsParser = nil // Will use default
	}
	
	// Create a map with all dependencies for the optimizer
	// The actual EnhancedOptimizer construction is done in the optimizer package
	// This returns a DIConfig that can be used by the optimizer builder
	return &EnhancedOptimizerConfig{
		DataSource:      c.dataSource,
		Parallelism:     parallelism,
		CostModel:       costModel,
		IndexSelector:   indexSelector,
		StatsCache:      statsCache,
		Estimator:       estimator,
		DPJoinReorder:   dpJoinReorder,
		BushyTree:       bushyTree,
		HintsParser:     hintsParser,
	}
}

// EnhancedOptimizerConfig holds the configuration for building an EnhancedOptimizer
// This is used to pass dependencies from the container to the optimizer package
type EnhancedOptimizerConfig struct {
	DataSource      domain.DataSource
	Parallelism     int
	CostModel       interface{}
	IndexSelector   interface{}
	StatsCache      interface{}
	Estimator       interface{}
	DPJoinReorder   interface{}
	BushyTree       interface{}
	HintsParser     interface{}
}

// BuildExecutor builds a plan executor using services from the container.
func (c *defaultContainer) BuildExecutor() interface{} {
	// This will be implemented in phase 4 when we refactor optimized_executor.go
	// For now, return nil to indicate it's not yet implemented
	return nil
}

// BuildShowProcessor builds a SHOW statement processor.
func (c *defaultContainer) BuildShowProcessor() interface{} {
	// Import the optimizer package's DefaultShowProcessor
	// Since we're in the container package, we can't directly import optimizer
	// This will be handled by the caller using the container
	return nil
}

// BuildVariableManager builds a variable manager.
func (c *defaultContainer) BuildVariableManager() interface{} {
	// This will be handled by the caller
	return nil
}

// BuildExpressionEvaluator builds an expression evaluator.
func (c *defaultContainer) BuildExpressionEvaluator() interface{} {
	// This will be handled by the caller
	return nil
}

// GetDataSource returns the data source used by the container.
func (c *defaultContainer) GetDataSource() domain.DataSource {
	return c.dataSource
}

// adapter implementations

type costCardinalityAdapter struct {
	estimator interface{}
}

func (a *costCardinalityAdapter) EstimateTableScan(tableName string) int64 {
	if est, ok := a.estimator.(interface{ EstimateTableScan(string) int64 }); ok {
		return est.EstimateTableScan(tableName)
	}
	return 10000 // default
}

func (a *costCardinalityAdapter) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	if est, ok := a.estimator.(interface{ EstimateFilter(string, []domain.Filter) int64 }); ok {
		return est.EstimateFilter(tableName, filters)
	}
	return 1000 // default
}

type joinCostAdapter struct {
	costModel interface{}
}

func (a *joinCostAdapter) ScanCost(tableName string, rowCount int64, useIndex bool) float64 {
	if cm, ok := a.costModel.(interface{ ScanCost(string, int64, bool) float64 }); ok {
		return cm.ScanCost(tableName, rowCount, useIndex)
	}
	return 0.0
}

func (a *joinCostAdapter) JoinCost(left, right join.LogicalPlan, joinType join.JoinType, conditions []*parser.Expression) float64 {
	if cm, ok := a.costModel.(interface {
		JoinCost(interface{}, interface{}, join.JoinType, []*parser.Expression) float64
	}); ok {
		return cm.JoinCost(left, right, joinType, conditions)
	}
	return 0.0
}

type joinCardinalityAdapter struct {
	estimator interface{}
}

func (a *joinCardinalityAdapter) EstimateTableScan(tableName string) int64 {
	if est, ok := a.estimator.(interface{ EstimateTableScan(string) int64 }); ok {
		return est.EstimateTableScan(tableName)
	}
	return 10000 // default
}
