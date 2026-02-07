package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/container"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/index"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// EnhancedOptimizerV2 is the refactored enhanced optimizer using dependency injection.
// It depends on interfaces rather than concrete implementations.
type EnhancedOptimizerV2 struct {
	baseOptimizer   *Optimizer
	costModel       cost.CostModel
	indexSelector   interface{}
	dpJoinReorder   interface{}
	bushyTree       interface{}
	statsCache      *statistics.AutoRefreshStatisticsCache
	parallelism     int
	estimator        cost.ExtendedCardinalityEstimator
	hintsParser     *parser.HintsParser
	dataSource      domain.DataSource
	container       container.Container
}

// NewEnhancedOptimizerV2 creates a new enhanced optimizer using dependency injection.
// It uses services from the provided container.
func NewEnhancedOptimizerV2(dataSource domain.DataSource, parallelism int, ctr container.Container) *EnhancedOptimizerV2 {
	// Create base optimizer
	baseOptimizer := NewOptimizer(dataSource)

	// Get services from container
	estimator := ctr.MustGet("estimator.enhanced").(cost.ExtendedCardinalityEstimator)
	costModel := ctr.MustGet("cost.model.adaptive").(cost.CostModel)
	indexSelector := ctr.MustGet("index.selector")
	dpJoinReorder := ctr.MustGet("join.reorder.dp")
	bushyTree := ctr.MustGet("join.bushy_tree")
	statsCache := ctr.MustGet("stats.cache.auto_refresh").(*statistics.AutoRefreshStatisticsCache)
	hintsParser := ctr.MustGet("parser.hints").(*parser.HintsParser)

	return &EnhancedOptimizerV2{
		baseOptimizer:   baseOptimizer,
		costModel:       costModel,
		indexSelector:   indexSelector,
		dpJoinReorder:   dpJoinReorder,
		bushyTree:       bushyTree,
		statsCache:      statsCache,
		parallelism:     parallelism,
		estimator:       estimator,
		hintsParser:     hintsParser,
		dataSource:      dataSource,
		container:       ctr,
	}
}

// Optimize optimizes a SQL statement using the enhanced optimizer.
func (eo *EnhancedOptimizerV2) Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error) {
	fmt.Println("=== Enhanced Optimizer V2 (DI) Started ===")

	// 1. Parse Hints (if any in SQL)
	var hints *OptimizerHints
	if stmt != nil && stmt.RawSQL != "" {
		parsedHints, cleanSQLStr, err := eo.hintsParser.ExtractHintsFromSQL(stmt.RawSQL)
		if err != nil {
			fmt.Printf("  [HINTS] Warning: Failed to parse hints: %v\n", err)
			hints = &OptimizerHints{}
		} else {
			// Convert ParsedHints to OptimizerHints
			hints = convertParsedHints(parsedHints)
			if hints != nil {
				fmt.Printf("  [HINTS] Parsed hints from SQL\n")
			}
			// Update SQL (remove hints)
			if cleanSQLStr != "" {
				stmt.RawSQL = cleanSQLStr
			}
		}
	} else {
		hints = &OptimizerHints{}
	}

	// 2. Convert to logical plan
	logicalPlan, err := eo.baseOptimizer.convertToLogicalPlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("convert to logical plan failed: %w", err)
	}
	fmt.Printf("  [ENHANCED V2] Logical Plan: %s\n", logicalPlan.Explain())

	// 3. Create enhanced optimization context (with hints)
	optCtx := &OptimizationContext{
		DataSource:   eo.baseOptimizer.dataSource,
		TableInfo:    make(map[string]*domain.TableInfo),
		Stats:        make(map[string]*Statistics),
		CostModel:    NewDefaultCostModel(),
		Hints:        hints,
	}

	// 4. Apply enhanced optimization rules (with hint-aware rules)
	optimizedPlan, err := eo.applyEnhancedRules(ctx, logicalPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("apply enhanced rules failed: %w", err)
	}
	fmt.Printf("  [ENHANCED V2] Optimized Plan: %s\n", optimizedPlan.Explain())

	// 5. Convert to serializable Plan (enhanced version) using PlanConverter
	converter := NewPlanConverter(eo.costModel, &indexSelectorAdapter{selector: eo.indexSelector})
	executionPlan, err := converter.ConvertToPlan(ctx, optimizedPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("convert to plan failed: %w", err)
	}
	fmt.Printf("  [ENHANCED V2] Execution Plan generated\n")

	return executionPlan, nil
}

// cardinalityEstimatorAdapterV2 adapts cost.ExtendedCardinalityEstimator to optimizer.CardinalityEstimator
type cardinalityEstimatorAdapterV2 struct {
	estimator cost.ExtendedCardinalityEstimator
}

func (a *cardinalityEstimatorAdapterV2) EstimateTableScan(tableName string) int64 {
	return a.estimator.EstimateTableScan(tableName)
}

func (a *cardinalityEstimatorAdapterV2) EstimateFilter(table string, filters []domain.Filter) int64 {
	return a.estimator.EstimateFilter(table, filters)
}

func (a *cardinalityEstimatorAdapterV2) EstimateJoin(left, right LogicalPlan, joinType JoinType) int64 {
	// Get row counts from plans
	leftRows := estimatePlanRows(left, a)
	rightRows := estimatePlanRows(right, a)
	
	// Convert JoinType to cost.JoinType
	var costJoinType cost.JoinType
	switch joinType {
	case InnerJoin:
		costJoinType = cost.InnerJoin
	case LeftOuterJoin:
		costJoinType = cost.LeftOuterJoin
	case RightOuterJoin:
		costJoinType = cost.RightOuterJoin
	case FullOuterJoin:
		costJoinType = cost.FullOuterJoin
	default:
		costJoinType = cost.InnerJoin
	}
	
	return a.estimator.EstimateJoin(leftRows, rightRows, costJoinType)
}

func (a *cardinalityEstimatorAdapterV2) EstimateDistinct(table string, columns []string) int64 {
	return a.estimator.EstimateDistinct(table, columns)
}

func (a *cardinalityEstimatorAdapterV2) UpdateStatistics(tableName string, stats *TableStatistics) {
	a.estimator.UpdateStatistics(tableName, stats)
}

func estimatePlanRows(plan LogicalPlan, estimator CardinalityEstimator) int64 {
	if plan == nil {
		return 0
	}
	if ds, ok := plan.(*LogicalDataSource); ok {
		return estimator.EstimateTableScan(ds.TableName)
	}
	return 10000 // default
}

// indexSelectorAdapter adapts the interface{} indexSelector to IndexSelector interface
type indexSelectorAdapter struct {
	selector interface{}
}

func (a *indexSelectorAdapter) SelectBestIndex(tableName string, filters []domain.Filter, requiredCols []string) *index.IndexSelection {
	if a.selector == nil {
		return &index.IndexSelection{
			SelectedIndex: nil,
			Reason:        "No index selector available",
			Cost:          0,
		}
	}

	// Try to call SelectBestIndex method using type assertion
	type selectorInterface interface {
		SelectBestIndex(string, []domain.Filter, []string) *index.IndexSelection
	}

	if s, ok := a.selector.(selectorInterface); ok {
		return s.SelectBestIndex(tableName, filters, requiredCols)
	}

	return &index.IndexSelection{
		SelectedIndex: nil,
		Reason:        "Index selector does not implement required interface",
		Cost:          0,
	}
}

// Helper methods for optimization rules

// applyEnhancedRules applies enhanced optimization rules with support for hints.
func (eo *EnhancedOptimizerV2) applyEnhancedRules(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// Create enhanced rule set with adapter
	estimatorAdapter := &cardinalityEstimatorAdapterV2{estimator: eo.estimator}
	enhancedRuleSet := EnhancedRuleSet(estimatorAdapter)

	// Add advanced rules (DP Join Reorder, Bushy Tree, Index Selection)
	advancedRules := []OptimizationRule{
		// DP JOIN Reorder (when no hints)
		&DPJoinReorderAdapterV2{
			dpReorder: eo.dpJoinReorder,
		},
		// Bushy Join Tree
		&BushyTreeAdapterV2{
			bushyTree: eo.bushyTree,
		},
		// Index Selection
		&IndexSelectionAdapterV2{
			indexSelector: eo.indexSelector,
			costModel:     eo.costModel,
		},
	}

	// Combine EnhancedRuleSet with advanced rules
	allRules := append(enhancedRuleSet, advancedRules...)
	ruleSet := RuleSet(allRules)

	fmt.Println("  [ENHANCED V2] Applying enhanced optimization rules...")
	fmt.Printf("  [ENHANCED V2] Total rules: %d\n", len(allRules))
	for i, r := range allRules {
		fmt.Printf("  [ENHANCED V2]   Rule %d: %s\n", i, r.Name())
	}

	optimizedPlan, err := ruleSet.Apply(ctx, plan, optCtx)
	if err != nil {
		return nil, err
	}

	return optimizedPlan, nil
}

// SetParallelism sets the parallelism for the optimizer.
func (eo *EnhancedOptimizerV2) SetParallelism(parallelism int) {
	eo.parallelism = parallelism
}

// GetParallelism returns the current parallelism.
func (eo *EnhancedOptimizerV2) GetParallelism() int {
	return eo.parallelism
}

// GetStatisticsCache returns the statistics cache.
func (eo *EnhancedOptimizerV2) GetStatisticsCache() *statistics.AutoRefreshStatisticsCache {
	return eo.statsCache
}

// Explain returns a string representation of the optimizer configuration.
func (eo *EnhancedOptimizerV2) Explain() string {
	factors := eo.costModel.GetCostFactors()
	costModelStr := fmt.Sprintf(
		"AdaptiveCostModel(IO=%.4f, CPU=%.4f, Mem=%.4f, Net=%.4f)",
		factors.IOFactor, factors.CPUFactor, factors.MemoryFactor, factors.NetworkFactor,
	)

	return fmt.Sprintf(
		"=== Enhanced Optimizer V2 (DI) ===\n"+
			"  Parallelism: %d\n"+
			"  Cost Model: %s\n"+
			"  Index Selector: %s\n"+
			"  DP Join Reorder: %s\n"+
			"  Bushy Tree Builder: %s\n"+
			"  Stats Cache: TTL=%v",
		eo.parallelism,
		costModelStr,
		"IndexSelectorV2",
		"DPJoinReorderV2",
		"BushyTreeV2",
		eo.statsCache.Stats().TTL,
	)
}

// Builder pattern for EnhancedOptimizerV2

// EnhancedOptimizerBuilder builds EnhancedOptimizerV2 instances with custom configuration.
type EnhancedOptimizerBuilder struct {
	dataSource  domain.DataSource
	parallelism int
	services    map[string]interface{}
}

// NewEnhancedOptimizerBuilder creates a new builder for EnhancedOptimizerV2.
func NewEnhancedOptimizerBuilder(dataSource domain.DataSource) *EnhancedOptimizerBuilder {
	return &EnhancedOptimizerBuilder{
		dataSource:  dataSource,
		parallelism: 0,
		services:    make(map[string]interface{}),
	}
}

// WithParallelism sets the parallelism for the optimizer.
func (b *EnhancedOptimizerBuilder) WithParallelism(parallelism int) *EnhancedOptimizerBuilder {
	b.parallelism = parallelism
	return b
}

// WithService adds a custom service to use instead of the default from container.
func (b *EnhancedOptimizerBuilder) WithService(name string, service interface{}) *EnhancedOptimizerBuilder {
	b.services[name] = service
	return b
}

// Build builds the EnhancedOptimizerV2 using the container.
func (b *EnhancedOptimizerBuilder) Build(ctr container.Container) *EnhancedOptimizerV2 {
	// Use custom services if provided, otherwise use from container
	estimator, _ := b.getServiceOrDefault(ctr, "estimator.enhanced")
	costModel, _ := b.getServiceOrDefault(ctr, "cost.model.adaptive")
	indexSelector, _ := b.getServiceOrDefault(ctr, "index.selector")
	dpJoinReorder, _ := b.getServiceOrDefault(ctr, "join.reorder.dp")
	bushyTree, _ := b.getServiceOrDefault(ctr, "join.bushy_tree")
	statsCache, _ := b.getServiceOrDefault(ctr, "stats.cache.auto_refresh")
	hintsParser, _ := b.getServiceOrDefault(ctr, "parser.hints")

	return &EnhancedOptimizerV2{
		baseOptimizer:   NewOptimizer(b.dataSource),
		costModel:       costModel.(cost.CostModel),
		indexSelector:   indexSelector,
		dpJoinReorder:   dpJoinReorder,
		bushyTree:       bushyTree,
		statsCache:      statsCache.(*statistics.AutoRefreshStatisticsCache),
		parallelism:     b.parallelism,
		estimator:       estimator.(cost.ExtendedCardinalityEstimator),
		hintsParser:     hintsParser.(*parser.HintsParser),
		dataSource:      b.dataSource,
		container:       ctr,
	}
}

// getServiceOrDefault gets a service from builder's custom services or falls back to container.
func (b *EnhancedOptimizerBuilder) getServiceOrDefault(ctr container.Container, name string) (interface{}, bool) {
	// Check custom services first
	if service, exists := b.services[name]; exists {
		return service, true
	}
	// Fall back to container
	return ctr.Get(name)
}

// DPJoinReorderAdapterV2 DP JOIN重排序适配器（支持 interface{}）
type DPJoinReorderAdapterV2 struct {
	dpReorder interface{}
}

func (a *DPJoinReorderAdapterV2) Name() string {
	return "DPJoinReorderV2"
}

func (a *DPJoinReorderAdapterV2) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalJoin)
	return ok
}

func (a *DPJoinReorderAdapterV2) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	join, ok := plan.(*LogicalJoin)
	if !ok {
		return plan, nil
	}

	fmt.Println("  [DP REORDER V2] 开始DP JOIN重排序优化")

	// Try to use dpReorder if available
	if a.dpReorder == nil {
		fmt.Println("  [DP REORDER V2] dpReorder not available")
		return join, nil
	}

	// Type assert to join reorder interface using anonymous interface
	// to avoid direct dependency on join.LogicalPlan type
	type joinLogicalPlan interface {
		Children() []joinLogicalPlan
		Explain() string
	}
	
	type reorderInterface interface {
		Reorder(context.Context, joinLogicalPlan) (joinLogicalPlan, error)
	}

	if _, ok := a.dpReorder.(reorderInterface); ok {
		// For now, just log that we would use DP reorder
		// Full implementation would need to bridge the type systems
		fmt.Println("  [DP REORDER V2] DP reorder available but type bridging needed")
	}

	fmt.Println("  [DP REORDER V2] 使用原计划")
	return join, nil
}

// BushyTreeAdapterV2 Bushy Tree适配器（支持 interface{}）
type BushyTreeAdapterV2 struct {
	bushyTree interface{}
}

func (a *BushyTreeAdapterV2) Name() string {
	return "BushyTreeBuilderV2"
}

func (a *BushyTreeAdapterV2) Match(plan LogicalPlan) bool {
	return containsJoinV2(plan)
}

func (a *BushyTreeAdapterV2) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	fmt.Println("  [BUSHY TREE V2] 开始 Bushy JOIN Tree 构建优化")

	// Check if there are enough tables for bushy tree
	tables := collectTablesV2(plan)
	if len(tables) < 3 {
		fmt.Printf("  [BUSHY TREE V2] 表数量 %d < 3，使用线性树\n", len(tables))
		return plan, nil
	}

	fmt.Printf("  [BUSHY TREE V2] 检测到 %d 个表，考虑 Bushy Tree\n", len(tables))

	if a.bushyTree == nil {
		fmt.Println("  [BUSHY TREE V2] bushyTree not available")
		return plan, nil
	}

	// Try to use bushy tree builder
	type bushyTreeInterface interface {
		BuildBushyTree([]string, interface{}) interface{}
	}

	if builder, ok := a.bushyTree.(bushyTreeInterface); ok {
		tableNames := make([]string, 0, len(tables))
		for table := range tables {
			tableNames = append(tableNames, table)
		}

		bushyTree := builder.BuildBushyTree(tableNames, nil)
		if bushyTree == nil {
			fmt.Println("  [BUSHY TREE V2] Bushy Tree 构建器返回 nil，保持原线性树")
			return plan, nil
		}

		fmt.Println("  [BUSHY TREE V2] Bushy Tree 构建成功")
		// Convert to optimizer.LogicalPlan
		if result := convertBushyTreeToPlanV2(bushyTree); result != nil {
			return result, nil
		}
	}

	return plan, nil
}

func containsJoinV2(plan LogicalPlan) bool {
	if _, ok := plan.(*LogicalJoin); ok {
		return true
	}

	if _, ok := plan.(*LogicalUnion); ok {
		for _, child := range plan.Children() {
			if containsJoinV2(child) {
				return true
			}
		}
	}

	return false
}

func collectTablesV2(plan LogicalPlan) map[string]bool {
	tables := make(map[string]bool)
	collectTablesRecursiveV2(plan, tables)
	return tables
}

func collectTablesRecursiveV2(plan LogicalPlan, tables map[string]bool) {
	if plan == nil {
		return
	}

	if dataSource, ok := plan.(*LogicalDataSource); ok {
		tables[dataSource.TableName] = true
		return
	}

	for _, child := range plan.Children() {
		collectTablesRecursiveV2(child, tables)
	}
}

func convertBushyTreeToPlanV2(bushyTree interface{}) LogicalPlan {
	fmt.Println("  [BUSHY TREE V2] Bushy Tree 转换功能（框架实现）")
	return nil
}

// IndexSelectionAdapterV2 索引选择适配器（支持 interface{}）
type IndexSelectionAdapterV2 struct {
	indexSelector interface{}
	costModel     cost.CostModel
}

func (a *IndexSelectionAdapterV2) Name() string {
	return "IndexSelectionV2"
}

func (a *IndexSelectionAdapterV2) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalDataSource)
	return ok
}

func (a *IndexSelectionAdapterV2) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	dataSource, ok := plan.(*LogicalDataSource)
	if !ok {
		return plan, nil
	}

	tableName := dataSource.TableName

	// Extract filters from predicates
	filters := convertPredicatesToFilters(dataSource.GetPushedDownPredicates())

	// Apply index selection
	requiredCols := make([]string, 0, len(dataSource.Columns))
	for _, col := range dataSource.Columns {
		requiredCols = append(requiredCols, col.Name)
	}

	// Try to use index selector
	type indexSelectorInterface interface {
		SelectBestIndex(string, []domain.Filter, []string) *index.IndexSelection
	}

	if selector, ok := a.indexSelector.(indexSelectorInterface); ok {
		indexSelection := selector.SelectBestIndex(tableName, filters, requiredCols)

		if indexSelection.SelectedIndex != nil {
			fmt.Printf("  [INDEX SELECT V2] Selected index: %s for table %s\n",
				indexSelection.SelectedIndex.Name, tableName)
		}
	}

	return plan, nil
}
