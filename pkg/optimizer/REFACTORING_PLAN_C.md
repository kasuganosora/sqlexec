# 重构方案C：接口解耦与依赖注入

## 当前问题分析

### 1. 紧密耦合的组件

#### enhanced_optimizer.go (1234行)
- 直接依赖具体实现: `*cost.AdaptiveCostModel`, `*index.IndexSelector`
- 硬编码适配器: `cardinalityEstimatorAdapter`, `joinCostModelAdapter`
- 构造函数复杂: 一次性创建所有依赖

#### optimized_executor.go (1011行)
- 混合职责: 执行逻辑、SHOW处理、系统变量管理、表达式求值
- 直接创建依赖: `builtin.NewFunctionAPI()`, `executor.NewExecutor()`
- 难以测试: 无法mock依赖

#### genetic_algorithm.go (588行)
- 算法与配置混合
- 算子实现耦合在核心逻辑中

### 2. 循环依赖风险

当前依赖关系:
```
enhanced_optimizer.go -> cost/, index/, join/, statistics/
   ↑ (循环风险)
   └---------------------
```

## 方案C设计原则

### 1. 依赖倒置原则 (DIP)
- 高层模块不依赖低层模块,两者都依赖抽象
- 抽象不依赖细节,细节依赖抽象

### 2. 接口隔离原则 (ISP)
- 每个接口只包含最小必要的方法集
- 避免臃肿接口

### 3. 依赖注入 (DI)
- 通过构造函数注入依赖
- 支持接口替换(便于测试)

## 接口设计

### 1. 核心优化器接口

```go
// pkg/optimizer/core/interfaces.go

package core

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/parser"
)

// Optimizer 优化器接口
type Optimizer interface {
    Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error)
    Explain() string
}

// OptimizerFactory 优化器工厂接口
type OptimizerFactory interface {
    CreateOptimizer(dataSource interface{}) Optimizer
    CreateEnhancedOptimizer(dataSource interface{}, parallelism int) Optimizer
}
```

### 2. 成本模型接口

```go
// pkg/optimizer/cost/interfaces.go

package cost

import (
    "github.com/kasuganosora/sqlexec/pkg/parser"
)

// CostModel 成本模型接口
type CostModel interface {
    ScanCost(tableName string, rowCount int64, useIndex bool) float64
    FilterCost(inputRows int64, selectivity float64, filters []interface{}) float64
    JoinCost(leftRows, rightRows int64, joinType JoinType, conditions []*parser.Expression) float64
    AggregateCost(inputRows int64, groupByCols, aggFuncs int) float64
    ProjectCost(inputRows int64, projCols int) float64
    SortCost(inputRows int64, sortCols int) float64
    GetCostFactors() CostFactors
}

// CardinalityEstimator 基数估算接口
type CardinalityEstimator interface {
    EstimateTableScan(tableName string) int64
    EstimateFilter(tableName string, filters []interface{}) int64
    EstimateJoin(leftRows, rightRows int64, joinType JoinType) int64
    EstimateDistinct(tableName string, columns []string) int64
    UpdateStatistics(tableName string, stats interface{})
}
```

### 3. 索引选择接口

```go
// pkg/optimizer/index/interfaces.go

package index

import (
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexSelector 索引选择器接口
type IndexSelector interface {
    SelectBestIndex(tableName string, filters []domain.Filter, requiredCols []string) *SelectionResult
    Explain(tableName string, filters []domain.Filter, requiredCols []string) string
}

// SelectionResult 选择结果
type SelectionResult struct {
    SelectedIndex *domain.IndexInfo
    EstimatedCost float64
    Confidence    float64
}
```

### 4. 执行器接口

```go
// pkg/optimizer/executor/interfaces.go

package executor

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/types"
)

// PlanExecutor 计划执行器接口
type PlanExecutor interface {
    Execute(ctx context.Context, plan *plan.Plan) (types.ResultSet, error)
    Explain(plan *plan.Plan) string
}

// ShowProcessor SHOW语句处理器接口
type ShowProcessor interface {
    ProcessShowTables(ctx context.Context) (types.ResultSet, error)
    ProcessShowDatabases(ctx context.Context) (types.ResultSet, error)
    ProcessShowColumns(ctx context.Context, tableName string) (types.ResultSet, error)
    ProcessShowIndex(ctx context.Context, tableName string) (types.ResultSet, error)
    ProcessShowProcessList(ctx context.Context) (types.ResultSet, error)
    ProcessShowVariables(ctx context.Context) (types.ResultSet, error)
}

// VariableManager 变量管理器接口
type VariableManager interface {
    GetVariable(name string) (interface{}, bool)
    SetVariable(name string, value interface{}) error
    ListVariables() map[string]interface{}
}
```

## 依赖注入容器设计

### 1. DI容器接口

```go
// pkg/optimizer/container/container.go

package container

import (
    "github.com/kasuganosora/sqlexec/pkg/optimizer/core"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/executor"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/index"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/join"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
)

// Container DI容器接口
type Container interface {
    // Register 注册服务
    Register(name string, service interface{})
    
    // Get 获取服务
    Get(name string) (interface{}, error)
    
    // MustGet 获取服务(失败panic)
    MustGet(name string) interface{}
    
    // BuildOptimizer 构建优化器
    BuildOptimizer() core.Optimizer
    
    // BuildEnhancedOptimizer 构建增强优化器
    BuildEnhancedOptimizer(parallelism int) core.Optimizer
    
    // BuildExecutor 构建执行器
    BuildExecutor() executor.PlanExecutor
}
```

### 2. 默认容器实现

```go
// pkg/optimizer/container/default_container.go

package container

import (
    "fmt"
    "sync"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/index"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/join"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

type defaultContainer struct {
    mu       sync.RWMutex
    services map[string]interface{}
    dataSource domain.DataSource
}

func NewContainer(dataSource domain.DataSource) Container {
    c := &defaultContainer{
        services:   make(map[string]interface{}),
        dataSource: dataSource,
    }
    c.registerDefaults()
    return c
}

func (c *defaultContainer) registerDefaults() {
    // 注册统计信息缓存
    statsCache := statistics.NewAutoRefreshStatisticsCache(
        statistics.NewSamplingCollector(c.dataSource, 0.02),
        c.dataSource,
        24*3600, // 24小时
    )
    c.Register("stats.cache", statsCache)
    
    // 注册基数估算器
    estimator := statistics.NewEnhancedCardinalityEstimator(statsCache)
    c.Register("estimator", estimator)
    
    // 注册成本模型
    costModel := cost.NewAdaptiveCostModel(&costCardinalityAdapter{estimator: estimator})
    c.Register("cost.model", costModel)
    
    // 注册索引选择器
    indexSelector := index.NewIndexSelector(estimator)
    c.Register("index.selector", indexSelector)
    
    // 注册JOIN重排序器
    costModelAdapter := &joinCostAdapter{costModel: costModel}
    cardinalityAdapter := &joinCardinalityAdapter{estimator: estimator}
    dpJoinReorder := join.NewDPJoinReorder(costModelAdapter, cardinalityAdapter, 10)
    c.Register("join.dp_reorder", dpJoinReorder)
    
    // 注册Bushy Tree构建器
    bushyTree := join.NewBushyJoinTreeBuilder(costModel, cardinalityAdapter, 3)
    c.Register("join.bushy_tree", bushyTree)
}

// 适配器实现...
type costCardinalityAdapter struct {
    estimator interface{}
}

func (a *costCardinalityAdapter) EstimateTableScan(tableName string) int64 {
    // 实现...
    return 0
}

type joinCostAdapter struct {
    costModel interface{}
}

type joinCardinalityAdapter struct {
    estimator interface{}
}
```

## 重构实施步骤

### 阶段1: 定义核心接口 (1-2天)

**目标**: 定义所有核心接口,不修改现有实现

**步骤**:
1. 创建 `pkg/optimizer/core/interfaces.go`
   - 定义 Optimizer, OptimizerFactory 接口
2. 创建 `pkg/optimizer/cost/interfaces.go`
   - 定义 CostModel, CardinalityEstimator 接口
3. 创建 `pkg/optimizer/executor/interfaces.go`
   - 定义 PlanExecutor, ShowProcessor, VariableManager 接口
4. 创建 `pkg/optimizer/index/interfaces.go`
   - 定义 IndexSelector 接口

**验证**: 编译通过,接口定义完整

### 阶段2: 实现DI容器 (2-3天)

**目标**: 创建依赖注入容器,注册现有实现

**步骤**:
1. 创建 `pkg/optimizer/container/container.go`
   - 定义 Container 接口
2. 创建 `pkg/optimizer/container/default_container.go`
   - 实现默认容器
   - 注册现有服务(statsCache, estimator, costModel等)
3. 创建适配器层
   - 将现有具体实现适配到接口

**验证**: 容器能正确构建和获取服务

### 阶段3: 重构 enhanced_optimizer.go (3-4天)

**目标**: 使用依赖注入解耦 EnhancedOptimizer

**步骤**:
1. 修改 `EnhancedOptimizer` 结构体:
```go
type EnhancedOptimizer struct {
    baseOptimizer   *Optimizer
    costModel       cost.CostModel           // 接口
    indexSelector   index.IndexSelector      // 接口
    dpJoinReorder   interface{}              // DP重排序器
    bushyTree       interface{}              // Bushy Tree构建器
    statsCache      interface{}              // 统计缓存
    parallelism     int
    estimator       cost.CardinalityEstimator // 接口
    hintsParser     *parser.HintsParser
}
```

2. 修改构造函数:
```go
func NewEnhancedOptimizer(
    dataSource domain.DataSource,
    parallelism int,
    costModel cost.CostModel,
    indexSelector index.IndexSelector,
    estimator cost.CardinalityEstimator,
) *EnhancedOptimizer {
    // 使用传入的接口实现
}
```

3. 创建 Builder 模式:
```go
func NewEnhancedOptimizerBuilder(dataSource domain.DataSource) *EnhancedOptimizerBuilder {
    return &EnhancedOptimizerBuilder{
        dataSource: dataSource,
        parallelism: 0,
    }
}

type EnhancedOptimizerBuilder struct {
    dataSource domain.DataSource
    parallelism int
}

func (b *EnhancedOptimizerBuilder) Build(container Container) *EnhancedOptimizer {
    costModel := container.MustGet("cost.model").(cost.CostModel)
    indexSelector := container.MustGet("index.selector").(index.IndexSelector)
    estimator := container.MustGet("estimator").(cost.CardinalityEstimator)
    
    return NewEnhancedOptimizer(
        b.dataSource,
        b.parallelism,
        costModel,
        indexSelector,
        estimator,
    )
}
```

**验证**: 所有现有测试通过

### 阶段4: 重构 optimized_executor.go (3-4天)

**目标**: 按职责拆分执行器

**步骤**:
1. 提取 `ShowProcessor` 接口实现
   - 创建 `pkg/optimizer/executor/show_processor.go`
   - 实现 `DefaultShowProcessor`

2. 提取 `VariableManager` 接口实现
   - 创建 `pkg/optimizer/executor/variable_manager.go`
   - 实现 `DefaultVariableManager`

3. 提取 `ExpressionEvaluator`
   - 创建 `pkg/optimizer/executor/expression_evaluator.go`
   - 实现 `DefaultExpressionEvaluator`

4. 重构 `OptimizedExecutor`:
```go
type OptimizedExecutor struct {
    dataSource    domain.DataSource
    dsManager     *application.DataSourceManager
    optimizer     core.Optimizer         // 接口
    planExecutor  executor.PlanExecutor  // 接口
    useOptimizer  bool
    useEnhanced   bool
    currentDB     string
    currentUser   string
    showProcessor executor.ShowProcessor // 接口
    varManager    executor.VariableManager // 接口
}

func NewOptimizedExecutor(
    dataSource domain.DataSource,
    optimizer core.Optimizer,
    planExecutor executor.PlanExecutor,
    showProcessor executor.ShowProcessor,
    varManager executor.VariableManager,
) *OptimizedExecutor {
    // ...
}
```

**验证**: SHOW语句执行正常

### 阶段5: 重构 genetic_algorithm.go (2-3天)

**目标**: 分离配置、核心逻辑和算子

**步骤**:
1. 创建 `pkg/optimizer/genetic/config.go`
   - 保持 `GeneticAlgorithmConfig` 和 `DefaultGeneticAlgorithmConfig`

2. 创建 `pkg/optimizer/genetic/core.go`
   - 提取 `GeneticAlgorithm` 核心逻辑
   - 依赖配置和算子接口

3. 创建 `pkg/optimizer/genetic/operators.go`
   - 提取选择、交叉、变异算子
   - 实现 `SelectionOperator`, `CrossoverOperator`, `MutationOperator` 接口

4. 重构后的依赖:
```go
type GeneticAlgorithm struct {
    config     *GeneticAlgorithmConfig
    selector   SelectionOperator    // 接口
    crossover  CrossoverOperator    // 接口
    mutator    MutationOperator     // 接口
    
    // 数据
    candidates []*IndexCandidate
    benefits   map[string]float64
    rng        *rand.Rand
    mu         sync.Mutex
}
```

**验证**: 遗传算法测试通过

### 阶段6: 重构 physical_scan.go (2-3天)

**目标**: 将物理算子独立为单独文件

**步骤**:
1. 创建 `pkg/optimizer/physical/table_scan.go`
   - 实现 `PhysicalTableScan` 算子

2. 创建 `pkg/optimizer/physical/selection.go`
   - 实现 `PhysicalSelection` 算子

3. 创建 `pkg/optimizer/physical/projection.go`
   - 实现 `PhysicalProjection` 算子

4. 创建 `pkg/optimizer/physical/join.go`
   - 实现 `PhysicalJoin` 算子

5. 创建 `pkg/optimizer/physical/interfaces.go`
   - 定义 `PhysicalOperator` 接口

**验证**: 物理计划执行正常

### 阶段7: 集成测试 (2-3天)

**目标**: 确保重构后功能完整

**步骤**:
1. 创建 `pkg/optimizer/container/builder_test.go`
   - 测试容器构建

2. 更新集成测试
   - `pkg/optimizer/integration_test.go`
   - 使用DI容器构建完整流程

3. 性能测试
   - 确保重构后性能无退化

**验证**: 所有测试通过,性能无显著下降

## 重构收益

### 1. 代码质量提升
- **模块化**: 每个文件职责单一
- **可测试性**: 可以mock任意接口
- **可维护性**: 依赖清晰,修改影响范围小

### 2. 性能优化机会
- **可替换实现**: 可以轻松替换高性能实现
- **缓存策略**: DI容器支持单例/作用域缓存
- **并行执行**: 接口设计支持并行优化

### 3. 扩展性
- **新算法**: 添加新的CostModel或IndexSelector实现
- **配置化**: 通过配置文件选择实现
- **插件化**: 未来支持动态加载优化器插件

## 风险与应对

### 风险1: 接口设计不完善
- **应对**: 采用渐进式接口设计,先定义最小必要接口
- **验证**: 每个阶段后评估接口合理性

### 风险2: 重构引入bug
- **应对**: 
  - 保持原有测试不变
  - 每个阶段后运行完整测试套件
  - 添加集成测试覆盖核心流程

### 风险3: 性能退化
- **应对**:
  - 重构前后运行性能基准测试
  - 使用pprof分析性能瓶颈
  - 接口设计考虑性能(避免过度抽象)

## 时间估算

总估算: **15-20天**

| 阶段 | 任务 | 时间 | 优先级 |
|------|------|------|--------|
| 1 | 定义核心接口 | 1-2天 | P0 |
| 2 | 实现DI容器 | 2-3天 | P0 |
| 3 | 重构enhanced_optimizer | 3-4天 | P0 |
| 4 | 重构optimized_executor | 3-4天 | P1 |
| 5 | 重构genetic_algorithm | 2-3天 | P2 |
| 6 | 重构physical_scan | 2-3天 | P3 |
| 7 | 集成测试 | 2-3天 | P0 |

## 总结

方案C通过接口解耦和依赖注入,从根本上解决了代码耦合问题。虽然实施周期较长(15-20天),但能带来:

1. **彻底模块化**: 每个组件职责清晰
2. **高度可测试**: 支持单元测试和集成测试
3. **易于扩展**: 添加新算法或优化器简单
4. **性能可控**: 可以替换高性能实现
5. **长期收益**: 降低维护成本,提高开发效率

建议按阶段实施,每个阶段完成后进行测试验证,确保重构过程可控。
