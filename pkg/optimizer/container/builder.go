package container

import (
	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/executor"
	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/index"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
)

// Executor 是 executor.Executor 的别名，用于避免返回指针到接口类型
type Executor = executor.Executor

// Builder 使用 Container 构建各种组件
type Builder struct {
	container Container
}

// NewBuilder 创建新的构建器
func NewBuilder(container Container) *Builder {
	return &Builder{container: container}
}

// BuildOptimizer 构建基础优化器
func (b *Builder) BuildOptimizer() *optimizer.Optimizer {
	return optimizer.NewOptimizer(b.container.GetDataSource())
}

// BuildEnhancedOptimizer 构建增强优化器
func (b *Builder) BuildEnhancedOptimizer(parallelism int) *optimizer.EnhancedOptimizer {
	return optimizer.NewEnhancedOptimizer(b.container.GetDataSource(), parallelism)
}

// BuildExecutor 构建执行器
func (b *Builder) BuildExecutor() executor.Executor {
	dataAccessService := dataaccess.NewDataService(b.container.GetDataSource())
	return executor.NewExecutor(dataAccessService)
}

// BuildOptimizedExecutor 构建优化执行器
func (b *Builder) BuildOptimizedExecutor(useOptimizer bool) *optimizer.OptimizedExecutor {
	return optimizer.NewOptimizedExecutor(b.container.GetDataSource(), useOptimizer)
}

// BuildOptimizedExecutorWithDSManager 构建带数据源管理器的优化执行器
func (b *Builder) BuildOptimizedExecutorWithDSManager(dsManager *application.DataSourceManager, useOptimizer bool) *optimizer.OptimizedExecutor {
	return optimizer.NewOptimizedExecutorWithDSManager(b.container.GetDataSource(), dsManager, useOptimizer)
}

// BuildShowProcessor 构建 SHOW 处理器
func (b *Builder) BuildShowProcessor() *optimizer.DefaultShowProcessor {
	return optimizer.NewDefaultShowProcessor(b.container.GetDataSource())
}

// BuildVariableManager 构建变量管理器
func (b *Builder) BuildVariableManager() *optimizer.DefaultVariableManager {
	return optimizer.NewDefaultVariableManager()
}

// BuildExpressionEvaluator 构建表达式求值器
func (b *Builder) BuildExpressionEvaluator() *optimizer.ExpressionEvaluator {
	functionAPI := builtin.NewFunctionAPI()
	return optimizer.NewExpressionEvaluator(functionAPI)
}

// GetCostModel 从容器获取成本模型
func (b *Builder) GetCostModel() cost.CostModel {
	if cm, ok := b.container.Get("cost.model.adaptive"); ok {
		if costModel, ok := cm.(cost.CostModel); ok {
			return costModel
		}
	}
	return nil
}

// GetIndexSelector 从容器获取索引选择器
func (b *Builder) GetIndexSelector() *index.IndexSelector {
	if is, ok := b.container.Get("index.selector"); ok {
		if selector, ok := is.(*index.IndexSelector); ok {
			return selector
		}
	}
	return nil
}

// GetStatisticsCache 从容器获取统计缓存
func (b *Builder) GetStatisticsCache() *statistics.AutoRefreshStatisticsCache {
	if sc, ok := b.container.Get("stats.cache.auto_refresh"); ok {
		if cache, ok := sc.(*statistics.AutoRefreshStatisticsCache); ok {
			return cache
		}
	}
	return nil
}
