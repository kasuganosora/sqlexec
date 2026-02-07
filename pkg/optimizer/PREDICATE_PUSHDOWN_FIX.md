# PredicatePushDown 无限递归修复

## 问题描述

测试 `TestEnhancedOptimizer_Aggregation` 在执行到 `PredicatePushDown` 规则时卡住，无法完成。

## 根本原因

`pkg/optimizer/rules.go` 中的 `RuleExecutor.Execute` 方法存在严重的递归逻辑错误：

### 错误实现（已修复）

```go
func (re *RuleExecutor) Execute(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
    for iterations < maxIterations {
        for _, rule := range re.rules {
            // 1. 应用当前规则
            if rule.Match(current) {
                newPlan, err := rule.Apply(ctx, current, optCtx)
                // ...
            }

            // ❌ 错误：在规则循环中递归处理子节点
            children := current.Children()
            for i, child := range children {
                newChild, err := re.Execute(ctx, child, optCtx)  // 无限递归！
                // ...
            }
        }
    }
}
```

**问题分析**：
1. 在遍历规则的循环内部递归调用 `re.Execute`
2. 当规则修改了计划树后，又在新计划上递归处理子节点
3. 每次递归又进入规则遍历，导致无限循环
4. 特别是 `PredicatePushDown` 规则会改变树结构，触发无限递归

## 修复方案

采用**后序遍历（Post-order Traversal）**策略：
1. 先递归处理所有子节点（自底向上）
2. 然后在处理完的节点上应用规则
3. 避免在规则循环中重复递归

### 正确实现（已修复）

```go
func (re *RuleExecutor) Execute(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
    // 1. 首先递归处理所有子节点（后序遍历）
    children := plan.Children()
    if len(children) > 0 {
        newChildren := make([]LogicalPlan, len(children))
        anyChildChanged := false

        for i, child := range children {
            newChild, err := re.Execute(ctx, child, optCtx)
            // ...
            newChildren[i] = newChild
            if newChild != child {
                anyChildChanged = true
            }
        }

        if anyChildChanged {
            plan.SetChildren(newChildren...)
        }
    }

    // 2. 然后在处理完的节点上应用规则
    current := plan
    for iterations < maxIterations {
        changed := false
        for _, rule := range re.rules {
            if rule.Match(current) {
                newPlan, err := rule.Apply(ctx, current, optCtx)
                // ...
            }
        }
        if !changed {
            break
        }
    }

    return current, nil
}
```

## 测试验证

### 通过的测试

```bash
✓ TestEnhancedOptimizer_BasicOptimization
✓ TestEnhancedOptimizer_ComplexQuery
✓ TestEnhancedOptimizer_Aggregation       # 之前卡住的测试
✓ TestEnhancedOptimizer_SortAndLimit
✓ TestEnhancedOptimizer_MultipleTables
✓ TestEnhancedOptimizer_TableNotFound
✓ TestEnhancedOptimizer_EmptyWhere
✓ TestEnhancedOptimizer_Subquery
✓ TestEnhancedOptimizer_Distinct
✓ TestEnhancedOptimizer_ComplexWhere
✓ TestEnhancedOptimizer_NullHandling
✓ TestEnhancedOptimizer_ContextCancellation
✓ TestGeneticAlgorithmSimple
```

### 清理的废弃测试

备份了以下使用已废弃 `Execute()` 方法的测试文件：
- `integration_test.go.bak`
- `optimized_aggregate_test.go.bak`

这些测试调用已被废弃的 `OptimizedAggregate.Execute()` 和相关物理算子的 `Execute()` 方法，这些方法的执行逻辑已迁移到 `pkg/executor` 包。

## 影响范围

- **修改文件**：`pkg/optimizer/rules.go` (第509-563行)
- **修复类型**：逻辑错误修复
- **回归风险**：低（修复了根本问题，提高了算法正确性）

## 最佳实践

1. **避免在遍历循环中递归**：确保递归调用与遍历逻辑分离
2. **采用后序遍历**：对于树形结构，先处理子节点再处理父节点
3. **设置递归深度限制**：虽然 `maxIterations` 有保护，但应该明确递归终止条件
4. **避免无限循环**：在每次递归/迭代中确保有明确的进度

## 相关技术文档

- [Go 递归最佳实践](https://go.dev/doc/effective_go#init)
- [树遍历算法](https://en.wikipedia.org/wiki/Tree_traversal)
- [查询优化器规则应用](https://en.wikipedia.org/wiki/Query_optimization)

## 修订历史

- **2026-02-07**: 修复 `RuleExecutor.Execute` 的无限递归问题
