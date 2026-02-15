# SQLExec 查询优化器技术原理

## 目录

1. [优化器架构](#1-优化器架构)
2. [查询计划生成](#2-查询计划生成)
3. [代价模型](#3-代价模型)
4. [JOIN 算法](#4-join-算法)
5. [谓词下推](#5-谓词下推)
6. [并行执行](#6-并行执行)
7. [索引优化](#7-索引优化)

---

## 1. 优化器架构

### 1.1 整体架构

SQLExec 的查询优化器采用经典的**多阶段流水线架构**，将查询从 SQL 文本逐步转换为可高效执行的物理计划。整体流程如下：

```
SQL 文本
  |
  v
[Parser] --- SQL 解析 ---> AST (SQLStatement)
  |
  v
[Optimizer] --- 逻辑计划生成 ---> LogicalPlan (逻辑算子树)
  |
  v
[RuleSet] --- 优化规则应用 ---> Optimized LogicalPlan
  |
  v
[PlanConverter] --- 物理计划生成 ---> plan.Plan (可序列化执行计划)
  |
  v
[Executor] --- 算子树构建 ---> operators.Operator (物理算子)
  |
  v
[执行] --- 数据读取/计算 ---> QueryResult
```

### 1.2 核心组件

优化器的核心组件定义在 `pkg/optimizer/` 目录下，包括以下子系统：

| 组件 | 路径 | 职责 |
|------|------|------|
| `OptimizedExecutor` | `optimized_executor.go` | 查询执行入口，协调优化器与执行器 |
| `EnhancedOptimizer` | `enhanced_optimizer.go` | 增强优化器，集成所有优化模块 |
| `Optimizer` | `optimizer.go` | 基础优化器，负责逻辑计划转换和规则应用 |
| `RuleSet` | `rules.go` | 优化规则集合，包含谓词下推、列裁剪等规则 |
| `AdaptiveCostModel` | `cost/adaptive_model.go` | 自适应代价模型，基于硬件动态调整 |
| `StatisticsCache` | `statistics/cache.go` | 统计信息缓存，支持自动刷新 |
| `IndexSelector` | `index/selector.go` | 索引选择器，选择最优索引方案 |
| `DPJoinReorder` | `join/dp_reorder.go` | 动态规划 JOIN 重排序器 |
| `ParallelScanner` | `parallel/scanner.go` | 并行表扫描器 |
| `ExecutionFeedback` | `feedback/feedback.go` | 执行反馈收集，用于代价校准 |
| `PlanCache` | `plan_cache.go` | 查询计划缓存 |

### 1.3 双层优化器设计

系统采用双层优化器设计：

- **基础优化器 (`Optimizer`)**：提供基本的逻辑计划转换和默认优化规则集（谓词下推、列裁剪、投影消除、Limit 下推、常量折叠等）。
- **增强优化器 (`EnhancedOptimizer`)**：在基础优化器之上集成了高级优化模块，包括自适应代价模型、DP JOIN 重排序、Bushy Tree 构建、索引选择、Hint 解析等。增强优化器通过适配器模式（Adapter Pattern）桥接各子系统的接口差异，避免循环依赖。

`OptimizedExecutor` 作为入口，统一使用 `EnhancedOptimizer`：

```go
// 统一使用增强优化器
opt := NewEnhancedOptimizer(dataSource, 0) // parallelism=0 表示自动选择最优并行度
```

### 1.4 计划缓存

优化器内置了基于 DQ（Deep Q-learning）思想启发的计划缓存（`PlanCache`）：

- **缓存键**：使用 FNV-1a 哈希算法对 SQL 语句结构生成指纹（`SQLFingerprint`），包括表名、列名、WHERE 结构、JOIN 信息、ORDER BY、GROUP BY 等。
- **缓存策略**：LRU（Least Recently Used）淘汰策略，默认最大缓存 1024 个计划。
- **代价反馈**：缓存条目记录实际执行代价（`ActualCost`），使用指数移动平均（EMA）平滑代价估算：`new = old * 0.7 + observed * 0.3`。
- **缓存失效**：DDL 操作后全量失效。

---

## 2. 查询计划生成

### 2.1 逻辑计划节点类型

逻辑计划是一棵算子树，每个节点代表一种关系代数操作。系统定义了 `LogicalPlan` 接口：

```go
type LogicalPlan interface {
    Children() []LogicalPlan      // 获取子节点
    SetChildren(children ...LogicalPlan)  // 设置子节点
    Schema() []ColumnInfo         // 返回输出列
    Explain() string              // 返回计划说明
}
```

主要逻辑算子包括：

| 逻辑算子 | 源文件 | 关系代数等价 |
|---------|--------|------------|
| `LogicalDataSource` | `logical_datasource.go` | 基表访问 (R) |
| `LogicalSelection` | `logical_selection.go` | 选择 (sigma) |
| `LogicalProjection` | `logical_projection.go` | 投影 (pi) |
| `LogicalJoin` | `logical_join.go` | 连接 (join) |
| `LogicalAggregate` | `logical_aggregate.go` | 聚合 (gamma) |
| `LogicalSort` | `logical_sort.go` | 排序 (tau) |
| `LogicalLimit` | `logical_limit.go` | 限制 |
| `LogicalUnion` | `logical_union.go` | 集合并 |
| `LogicalWindow` | `logical_window.go` | 窗口函数 |
| `LogicalInsert` | `logical_insert.go` | 插入 |
| `LogicalUpdate` | `logical_update.go` | 更新 |
| `LogicalDelete` | `logical_delete.go` | 删除 |
| `LogicalApply` | `logical_apply.go` | 子查询关联 |
| `LogicalTopN` | `logical_topn.go` | Top-N 操作 |

### 2.2 SQL 到逻辑计划的转换

`Optimizer.convertSelect()` 方法将 `SelectStatement` AST 按以下顺序构建逻辑计划树（自底向上）：

```
1. DataSource (FROM)      -- 创建表数据源节点
      |
2. Selection (WHERE)      -- 添加过滤条件
      |
3. Aggregate (GROUP BY)   -- 添加分组聚合
      |
4. Sort (ORDER BY)        -- 添加排序
      |
5. Limit (LIMIT/OFFSET)   -- 添加行数限制
      |
6. Projection (SELECT)    -- 添加列投影
```

对于 JOIN 查询，解析器会识别 `SELECT ... FROM a JOIN b ON ...` 语法，在 DataSource 之上构建 `LogicalJoin` 节点，左右子节点分别是两个表的 DataSource。

对于无 FROM 子句的查询（如 `SELECT DATABASE()`），使用虚拟的 `dual` 表作为数据源。

### 2.3 优化规则应用

逻辑计划构建完成后，进入优化阶段。优化规则实现 `OptimizationRule` 接口：

```go
type OptimizationRule interface {
    Name() string                                                              // 规则名称
    Match(plan LogicalPlan) bool                                               // 检查是否匹配
    Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) // 应用规则
}
```

规则应用采用**迭代固定点（Iterative Fixed-Point）算法**：循环应用所有规则，直到计划不再发生变化或达到最大迭代次数（10 次）。每轮迭代中，先在当前节点上应用所有匹配的规则，再递归应用到子节点。

**默认规则集**（`DefaultRuleSet`）包含 9 条基础规则：

1. `PredicatePushDown` -- 谓词下推
2. `ColumnPruning` -- 列裁剪
3. `ProjectionElimination` -- 投影消除
4. `LimitPushDown` -- Limit 下推
5. `ConstantFolding` -- 常量折叠
6. `JoinReorder` -- JOIN 重排序（贪心）
7. `JoinElimination` -- JOIN 消除
8. `SemiJoinRewrite` -- Semi-Join 改写
9. `VectorIndexRule` -- 向量索引规则

**增强规则集**（`EnhancedRuleSet`）在默认规则基础上增加了 18+ 条高级规则：

- `EnhancedPredicatePushdown` -- 增强谓词下推（跨 JOIN 下推、OR 转 UNION）
- `EnhancedColumnPruning` -- 增强列裁剪
- `TopNPushDown` -- TopN 下推
- `DeriveTopNFromWindow` -- 从窗口函数推导 TopN
- `HintAwareJoinReorder` -- 支持 Hint 的 JOIN 重排序
- `HintAwareIndex` -- 支持 Hint 的索引选择
- `HintAwareAgg` -- 支持 Hint 的聚合算法选择
- `OrderByHint` -- ORDER BY Hint
- `Decorrelate` -- 子查询去关联
- `SubqueryMaterialization` -- 子查询物化
- `SubqueryFlattening` -- 子查询扁平化
- `ORToUnion` -- OR 条件转 UNION
- `MaxMinElimination` -- MAX/MIN 消除优化
- `DPJoinReorder` -- DP 算法 JOIN 重排序
- `BushyTreeBuilder` -- Bushy JOIN Tree 构建
- `IndexSelection` -- 索引选择

### 2.4 逻辑计划到物理计划的转换

优化后的逻辑计划通过 `convertToPlanEnhanced()` 方法转换为可序列化的 `plan.Plan` 结构：

```go
type Plan struct {
    ID           string            // 算子标识
    Type         PlanType          // 算子类型 (TableScan, HashJoin, Sort, ...)
    OutputSchema []types.ColumnInfo // 输出列 schema
    Children     []*Plan           // 子计划
    Config       interface{}       // 算子特定配置
    EstimatedCost float64          // 估算代价
}
```

`plan.Plan` 是一个纯数据结构，不包含任何数据源引用，可以被序列化、缓存和传输。转换过程中，每个逻辑算子映射到对应的物理算子配置：

| 逻辑算子 | 物理计划类型 | 配置类型 |
|---------|------------|---------|
| `LogicalDataSource` | `TypeTableScan` | `TableScanConfig` |
| `LogicalSelection` | `TypeSelection` | `SelectionConfig` |
| `LogicalProjection` | `TypeProjection` | `ProjectionConfig` |
| `LogicalJoin` | `TypeHashJoin` | `HashJoinConfig` |
| `LogicalAggregate` | `TypeAggregate` | `AggregateConfig` |
| `LogicalSort` | `TypeSort` | `SortConfig` |
| `LogicalLimit` | `TypeLimit` | `LimitConfig` |
| `LogicalInsert` | `TypeInsert` | `InsertConfig` |
| `LogicalUpdate` | `TypeUpdate` | `UpdateConfig` |
| `LogicalDelete` | `TypeDelete` | `DeleteConfig` |
| `LogicalUnion` | `TypeUnion` | `UnionConfig` |

### 2.5 物理计划到算子的构建

`pkg/executor/executor.go` 中的 `BaseExecutor` 将 `plan.Plan` 转换为可执行的算子树。`buildOperator()` 方法根据 `Plan.Type` 选择对应的算子实现：

```go
func (e *BaseExecutor) buildOperator(p *plan.Plan) (operators.Operator, error) {
    switch p.Type {
    case plan.TypeTableScan:
        return operators.NewTableScanOperator(p, e.dataAccessService)
    case plan.TypeSelection:
        return operators.NewSelectionOperator(p, e.dataAccessService)
    case plan.TypeHashJoin:
        return operators.NewHashJoinOperator(p, e.dataAccessService)
    // ... 其他算子
    }
}
```

每个算子实现 `Operator` 接口，通过递归调用 `Execute(ctx)` 方法实现 Volcano 模型的拉取式执行。

---

## 3. 代价模型

### 3.1 自适应代价模型

系统的核心代价模型是 `AdaptiveCostModel`（`pkg/optimizer/cost/adaptive_model.go`），它根据硬件配置和运行时统计信息动态调整代价估算。

**代价因子**：代价模型使用四个基础因子：

| 因子 | 含义 | 基准值 |
|------|------|--------|
| `IOFactor` | 磁盘 I/O 代价系数 | 0.1 (SSD@500MB/s) |
| `CPUFactor` | CPU 计算代价系数 | 0.01 (4核@2.4GHz) |
| `MemoryFactor` | 内存访问代价系数 | 0.001 |
| `NetworkFactor` | 网络传输代价系数 | 0.01 (1Gbps) |

这些因子由 `HardwareProfile` 根据实际硬件自动计算。

### 3.2 硬件配置检测

`HardwareProfile`（`pkg/optimizer/cost/hardware_profile.go`）在优化器初始化时自动检测硬件环境：

- **CPU**：检测核心数（`runtime.NumCPU()`）和频率，计算归一化 CPU 速度。
- **内存**：检测总内存和可用内存。
- **磁盘**：识别磁盘类型（NVMe/SSD/HDD），据此估算 I/O 速度和寻道时间。NVMe 的 I/O 速度约为 3500 MB/s，SSD 约为 500 MB/s，HDD 约为 100 MB/s。
- **环境**：检测是否为云环境（通过 AWS_REGION、GOOGLE_CLOUD_PROJECT 等环境变量），云环境下网络因子增大 1.5 倍。

代价因子的计算公式：

```
IOFactor     = 0.1 * (baseDiskIO / actualDiskIO)
CPUFactor    = 0.01 / normalizedCPUSpeed
MemoryFactor = 0.001 * memorySpeed
NetworkFactor = 0.01 * (baseBandwidth / actualBandwidth)
```

### 3.3 各操作的代价计算

**表扫描代价**：

```
ScanCost = rows * IOFactor                    -- 全表扫描
ScanCost = indexHeight * CPUFactor + rows * IOFactor * 0.1  -- 索引扫描（约为全表扫描的 1/10）
```

扫描代价还考虑缓存命中率：`AdjustedCost = BaseCost * (1 - cacheHitRate)`。

**过滤代价**：

```
FilterCost = outputRows * CPUFactor + inputRows * MemoryFactor * 0.01
```

如果列上有索引，CPU 比较代价降低至 30%。

**JOIN 代价**（基于 Hash Join）：

```
JoinCost = BuildHashCost + ProbeCost
BuildHashCost = buildRows * CPUFactor * 2 + buildRows * MemoryFactor * 0.01
ProbeCost = probeRows * (conditionCount + 1) * CPUFactor + probeRows * MemoryFactor * 0.001
```

不同 JOIN 类型的代价计算略有差异：LEFT/RIGHT OUTER JOIN 需要额外的物化代价，FULL OUTER JOIN 需要双向构建和探测。

**聚合代价**：

```
AggregateCost = groupingCost + aggregationCost + hashTableCost + sortingCost
groupingCost = inputRows * groupByCols * CPUFactor
aggregationCost = inputRows * aggFuncs * CPUFactor
hashTableCost = inputRows * MemoryFactor * 0.05
sortingCost = inputRows * log2(inputRows) * sortCols * CPUFactor
```

**排序代价**：

```
SortCost = inputRows * log2(inputRows) * CPUFactor
```

**向量搜索代价**（支持 HNSW、Flat、IVF-Flat 三种索引）：

```
HNSW:     log2(N) * k * CPUFactor + log2(N) * k * MemoryFactor * 0.01
Flat:     N * CPUFactor + k * log2(k) * CPUFactor
IVF-Flat: (N / sqrt(N)) * CPUFactor
```

### 3.4 基数估算

基数估算由 `EnhancedCardinalityEstimator`（`pkg/optimizer/statistics/estimator.go`）负责，它使用分层策略：

1. **直方图估算**（优先级最高）：如果列有直方图统计信息，使用直方图桶的分布估算选择率。
2. **NDV 估算**：如果没有直方图但有 NDV（Number of Distinct Values），使用 `1/NDV` 估算等值查询选择率。
3. **默认选择率**：无统计信息时，使用经验默认值。

各运算符的默认选择率：

| 运算符 | 默认选择率 |
|--------|----------|
| `=` / `!=` | 10% |
| `>` / `>=` / `<` / `<=` | 30% |
| `IN` | 20% |
| `BETWEEN` | 30% |
| `LIKE` | 25% |

AND 条件的选择率按乘法规则组合，OR 条件使用包含-排斥原则：`P(A OR B) = P(A) + P(B) - P(A) * P(B)`。

### 3.5 统计信息收集

`SamplingCollector`（`pkg/optimizer/statistics/collector.go`）使用**系统采样（Systematic Sampling）**算法收集统计信息：

- **采样率**：默认 2%，可配置。
- **最大采样行数**：默认 10000 行。
- **采样方式**：计算步长 `step = totalRows / sampleSize`，按等间距采样。
- **并行收集**：各列的统计信息使用 goroutine 并行收集。

为每列构建的统计信息包括：
- 数据类型推断
- NULL 值计数和比例
- NDV（唯一值数）
- 最小值/最大值
- 平均宽度
- **等宽直方图**（默认 10 个桶）

直方图支持两种类型（`pkg/optimizer/statistics/histogram.go`）：
- **等宽直方图**（Equi-Width）：值域等分，每个桶的宽度相同。
- **频率直方图**（Frequency）：按值出现频率分桶。

`AutoRefreshStatisticsCache` 提供自动刷新机制：统计信息有 TTL（默认 24 小时），过期后在下次访问时自动重新采样。同时支持后台定期刷新和批量预加载。

### 3.6 执行反馈机制

系统实现了受 DQ（Deep Q-learning）论文启发的执行反馈机制（`pkg/optimizer/feedback/feedback.go`）。`ExecutionFeedback` 全局单例收集运行时的实际执行统计：

- **表大小反馈**：记录实际行数，用 EMA 平滑：`new = old * 0.7 + observed * 0.3`。
- **选择率反馈**：记录实际过滤后的输出行数/输入行数比。
- **JOIN 因子反馈**：记录实际 JOIN 输出行数/（左表行数 * 右表行数）比。

反馈数据通过 `FeedbackProvider` 接口注入到 `AdaptiveCostModel`，在代价估算时优先使用学习到的实际值，而非默认估算值。

---

## 4. JOIN 算法

### 4.1 Hash Join

Hash Join 是 SQLExec 的默认 JOIN 算法，适用于等值连接场景。

**算法原理**：

```
阶段 1: 构建哈希表 (Build Phase)
  对较小的表（构建侧，通常选左表）的连接键计算哈希值
  将 <哈希值, 行数据> 插入哈希表
  时间复杂度: O(n), 空间复杂度: O(n)

阶段 2: 探测 (Probe Phase)
  逐行扫描较大的表（探测侧）
  对每行的连接键计算哈希值
  在哈希表中查找匹配的行
  输出匹配的组合行
  时间复杂度: O(m), 整体: O(n + m)
```

**哈希函数**：使用 FNV-64a 哈希算法（`hash/fnv`），对连接列的值计算 64 位哈希。

**实现位置**：
- 逻辑计划：`pkg/optimizer/logical_join.go` -- `LogicalJoin` 节点
- 物理计划：`pkg/optimizer/physical_scan.go` -- `PhysicalHashJoin`
- 执行算子：`pkg/executor/operators/` -- `HashJoinOperator`
- 并行版本：`pkg/optimizer/parallel/join_executor.go` -- `ParallelHashJoinExecutor`

**代价公式**：

```
HashJoinCost = BuildCost + ProbeCost
BuildCost = |build_side| * CPUFactor * 2 + |build_side| * MemoryFactor * 0.01
ProbeCost = |probe_side| * (|conditions| + 1) * CPUFactor + |probe_side| * MemoryFactor * 0.001
```

**构建侧选择**：默认选择左表作为构建侧。在增强优化器中，通过 `HashJoinConfig.BuildSide` 字段控制。

### 4.2 Sort-Merge Join

Sort-Merge Join（归并连接）适用于数据已排序或需要排序输出的场景。

**算法原理**：

```
阶段 1: 排序 (Sort Phase)
  对左表和右表分别按连接键排序
  时间复杂度: O(n*log(n) + m*log(m))

阶段 2: 归并 (Merge Phase)
  使用双指针同步扫描两个有序序列
  当左值 < 右值时，左指针前进
  当左值 > 右值时，右指针前进
  当左值 == 右值时，输出匹配行，双指针均前进
  时间复杂度: O(n + m)
```

**实现位置**：`pkg/optimizer/merge_join.go` -- `PhysicalMergeJoin`

**代价公式**：

```
MergeJoinCost = LeftSortCost + RightSortCost + MergeCost
MergeCost = (|left| + |right|) * 0.05
```

**JOIN 类型支持**：
- **INNER JOIN**：只输出两边都有的匹配行。
- **LEFT OUTER JOIN**：输出左表所有行；右表不匹配时填充 NULL。
- **RIGHT OUTER JOIN**：输出右表所有行；左表不匹配时填充 NULL。

**列名冲突处理**：当左右表有同名列时，右表列名自动加 `right_` 前缀。

### 4.3 Nested Loop Join

Nested Loop Join（嵌套循环连接）是最基础的 JOIN 算法，当其他算法不适用时作为回退方案。

**算法原理**：

```
对于外表的每一行 outer_row:
  对于内表的每一行 inner_row:
    如果 JOIN 条件匹配:
      输出 (outer_row, inner_row)

时间复杂度: O(n * m)
```

在 SQLExec 中，Index Nested Loop Join（INL Join）通过 Hint 支持：`/*+ INL_JOIN(t1) */`，利用内表的索引将内层循环的复杂度降至 O(log(k))。

### 4.4 JOIN 重排序

当查询涉及多个表的 JOIN 时，JOIN 的执行顺序对性能影响巨大。SQLExec 提供两种重排序策略：

#### 4.4.1 贪心算法 (JoinReorderRule)

位于 `pkg/optimizer/join_reorder.go`，适用于默认规则集：

```
1. 选择基数最小的表作为起始表
2. 从剩余表中，选择与当前已选表集 JOIN 代价最小的表加入
3. 重复步骤 2 直到所有表都被选入
4. 根据最优顺序重建 JOIN 树（左深树）
```

时间复杂度：O(n^2)，适合表数量不多的场景。

#### 4.4.2 动态规划算法 (DPJoinReorder)

位于 `pkg/optimizer/join/dp_reorder.go`，由增强优化器使用：

```
1. 初始化: dp[{t}] = ScanCost(t), 对每个单表 t
2. 枚举所有表集合 S 的非空真子集分割 (A, B)
   其中 S = A ∪ B, A ∩ B = ∅
3. 对每种分割, 计算: Cost(S) = Cost(A) + Cost(B) + JoinCost(A, B)
4. 取最小代价的分割作为最优方案
5. 回溯最优分割, 构建 JOIN 树
```

时间复杂度：O(3^n)，当表数量超过 `maxTables`（默认 10）时自动回退为贪心算法。

DP 重排序支持结果缓存（`ReorderCache`），避免重复计算。

#### 4.4.3 Bushy Join Tree

位于 `pkg/optimizer/join/bushy_tree.go`，用于大量表的 JOIN 优化。传统左深树（Left-deep tree）的最大并行度为 1，而 Bushy Tree 允许 JOIN 的左右子节点也是 JOIN 节点，从而提升并行度。

当检测到 3 个及以上表的 JOIN 时，`BushyTreeAdapter` 会尝试构建 Bushy Tree。

### 4.5 JOIN 图

`JoinGraph`（`pkg/optimizer/join/graph.go`）使用图结构表示表之间的连接关系：

- **节点**：表，包含基数信息。
- **边**：连接关系，包含 JOIN 类型和估算基数。

JOIN 图支持以下分析操作：
- **连通分量检测**（BFS）：识别独立的 JOIN 组。
- **星型图检测**：判断是否存在事实表-维表的星型结构。
- **最小生成树**（Kruskal 算法）：找到代价最小的 JOIN 顺序。

### 4.6 JOIN 消除

`JoinEliminationRule`（`pkg/optimizer/join_elimination.go`）识别可以消除的冗余 JOIN：
- 当 JOIN 的一侧是唯一键且不被后续算子引用时，该 JOIN 可以安全消除。

### 4.7 Semi-Join 改写

`SemiJoinRewriteRule`（`pkg/optimizer/semi_join_rewrite.go`）将 `EXISTS`/`IN` 子查询改写为 Semi-Join，避免重复输出行。

### 4.8 Hint 支持

SQLExec 兼容 TiDB 风格的 Hint 语法，支持以下 JOIN Hint：

| Hint | 含义 |
|------|------|
| `HASH_JOIN(t1, t2)` | 强制使用 Hash Join |
| `MERGE_JOIN(t1, t2)` | 强制使用 Merge Join |
| `INL_JOIN(t1)` | 强制使用 Index Nested Loop Join |
| `INL_HASH_JOIN(t1)` | 强制使用 Index Hash Join |
| `NO_HASH_JOIN(t1)` | 禁止使用 Hash Join |
| `LEADING(t1, t2, t3)` | 指定 JOIN 顺序 |
| `STRAIGHT_JOIN` | 按 FROM 子句顺序 JOIN |

---

## 5. 谓词下推

### 5.1 基础谓词下推

`PredicatePushDownRule`（`pkg/optimizer/rules.go`）是最核心的优化规则之一。其原理是将 WHERE 条件尽可能地推到更接近数据源的位置，从而在早期阶段过滤掉不需要的行，减少后续算子处理的数据量。

**基本规则**：

```
                 Selection(cond)           DataSource
                    |                         |
                 DataSource        =>    [cond 标记到 DataSource]
```

当 `Selection` 的子节点是 `DataSource` 时，将过滤条件标记（下推）到 `DataSource`，消除 `Selection` 节点。DataSource 在扫描时使用下推的谓词条件进行过滤。

**Selection 合并**：当连续出现多个 `Selection` 节点时，合并条件列表：

```
Selection(cond1)            Selection(cond1 AND cond2)
    |                            |
Selection(cond2)    =>      Child
    |
  Child
```

### 5.2 增强谓词下推

`EnhancedPredicatePushdownRule`（`pkg/optimizer/enhanced_predicate_pushdown.go`）在基础规则之上增加了三项能力：

#### 5.2.1 跨 JOIN 谓词下推

分析 Selection 中每个条件引用的列属于 JOIN 的哪一侧，将条件推到对应的子节点：

```
Selection(a.x > 10 AND b.y = 'foo')          Join
         |                                   /    \
        Join                     Selection(a.x>10)  Selection(b.y='foo')
       /    \              =>        |                    |
    TableA  TableB                TableA              TableB
```

条件分类的决策枚举：
- `PushLeft`：条件只引用左表列，推到左子节点。
- `PushRight`：条件只引用右表列，推到右子节点。
- `PushBoth`：等值连接条件，推到两侧。
- `PushNone`：无法下推，保留在 Selection 中。

#### 5.2.2 OR 条件转 UNION

当 WHERE 子句包含 OR 条件且各分支引用不同列/索引时，将其转换为 UNION：

```
Selection(a.x = 1 OR a.x = 2)       Union
         |                          /      \
      DataSource       =>   Selection(a.x=1)  Selection(a.x=2)
                                  |                  |
                             DataSource          DataSource
```

这允许每个分支独立利用索引。

#### 5.2.3 相邻 Selection 合并

自动检测并合并相邻的 Selection 节点，减少计划树深度。

### 5.3 Limit 下推

`LimitPushDownRule` 将 `Limit` 节点下推到 `DataSource`：

```
Limit(10, 0)                DataSource
     |                          |
 DataSource      =>    [Limit 标记到 DataSource]
```

这允许数据源在扫描时提前终止，只读取需要的行数。

当 Limit 的子节点是 Selection 时，交换顺序：

```
Limit                      Selection
  |                           |
Selection       =>          Limit
  |                           |
Child                       Child
```

### 5.4 TopN 下推

`TopNPushDownRule`（`pkg/optimizer/topn_pushdown.go`）识别 `Sort + Limit` 的组合模式，将其融合为 `TopN` 操作，避免对全量数据排序：

```
Limit(N)                  TopN(N, orderBy)
  |                           |
Sort(orderBy)    =>        Child
  |
Child
```

TopN 使用堆排序维护前 N 个元素，时间复杂度从 O(n*log(n)) 降至 O(n*log(N))。

### 5.5 列裁剪

`ColumnPruningRule` 分析 Projection 引用的列集合，将不需要的列从 DataSource 中剔除：

```
Projection(a, b)              Projection(a, b)
     |                              |
DataSource(a,b,c,d,e)  =>  DataSource(a, b)  -- 只读取需要的列
```

增强版 `EnhancedColumnPruningRule`（`pkg/optimizer/enhanced_column_pruning.go`）还考虑了 Selection、Join 等算子引用的列，确保裁剪不会遗漏中间计算需要的列。

### 5.6 其他优化规则

- **投影消除**（`ProjectionEliminationRule`）：如果 Projection 只是简单地传递所有列（无计算、无重命名），则消除该节点。
- **常量折叠**（`ConstantFoldingRule`）：在编译阶段计算常量表达式，如 `WHERE 1 + 1 = 2` 直接求值为 TRUE。支持递归折叠 Selection、Projection、Join 中的常量表达式。
- **MAX/MIN 消除**（`MaxMinEliminationRule`）：当查询 `SELECT MAX(col) FROM t` 且 col 上有有序索引时，直接取索引的第一个/最后一个值。
- **子查询去关联**（`DecorrelateRule`）：将关联子查询转换为 JOIN，消除逐行执行子查询的开销。
- **子查询物化**（`SubqueryMaterializationRule`）：对多次执行的子查询，先物化结果再复用。
- **窗口函数 TopN 推导**（`DeriveTopNFromWindowRule`）：从 `ROW_NUMBER() OVER(...) <= N` 模式推导出 TopN 操作。

---

## 6. 并行执行

### 6.1 并行扫描

`ParallelScanner`（`pkg/optimizer/parallel/scanner.go`）实现了并行表扫描：

**原理**：

```
1. 确定并行度 P (默认为 runtime.NumCPU())
2. 将扫描范围 [offset, offset+limit) 均匀划分为 P 个子范围:
   Worker 0: [offset, offset + limit/P)
   Worker 1: [offset + limit/P, offset + 2*limit/P)
   ...
   Worker P-1: [offset + (P-1)*limit/P, offset + limit)
3. 每个 Worker 独立并行执行子范围的扫描查询
4. 收集所有 Worker 的结果并合并
```

**启用条件**：
- 数据行数 >= `minParallelScanRows`（默认 100 行）
- 无过滤条件（有过滤条件时回退为串行扫描）

**Worker Pool**：`WorkerPool`（`pkg/optimizer/parallel/worker_pool.go`）管理并行执行的 goroutine 池，限制最大并行度不超过 64。

**错误处理**：
- 使用 `errChan` 收集 Worker 错误。
- 支持 `context` 取消，通过 `ctx.Done()` 优雅终止所有 Worker。
- Worker 内部使用 `recover()` 捕获 panic。

### 6.2 并行 Hash Join

`ParallelHashJoinExecutor`（`pkg/optimizer/parallel/join_executor.go`）实现了并行的 Hash Join：

**构建阶段并行化**：

```
1. 将构建侧的行按 Worker 数量均匀分配
2. 每个 Worker 独立计算其分配行的哈希键（FNV-64a）
3. 通过互斥锁（sync.Mutex）将结果安全地写入共享哈希表
4. 等待所有 Worker 完成
```

**探测阶段并行化**：

```
1. 将探测侧的行按 Worker 数量均匀分配
2. 每个 Worker 独立读取哈希表（读操作无需加锁）
3. 对匹配的行进行合并，通过互斥锁写入结果集
4. 等待所有 Worker 完成
```

**上下文取消支持**：两个阶段都支持通过 `ctx.Done()` 和内部取消通道（`ctxCancelChan`）优雅终止。当 context 被取消时，关闭取消通道通知所有 Worker 停止工作，然后等待它们退出。

### 6.3 自动并行度选择

`OptimizedParallelScanner`（`pkg/optimizer/optimized_parallel.go`）提供更智能的并行度选择：

```
当 parallelism = 0 时：
  自动选择 min(CPU核心数, 8) 作为并行度
  范围限制在 [4, 8]
```

小于 100 行的表自动使用串行扫描，避免并行化的调度开销。

---

## 7. 索引优化

### 7.1 索引选择器

`IndexSelector`（`pkg/optimizer/index/selector.go`）负责为查询选择最优索引。其工作流程：

```
1. 获取表的所有可用索引
2. 对每个索引进行评估:
   a. 检查索引是否可用（过滤条件是否匹配索引前导列）
   b. 估算索引扫描代价（基于直方图选择率和索引高度）
   c. 检查是否为覆盖索引（所有查询列都在索引中）
   d. 非覆盖索引需加上回表代价
3. 选择总代价最低的索引
```

**索引可用性判断**：过滤条件必须匹配索引的前导列（最左前缀原则）。

**索引代价估算**：

```
IndexScanCost = IndexHeight + EstimatedRows * 0.01
如果不是覆盖索引:
  TotalCost = IndexScanCost + TableLookupCost (约 15 次随机 I/O)
```

**索引高度估算**：基于 B+ 树模型，高度 = ceil(log2(基数))，最小为 2。

**覆盖索引检测**：如果索引包含查询需要的所有列，则为覆盖索引，无需回表查询。覆盖索引可以显著降低 I/O 代价。

### 7.2 索引类型

系统支持四种索引类型：

| 类型 | 常量 | 适用场景 |
|------|------|---------|
| B-Tree 索引 | `BTreeIndex` | 等值查询、范围查询、排序 |
| Hash 索引 | `HashIndex` | 等值查询（O(1) 查找） |
| Bitmap 索引 | `BitmapIndex` | 低基数列的批量过滤 |
| 全文索引 | `FullTextIndex` | 文本搜索（MATCH AGAINST） |

此外，系统还支持：
- **空间索引**（`pkg/optimizer/spatial_index_support.go`）：用于地理空间查询（ST_Contains、ST_Intersects 等）。
- **向量索引**（`pkg/optimizer/rules_vector.go`）：用于向量相似度搜索，支持 HNSW、Flat、IVF-Flat 三种索引类型。

### 7.3 索引建议器

`IndexAdvisor`（`pkg/optimizer/index_advisor.go`）分析查询负载，自动推荐索引创建方案：

**候选索引提取**（`IndexCandidateExtractor`，`pkg/optimizer/index_candidate_extractor.go`）从查询中提取索引候选，来源和优先级：

| 来源 | 优先级 |
|------|--------|
| WHERE 子句 | 4（最高） |
| JOIN 条件 | 3 |
| GROUP BY | 2 |
| ORDER BY | 1（最低） |

**代价-收益分析**（`IndexCostEstimator`，`pkg/optimizer/index_cost_estimator.go`）：
- 使用假想索引（`HypotheticalIndex`）模拟索引存在时的查询代价。
- 比较有索引和无索引的代价差，计算收益百分比。
- 考虑索引的维护代价（写入放大）。

**索引合并**（`IndexMerger`，`pkg/optimizer/index_merger.go`）：
- 将频繁一起出现的候选列合并为复合索引。
- 消除冗余索引（如 (a) 可以被 (a, b) 覆盖）。

### 7.4 索引 Hint

支持以下索引 Hint：

| Hint | 含义 |
|------|------|
| `USE_INDEX(t, idx)` | 建议使用指定索引 |
| `FORCE_INDEX(t, idx)` | 强制使用指定索引 |
| `IGNORE_INDEX(t, idx)` | 忽略指定索引 |
| `ORDER_INDEX(t, idx)` | 使用索引顺序避免排序 |
| `NO_ORDER_INDEX(t, idx)` | 禁止使用索引顺序 |

### 7.5 假想索引

`HypotheticalIndexStore`（`pkg/optimizer/hypothetical_index_store.go`）支持在不实际创建索引的情况下评估索引效果：

```sql
-- 创建假想索引
EXPLAIN HYPOTHETICAL CREATE INDEX idx_name ON table(col1, col2);

-- 评估查询在假想索引下的代价变化
EXPLAIN SELECT * FROM table WHERE col1 = ? AND col2 = ?;
```

假想索引的统计信息（`HypotheticalIndexStats`）包括：
- NDV（唯一值数）
- 选择性（0-1）
- 预估索引大小
- NULL 值比例
- 列相关性因子

---

## 附录：关键数据流

### 完整查询执行流程

以 `SELECT name, age FROM users WHERE age > 20 ORDER BY name LIMIT 10` 为例：

```
1. [OptimizedExecutor.ExecuteSelect]
   |
   v
2. [OptimizedExecutor.executeWithOptimizer]
   构建 SQLStatement，调用 EnhancedOptimizer.Optimize()
   |
   v
3. [EnhancedOptimizer.Optimize]
   3.1 解析 Hint（如果有）
   3.2 调用 baseOptimizer.convertToLogicalPlan()
       转换为逻辑计划:
       Projection(name, age)
         -> Limit(10, 0)
           -> Sort(name ASC)
             -> Selection(age > 20)
               -> DataSource(users)
   |
   v
4. [EnhancedOptimizer.applyEnhancedRules]
   应用优化规则:
   - PredicatePushDown: Selection -> DataSource (下推 age > 20)
   - LimitPushDown: Limit -> DataSource (下推 Limit 10)
   - ColumnPruning: DataSource 只保留 name, age 列
   优化后:
     Projection(name, age)
       -> Sort(name ASC)
         -> DataSource(users, filters=[age>20], limit=10, cols=[name,age])
   |
   v
5. [EnhancedOptimizer.convertToPlanEnhanced]
   转换为可序列化的 Plan:
   Plan{Type: Projection, Children: [
     Plan{Type: Sort, Children: [
       Plan{Type: TableScan, Config: TableScanConfig{
         TableName: "users",
         Filters: [{Field:"age", Op:">", Value:20}],
         LimitInfo: {Limit:10, Offset:0},
         EnableParallel: true
       }}
     ]}
   ]}
   |
   v
6. [BaseExecutor.Execute]
   构建算子树:
   ProjectionOperator
     -> SortOperator
       -> TableScanOperator
   执行: TableScan 读数据 -> Sort 排序 -> Projection 选列
   |
   v
7. 返回 QueryResult{Columns: [name, age], Rows: [...], Total: 10}
```
