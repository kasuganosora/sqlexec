# Index Advisor 和 Optimizer Hints 实现计划（修订版）

## 1. 核心需求

### 1.1 功能优先级
1. **Optimizer Hints**（高优先级）
   - 实现所有 TiDB Optimizer Hints（30+ 种）
   - JOIN、INDEX、AGG、Subquery、全局 hints
   
2. **Index Advisor**（中优先级）
   - RECOMMEND INDEX 语句
   - Hypothetical Indexes（虚拟索引）
   - 遗传算法优化
   - What-If 成本分析

### 1.2 技术约束
- ❌ **不实现 HTTP API**
- ✅ **系统表**：内存表，不持久化
  - `information_schema.index_advisor_results`
  - `information_schema.schema_unused_indexes`
  - `information_schema.hypothetical_indexes`
- ✅ **Index Advisor 精度**：估算值
- ✅ **测试覆盖率**：≥ 90%

## 2. 现有架构分析

### 2.1 核心组件

```
pkg/optimizer/
├── optimizer.go              # 基础优化器（convertToLogicalPlan + rules + convertToPhysicalPlan）
├── enhanced_optimizer.go     # 增强优化器（集成 DPJoinReorder、IndexSelector）
├── rules.go                  # 优化规则（PredicatePushDown、ColumnPruning 等）
├── types.go                  # LogicalPlan、PhysicalPlan、JoinType
└── logical_*.go              # 逻辑算子

pkg/parser/
├── parser.go                 # 封装 TiDB parser
├── types.go                  # AST 结构（SQLStatement、SelectStatement）
└── expressions.go            # Expression 定义

pkg/api/
├── query.go                  # Query 结果封装
└── db.go                     # 数据库操作接口
```

### 2.2 关键设计模式
- **Rule-based Optimization**: `OptimizationRule` 接口
- **Cost-based Optimization**: `AdaptiveCostModel`
- **Visitor Pattern**: LogicalPlan 递归遍历

## 3. Optimizer Hints 实现

### 3.1 核心数据结构

**文件**: `pkg/optimizer/types.go`（扩展，新增 150 行）

```go
type OptimizerHints struct {
    // JOIN hints
    HashJoinTables     []string
    MergeJoinTables    []string
    INLJoinTables      []string
    LeadingOrder       []string
    StraightJoin       bool
    
    // INDEX hints
    UseIndex       map[string][]string
    ForceIndex     map[string][]string
    IgnoreIndex    map[string][]string
    OrderIndex     map[string]string
    
    // AGG hints
    HashAgg     bool
    StreamAgg   bool
    
    // Subquery hints
    SemiJoinRewrite  bool
    NoDecorrelate    bool
    
    // 全局 hints
    MaxExecutionTime  time.Duration
    MemoryQuota       int64
}

type HintAwareRule interface {
    OptimizationRule
    ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error)
}
```

**文件**: `pkg/parser/types.go`（扩展，新增 15 行）

```go
type SelectStatement struct {
    // ... existing fields ...
    Hints      *OptimizerHints `json:"hints,omitempty"`  // 新增
}
```

### 3.2 核心实现文件

#### 3.2.1 Hints Parser（新增）
- **文件**: `pkg/parser/hints_parser.go`
- **功能**: 解析 `/*+ HINT() */` 语法
- **测试覆盖率**: > 95%
- **复杂度**: 300 行

**实现要点**:
- 正则提取 hints 注释
- 支持 30+ 种 hint 类型
- 参数解析和验证
- 错误处理和警告

#### 3.2.2 JOIN Hints 规则（新增）
- **文件**: `pkg/optimizer/hint_join.go`
- **功能**: HASH_JOIN, MERGE_JOIN, INL_JOIN, LEADING, STRAIGHT_JOIN
- **测试覆盖率**: > 90%
- **复杂度**: 400 行

**实现要点**:
- 表名匹配（别名和全名）
- JOIN 类型强制
- 连接顺序强制
- Hints 冲突处理

#### 3.2.3 INDEX Hints 规则（新增）
- **文件**: `pkg/optimizer/hint_index.go`
- **功能**: USE_INDEX, FORCE_INDEX, IGNORE_INDEX, ORDER_INDEX
- **测试覆盖率**: > 90%
- **复杂度**: 350 行

**实现要点**:
- 候选索引过滤
- 索引排除
- 顺序控制（keep_order）
- 多列索引支持

#### 3.2.4 AGG Hints 规则（新增）
- **文件**: `pkg/optimizer/hint_agg.go`
- **功能**: HASH_AGG, STREAM_AGG, MPP_1PHASE_AGG, MPP_2PHASE_AGG
- **测试覆盖率**: > 90%
- **复杂度**: 200 行

**实现要点**:
- 聚合算法强制
- LogicalAggregate 扩展
- 算法选择逻辑

#### 3.2.5 Subquery Hints 规则（新增）
- **文件**: `pkg/optimizer/hint_subquery.go`
- **功能**: SEMI_JOIN_REWRITE, NO_DECORRELATE, USE_TOJA
- **测试覆盖率**: > 90%
- **复杂度**: 250 行

**实现要点**:
- Semi Join 改写
- 去关联控制
- 复用 DecorrelateRule

#### 3.2.6 EnhancedOptimizer 集成（修改）
- **文件**: `pkg/optimizer/enhanced_optimizer.go`
- **修改**: 50 行新增
- **功能**: 集成 hints 解析和应用

**修改内容**:
```go
// 新增字段
type EnhancedOptimizer struct {
    // ... existing fields ...
    hintsParser *parser.HintsParser  // 新增
}

// Optimize 方法扩展
func (eo *EnhancedOptimizer) Optimize(ctx context.Context, stmt *parser.SQLStatement) (PhysicalPlan, error) {
    // 1. 提取并解析 hints
    cleanSQL, hints, err := eo.hintsParser.ExtractAndParse(stmt.Select.RawSQL)
    // ...
    
    // 2. 应用 Hint-Aware 规则
    optimizedPlan, err := eo.applyEnhancedRulesWithHints(ctx, logicalPlan, optCtx, hints)
    // ...
}
```

#### 3.2.7 RuleSet 增强（修改）
- **文件**: `pkg/optimizer/rules.go`
- **修改**: 20 行新增
- **功能**: 添加 Hint-Aware 规则到 EnhancedRuleSet

## 4. Index Advisor 实现

### 4.1 核心数据结构

**文件**: `pkg/optimizer/types.go`（扩展）

```go
type HypotheticalIndex struct {
    ID          string
    TableName   string
    Columns     []string
    IsUnique    bool
    Stats       *HypotheticalIndexStats
}

type IndexRecommendation struct {
    TableName        string
    Columns          []string
    EstimatedBenefit float64  // 收益（0-1）
    Reason           string
    CreateStatement  string
}
```

### 4.2 核心实现文件

#### 4.2.1 Hypothetical Index Store（新增）
- **文件**: `pkg/optimizer/hypothetical_index_store.go`
- **功能**: 管理虚拟索引（创建、查询、删除）
- **持久化**: 内存存储，不持久化
- **测试覆盖率**: > 90%
- **复杂度**: 200 行

**实现要点**:
- sync.RWMutex 保证并发安全
- map 存储索引（indexID -> HypotheticalIndex）
- 表到索引的映射（tableName -> indexIDs）
- 自动 ID 生成

#### 4.2.2 虚拟索引统计信息（新增）
- **文件**: `pkg/optimizer/hypothetical_stats.go`
- **功能**: 基于表统计信息生成虚拟索引统计
- **算法**: NDV 计算、选择性估算、大小估算
- **测试覆盖率**: > 90%
- **复杂度**: 300 行

**实现要点**:
- 复用 statistics.AutoRefreshStatisticsCache
- NDV 计算（单列和多列）
- 相关性因子估算
- 索引大小估算

#### 4.2.3 索引候选提取（新增）
- **文件**: `pkg/optimizer/index_candidate_extractor.go`
- **功能**: 从 SQL 提取可索引列
- **来源**: WHERE、JOIN、ORDER BY、GROUP BY
- **测试覆盖率**: > 90%
- **复杂度**: 400 行

**实现要点**:
- 表达式树遍历
- 可索引运算符识别（=, >, <, LIKE, IN）
- 列类型检查（排除 BLOB/TEXT/JSON）
- 优先级排序（WHERE > JOIN > GROUP > ORDER）
- 候选去重和合并

#### 4.2.4 遗传算法（新增）
- **文件**: `pkg/optimizer/genetic_algorithm.go`
- **功能**: 搜索最优索引组合
- **参数**:
  - PopulationSize: 50
  - MaxGenerations: 100
  - MutationRate: 0.1
  - CrossoverRate: 0.8
- **测试覆盖率**: > 90%
- **复杂度**: 500 行

**实现要点**:
- 种群初始化（随机选择 30% 索引）
- 适应度函数（成本降低）
- 选择（轮盘赌 + 精英保留）
- 交叉（单点交叉）
- 变异（随机翻转）
- 约束检查（数量、大小、列数）
- 收敛判断（适应度差异 < 1%）

#### 4.2.5 索引收益评估器（新增）
- **文件**: `pkg/optimizer/index_benefit_evaluator.go`
- **功能**: What-If 分析，评估索引收益
- **算法**: 成本对比（无索引 vs 虚拟索引）
- **测试覆盖率**: > 90%
- **复杂度**: 350 行

**实现要点**:
- 创建虚拟索引
- 生成统计信息
- 注入优化器
- 生成执行计划
- 成本对比计算收益
- 清理虚拟索引

#### 4.2.6 Index Advisor 主模块（新增）
- **文件**: `pkg/optimizer/index_advisor.go`
- **功能**: 整合所有组件，生成推荐
- **测试覆盖率**: > 90%
- **复杂度**: 450 行

**实现要点**:
- **单查询推荐**: 解析 → 提取 → 评估 → 遗传算法 → 生成结果
- **工作负载推荐**: 获取查询 → 分析每查询 → 合并候选 → 综合收益 → 遗传算法
- 配置参数管理
- 收益计算（加权平均）

**配置参数**:
```go
MaxNumIndexes:   5   // 最大推荐索引数
MaxIndexColumns: 3   // 单列索引最大列数
MaxNumQuery:     1000 // 工作负载最大查询数
Timeout:         30s  // 超时时间
```

#### 4.2.7 系统视图（新增）
- **文件**: `pkg/optimizer/system_views.go`
- **功能**: information_schema 内存表
- **视图**:
  - `index_advisor_results`：索引推荐结果
  - `schema_unused_indexes`：未使用索引
  - `hypothetical_indexes`：虚拟索引
- **持久化**: 无（内存表）
- **测试覆盖率**: > 85%
- **复杂度**: 300 行

**实现要点**:
- sync.RWMutex 保证并发安全
- 切片存储数据
- 定时扫描（每天）或手动触发
- 内存限制（最多 500 条推荐）

#### 4.2.8 RECOMMEND INDEX Parser（新增）
- **文件**: `pkg/parser/recommend.go`
- **功能**: 解析 RECOMMEND INDEX 语句
- **支持语法**:
  ```sql
  RECOMMEND INDEX RUN [FOR "SQL"];
  RECOMMEND INDEX SHOW OPTION;
  RECOMMEND INDEX SET option = value;
  ```
- **测试覆盖率**: > 90%
- **复杂度**: 250 行

## 5. 测试策略

### 5.1 测试覆盖率目标

| 模块 | 目标覆盖率 | 测试类型 |
|------|-----------|----------|
| Hints Parser | > 95% | 单元测试 |
| Hint-Aware Rules | > 90% | 单元 + 集成 |
| Hypo Index Store | > 90% | 单元测试 |
| Hypo Stats | > 90% | 单元测试 |
| Candidate Extractor | > 90% | 单元测试 |
| Genetic Algorithm | > 90% | 单元测试 |
| Benefit Evaluator | > 90% | 单元测试 |
| Index Advisor | > 90% | 单元 + 集成 |
| System Views | > 85% | 单元测试 |
| **整体** | **> 90%** | **全类型** |

### 5.2 测试文件清单

**Optimizer Hints 测试**:
- `pkg/parser/hints_parser_test.go`（200 行）
- `pkg/optimizer/hint_join_test.go`（300 行）
- `pkg/optimizer/hint_index_test.go`（250 行）
- `pkg/optimizer/hint_agg_test.go`（150 行）
- `pkg/optimizer/hint_subquery_test.go`（200 行）

**Index Advisor 测试**:
- `pkg/optimizer/hypothetical_index_store_test.go`（200 行）
- `pkg/optimizer/hypothetical_stats_test.go`（250 行）
- `pkg/optimizer/index_candidate_extractor_test.go`（300 行）
- `pkg/optimizer/genetic_algorithm_test.go`（350 行）
- `pkg/optimizer/index_benefit_evaluator_test.go`（250 行）
- `pkg/optimizer/index_advisor_test.go`（400 行）
- `pkg/optimizer/system_views_test.go`（200 行）

**Parser 测试**:
- `pkg/parser/recommend_test.go`（150 行）

**集成测试**:
- `pkg/optimizer/hints_integration_test.go`（300 行）
- `pkg/optimizer/index_advisor_integration_test.go`（350 行）

**总计**: 约 4200 行测试代码

### 5.3 关键测试场景

**Optimizer Hints**:
1. ✅ 所有 hint 类型解析
2. ✅ 多 hints 组合
3. ✅ 无效 hint 处理
4. ✅ LEADING 顺序强制
5. ✅ STRAIGHT_JOIN 应用
6. ✅ JOIN 类型强制
7. ✅ INDEX 过滤/排除
8. ✅ AGG 算法强制
9. ✅ Subquery 改写
10. ✅ Hints 冲突处理

**Index Advisor**:
1. ✅ WHERE 子句提取（等值、范围、LIKE、IN）
2. ✅ JOIN 条件提取
3. ✅ ORDER BY 提取（单列/多列）
4. ✅ GROUP BY 提取
5. ✅ 列类型检查（BLOB/TEXT/JSON 排除）
6. ✅ NDV 计算（单列/多列）
7. ✅ 遗传算法收敛
8. ✅ What-If 成本对比
9. ✅ 单查询推荐
10. ✅ 工作负载推荐
11. ✅ 系统视图查询

**集成场景**:
1. ✅ 带 hints 的查询优化
2. ✅ EXPLAIN 输出验证
3. ✅ 虚拟索引评估
4. ✅ RECOMMEND INDEX 端到端
5. ✅ 性能对比测试

### 5.4 测试执行命令

```bash
# 运行所有优化器测试
go test ./pkg/optimizer/... -v -cover -coverprofile=coverage.out

# 查看覆盖率报告
go tool cover -html=coverage.out -o coverage.html

# 运行特定测试
go test ./pkg/optimizer -run TestHintAwareJoinReorderRule -v

# 运行集成测试
go test ./pkg/optimizer -run Integration -v

# 性能测试
go test ./pkg/optimizer -bench=. -benchmem
```

**覆盖率检查**:
```bash
# 确保总体覆盖率 >= 90%
coverage=$(go test ./pkg/optimizer/... -cover | grep -o '[0-9.]*%' | head -1 | tr -d '%')
if (( $(echo "$coverage < 90" | bc -l) )); then
    echo "Coverage $coverage% is below 90%"
    exit 1
fi
```

## 6. 修改点汇总

### 6.1 新增文件（13 个）

| 文件 | 功能 | 复杂度 | 测试文件 |
|------|------|--------|----------|
| `pkg/parser/hints_parser.go` | Hints 解析 | 300 行 | `hints_parser_test.go` (200 行) |
| `pkg/optimizer/hint_join.go` | JOIN hints | 400 行 | `hint_join_test.go` (300 行) |
| `pkg/optimizer/hint_index.go` | INDEX hints | 350 行 | `hint_index_test.go` (250 行) |
| `pkg/optimizer/hint_agg.go` | AGG hints | 200 行 | `hint_agg_test.go` (150 行) |
| `pkg/optimizer/hint_subquery.go` | Subquery hints | 250 行 | `hint_subquery_test.go` (200 行) |
| `pkg/optimizer/hypothetical_index_store.go` | 虚拟索引存储 | 200 行 | `hypothetical_index_store_test.go` (200 行) |
| `pkg/optimizer/hypothetical_stats.go` | 虚拟索引统计 | 300 行 | `hypothetical_stats_test.go` (250 行) |
| `pkg/optimizer/index_candidate_extractor.go` | 候选提取 | 400 行 | `index_candidate_extractor_test.go` (300 行) |
| `pkg/optimizer/genetic_algorithm.go` | 遗传算法 | 500 行 | `genetic_algorithm_test.go` (350 行) |
| `pkg/optimizer/index_benefit_evaluator.go` | 收益评估 | 350 行 | `index_benefit_evaluator_test.go` (250 行) |
| `pkg/optimizer/index_advisor.go` | Index Advisor | 450 行 | `index_advisor_test.go` (400 行) |
| `pkg/optimizer/system_views.go` | 系统视图 | 300 行 | `system_views_test.go` (200 行) |
| `pkg/parser/recommend.go` | RECOMMEND 解析 | 250 行 | `recommend_test.go` (150 行) |

**总计**: 4000 行实现 + 3000 行测试 = 7000 行

### 6.2 修改文件（4 个）

| 文件 | 修改内容 | 新增行数 |
|------|----------|----------|
| `pkg/optimizer/types.go` | OptimizerHints、HintAwareRule、AggregationAlgorithm | 150 |
| `pkg/parser/types.go` | AST Hints 字段 | 15 |
| `pkg/optimizer/enhanced_optimizer.go` | Hints 解析和应用 | 50 |
| `pkg/optimizer/rules.go` | EnhancedRuleSet 扩展 | 20 |

**总计**: 235 行

### 6.3 整体影响

**代码量**:
- 新增: 4000 行
- 修改: 235 行
- 测试: 3000 行
- **总计**: 7235 行

**文件数**:
- 新增: 13 个实现 + 13 个测试 = 26 个
- 修改: 4 个
- **总计**: 30 个文件

**性能影响**:
- Hints 解析: < 1ms
- Hints 应用: < 5ms
- Index Advisor 单查询: < 500ms
- Index Advisor 工作负载（1000 查询）: < 30s

**向后兼容性**:
- 完全兼容（无 hints 时使用原有逻辑）
- 默认行为不变
- 可配置开关（通过 feature flag）

## 7. 实施步骤

### 阶段 1: Optimizer Hints 基础（3 天）
1. ✅ 扩展 types.go（OptimizerHints、HintAwareRule）
2. ✅ 扩展 parser/types.go（AST Hints 字段）
3. ✅ 实现 hints_parser.go
4. ✅ 编写 hints_parser_test.go

### 阶段 2: JOIN Hints（2 天）
1. ✅ 实现 hint_join.go
2. ✅ 编写 hint_join_test.go
3. ✅ 集成到 enhanced_optimizer.go
4. ✅ 验证 LEADING 和 STRAIGHT_JOIN

### 阶段 3: INDEX & AGG Hints（2 天）
1. ✅ 实现 hint_index.go
2. ✅ 实现 hint_agg.go
3. ✅ 编写测试文件
4. ✅ 验证 USE_INDEX、HASH_AGG

### 阶段 4: Subquery Hints（1 天）
1. ✅ 实现 hint_subquery.go
2. ✅ 编写测试文件
3. ✅ 验证 SEMI_JOIN_REWRITE

### 阶段 5: Index Advisor 基础（3 天）
1. ✅ 实现 hypothetical_index_store.go
2. ✅ 实现 hypothetical_stats.go
3. ✅ 实现 index_candidate_extractor.go
4. ✅ 编写对应测试

### 阶段 6: 遗传算法（2 天）
1. ✅ 实现 genetic_algorithm.go
2. ✅ 编写测试（收敛性、约束检查）
3. ✅ 调优算法参数

### 阶段 7: 收益评估（2 天）
1. ✅ 实现 index_benefit_evaluator.go
2. ✅ 实现 index_advisor.go
3. ✅ 编写 What-If 测试

### 阶段 8: 系统视图（1 天）
1. ✅ 实现 system_views.go
2. ✅ 实现 recommend.go
3. ✅ 编写集成测试

### 阶段 9: 集成验证（2 天）
1. ✅ 运行完整测试套件
2. ✅ 验证覆盖率 >= 90%
3. ✅ 性能基准测试
4. ✅ 编写使用文档

**总计**: 18 天

## 9. 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|----------|
| Hints 解析性能差 | 中 | 正则优化 + 缓存 |
| 遗传算法不收敛 | 中 | 参数调优 + 超时保护 |
| 成本估算不准确 | 中 | 保守估算 + 统计校验 |
| 测试覆盖不达标 | 低 | 增加边界场景测试 |
| 与现有代码冲突 | 低 | 代码审查 + 集成测试 |

## 10. 验收标准

### 10.1 功能验收
- ✅ 所有 30+ 种 hints 正确解析
- ✅ Hints 正确影响执行计划
- ✅ Index Advisor 单查询推荐 < 500ms
- ✅ Index Advisor 工作负载推荐 < 30s
- ✅ 系统视图可查询
- ✅ RECOMMEND INDEX 语法支持

### 10.2 测试验收
- ✅ 总体覆盖率 >= 90%
- ✅ 核心模块覆盖率 >= 90%
- ✅ 所有集成测试通过
- ✅ 性能基准测试通过

### 10.3 代码质量
- ✅ 遵循 Go 编码规范
- ✅ 无 UTF-8 编码问题
- ✅ 错误处理完善
- ✅ 日志记录清晰
- ✅ 文档完整

## 11. 后续扩展（预留）

### 11.1 持久化
- Index Advisor 结果持久化到文件
- 历史推荐查询
- 推荐效果跟踪

### 11.2 高级功能
- 索引合并推荐
- 覆盖索引推荐
- 分区索引推荐

### 11.3 可视化
- Web UI 展示推荐结果
- 执行计划对比
- 性能提升分析
