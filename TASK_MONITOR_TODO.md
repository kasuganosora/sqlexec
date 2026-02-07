# Task Monitor - 优化器测试修复任务清单

## 原始需求
修复优化器包中的业务逻辑测试失败问题，运行完整测试套件，识别所有失败的测试，并逐一修复，确保所有测试通过。

## 测试失败汇总

### pkg/optimizer (12个测试)
1. ❌ `TestAdaptiveParameterAdjustment` - 自适应参数调整断言失败
2. ❌ `TestNewIndexCandidateExtractor` - TEXT 类型未在 excludeTypes 中
3. ❌ `TestIsColumnTypeSupported` - TEXT 类型支持判定错误
4. ❌ `TestEstimateSelectivityFromNDV` - 选择率估计值错误 (期望0.1, 得到5e-05)
5. ❌ `TestEstimateRangeSelectivity` - 范围选择率断言失败
6. ❌ `TestEstimateAggregateCost` - 聚合成本断言失败
7. ❌ `TestAdjustCostForSkew` - 数据倾斜成本调整错误 (期望150, 得到125)
8. ❌ `TestEstimateCardinality` - 基数估计错误 (期望10000, 得到1000)
9. ❌ `TestFullTextIndexSupport` - 全文索引支持断言失败
10. ❌ `TestFullTextSearchBenefit` - 全文搜索收益断言失败
11. ❌ `TestSpatialFunctionExtraction` - 空间函数提取错误 (期望ST_CONTAINS, 得到POINT)
12. ❌ `TestSpatialIndexExtraction` - 空间索引提取失败
13. ❌ `TestSpatialQueryBenefit` - 空间查询收益断言失败

### pkg/optimizer/index (2个测试)
14. ❌ `TestIndexSelector_SelectBestIndex_NoIndices` - 无索引时成本值错误
15. ❌ `TestIndexSelector_IsIndexUsable` - 索引可用性判定错误

### pkg/optimizer/join (13个测试)
16. ❌ `TestNewBushyJoinTreeBuilder` - 返回 nil
17. ❌ `TestBushyJoinTreeBuilder_BuildBushyTree_ManyTables` - 构建失败返回 nil (3个子测试)
18. ❌ `TestBushyJoinTreeBuilder_BuilderProperties` - 属性为 nil
19. ❌ `TestBushyJoinTreeBuilder_ZeroMaxBushiness` - 返回 nil
20. ❌ `TestBushyJoinTreeBuilder_NegativeMaxBushiness` - 返回 nil
21. ❌ `TestBushyJoinTreeBuilder_DuplicateTables` - 返回 nil
22. ❌ `TestBushyJoinTreeBuilder_LargeTableSet` - 返回 nil
23. ❌ `TestBushyJoinTreeBuilder_NilCostModel` - 返回 nil
24. ❌ `TestBushyJoinTreeBuilder_NilEstimator` - 返回 nil
25. ❌ `TestBushyJoinTreeBuilder_EdgeCases` - 返回 nil (4个子测试)
26. ❌ `TestBushyJoinTreeBuilder_MultipleBuilds` - 返回 nil
27. ❌ `TestDPJoinReorder_DpSearch` - DP搜索返回 nil
28. ❌ `TestDPJoinReorder_BuildPlanFromOrder` - 构建计划错误 (2个子测试)

### pkg/optimizer/parallel (1个测试超时)
29. ❌ `TestParallelAggregator_SimpleJoin` - 超时 (600.955s, > 10分钟)

## 修复计划

### Phase 1: 修复 pkg/optimizer 的测试
- 修复遗传算法自适应参数调整逻辑
- 修复索引候选提取器的类型支持
- 修复索引成本估算器的数值计算
- 修复全文索引和空间索引的支持逻辑

### Phase 2: 修复 pkg/optimizer/index 的测试
- 修复索引选择器的成本计算
- 修复索引可用性判定逻辑

### Phase 3: 修复 pkg/optimizer/join 的测试
- 修复 BushyJoinTreeBuilder 的实现
- 修复 DPJoinReorder 的实现

### Phase 4: 修复 pkg/optimizer/parallel 的测试
- 修复并行执行器的超时/死锁问题

## 执行记录

- [ ] 开始修复
