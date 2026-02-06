# Task Monitor 任务列表 - Index Advisor 和 Optimizer Hints

## 原始需求
执行 INDEX_ADVISOR_AND_HINTS_PLAN.md 中的所有剩余任务，包括：
- Phase 2 剩余：集成 JOIN hints 到 enhanced_optimizer.go
- Phase 3: INDEX & AGG Hints
- Phase 4: Subquery Hints
- Phase 5: Index Advisor 基础组件
- Phase 6: 遗传算法
- Phase 7: 收益评估和主模块
- Phase 8: 系统视图和 RECOMMEND 解析
- Phase 9: 集成验证和文档

## 已完成 ✓
- Phase 1: Optimizer Hints 基础（types.go, hints_parser.go, 测试）
- Phase 2 部分完成（hint_join.go, hint_join_test.go）

## 待完成任务

### Phase 2: JOIN Hints 集成（剩余部分）
- [ ] 修改 enhanced_optimizer.go 添加 hints 解析
- [ ] 创建 hint-aware 规则适配器
- [ ] 将 hint-aware 规则添加到 EnhancedRuleSet
- [ ] 运行测试验证 JOIN hints 功能

### Phase 3: INDEX & AGG Hints
- [ ] 创建 hint_index.go (约350行)
  - USE_INDEX, FORCE_INDEX, IGNORE_INDEX, ORDER_INDEX, NO_ORDER_INDEX
- [ ] 创建 hint_agg.go (约200行)
  - HASH_AGG, STREAM_AGG, MPP_1PHASE_AGG, MPP_2PHASE_AGG
- [ ] 创建 hint_index_test.go (约250行)
- [ ] 创建 hint_agg_test.go (约150行)
- [ ] 集成到 enhanced_optimizer.go
- [ ] 运行测试验证

### Phase 4: Subquery Hints
- [ ] 创建 hint_subquery.go (约250行)
  - SEMI_JOIN_REWRITE, NO_DECORRELATE, USE_TOJA
- [ ] 创建 hint_subquery_test.go (约200行)
- [ ] 运行测试验证

### Phase 5: Index Advisor 基础组件
- [ ] 创建 hypothetical_index_store.go (约200行)
- [ ] 创建 hypothetical_index_store_test.go
- [ ] 创建 hypothetical_stats.go (约300行)
- [ ] 创建 hypothetical_stats_test.go
- [ ] 创建 index_candidate_extractor.go (约400行)
- [ ] 创建 index_candidate_extractor_test.go

### Phase 6: 遗传算法
- [ ] 创建 genetic_algorithm.go (约500行)
- [ ] 创建 genetic_algorithm_test.go (约350行)
- [ ] 运行测试验证收敛性

### Phase 7: 收益评估和主模块
- [ ] 创建 index_benefit_evaluator.go (约350行)
- [ ] 创建 index_benefit_evaluator_test.go
- [ ] 创建 index_advisor.go (约450行)
- [ ] 创建 index_advisor_test.go (约400行)
- [ ] 集成测试

### Phase 8: 系统视图和 RECOMMEND 解析
- [ ] 创建 system_views.go (约300行)
- [ ] 创建 system_views_test.go
- [ ] 创建 parser/recommend.go (约250行)
- [ ] 创建 parser/recommend_test.go
- [ ] 集成测试

### Phase 9: 集成验证
- [ ] 运行完整测试套件
- [ ] 验证测试覆盖率 >= 90%
- [ ] 编写使用文档
- [ ] 最终验收

## 进度跟踪
- [ ] Phase 2 剩余任务 (0/4)
- [ ] Phase 3 INDEX & AGG Hints (0/6)
- [ ] Phase 4 Subquery Hints (0/3)
- [ ] Phase 5 Index Advisor 基础 (0/6)
- [ ] Phase 6 遗传算法 (0/3)
- [ ] Phase 7 收益评估 (0/5)
- [ ] Phase 8 系统视图 (0/5)
- [ ] Phase 9 集成验证 (0/4)

## 执行日志
