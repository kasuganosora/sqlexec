# 优化器-执行器拆分任务执行计划

## 原始需求
执行优化器-执行器拆分计划，确保100%完成所有任务

## 架构理解

### 当前架构
- `pkg/optimizer/types.go`: PhysicalPlan接口包含Execute()方法
- `pkg/optimizer/optimizer.go`: Optimize()返回PhysicalPlan
- `pkg/optimizer/physical_*.go`: 物理算子包含执行逻辑
- `pkg/optimizer/optimized_executor.go`: 集成执行器
- `pkg/api/query.go`, `session_query.go`: API层使用OptimizedExecutor

### 目标架构
- `pkg/optimizer/plan`: 可序列化的Plan结构体（不含数据源）
- `pkg/optimizer/optimizer.go`: 返回plan.Plan
- `pkg/executor`: 独立执行器包
- `pkg/executor/operators`: 算子执行逻辑
- `pkg/executor/parallel`: 并行执行框架（从optimizer/parallel迁移）
- `pkg/dataaccess`: 统一数据访问服务
- `pkg/api`: 使用新的executor

## 执行计划

### 阶段1：核心接口重构

#### 任务1.1：创建pkg/optimizer/plan包
- [ ] 1.1.1 创建pkg/optimizer/plan目录
- [ ] 1.1.2 创建types.go - 定义PlanType常量和Plan结构
- [ ] 1.1.3 创建table_scan.go - TableScanConfig
- [ ] 1.1.4 创建hash_join.go - HashJoinConfig
- [ ] 1.1.5 创建sort.go - SortConfig
- [ ] 1.1.6 创建aggregate.go - AggregateConfig
- [ ] 1.1.7 创建projection.go - ProjectionConfig
- [ ] 1.1.8 创建selection.go - SelectionConfig
- [ ] 1.1.9 创建limit.go - LimitConfig

#### 任务1.2：重构PhysicalPlan接口
- [ ] 1.2.1 修改pkg/optimizer/types.go，移除PhysicalPlan.Execute()方法
- [ ] 1.2.2 修改optimizer.go中Optimize()方法签名，改为返回*plan.Plan

### 阶段2：执行器独立

#### 任务2.1：创建pkg/executor基础结构
- [ ] 2.1.1 创建pkg/executor目录
- [ ] 2.1.2 创建executor.go - Executor接口和BaseExecutor实现
- [ ] 2.1.3 创建runtime.go - 执行运行时
- [ ] 2.1.4 创建aggregator.go - 结果聚合器

#### 任务2.2：创建pkg/executor/operators
- [ ] 2.2.1 创建operators目录和base.go - 算子基类
- [ ] 2.2.2 从physical_scan.go迁移执行逻辑到operators/table_scan.go
- [ ] 2.2.3 创建operators/hash_join.go
- [ ] 2.2.4 从physical_sort.go迁移到operators/sort.go
- [ ] 2.2.5 从optimized_aggregate.go迁移到operators/aggregate.go（保留DuckDB Perfect Hash优化）
- [ ] 2.2.6 创建operators/projection.go
- [ ] 2.2.7 创建operators/selection.go

#### 任务2.3：创建pkg/dataaccess
- [ ] 2.3.1 创建pkg/dataaccess目录
- [ ] 2.3.2 创建service.go - 数据访问服务接口和实现
- [ ] 2.3.3 创建manager.go - 数据源管理器
- [ ] 2.3.4 创建router.go - 数据源路由

#### 任务2.4：迁移并行执行框架
- [ ] 2.4.1 创建executor/parallel目录
- [ ] 2.4.2 从optimizer/parallel迁移scanner.go
- [ ] 2.4.3 从optimizer/parallel迁移join_executor.go
- [ ] 2.4.4 从optimizer/parallel迁移worker_pool.go
- [ ] 2.4.5 创建parallel/aggregator.go - 并行结果聚合
- [ ] 2.4.6 删除optimizer/parallel目录

### 阶段3：集成与适配

#### 任务3.1：修改优化器返回plan.Plan
- [ ] 3.1.1 修改optimizer.go中的convertToPhysicalPlan()为convertToPlan()
- [ ] 3.1.2 实现convertToPlan()，将逻辑计划转换为*plan.Plan
- [ ] 3.1.3 更新enhanced_optimizer.go

#### 任务3.2：修改API层使用新的executor
- [ ] 3.2.1 修改pkg/api/query.go使用executor
- [ ] 3.2.2 修改pkg/api/session_query.go使用executor
- [ ] 3.2.3 更新optimized_executor.go适配新架构

#### 任务3.3：更新所有测试
- [ ] 3.3.1 更新pkg/optimizer中的测试
- [ ] 3.3.2 更新pkg/api中的测试
- [ ] 3.3.3 更新pkg/executor中的测试
- [ ] 3.3.4 创建executor/operators的测试

### 阶段4：验证与清理

#### 任务4.1：验证编译
- [ ] 4.1.1 执行go build验证所有包编译通过
- [ ] 4.1.2 执行go test验证测试通过

#### 任务4.2：功能验证
- [ ] 4.2.1 验证基本查询功能
- [ ] 4.2.2 验证JOIN查询功能
- [ ] 4.2.3 验证聚合查询功能
- [ ] 4.2.4 验证并行扫描功能

#### 任务4.3：清理临时文件
- [ ] 4.3.1 清理所有.bak文件
- [ ] 4.3.2 清理临时生成的文件
- [ ] 4.3.3 验证代码整洁度

## 执行统计

- 总任务数: 52
- 已完成: 0
- 进行中: 0
- 待执行: 52
