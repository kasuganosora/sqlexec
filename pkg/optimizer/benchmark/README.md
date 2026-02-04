# Optimizer 性能基准测试

## 概述

本目录包含 `pkg/optimizer` 的性能基准测试套件，用于测量和跟踪优化器在各种查询场景下的性能表现。

## 测试场景

### 1. 单表查询基准
- **BenchmarkSingleTable_Small**: 100行小数据集扫描
- **BenchmarkSingleTable_Medium**: 10,000行中等数据集扫描
- **BenchmarkSingleTable_Large**: 100,000行大数据集扫描

### 2. JOIN性能基准
- **BenchmarkJoin2Table_Inner**: 2表INNER JOIN（1,000 x 1,000）
- **BenchmarkJoin2Table_Left**: 2表LEFT JOIN（10,000 x 1,000）
- **BenchmarkJoin2Table_Right**: 2表RIGHT JOIN（1,000 x 10,000）
- **BenchmarkJoin2Table_Full**: 2表FULL OUTER JOIN（暂时跳过，UNION未支持）

### 3. 多表JOIN（部分暂不支持）
- **BenchmarkJoin3Table_Chain**: 3表链式JOIN（暂时跳过）
- **BenchmarkJoin3Table_Star**: 3表星型JOIN（暂时跳过）
- **BenchmarkJoin4Table_Chain**: 4表链式JOIN（暂时跳过）
- **BenchmarkJoin4Table_Star**: 4表星型JOIN（暂时跳过）
- **BenchmarkJoin_BushyTree_4Tables**: 4表Bushy Tree JOIN（暂时跳过）

### 4. 复杂查询基准
- **BenchmarkComplexQuery_MultiCondition**: 多条件WHERE查询（3-5个AND条件）
- **BenchmarkComplexQuery_OR**: 多OR条件查询
- **BenchmarkComplexQuery_GroupBy**: GROUP BY聚合
- **BenchmarkComplexQuery_Subquery**: 嵌套子查询（使用JOIN模拟）
- **BenchmarkComplexQuery_Complex**: 复合查询（暂时跳过）

### 5. 优化规则性能
- **BenchmarkOptimization_PredicatePushdown**: 谓词下推优化效果（带/不带谓词对比）
- **BenchmarkOptimization_JoinReorder**: JOIN重排序优化效果（暂时跳过）

### 6. 并行执行性能
- **BenchmarkParallel_Scan**: 并行扫描 vs 串行扫描（1, 2, 4并行度）
- **BenchmarkParallel_Join**: 并行JOIN vs 串行JOIN

### 7. 聚合性能基准
- **BenchmarkAggregate_COUNT**: COUNT聚合
- **BenchmarkAggregate_SUM**: SUM聚合
- **BenchmarkAggregate_AVG**: AVG聚合
- **BenchmarkAggregate_GroupByWithCount**: GROUP BY + COUNT
- **BenchmarkAggregate_GroupByWithMultiple**: GROUP BY + 多个聚合函数

### 8. 排序性能基准
- **BenchmarkSort_SmallLimit**: 小LIMIT查询
- **BenchmarkSort_SingleColumn**: 单列排序
- **BenchmarkSort_OrderByWithLimit**: ORDER BY + LIMIT

### 9. 端到端场景基准（暂时跳过）
- **BenchmarkECommerce_OrderQuery**: 电商订单查询
- **BenchmarkECommerce_ProductAnalysis**: 产品分析查询

## 运行基准测试

### 运行所有基准测试
```bash
cd d:/code/db
go test -bench=. -benchmem -run=^$ -benchtime=1s ./pkg/optimizer/
```

### 运行特定类型的基准测试
```bash
# 单表查询
go test -bench=BenchmarkSingleTable -benchmem -run=^$ ./pkg/optimizer/

# JOIN测试
go test -bench=BenchmarkJoin2Table -benchmem -run=^$ ./pkg/optimizer/

# 复杂查询
go test -bench=BenchmarkComplexQuery -benchmem -run=^$ ./pkg/optimizer/

# 聚合查询
go test -bench=BenchmarkAggregate -benchmem -run=^$ ./pkg/optimizer/
```

### 运行并保存结果
```bash
# Linux/Mac
go test -bench=. -benchmem -run=^$ ./pkg/optimizer/ | tee benchmark_output.txt

# Windows PowerShell
go test -bench=. -benchmem -run=^$ ./pkg/optimizer/ | Tee-Object -FilePath benchmark_output.txt
```

## 基准测试文件说明

### performance_benchmark_test.go
主基准测试文件，包含所有基准测试函数。

### benchmark/baseline.json
基准分数记录文件，包含：
- 时间戳
- Go版本
- 系统信息（CPU核心数、内存）
- 各基准测试的性能指标（ops/sec, ns/op, B/op, allocs/op）

### benchmark/run_benchmarks.sh
基准测试运行脚本（Linux/Mac）。

### benchmark/generate_baseline.sh
生成基准测试结果并收集到文本文件。

## 基准测试输出说明

基准测试输出格式如下：
```
BenchmarkSingleTable_Small-12    898034    297.4 ns/op    192 B/op    2 allocs/op
```

其中：
- `12`: GOMAXPROCS值
- `898034`: 执行次数（b.N）
- `297.4 ns/op`: 每次操作的平均纳秒数
- `192 B/op`: 每次操作分配的平均字节数
- `2 allocs/op`: 每次操作的平均分配次数

## 更新基准分数

当系统或代码变更后，运行基准测试并更新baseline.json：

```bash
# 运行基准测试
go test -bench=. -benchmem -run=^$ -benchtime=1s ./pkg/optimizer/ > benchmark_output.txt

# 提取关键结果
grep "^Benchmark.*ns/op" benchmark_output.txt

# 手动更新 baseline.json 中的数值
```

## 性能对比

使用 `benchcmp` 工具对比基准测试结果：

```bash
# 安装 benchcmp
go install golang.org/x/perf/cmd/benchstat@latest

# 对比两次运行结果
benchstat old.txt new.txt
```

## 注意事项

1. **测试隔离**: 每个基准测试都会创建独立的测试数据，确保测试可重复
2. **数据清理**: 使用 `defer suite.Cleanup()` 确保测试后清理数据
3. **跳过标记**: 暂不支持的SQL特性已用 `b.Skip()` 标记
4. **内存报告**: 所有基准测试都使用 `b.ReportAllocs()` 报告内存分配
5. **计时重置**: 使用 `b.ResetTimer()` 排除setup时间

## 已知限制

以下场景暂时不支持，已跳过：
- FULL OUTER JOIN（需要UNION ALL支持）
- 3表及以上链式/星型JOIN（解析器限制）
- Bushy Tree JOIN（执行计划限制）
- 复杂ORDER BY + LIMIT（执行器限制）
- 原生子查询（使用JOIN模拟）

## 性能优化建议

基于当前基准测试结果，建议的优化方向：

1. **JOIN优化**: 实现真正的多表JOIN支持，避免笛卡尔积
2. **并行执行**: 提高并行扫描和并行JOIN的效率
3. **谓词下推**: 加强谓词下推优化，减少中间结果集
4. **列裁剪**: 优化列裁剪逻辑，减少内存分配
5. **批量操作**: 在数据加载和插入时使用批量操作

## 参考资料

- [Go testing/benchmark](https://golang.org/pkg/testing/#hdr-Benchmark)
- [pkg/optimizer 文档](../../README.md)
- [性能分析工具](https://github.com/golang/go/wiki/Performance)
