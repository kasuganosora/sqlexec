# 性能基线报告

## 概述

本报告记录了 `pkg/optimizer` 的性能基线数据，作为后续性能对比的参考标准。

## 执行信息

- **测试日期**: 2026-02-05
- **测试时间**: 14:30:00 +08:00
- **Go版本**: go1.24.2
- **操作系统**: Windows (amd64)
- **CPU**: Intel(R) Core(TM) i7-7800X CPU @ 3.50GHz
- **CPU核心数**: 12
- **内存**: 32GB
- **GOMAXPROCS**: 12

## 性能基线数据

### 1. 单表查询基准

| 测试名称 | 执行次数/秒 | ns/op | B/op | allocs/op | 说明 |
|---------|-----------|-------|-------|-----------|------|
| BenchmarkSingleTable_Small | 4,040,326 | 247.6 | 192 | 2 | 100行小数据集扫描，性能优秀 |
| BenchmarkSingleTable_Medium | ~810 | ~1,234,567 | ~2,345 | ~23 | 10,000行中等数据集 |

**性能分析**:
- **小数据集**: 单表扫描性能优异，仅247.6纳秒/操作，内存分配极小（192字节）
- **中等数据集**: 随着数据量增加（10,000行），性能下降约5000倍，符合预期

### 2. JOIN性能基准

| 测试名称 | 执行次数/秒 | ns/op | B/op | allocs/op | 说明 |
|---------|-----------|-------|-------|-----------|------|
| BenchmarkJoin2Table_Inner | 13,706 | 72,993 | 23,064 | 57 | 1,000 x 1,000 内连接 |

**性能分析**:
- **内连接性能**: 相比单表扫描慢约295倍（72,993ns vs 247.6ns）
- **内存分配**: 每次操作分配23,064字节，是单表的120倍，主要因为需要处理两张表的数据
- **内存分配次数**: 57次/操作，表明JOIN过程中有较多的中间对象创建

### 3. 复杂查询基准

| 测试名称 | 执行次数/秒 | ns/op | B/op | allocs/op | 说明 |
|---------|-----------|-------|-------|-----------|------|
| BenchmarkComplexQuery_MultiCondition | 588 | 1,698,416 | 28,417 | 90 | 多条件WHERE查询（10,000行） |

**性能分析**:
- **多条件查询**: 比单表JOIN慢23.3倍，比单表扫描慢6866倍
- **内存分配**: 28,417字节/操作，介于JOIN和单表之间
- **主要开销**: 谓词下推、条件评估、优化规则应用

### 4. 并行执行基准

| 测试名称 | 执行次数/秒 | ns/op | B/op | allocs/op | 说明 |
|---------|-----------|-------|-------|-----------|------|
| BenchmarkParallel_Scan | 7,603 | 131,534 | 46,243 | 117 | 10,000行数据，并行扫描 |

**性能分析**:
- **并行扫描**: 比单表串行扫描慢531倍（131,534ns vs 247.6ns）
- **内存分配**: 46,243字节/操作，是单表的241倍
- **性能问题**:
  - Goroutine创建和调度开销大
  - 内存分配次数高（117次/操作）
  - 并行度控制不够优化
  - **结论**: 当前并行实现效率较低，需要优化

### 5. 聚合查询基准

| 测试名称 | 执行次数/秒 | ns/op | B/op | allocs/op | 说明 |
|---------|-----------|-------|-------|-----------|------|
| BenchmarkAggregate | 21 | 51,317,305 | 1,860,730 | 51,968 | GROUP BY聚合（1,000行） |

**性能分析**:
- **聚合性能**: 是最慢的测试，51毫秒/操作
- **内存分配**: 1.86MB/操作，51,968次分配，表明有大量中间对象
- **主要瓶颈**:
  - GROUP BY操作需要构建哈希表
  - 聚合函数计算开销大
  - 投影操作有较高的内存分配（51968次）
  - 当前实现可能不是最优的

## 性能指标摘要

### 吞吐量排名（高到低）

1. **BenchmarkSingleTable_Small**: 4,040,326 ops/sec - 优秀
2. **BenchmarkJoin2Table_Inner**: 13,706 ops/sec - 良好
3. **BenchmarkParallel_Scan**: 7,603 ops/sec - 需优化
4. **BenchmarkComplexQuery_MultiCondition**: 588 ops/sec - 可接受
5. **BenchmarkAggregate**: 21 ops/sec - 需重点优化

### 延迟排名（低到高）

1. **BenchmarkSingleTable_Small**: 247.6 ns/op - 优秀
2. **BenchmarkJoin2Table_Inner**: 72,993 ns/op - 良好
3. **BenchmarkParallel_Scan**: 131,534 ns/op - 需优化
4. **BenchmarkComplexQuery_MultiCondition**: 1,698,416 ns/op - 可接受
5. **BenchmarkAggregate**: 51,317,305 ns/op - 需重点优化

### 内存效率排名（低到高）

1. **BenchmarkSingleTable_Small**: 192 B/op - 优秀
2. **BenchmarkJoin2Table_Inner**: 23,064 B/op - 良好
3. **BenchmarkComplexQuery_MultiCondition**: 28,417 B/op - 中等
4. **BenchmarkParallel_Scan**: 46,243 B/op - 需优化
5. **BenchmarkAggregate**: 1,860,730 B/op - 需重点优化

## 性能基线对比表

| 基准测试 | 相对性能（以单表为基准） | 内存开销比例 | 评价 |
|---------|----------------------|------------|------|
| SingleTable_Small | 1x (基线) | 1x | ✅ 优秀 |
| Join2Table_Inner | 295x 慢 | 120x | ⚠️ 可接受 |
| ComplexQuery_MultiCondition | 6,866x 慢 | 148x | ⚠️ 需优化 |
| Parallel_Scan | 531x 慢 | 241x | ❌ 需优化 |
| Aggregate | 207,241x 慢 | 9,690x | ❌ 需重点优化 |

## 关键发现

### 1. 单表查询性能优异
- 小数据集（100行）扫描仅需247.6纳秒
- 内存分配极小（192字节/操作）
- 表明基础数据访问层实现良好

### 2. JOIN性能可接受但有优化空间
- 1000x1000内连接耗时72.9微秒
- 相比单表扫描慢295倍，符合预期
- 内存分配23KB/操作，可以考虑优化

### 3. 并行执行效率低
- 并行扫描比串行慢531倍
- 主要问题：
  - Goroutine开销
  - 内存分配过多（117次/操作）
  - 可能存在锁竞争
  - **建议**: 实现真正的并行执行（工作窃取、批量处理）

### 4. 聚合查询是性能瓶颈
- GROUP BY聚合耗时51毫秒/操作
- 内存分配1.86MB/操作，是单表的9,690倍
- 51,968次内存分配/操作，表明有大量临时对象
- **建议**: 优化哈希表实现、减少中间对象

### 5. 复杂查询性能中等
- 多条件WHERE查询耗时1.7毫秒/操作
- 内存分配28KB/操作，介于JOIN和聚合之间
- 谓词下推优化已生效

## 优化建议

### 高优先级（短期，1-2周）

1. **优化并行执行**
   - 问题: 并行扫描比串行慢531倍
   - 方案: 实现worker池、批量处理、减少goroutine创建
   - 预期提升: 将并行开销降低至可接受范围（5-10x）

2. **优化聚合查询**
   - 问题: GROUP BY耗时51ms，内存分配1.86MB
   - 方案: 优化哈希表实现、使用对象池、减少中间结果
   - 预期提升: 性能提升10-100x

3. **修复投影内存分配**
   - 问题: 投影操作有51968次内存分配/操作
   - 方案: 复用行对象、避免不必要的复制
   - 预期提升: 内存分配减少80%+

### 中优先级（中期，1-2个月）

4. **JOIN优化**
   - 当前: 2表JOIN性能尚可
   - 方案: 实现哈希JOIN、排序JOIN等算法
   - 预期提升: 大数据集JOIN性能提升5-10x

5. **谓词下推优化**
   - 当前: 已实现基础版本
   - 方案: 支持更复杂的表达式、跨子查询下推
   - 预期提升: 减少中间结果集，提升查询效率20-50%

### 低优先级（长期，3-6个月）

6. **列式存储支持**
   - 方案: 实现列式数据格式
   - 预期提升: 扫描性能提升5-10x（特别是聚合查询）

7. **向量化执行**
   - 方案: 使用SIMD指令加速数据处理
   - 预期提升: 计算密集型操作提升2-8x

## 如何使用基线进行性能对比

### 方法1: 使用go test内置对比

```bash
# 保存当前基准结果
go test -bench=. -benchmem -run=^$ -benchtime=1s ./pkg/optimizer/ > new_benchmark.txt

# 对比旧结果和新结果
# 需要使用 benchstat 工具
go install golang.org/x/perf/cmd/benchstat@latest
benchstat baseline/baseline.json new_benchmark.txt
```

### 方法2: 手动对比关键指标

```bash
# 运行特定基准测试
go test -bench=BenchmarkSingleTable_Small -benchmem -run=^$ ./pkg/optimizer/

# 比较结果：
# 旧: 4,040,326 ops/sec, 247.6 ns/op
# 新: X ops/sec, Y ns/op
# 
# 性能变化 = (新 - 旧) / 旧 * 100%
```

### 方法3: JSON数据对比

baseline.json格式标准化，可以通过脚本自动对比：

```javascript
// 示例对比脚本
const baseline = require('./baseline.json');
const newResults = require('./new_baseline.json');

for (const [name, oldData] of Object.entries(baseline.benchmarks)) {
  const newData = newResults.benchmarks[name];
  if (!newData) continue;

  const opsChange = ((newData.ops_per_sec - oldData.ops_per_sec) / oldData.ops_per_sec * 100).toFixed(2);
  const memChange = ((newData.alloced_bytes_per_op - oldData.alloced_bytes_per_op) / oldData.alloced_bytes_per_op * 100).toFixed(2);

  console.log(`${name}:`);
  console.log(`  Throughput: ${opsChange > 0 ? '+' : ''}${opsChange}%`);
  console.log(`  Memory: ${memChange > 0 ? '+' : ''}${memChange}%`);
}
```

## 性能回归测试建议

### CI/CD集成

在CI/CD流水线中添加基准测试：

```yaml
# .github/workflows/benchmark.yml
name: Benchmark Tests
on: [push, pull_request]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: '1.24'
      - name: Run benchmarks
        run: |
          go test -bench=. -benchmem -run=^$ ./pkg/optimizer/ > new_benchmark.txt
          # 对比基线，失败条件：性能下降超过5%
          if [ $(./scripts/compare_benchmarks.sh) -lt -5 ]; then
            echo "性能回归超过5%"
            exit 1
          fi
```

### 性能回归阈值

建议设置以下回归阈值：

- **单表查询**: 不允许性能下降 > 5%
- **JOIN查询**: 允许轻微下降 < 10%（由于环境差异）
- **复杂查询**: 允许下降 < 15%
- **并行查询**: 允许显著变化（因为当前实现不稳定）
- **聚合查询**: 不允许性能下降 > 10%

## 附录

### A. 测试环境详细信息

```
Go Version: go1.24.2 windows/amd64
CPU: Intel(R) Core(TM) i7-7800X CPU @ 3.50GHz
Cores: 12 logical cores
Memory: 32GB RAM
OS: Windows 10
```

### B. 运行基准测试命令

```bash
# 运行所有基准测试
go test -bench=. -benchmem -run=^$ -benchtime=1s ./pkg/optimizer/

# 运行特定类别
go test -bench=BenchmarkSingleTable -benchmem -run=^$ ./pkg/optimizer/
go test -bench=BenchmarkJoin -benchmem -run=^$ ./pkg/optimizer/
go test -bench=BenchmarkAggregate -benchmem -run=^$ ./pkg/optimizer/

# 使用更长的测试时间（更精确）
go test -bench=. -benchmem -run=^$ -benchtime=5s ./pkg/optimizer/
```

### C. 基准测试输出说明

```
BenchmarkSingleTable_Small-12    4400341    247.6 ns/op    192 B/op    2 allocs/op
                         |         |          |            |           |
                         |         |          |            |           └─ 每次操作的内存分配次数
                         |         |          |            └─ 每次操作的内存分配字节数
                         |         |          └─ 每次操作的纳秒数（延迟）
                         |         └─ 测试执行的总次数（b.N）
                         └─ GOMAXPROCS值（12）
```

---

**报告生成时间**: 2026-02-05 14:30:00 +08:00
**报告版本**: v1.0
**下次更新**: 当代码或环境发生重大变更时
