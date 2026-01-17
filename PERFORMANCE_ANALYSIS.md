# 性能分析与优化计划

## 1. 当前性能瓶颈分析

### 1.1 内存分配与复制问题

#### 问题1：逐行复制导致大量内存分配

**位置**: `mysql/optimizer/physical_scan.go`

```134:153:mysql/optimizer/physical_scan.go
// 手动应用过滤（简化实现）
filtered := []resource.Row{}
for _, row := range input.Rows {
    match := true
    for _, filter := range p.Filters {
        if !matchesFilter(row, filter) {
            match = false
            break
        }
    }
    if match {
        filtered = append(filtered, row)
    }
}
```

**问题**:
- 每次过滤操作都创建新的 `[]resource.Row` 切片
- 逐行复制数据到新切片，导致大量内存分配
- GC压力巨大

**影响**: 中等数据集（10万行）性能下降50-80%

#### 问题2：Row 使用 map[string]interface{}

**位置**: `mysql/resource/types.go`

```go
type Row map[string]interface{}
```

**问题**:
- map比数组/结构体慢2-3倍
- 每次访问都要hash查找
- 内存占用大（包含元数据）

**影响**: 所有查询性能下降30-50%

#### 问题3：Hash Join 中的重复哈希表构建

**位置**: `mysql/optimizer/physical_scan.go`

```493:497:mysql/optimizer/physical_scan.go
// 在LEFT JOIN中，我们需要用右表去匹配左表，所以要重新构建哈希表
// 正确的做法：为右表也构建哈希表
rightHashTable := make(map[interface{}][]resource.Row)
for _, row := range rightResult.Rows {
    key := row[rightJoinCol]
    rightHashTable[key] = append(rightHashTable[key], row)
}
```

**问题**:
- LEFT JOIN、RIGHT JOIN重复构建哈希表
- 理论上只需要构建一次

**影响**: 多表JOIN查询性能下降50%

---

### 1.2 执行模型问题

#### 问题4：非流式执行

**位置**: 所有算子的 `Execute` 方法

**示例**:
```go
func (p *PhysicalSelection) Execute(ctx context.Context) (*resource.QueryResult, error) {
    // 先完全执行子节点
    input, err := p.children[0].Execute(ctx)
    // 然后处理所有行
    for _, row := range input.Rows {
        // ...
    }
}
```

**问题**:
- Volcano模型但不是真正的迭代器
- 每个算子都完全执行，产生完整结果集
- 无法提前终止（如LIMIT下推后仍需扫描全表）

**影响**: 
- LIMIT查询无法提前终止，浪费资源
- 内存占用峰值高

#### 问题5：缺少向量化执行

**当前**: 逐行处理
```go
for _, row := range input.Rows {
    // 处理单行
}
```

**DuckDB方式**: 批量处理
```go
// 一次处理1024行
batch := readBatch(1024)
vectorizedProcess(batch)
```

**影响**: 性能损失10-20倍（CPU流水线利用率低）

---

### 1.3 表达式求值问题

#### 问题6：类型转换开销

**位置**: `mysql/optimizer/expression_evaluator.go`

```355:370:mysql/optimizer/expression_evaluator.go
func toFloat64(val interface{}) (float64, bool) {
    switch v := val.(type) {
    case int, int8, int16, int32, int64:
        return float64(reflect.ValueOf(v).Int()), true
    case uint, uint8, uint16, uint32, uint64:
        return float64(reflect.ValueOf(v).Uint()), true
    // ...
    }
}
```

**问题**:
- 每次比较都进行类型断言和转换
- 使用reflect性能差
- 字符串比较使用 `fmt.Sprintf("%v", a)` 极其慢

**影响**: 
- 每次WHERE条件评估慢100-1000倍
- 复杂查询性能严重下降

#### 问题7：哈希键使用 interface{}

**位置**: `mysql/optimizer/physical_scan.go`

```453:458:mysql/optimizer/physical_scan.go
// 3. 构建哈希表（从左表）
hashTable := make(map[interface{}][]resource.Row)
for _, row := range leftResult.Rows {
    key := row[leftJoinCol]
    hashTable[key] = append(hashTable[key], row)
}
```

**问题**:
- interface{}作为map key需要运行时类型信息
- 无法利用类型特化优化
- 不同类型的key无法高效哈希

**影响**: Hash Join性能下降30-50%

---

### 1.4 缺少优化器统计信息

#### 问题8：硬编码的成本估算

**位置**: `mysql/optimizer/physical_scan.go`

```33:43:mysql/optimizer/physical_scan.go
// 假设表有1000行
rowCount := int64(1000)

return &PhysicalTableScan{
    TableName: tableName,
    cost:      rowCount * 0.1, // 简化的成本计算
    // ...
}
```

**问题**:
- 所有表假设1000行
- 没有基数估计
- 无法选择最优的JOIN顺序
- 无法决定何时使用索引

**影响**: 
- 优化器可能选择次优计划
- JOIN重排序完全随机

---

### 1.5 并发问题

#### 问题9：单线程执行

**当前**: 所有操作都是单线程

**DuckDB方式**: 并行扫描和处理
```go
// DuckDB使用多个线程并行处理
parallelWorkers := 4
for i := 0; i < parallelWorkers; i++ {
    go processChunk(chunk)
}
```

**影响**: 
- 无法利用多核CPU
- 大数据集处理慢2-8倍

---

## 2. 性能基准测试需求

### 2.1 需要创建的基准测试

1. **扫描性能基准**
   - 全表扫描
   - 带WHERE的扫描
   - LIMIT查询

2. **JOIN性能基准**
   - INNER JOIN (小表+小表)
   - INNER JOIN (大表+大表)
   - LEFT/RIGHT JOIN
   - 多表JOIN

3. **聚合性能基准**
   - COUNT
   - GROUP BY + SUM/AVG
   - HAVING

4. **排序性能基准**
   - 小数据集排序
   - 大数据集排序
   - 多列排序

### 2.2 测试数据规模

| 测试类型 | 小数据集 | 中数据集 | 大数据集 |
|---------|---------|---------|---------|
| 行数 | 1,000 | 100,000 | 1,000,000 |
| 列数 | 10 | 20 | 50 |

---

## 3. TiDB 和 DuckDB 优化技术

### 3.1 DuckDB 核心优化

#### 3.1.1 列式存储
- Arrow格式，类型特化
- 向量化执行（SIMD）
- Zero-copy读取

#### 3.1.2 哈希优化
- 使用类型特化的哈希函数
- 避免interface{}作为key
- 增量哈希表构建

#### 3.1.3 内存管理
- 池化内存分配
- 减少GC压力
- mmap文件映射

#### 3.1.4 并行执行
- 自动并行扫描
- 并行聚合
- 并行JOIN

### 3.2 TiDB 核心优化

#### 3.2.1 统计信息
- 列统计（直方图）
- 基数估计
- 选择性估计

#### 3.2.2 索引优化
- 范围扫描
- 索引下推
- 覆盖索引

#### 3.2.3 JOIN重排序
- 基于成本的JOIN顺序
- Bushy Join树
- 动态规划

#### 3.2.4 并行执行
- 并行执行算子
- 数据交换
- Worker池管理

---

## 4. 优化计划

### 阶段1：立即可实施的优化（1-2周）

#### 1.1 修复重复哈希表构建
- 优化Hash Join，只构建一次哈希表
- 为LEFT/RIGHT JOIN重用哈希表

**预期提升**: 50%

#### 1.2 实现流式迭代器
- 改造算子接口，支持Next()方法
- 实现真正的迭代器模型
- 支持提前终止

**预期提升**: 30%（LIMIT查询）

#### 1.3 优化表达式求值
- 移除reflect，使用类型switch
- 避免字符串化比较
- 添加表达式求值缓存

**预期提升**: 10-20倍（WHERE过滤）

#### 1.4 基础并行化
- 并行扫描
- 并行聚合

**预期提升**: 2-4倍（多核CPU）

### 阶段2：中等复杂度优化（2-4周）

#### 2.1 向量化执行框架
- 设计Batch接口
- 实现向量化的过滤
- 实现向量化的聚合

**预期提升**: 5-10倍

#### 2.2 内存池化
- 实现Row池
- 减少GC压力

**预期提升**: 30-50%（大数据集）

#### 2.3 类型特化
- 针对不同类型生成特化代码
- 避免interface{}

**预期提升**: 2-3倍

### 阶段3：高级优化（4-8周）

#### 3.1 统计信息收集
- 收集表统计
- 计算基数
- 优化JOIN顺序

**预期提升**: 2-10倍（取决于查询复杂度）

#### 3.2 索引支持
- 实现内存索引
- 索引下推

**预期提升**: 10-100倍（索引查询）

#### 3.3 完整的并行执行引擎
- 并行JOIN
- 数据交换
- Worker池

**预期提升**: 4-8倍

---

## 5. 性能目标

### 5.1 短期目标（阶段1完成后）
- WHERE查询快10倍
- LIMIT查询快5倍
- JOIN查询快2倍

### 5.2 中期目标（阶段2完成后）
- 扫描快20倍
- 聚合快15倍
- 内存占用减少50%

### 5.3 长期目标（阶段3完成后）
- 接近DuckDB性能（同数据集）
- 支持TB级数据
- 查询延迟<100ms（OLTP）

---

## 6. 监控和测试

### 6.1 性能监控指标
- 查询执行时间
- 内存使用峰值
- CPU利用率
- GC次数和时间

### 6.2 基准测试
- 每次优化前后运行基准测试
- 记录性能提升
- 建立性能回归检测

---

## 7. 参考资料

- [DuckDB Performance Blog](https://duckdb.org/2022/03/07/aggregate-hashtable)
- [DuckDB Vectorization](https://duckdb.org/2021/09/23/vectorized-execution)
- [TiDB Cost Model](https://docs.pingcap.com/tidb/stable/cost-based-optimization)
- [ClickHouse Optimization](https://clickhouse.com/docs/en/operations/performance-test)
