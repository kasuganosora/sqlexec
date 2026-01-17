# 阶段 6：性能优化与监控 - 实施计划

## 📊 执行概览

**阶段目标**: 优化系统性能并增加监控能力

**参考技术**:
- **TiDB**: Cascades优化器、成本模型、统计信息、算子下推
- **DuckDB**: 向量化执行、列式存储、并行处理、SIMD优化

**预期时间**: 8-12周

---

## 🔍 阶段1：立即可实施的优化（第1-2周）

### 1.1 修复Hash Join重复构建哈希表

**问题**: LEFT JOIN、RIGHT JOIN重复构建哈希表，浪费50%资源

**实现方案**:
```go
// 为LEFT/RIGHT JOIN预构建右表哈希表
func (p *PhysicalHashJoin) Execute(ctx context.Context) (*resource.QueryResult, error) {
    // 1. 执行左右表
    leftResult, _ := p.children[0].Execute(ctx)
    rightResult, _ := p.children[1].Execute(ctx)
    
    // 2. 根据JOIN类型决定哈希表构建策略
    switch p.JoinType {
    case JoinTypeInner, JoinTypeLeft:
        // 为右表构建哈希表（探测端）
        rightHashTable := buildHashTable(rightResult, rightJoinCol)
        // 用左表探测
    case JoinTypeRight:
        // 为左表构建哈希表
        leftHashTable := buildHashTable(leftResult, leftJoinCol)
        // 用右表探测
    }
}
```

**文件**: `mysql/optimizer/physical_scan.go`

**预期提升**: 50% (JOIN查询)

---

### 1.2 实现流式迭代器接口

**问题**: 当前不是真正的迭代器，无法提前终止

**实现方案**:
```go
// 新增迭代器接口
type RowIterator interface {
    Next() (resource.Row, bool, error)
    Close() error
}

// 改造算子
type PhysicalSelection struct {
    child RowIterator
    filters []*parser.Expression
}

func (p *PhysicalSelection) Next() (resource.Row, bool, error) {
    for {
        row, hasNext, err := p.child.Next()
        if !hasNext || err != nil {
            return nil, false, err
        }
        
        if p.matchesFilters(row) {
            return row, true, nil
        }
        // 不匹配则继续，直到找到匹配或耗尽
    }
}
```

**优势**:
- LIMIT可以提前终止
- 减少内存占用
- 支持管道执行

**文件**:
- `mysql/optimizer/iterator.go` (新建)
- `mysql/optimizer/physical_scan.go` (修改)

**预期提升**: 30% (LIMIT查询)

---

### 1.3 优化表达式求值

**问题1**: 使用reflect性能差
**问题2**: 字符串化比较极其慢

**实现方案**:
```go
// 1. 移除reflect，使用类型switch
func toFloat64Fast(val interface{}) (float64, bool) {
    switch v := val.(type) {
    case int:
        return float64(v), true
    case int64:
        return float64(v), true
    case float64:
        return v, true
    // ... 其他类型
    default:
        return 0, false
    }
}

// 2. 添加表达式编译缓存
type CompiledExpr struct {
    evaluator func(resource.Row) (interface{}, error)
}

// 3. 预编译表达式
func (e *ExpressionEvaluator) Compile(expr *parser.Expression) *CompiledExpr {
    // 根据表达式类型生成特化的求值函数
    if expr.Type == parser.ExprTypeColumn {
        return &CompiledExpr{
            evaluator: func(row resource.Row) (interface{}, error) {
                return row[expr.Column], nil
            },
        }
    }
    // ... 其他类型
}
```

**文件**: `mysql/optimizer/expression_evaluator.go`

**预期提升**: 10-20倍 (WHERE条件评估)

---

### 1.4 基础并行扫描

**问题**: 单线程执行，无法利用多核CPU

**实现方案**:
```go
// 并行扫描器
type ParallelTableScan struct {
    tableName string
    workers   int
    chunkSize int
    dataSource resource.DataSource
}

func (p *ParallelTableScan) Execute(ctx context.Context) (*resource.QueryResult, error) {
    // 1. 获取总行数
    totalRows, _ := p.dataSource.RowCount(ctx, p.tableName)
    
    // 2. 分块
    chunks := calculateChunks(totalRows, p.workers, p.chunkSize)
    
    // 3. 并行扫描
    resultChan := make(chan []resource.Row, len(chunks))
    errChan := make(chan error, len(chunks))
    
    for i, chunk := range chunks {
        go func(idx int, c Chunk) {
            rows, err := p.scanChunk(ctx, c)
            if err != nil {
                errChan <- err
                return
            }
            resultChan <- rows
        }(i, chunk)
    }
    
    // 4. 收集结果
    allRows := []resource.Row{}
    for i := 0; i < len(chunks); i++ {
        select {
        case rows := <-resultChan:
            allRows = append(allRows, rows...)
        case err := <-errChan:
            return nil, err
        }
    }
    
    return &resource.QueryResult{Rows: allRows}, nil
}
```

**文件**: `mysql/optimizer/parallel_scan.go` (新建)

**预期提升**: 2-4倍 (多核CPU)

---

## 🚀 阶段2：中等复杂度优化（第3-6周）

### 2.1 向量化执行框架

**设计**:
```go
// Batch 接口
type Batch struct {
    columns map[string]*Vector
    rows    int
}

type Vector interface {
    Get(i int) interface{}
    Set(i int, val interface{})
    Len() int
    Type() reflect.Type
}

// 类型特化的向量
type Int64Vector struct {
    data []int64
}

type Float64Vector struct {
    data []float64
}

type StringVector struct {
    data []string
}

// 向量化过滤算子
type VectorizedFilter struct {
    child RowIterator
    filter func(*Batch) []bool
}

func (vf *VectorizedFilter) Execute(ctx context.Context) (*resource.QueryResult, error) {
    for {
        batch, hasNext, err := vf.child.NextBatch()
        if !hasNext || err != nil {
            break
        }
        
        // 向量化应用过滤器
        keepMask := vf.filter(batch)
        output := batch.Filter(keepMask)
        
        // 输出过滤后的batch
        yield(output)
    }
}
```

**优势**:
- SIMD指令加速
- 减少函数调用开销
- CPU流水线利用率高

**文件**:
- `mysql/optimizer/vector/batch.go` (新建)
- `mysql/optimizer/vector/vector.go` (新建)
- `mysql/optimizer/vector/vector_filter.go` (新建)

**预期提升**: 5-10倍

---

### 2.2 内存池化

**实现方案**:
```go
// Row池
type RowPool struct {
    pool sync.Pool
}

func NewRowPool() *RowPool {
    return &RowPool{
        pool: sync.Pool{
            New: func() interface{} {
                return make(resource.Row, 10)
            },
        },
    }
}

func (rp *RowPool) Get() resource.Row {
    return rp.pool.Get().(resource.Row)
}

func (rp *RowPool) Put(row resource.Row) {
    // 清空行
    for k := range row {
        delete(row, k)
    }
    rp.pool.Put(row)
}

// Batch池
type BatchPool struct {
    pool sync.Pool
}

func (bp *BatchPool) Get(columns int) *Batch {
    batch := bp.pool.Get().(*Batch)
    if batch == nil {
        batch = &Batch{
            columns: make(map[string]*Vector),
            rows:    0,
        }
    }
    return batch
}

func (bp *BatchPool) Put(batch *Batch) {
    bp.pool.Put(batch)
}
```

**文件**: `mysql/optimizer/pool.go` (新建)

**预期提升**: 30-50% (减少GC)

---

### 2.3 类型特化

**实现方案**:
```go
// 为不同类型生成特化代码
type TypeSpecializedFilter struct {
    columnType reflect.Type
    value      interface{}
    operator   string
}

func (tsf *TypeSpecializedFilter) Evaluate(row resource.Row) bool {
    val := row[tsf.columnName]
    
    // 类型特化比较
    switch tsf.columnType {
    case reflect.TypeOf(int64(0)):
        valInt := val.(int64)
        valueInt := tsf.value.(int64)
        return tsf.compareInt64(valInt, valueInt)
    case reflect.TypeOf(float64(0)):
        valFloat := val.(float64)
        valueFloat := tsf.value.(float64)
        return tsf.compareFloat64(valFloat, valueFloat)
    case reflect.TypeOf(""):
        valStr := val.(string)
        valueStr := tsf.value.(string)
        return tsf.compareString(valStr, valueStr)
    }
    return false
}
```

**文件**: `mysql/optimizer/type_specialized.go` (新建)

**预期提升**: 2-3倍

---

## 🎯 阶段3：高级优化（第7-12周）

### 3.1 统计信息收集器

**实现方案**:
```go
// 表统计信息
type TableStatistics struct {
    RowCount      int64
    ColumnStats   map[string]*ColumnStatistics
    LastUpdated   time.Time
}

type ColumnStatistics struct {
    Name         string
    Type         string
    Distinct     int64
    NullCount    int64
    Min          interface{}
    Max          interface{}
    Histogram    *Histogram
}

// 直方图
type Histogram struct {
    Buckets []HistogramBucket
}

type HistogramBucket struct {
    LowerBound float64
    UpperBound float64
    Count      int64
}

// 统计信息收集器
type StatisticsCollector struct {
    stats map[string]*TableStatistics
}

func (sc *StatisticsCollector) CollectStatistics(ctx context.Context, tableName string) error {
    // 1. 扫描全表
    // 2. 计算基数、空值、最小/最大值
    // 3. 构建直方图
    // 4. 保存统计信息
}
```

**文件**:
- `mysql/optimizer/statistics/collector.go` (新建)
- `mysql/optimizer/statistics/histogram.go` (新建)

**优势**:
- 准确的成本估算
- 智能JOIN顺序选择
- 索引选择优化

**预期提升**: 2-10倍 (复杂查询)

---

### 3.2 改进的成本模型

**实现方案**:
```go
// 改进的成本模型
type ImprovedCostModel struct {
    statistics *StatisticsCollector
}

func (icm *ImprovedCostModel) EstimateScanCost(table string, filters []*parser.Expression) float64 {
    stats := icm.statistics.GetTableStats(table)
    
    // 1. 估算过滤后行数
    selectivity := icm.estimateSelectivity(filters, stats)
    outputRows := float64(stats.RowCount) * selectivity
    
    // 2. 扫描成本 = IO成本 + CPU成本
    ioCost := float64(stats.RowCount) * ioCostPerRow
    cpuCost := outputRows * cpuCostPerRow
    
    return ioCost + cpuCost
}

func (icm *ImprovedCostModel) EstimateJoinCost(left, right string, joinType JoinType, conditions []*parser.Expression) float64 {
    leftStats := icm.statistics.GetTableStats(left)
    rightStats := icm.statistics.GetTableStats(right)
    
    // Hash Join成本
    buildCost := float64(leftStats.RowCount) * hashBuildCostPerRow
    probeCost := float64(rightStats.RowCount) * hashProbeCostPerRow
    
    // 估算输出行数
    outputRows := icm.estimateJoinOutput(leftStats, rightStats, conditions)
    outputCost := outputRows * outputCostPerRow
    
    return buildCost + probeCost + outputCost
}
```

**文件**: `mysql/optimizer/cost_model_improved.go` (新建)

**预期提升**: 2-5倍 (优化计划选择)

---

### 3.3 JOIN重排序优化器

**实现方案**:
```go
// 动态规划JOIN顺序
type JoinReorderOptimizer struct {
    costModel   *ImprovedCostModel
    tables      []string
    joinGraph   *JoinGraph
}

func (jro *JoinReorderOptimizer) FindBestJoinOrder() (JoinPlan, float64) {
    // 使用动态规划
    // DP[S] = min_{k in S} (DP[S-{k}] + cost(join(k, S-{k})))
    
    memo := make(map[string]JoinPlan)
    return jro.dp(jro.tables, memo)
}

func (jro *JoinReorderOptimizer) dp(tables []string, memo map[string]JoinPlan) (JoinPlan, float64) {
    key := strings.Join(sorted(tables), ",")
    if plan, exists := memo[key]; exists {
        return plan, plan.Cost
    }
    
    if len(tables) == 1 {
        return JoinPlan{Tables: tables, Cost: jro.costModel.EstimateScanCost(tables[0], nil)}, 0
    }
    
    bestPlan := JoinPlan{Cost: math.Inf(1)}
    
    // 尝试每个表作为第一个表
    for _, first := range tables {
        remaining := remove(tables, first)
        subPlan, subCost := jro.dp(remaining, memo)
        
        // 将first表join到subPlan
        joinCost := jro.costModel.EstimateJoinCost(first, subPlan.LastTable, JoinTypeInner, nil)
        totalCost := subCost + joinCost
        
        if totalCost < bestPlan.Cost {
            bestPlan = JoinPlan{
                Tables:    append([]string{first}, subPlan.Tables...),
                LastTable: first,
                Cost:      totalCost,
            }
        }
    }
    
    memo[key] = bestPlan
    return bestPlan, bestPlan.Cost
}
```

**文件**: `mysql/optimizer/join_reorder_dp.go` (新建)

**预期提升**: 2-10倍 (多表JOIN)

---

### 3.4 内存索引支持

**实现方案**:
```go
// B-Tree索引
type BTreeIndex struct {
    root    *BTreeNode
    column  string
    compare func(a, b interface{}) int
}

type BTreeNode struct {
    isLeaf   bool
    keys     []interface{}
    children []*BTreeNode
    values   []int64 // 行ID
}

// 索引扫描
type IndexScan struct {
    index     *BTreeIndex
    lower     interface{}
    upper     interface{}
    includeLower bool
    includeUpper bool
}

func (is *IndexScan) Execute(ctx context.Context) (*resource.QueryResult, error) {
    // 使用索引范围扫描，避免全表扫描
    // 返回匹配的行
}

// 索引管理器
type IndexManager struct {
    indexes map[string]map[string]*BTreeIndex // table -> column -> index
}

func (im *IndexManager) BuildIndex(tableName, column string, rows []resource.Row) error {
    // 构建B-Tree索引
    index := &BTreeIndex{column: column}
    for i, row := range rows {
        index.Insert(row[column], int64(i))
    }
    
    if im.indexes[tableName] == nil {
        im.indexes[tableName] = make(map[string]*BTreeIndex)
    }
    im.indexes[tableName][column] = index
}
```

**文件**:
- `mysql/optimizer/index/btree.go` (新建)
- `mysql/optimizer/index/manager.go` (新建)
- `mysql/optimizer/index/scan.go` (新建)

**预期提升**: 10-100倍 (索引查询)

---

### 3.5 完整的并行执行引擎

**实现方案**:
```go
// Worker池
type WorkerPool struct {
    workers   []*Worker
    taskQueue chan Task
    wg        sync.WaitGroup
}

type Worker struct {
    id   int
    pool *WorkerPool
}

func (w *Worker) Run() {
    for task := range w.pool.taskQueue {
        task.Execute()
        w.pool.wg.Done()
    }
}

// 并行执行算子
type ParallelHashJoin struct {
    left   PhysicalPlan
    right  PhysicalPlan
    workers int
}

func (phj *ParallelHashJoin) Execute(ctx context.Context) (*resource.QueryResult, error) {
    // 1. 并行扫描左表，构建分布式哈希表
    hashPartitions := make([]map[interface{}][]resource.Row, phj.workers)
    
    // 并行分区
    for _, leftRow := range phj.left.Execute(ctx).Rows {
        partitionIdx := hash(leftRow[key]) % phj.workers
        hashPartitions[partitionIdx][key] = append(hashPartitions[partitionIdx][key], leftRow)
    }
    
    // 2. 并行探测
    resultChan := make(chan resource.Row)
    for i := 0; i < phj.workers; i++ {
        go func(idx int) {
            for _, rightRow := range phj.right.Execute(ctx).Rows {
                key := rightRow[key]
                if leftRows, exists := hashPartitions[idx][key]; exists {
                    for _, leftRow := range leftRows {
                        resultChan <- merge(leftRow, rightRow)
                    }
                }
            }
        }(i)
    }
    
    // 3. 收集结果
    results := []resource.Row{}
    for row := range resultChan {
        results = append(results, row)
    }
    
    return &resource.QueryResult{Rows: results}, nil
}
```

**文件**: `mysql/optimizer/parallel/engine.go` (新建)

**预期提升**: 4-8倍 (大查询)

---

## 📊 监控和慢查询分析（第12周）

### 4.1 性能监控器

**实现方案**:
```go
// 性能监控器
type PerformanceMonitor struct {
    queryHistory map[string]*QueryMetrics
    mu          sync.RWMutex
}

type QueryMetrics struct {
    QueryID      string
    SQL          string
    StartTime    time.Time
    EndTime      time.Time
    Duration     time.Duration
    Plan         string
    MemoryUsed   int64
    CPUUsed      float64
    RowsAffected int64
}

// 查询追踪器
type QueryTracer struct {
    monitor *PerformanceMonitor
}

func (qt *QueryTracer) TraceQuery(ctx context.Context, query string, plan PhysicalPlan, executeFunc func() (*resource.QueryResult, error)) (*resource.QueryResult, error) {
    metrics := &QueryMetrics{
        QueryID:   generateQueryID(),
        SQL:       query,
        Plan:      ExplainPlan(plan),
        StartTime: time.Now(),
    }
    
    // 开始监控
    startMem := getCurrentMemoryUsage()
    startCPU := getCurrentCPUUsage()
    
    // 执行查询
    result, err := executeFunc()
    
    // 结束监控
    metrics.EndTime = time.Now()
    metrics.Duration = metrics.EndTime.Sub(metrics.StartTime)
    metrics.MemoryUsed = getCurrentMemoryUsage() - startMem
    metrics.CPUUsed = getCurrentCPUUsage() - startCPU
    metrics.RowsAffected = result.Total
    
    // 保存指标
    qt.monitor.SaveMetrics(metrics)
    
    return result, err
}
```

**文件**:
- `mysql/monitor/monitor.go` (新建)
- `mysql/monitor/metrics.go` (新建)
- `mysql/monitor/tracer.go` (新建)

---

### 4.2 慢查询分析器

**实现方案**:
```go
// 慢查询分析器
type SlowQueryAnalyzer struct {
    monitor *PerformanceMonitor
    threshold time.Duration
}

func (sqa *SlowQueryAnalyzer) AnalyzeSlowQueries() []*SlowQueryReport {
    reports := []*SlowQueryReport{}
    
    for _, metrics := range sqa.monitor.GetAllMetrics() {
        if metrics.Duration > sqa.threshold {
            report := &SlowQueryReport{
                QueryID:      metrics.QueryID,
                SQL:          metrics.SQL,
                Duration:     metrics.Duration,
                Plan:         metrics.Plan,
                MemoryUsed:   metrics.MemoryUsed,
                CPUUsed:      metrics.CPUUsed,
                Suggestions:  sqa.generateSuggestions(metrics),
            }
            reports = append(reports, report)
        }
    }
    
    return reports
}

func (sqa *SlowQueryAnalyzer) generateSuggestions(metrics *QueryMetrics) []string {
    suggestions := []string{}
    
    // 分析执行计划
    if strings.Contains(metrics.Plan, "TableScan") && !strings.Contains(metrics.Plan, "IndexScan") {
        suggestions = append(suggestions, "考虑添加索引以避免全表扫描")
    }
    
    if strings.Contains(metrics.Plan, "HashJoin") && metrics.RowsAffected > 10000 {
        suggestions = append(suggestions, "JOIN返回大量行，考虑添加WHERE条件过滤")
    }
    
    if metrics.MemoryUsed > 100*1024*1024 { // >100MB
        suggestions = append(suggestions, "内存使用较高，考虑使用LIMIT或分页")
    }
    
    return suggestions
}
```

**文件**:
- `mysql/monitor/slow_query_analyzer.go` (新建)

---

## 📈 性能基准测试

### 基准测试套件

```go
// 基准测试
func BenchmarkTableScan_1K(b *testing.B) {
    benchmarkScan(b, 1000)
}

func BenchmarkTableScan_100K(b *testing.B) {
    benchmarkScan(b, 100000)
}

func BenchmarkTableScan_1M(b *testing.B) {
    benchmarkScan(b, 1000000)
}

func BenchmarkJoin_Inner_SmallSmall(b *testing.B) {
    benchmarkJoin(b, 1000, 1000, JoinTypeInner)
}

func BenchmarkJoin_Inner_LargeLarge(b *testing.B) {
    benchmarkJoin(b, 100000, 100000, JoinTypeInner)
}

func BenchmarkAggregate_Count(b *testing.B) {
    benchmarkAggregate(b, 100000, "COUNT", "*")
}

func BenchmarkAggregate_GroupBy(b *testing.B) {
    benchmarkAggregate(b, 100000, "GROUP BY", "category")
}
```

**文件**: `mysql/optimizer/benchmark/benchmarks.go` (新建)

---

## ✅ 验收标准

### 阶段1验收
- [x] Hash Join不再重复构建哈希表
- [x] 实现流式迭代器接口
- [x] 表达式求值性能提升10倍以上
- [x] 实现并行扫描（2-4倍提升）

### 阶段2验收
- [x] 实现Batch接口和向量操作
- [x] 向量化过滤和聚合（5-10倍提升）
- [x] 内存池化，GC减少30%以上
- [x] 类型特化，性能提升2-3倍

### 阶段3验收
- [x] 统计信息收集器
- [x] 基于成本的最优计划选择
- [x] JOIN重排序（动态规划）
- [x] 内存索引（B-Tree）
- [x] 完整并行执行引擎（4-8倍提升）

### 监控验收
- [x] 性能监控器
- [x] 慢查询分析器
- [x] 自动优化建议

---

## 📚 参考资料

- [DuckDB Architecture](https://duckdb.org/docs/architecture)
- [TiDB Cost Model](https://docs.pingcap.com/tidb/stable/cost-based-optimization)
- [ClickHouse Performance](https://clickhouse.com/docs/en/operations/performance-test)
- [Volcano Model](https://dsf.berkeley.edu/papers/fonkika.pdf)
