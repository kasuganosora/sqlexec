# MySQL 查询优化器

## 概述

这是一个简化的查询优化器实现，基于 TiDB 的优化器架构，支持基本的查询优化和执行。

## 架构

```
SQL 语句
    ↓
SQL Parser (AST)
    ↓
Logical Plan (逻辑计划）
    ↓
Optimization Rules (优化规则）
    ↓
Optimized Logical Plan
    ↓
Physical Plan (物理计划）
    ↓
Execution (执行）
```

## 核心组件

### 1. 类型定义 (`types.go`)

**LogicalPlan 接口**: 逻辑计划接口
- `Children()` - 获取子节点
- `SetChildren()` - 设置子节点
- `Schema()` - 返回输出列
- `Explain()` - 返回计划说明

**PhysicalPlan 接口**: 物理计划接口
- `Children()` - 获取子节点
- `SetChildren()` - 设置子节点
- `Schema()` - 返回输出列
- `Cost()` - 返回执行成本
- `Execute()` - 执行计划
- `Explain()` - 返回计划说明

**CostModel 接口**: 成本模型
- `ScanCost()` - 计算扫描成本
- `FilterCost()` - 计算过滤成本
- `JoinCost()` - 计算连接成本
- `AggregateCost()` - 计算聚合成本
- `ProjectCost()` - 计算投影成本

**OptimizationRule 接口**: 优化规则接口
- `Name()` - 规则名称
- `Match()` - 检查规则是否匹配
- `Apply()` - 应用规则

### 2. 逻辑算子 (`logical_scan.go`)

已实现的逻辑算子：
- **LogicalDataSource** - 数据源（表扫描）
- **LogicalSelection** - 过滤（WHERE 子句）
- **LogicalProjection** - 投影（SELECT 列）
- **LogicalLimit** - 限制（LIMIT/OFFSET）
- **LogicalSort** - 排序（ORDER BY）
- **LogicalJoin** - 连接（JOIN）
- **LogicalAggregate** - 聚合（GROUP BY 和聚合函数）
- **LogicalUnion** - 联合（UNION）

### 3. 物理算子 (`physical_scan.go`)

已实现的物理算子：
- **PhysicalTableScan** - 物理表扫描（✅ 可执行）
- **PhysicalSelection** - 物理过滤（✅ 可执行）
- **PhysicalProjection** - 物理投影（✅ 可执行）
- **PhysicalLimit** - 物理限制（✅ 可执行）
- **PhysicalHashJoin** - 物理哈希连接（⚠️ 未实现执行）
- **PhysicalHashAggregate** - 物理哈希聚合（⚠️ 未实现执行）

### 4. 优化规则 (`rules.go`)

已实现的优化规则：
- **PredicatePushDownRule** - 谓词下推
- **ColumnPruningRule** - 列裁剪
- **ProjectionEliminationRule** - 投影消除
- **LimitPushDownRule** - Limit 下推
- **ConstantFoldingRule** - 常量折叠

### 5. 优化器 (`optimizer.go`)

**Optimizer** - 主优化器
- `Optimize()` - 优化查询计划
  1. 转换为逻辑计划
  2. 应用优化规则
  3. 转换为物理计划
  4. 返回可执行计划

## 使用示例

```go
import (
    "context"
    "mysql-proxy/mysql/optimizer"
    "mysql-proxy/mysql/parser"
    "mysql-proxy/mysql/resource"
)

// 创建数据源
dsConfig := &resource.DataSourceConfig{
    Type: resource.DataSourceTypeMemory,
    Name: "test",
}
dataSource, _ := resource.CreateDataSource(dsConfig)
dataSource.Connect(context.Background())
defer dataSource.Close(context.Background())

// 创建表
tableInfo := &resource.TableInfo{
    Name: "products",
    Columns: []resource.ColumnInfo{
        {Name: "id", Type: "int", Primary: true},
        {Name: "name", Type: "varchar"},
        {Name: "price", Type: "decimal"},
    },
}
dataSource.CreateTable(context.Background(), tableInfo)

// 创建优化器
opt := optimizer.NewOptimizer(dataSource)

// 解析 SQL
adapter := parser.NewSQLAdapter()
parseResult, _ := adapter.Parse("SELECT * FROM products WHERE price > 100")

// 优化查询
plan, _ := opt.Optimize(context.Background(), parseResult.Statement)

// 查看执行计划
fmt.Println(optimizer.ExplainPlanV2(plan))

// 执行查询（使用executor）
das := dataaccess.NewDataService(dataSource)
exec := executor.NewExecutor(das)
result, _ := exec.Execute(context.Background(), plan)
fmt.Printf("返回 %d 行\n", len(result.Rows))
```

## 执行计划示例

### 简单查询
```sql
SELECT * FROM products
```

执行计划：
```
TableScan(products, cost=100.00)
```

### 带 WHERE 条件
```sql
SELECT * FROM products WHERE price > 100
```

执行计划：
```
Selection(cost=130.00)
  TableScan(products, cost=100.00)
```

### 带 LIMIT
```sql
SELECT * FROM products LIMIT 10
```

执行计划：
```
Limit(offset=0, limit=10, cost=100.10)
  TableScan(products, cost=100.00)
```

## 成本模型

使用简化的成本模型：
- **IO Factor**: 0.1 - 磁盘 IO 成本
- **CPU Factor**: 0.01 - CPU 计算成本
- **Memory Factor**: 0.001 - 内存使用成本

各算子的成本计算：
- **Scan**: `rowCount * IOFactor + rowCount * CPUFactor`
- **Filter**: `inputRows * CPUFactor + outputRows`
- **Project**: `inputRows * projCols * CPUFactor`
- **Limit**: `inputCost + limit * 0.01`
- **HashJoin**: `left.Cost + right.Cost + build + probe`
- **HashAgg**: `input.Cost + groupCost + aggCost`

## 已实现功能

✅ 逻辑计划接口和算子
✅ 物理计划接口和算子
✅ 基础优化规则（谓词下推、列裁剪、投影消除等）
✅ 简化的成本模型
✅ 基础算子执行（Scan, Filter, Project, Limit）
✅ SQL 到逻辑计划转换
✅ 逻辑计划到物理计划转换
✅ 执行计划解释

## 待实现功能

⚠️ JOIN 算子执行（物理执行逻辑）
⚠️ Aggregate 算子执行（物理执行逻辑）
⚠️ Sort 算子执行
⚠️ 更精确的统计信息
⚠️ 基于成本的物理计划选择
⚠️ 更多优化规则（JOIN 重排序等）
⚠️ 子查询优化
⚠️ 索引选择优化

## 测试

运行测试：
```bash
go run test_optimizer.go
```

测试覆盖：
- 简单查询优化
- WHERE 条件查询优化
- ORDER BY 查询优化
- LIMIT 查询优化
- 组合条件查询优化

## 文件结构

```
mysql/optimizer/
├── types.go              # 类型定义
├── logical_scan.go       # 逻辑算子
├── physical_scan.go      # 物理算子
├── rules.go             # 优化规则
├── optimizer.go         # 主优化器
└── README.md           # 本文档
```

## 参考资料

本实现参考了 TiDB 的优化器架构：
- [TiDB Planner](https://github.com/pingcap/tidb/tree/master/pkg/planner)
- [TiDB Optimization Rules](https://github.com/pingcap/tidb/tree/master/pkg/planner/core)
- [Volcano Execution Model](https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.24.5418&rep=rep1&type=pdf)
