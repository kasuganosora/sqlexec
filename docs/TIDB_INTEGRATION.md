# TiDB 查询处理研究与集成方案

## 1. 研究概述

通过深入克隆和研究 TiDB 仓库（`https://github.com/pingcap/tidb`），我们分析了其查询处理架构，并识别出可以复用和集成的关键组件。

## 2. TiDB 查询处理架构

### 2.1 整体流程

```
SQL输入 → Parser（解析） → Session（会话） → Planner（优化） → Executor（执行） → 存储层
```

### 2.2 核心组件

#### A. SQL Parser（解析器）
- **路径**: `d:/code/db/tidb/pkg/parser/`
- **核心文件**:
  - `parser.go` (7.43 MB, 由 goyacc 生成)
  - `ast/` - 抽象语法树定义
  - `lexer.go` - 词法分析器
  - `hintparser.go` - Hint 解析器
- **特性**:
  - 完整的 MySQL 5.7/8.0 语法兼容
  - 支持复杂的 JOIN 查询
  - 支持子查询、窗口函数、CTE
  - 可扩展的语法定义

#### B. Query Planner（查询优化器）
- **路径**: `d:/code/db/tidb/pkg/planner/`
- **核心组件**:
  - `core/optimizer.go` - 主优化器
  - `core/operator/logicalop/` - 逻辑算子
  - `core/operator/physicalop/` - 物理算子
  - `cardinality/` - 基数估算
- **优化规则**:
  - JOIN 重排序（贪心、动态规划）
  - 谓词下推
  - 外连接转内连接
  - JOIN 消除
  - 列裁剪

#### C. Expression Engine（表达式引擎）
- **路径**: `d:/code/db/tidb/pkg/expression/`
- **核心文件**:
  - `expression.go` - 表达式接口
  - `builtin_*.go` - 内置函数（算术、字符串、时间、JSON等）
  - `schema.go` - Schema 管理
- **特性**:
  - 向量化执行支持
  - 丰富的内置函数
  - 完整的类型系统

#### D. Executor（执行引擎）
- **路径**: `d:/code/db/tidb/pkg/executor/`
- **主要执行器**:
  - `TableReaderExecutor` - 表读取
  - `HashJoinV2Exec` - 哈希连接
  - `MergeJoinExec` - 归并连接
  - `SelectionExec` - WHERE 过滤
  - `ProjectionExec` - 列投影
  - `AggregationExec` - 聚合

## 3. JOIN 查询处理机制

### 3.1 逻辑层（LogicalJoin）
```go
type LogicalJoin struct {
    JoinType     base.JoinType  // Inner, LeftOuter, RightOuter, Semi, Anti
    EqualConditions []*expression.ScalarFunction
    NAEQConditions  []*expression.ScalarFunction
    LeftConditions  expression.CNFExprs
    RightConditions expression.CNFExprs
    OtherConditions expression.CNFExprs
}
```

### 3.2 优化规则
- **rule_join_reorder.go** - JOIN 重排序
- **rule_semi_join_rewrite.go** - 半连接重写
- **rule_outer_to_inner_join.go** - 外连接转内连接
- **rule_join_elimination.go** - JOIN 消除

### 3.3 物理执行
- **HashJoin** - 基于哈希的连接算法
- **MergeJoin** - 基于归并的连接算法
- **IndexJoin** - 基于索引的连接算法
- **IndexLookupJoin** - 索引查找连接

## 4. WHERE 条件处理机制

### 4.1 LogicalSelection
```go
type LogicalSelection struct {
    Conditions []expression.Expression
}
```

### 4.2 谓词下推（Predicate Pushdown）
- **规则**: `rule_predicate_push_down.go`
- 将条件尽可能下推到数据源
- 减少中间结果集大小

### 4.3 SelectionExec 执行
- 评估过滤器表达式
- 逐行过滤数据
- 支持短路求值

## 5. 可复用的组件分析

### ✅ 高度推荐独立使用

#### A. SQL Parser（最推荐）
- **可复用性**: ⭐⭐⭐⭐⭐
- **依赖**: 无
- **理由**:
  - 已被多个外部项目使用（SOAR、Gaea、Bytebase等）
  - 有独立文档和测试
  - 代码结构清晰
  - 支持完整的 MySQL 语法
- **使用方式**:
  ```go
  import "github.com/pingcap/tidb/pkg/parser"
  
  p := parser.New()
  stmtNodes, warns, err := p.Parse(sql, charset, collation)
  ```

#### B. Type System（推荐）
- **可复用性**: ⭐⭐⭐⭐
- **依赖**: 少量
- **理由**:
  - 完整的数据库类型系统
  - Datum 类型及其操作
  - 类型转换逻辑
- **可复用文件**:
  - `pkg/types/datum.go`
  - `pkg/types/field_type.go`
  - `pkg/types/convert.go`

### ⚠️ 有依赖但可考虑复用

#### C. Expression Engine（需改造）
- **可复用性**: ⭐⭐⭐
- **依赖**: Type System, Schema
- **理由**:
  - 完整的表达式求值
  - 丰富的内置函数
  - 向量化执行
- **限制**:
  - 依赖 TiDB 的 Session 上下文
  - 某些函数依赖统计信息

#### D. Logical Plan（需改造）
- **可复用性**: ⭐⭐
- **依赖**: Expression Engine, Type System, Statistics
- **理由**:
  - 完整的逻辑算子
  - 丰富的优化规则
- **限制**:
  - 依赖统计信息收集
  - 依赖存储层接口

### ❌ 不推荐独立使用

#### E. Executor Engine
- **理由**:
  - 严重依赖 TiKV 存储层
  - 依赖 TiDB 的事务模型
  - 依赖 Session 上下文

#### F. Session Layer
- **理由**:
  - 与 TiDB 架构深度耦合
  - 依赖 DDL Meta
  - 依赖分布式事务

## 6. 集成方案

### 6.1 当前项目状态

我们的项目已经实现了：
- ✅ 统一的数据源接口（`mysql/resource/`）
- ✅ 内存数据源实现
- ✅ MySQL 数据源实现
- ✅ 数据源管理器
- ✅ 基础的 CRUD 操作

### 6.2 推荐的集成策略

#### 策略 1：最小集成（推荐）
```
使用：TiDB Parser + Type System
用途：SQL 解析、类型处理
```

**优点**：
- 依赖最小
- 集成简单
- 可以解析任意复杂的 SQL

**实现步骤**：
1. 使用 TiDB Parser 解析 SQL
2. 提取查询的结构化信息（表、列、条件等）
3. 将解析结果转换为数据源操作
4. 处理解析错误和警告

#### 策略 2：中等集成（可选）
```
使用：上述 + Expression Engine
用途：表达式求值、内置函数
```

**优点**：
- 支持复杂的表达式计算
- 丰富的内置函数
- 更好的性能（向量化）

**实现步骤**：
1. 实现表达式求值适配层
2. 集成内置函数
3. 处理类型转换

#### 策略 3：深度集成（复杂）
```
使用：全部组件
用途：完整的查询优化和执行
```

**优点**：
- 完整的优化能力
- 高性能执行
- 支持复杂查询

**挑战**：
- 需要重写存储适配层
- 需要实现统计信息收集
- 需要适配事务模型

### 6.3 具体实现示例

#### 示例 1：使用 TiDB Parser 解析 SQL

```go
import "github.com/pingcap/tidb/pkg/parser"

func ParseSQL(sql string) ([]ast.StmtNode, error) {
    p := parser.New()
    stmtNodes, warns, err := p.Parse(sql, "", "")
    
    // 处理警告
    for _, w := range warns {
        log.Printf("解析警告: %s", w.Error())
    }
    
    return stmtNodes, err
}
```

#### 示例 2：提取查询信息

```go
func ExtractQueryInfo(stmtNode ast.StmtNode) (*QueryInfo, error) {
    selectStmt, ok := stmtNode.(*ast.SelectStmt)
    if !ok {
        return nil, fmt.Errorf("not a SELECT statement")
    }
    
    info := &QueryInfo{
        Tables:  extractTables(selectStmt.From.TableRefs),
        Columns: extractColumns(selectStmt.Fields),
        Where:   selectStmt.Where,
        OrderBy: selectStmt.OrderBy,
    }
    
    return info, nil
}
```

#### 示例 3：转换为数据源操作

```go
func ExecuteSQL(ds resource.DataSource, sql string) (*resource.QueryResult, error) {
    // 1. 解析 SQL
    stmtNodes, err := ParseSQL(sql)
    if err != nil {
        return nil, err
    }
    
    // 2. 提取查询信息
    info, err := ExtractQueryInfo(stmtNodes[0])
    if err != nil {
        return nil, err
    }
    
    // 3. 构建数据源查询
    options := &resource.QueryOptions{
        Filters:  extractFilters(info.Where),
        OrderBy:  extractOrderBy(info.OrderBy),
        Limit:    extractLimit(info.Limit),
    }
    
    // 4. 执行查询
    return ds.Query(ctx, info.Tables[0], options)
}
```

## 7. 未来扩展计划

### 7.1 短期目标
- [ ] 集成 TiDB Parser
- [ ] 实现基本的 SQL 解析和信息提取
- [ ] 支持 SELECT、INSERT、UPDATE、DELETE
- [ ] 支持基本的 WHERE 条件
- [ ] 支持 ORDER BY 和 LIMIT

### 7.2 中期目标
- [ ] 支持多表 JOIN 查询
- [ ] 支持子查询
- [ ] 支持复杂的 WHERE 条件
- [ ] 支持聚合函数（COUNT, SUM, AVG等）
- [ ] 支持 GROUP BY 和 HAVING

### 7.3 长期目标
- [ ] 集成 Expression Engine
- [ ] 集成 Type System
- [ ] 实现查询优化
- [ ] 支持窗口函数
- [ ] 支持 CTE（Common Table Expressions）

## 8. 测试文件

### 8.1 测试 TiDB Parser
- **文件**: `test_tidb_parser.go`
- **功能**:
  - 解析各种 SQL 语句
  - 测试 SELECT、INSERT、UPDATE、DELETE
  - 测试 JOIN 查询
  - 测试子查询

### 8.2 集成测试
- **文件**: `example_tidb_simple.go`
- **功能**:
  - 演示 TiDB Parser 的使用
  - 演示与数据源的配合
  - 演示 TiDB Parser 的特性

## 9. 编译和运行

### 9.1 编译项目
```bash
cd /code/db
go mod tidy
go build .
```

### 9.2 运行测试
```bash
# 测试 TiDB Parser
go run test_tidb_parser.go

# 运行集成示例
go run example_tidb_simple.go
```

## 10. 参考资料

- TiDB 官方文档: https://docs.pingcap.com/tidb/stable
- TiDB 源码: https://github.com/pingcap/tidb
- Parser 包文档: `d:/code/db/tidb/pkg/parser/README.md`
- 示例项目:
  - SOAR: https://github.com/XiaoMi/soar
  - Gaea: https://github.com/XiaoMi/Gaea

## 11. 总结

通过研究 TiDB 的查询处理架构，我们识别出了以下关键发现：

1. **TiDB Parser** 是最值得集成的组件，依赖最小、功能强大
2. **Expression Engine** 和 **Type System** 可以逐步集成以提供更强大的功能
3. **Planner** 和 **Executor** 依赖较重，需要大量改造才能复用
4. 我们的数据源接口已经具备了良好的基础，可以逐步增强

推荐的集成路径是：**先集成 Parser，再考虑 Expression Engine，最后是完整的优化器**。

这样可以在保持项目简洁的同时，逐步提升 SQL 处理能力。
