# 阶段5: 高级特性支持 - 实施计划

## 📊 概述

本阶段将实现SQL高级特性支持,包括CTE(公用表表达式)、窗口函数、子查询和存储过程。

**开始时间**: 2026年1月17日  
**预计完成**: 2026年1月17日

---

## 🎯 核心目标

### 1. CTE（公用表表达式）支持

**优先级**: 高  
**复杂度**: 中  
**预计工作量**: 3-4小时

**实现内容**:
- WITH子句的解析
- CTE的展开和优化
- 递归CTE支持（基础版本）
- CTE与主查询的集成

**使用场景**:
```sql
-- 简单CTE
WITH sales_by_region AS (
    SELECT region, SUM(amount) as total
    FROM sales
    GROUP BY region
)
SELECT * FROM sales_by_region WHERE total > 10000;

-- 多个CTE
WITH top_products AS (
    SELECT product_id, COUNT(*) as sales_count
    FROM orders
    GROUP BY product_id
    ORDER BY sales_count DESC
    LIMIT 10
),
revenue_by_product AS (
    SELECT product_id, SUM(price * quantity) as revenue
    FROM order_items
    GROUP BY product_id
)
SELECT p.name, t.sales_count, r.revenue
FROM top_products t
JOIN revenue_by_product r ON t.product_id = r.product_id
JOIN products p ON t.product_id = p.id;
```

### 2. 完善子查询支持

**优先级**: 高  
**复杂度**: 高  
**预计工作量**: 4-5小时

**当前状态**: 已有半连接重写规则(semi_join_rewrite.go)

**需要完善**:
- 标量子查询: `SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t2)`
- 相关子查询: `SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)`
- IN子查询: `SELECT * FROM t WHERE id IN (SELECT id FROM t2)`
- 优化规则集成

**实现策略**:
1. 子查询去相关化
2. 子查询物化
3. 半连接重写（已有）
4. 标量子查询优化

### 3. 窗口函数支持

**优先级**: 高  
**复杂度**: 高  
**预计工作量**: 5-6小时

**实现内容**:
- 基础窗口函数: ROW_NUMBER, RANK, DENSE_RANK
- 偏移函数: LAG, LEAD
- 聚合窗口函数: SUM, AVG, COUNT, MAX, MIN
- 分帧和排序支持
- 窗口函数执行算子

**使用场景**:
```sql
-- ROW_NUMBER - 排名
SELECT 
    name,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees;

-- LAG/LEAD - 访问前后行
SELECT 
    date,
    price,
    LAG(price) OVER (ORDER BY date) as prev_price,
    LEAD(price) OVER (ORDER BY date) as next_price
FROM stock_prices;

-- 聚合窗口函数 - 移动平均
SELECT 
    date,
    revenue,
    AVG(revenue) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg
FROM daily_revenue;
```

**技术要点**:
- 分区(PARTITION BY)处理
- 排序(ORDER BY)支持
- 帧定义(ROWS BETWEEN)实现
- 缓冲区管理(LAG/LEAD需要访问历史行)

### 4. 存储过程和函数支持

**优先级**: 中  
**复杂度**: 高  
**预计工作量**: 4-5小时

**实现内容**:
- CREATE PROCEDURE解析
- CREATE FUNCTION解析
- 变量管理(DECLARE, SET)
- 流程控制(IF, WHILE, CASE)
- 简单的执行引擎

**使用场景**:
```sql
-- 存储过程
CREATE PROCEDURE add_order(
    IN p_customer_id INT,
    IN p_product_id INT,
    IN p_quantity INT
)
BEGIN
    DECLARE v_total DECIMAL(10,2);
    DECLARE v_order_id INT;
    
    SELECT price * p_quantity INTO v_total
    FROM products
    WHERE id = p_product_id;
    
    INSERT INTO orders (customer_id, total)
    VALUES (p_customer_id, v_total);
    
    SET v_order_id = LAST_INSERT_ID();
    
    INSERT INTO order_items (order_id, product_id, quantity)
    VALUES (v_order_id, p_product_id, p_quantity);
END;

-- 调用
CALL add_order(1, 100, 2);

-- 自定义函数
CREATE FUNCTION calculate_tax(amount DECIMAL(10,2))
RETURNS DECIMAL(10,2)
BEGIN
    RETURN amount * 0.1;
END;

SELECT price, calculate_tax(price) as tax, price + calculate_tax(price) as total
FROM products;
```

**技术要点**:
- 词法分析扩展
- AST节点扩展
- 作用域管理
- 变量绑定
- 执行引擎扩展

---

## 📋 实施步骤

### 步骤1: CTE支持（3-4小时）

1. **扩展解析器** (1小时)
   - 识别WITH子句
   - 解析CTE定义
   - 构建CTE AST

2. **实现CTE优化规则** (1小时)
   - CTE内联优化
   - CTE复用优化
   - 递归CTE检测

3. **集成到执行引擎** (1-2小时)
   - CTE展开逻辑
   - CTE缓存机制
   - 递归CTE执行

4. **测试** (1小时)
   - 简单CTE
   - 多CTE
   - 嵌套CTE
   - 性能测试

### 步骤2: 窗口函数支持（5-6小时）

1. **扩展解析器** (1小时)
   - 识别OVER子句
   - 解析窗口规范
   - 支持多种窗口函数

2. **实现窗口函数算子** (2-3小时)
   - WindowOperator结构
   - 分区处理
   - 排序处理
   - 帧处理
   - 窗口函数计算

3. **实现具体窗口函数** (1.5-2小时)
   - ROW_NUMBER
   - RANK
   - DENSE_RANK
   - LAG/LEAD
   - 聚合窗口函数

4. **集成到优化器** (0.5小时)
   - 窗口函数规则
   - 执行计划生成

5. **测试** (1小时)
   - 基础窗口函数
   - 复杂窗口函数
   - 性能测试

### 步骤3: 完善子查询（4-5小时）

1. **子查询分析** (1小时)
   - 标量子查询检测
   - 相关子查询检测
   - 子查询类型分类

2. **实现子查询优化规则** (2小时)
   - 子查询去相关化
   - 子查询物化
   - 集成半连接重写

3. **实现子查询执行** (1小时)
   - 标量子查询执行
   - 子查询缓存

4. **测试** (1小时)
   - 标量子查询
   - 相关子查询
   - IN子查询
   - EXISTS子查询

### 步骤4: 存储过程和函数（4-5小时）

1. **扩展解析器** (2小时)
   - CREATE PROCEDURE语法
   - CREATE FUNCTION语法
   - 变量声明语法
   - 流程控制语法

2. **实现元数据管理** (1小时)
   - 存储过程存储
   - 函数存储
   - 元数据查询

3. **实现执行引擎** (1小时)
   - 变量作用域
   - 流程控制
   - 简单执行

4. **测试** (1小时)
   - 简单存储过程
   - 带参数的存储过程
   - 自定义函数
   - 复杂存储过程

---

## 📁 文件结构

```
mysql/
├── parser/
│   ├── cte.go                    # CTE解析器
│   ├── window.go                  # 窗口函数解析器
│   ├── procedure.go               # 存储过程解析器
│   └── types.go (update)         # 类型定义
├── optimizer/
│   ├── cte_optimizer.go           # CTE优化规则
│   ├── window_operator.go         # 窗口函数算子
│   ├── window_functions.go        # 窗口函数实现
│   └── subquery_optimizer.go     # 子查询优化
└── runtime/
    ├── procedure_executor.go      # 存储过程执行器
    ├── variable_manager.go        # 变量管理
    └── scope.go                # 作用域管理
```

---

## 🔧 技术挑战和解决方案

### 挑战1: CTE的递归执行

**问题**: 递归CTE需要特殊的执行策略

**解决方案**:
1. 使用迭代执行模型
2. 维护工作集和结果集
3. 检测固定点(fixed point)
4. 循环检测

### 挑战2: 窗口函数的状态管理

**问题**: 窗口函数需要访问当前分区的前后行

**解决方案**:
1. 分区缓冲区: 保存当前分区的所有行
2. 排序缓冲区: 支持窗口内的排序
3. 帧缓冲区: 支持ROW帧的定义
4. LAG/LEAD缓存: 访问历史行

### 挑战3: 相关子查询的优化

**问题**: 相关子查询难以优化,性能差

**解决方案**:
1. 子查询去相关化: 将相关子查询转换为JOIN
2. 子查询物化: 缓存子查询结果
3. 半连接重写: EXISTS/IN转换为半连接
4. 去相关化算法: 如Magic Set算法

### 挑战4: 存储过程的执行控制

**问题**: 需要实现完整的执行引擎

**解决方案**:
1. 简化实现: 先支持基本的流程控制
2. 解释执行: 不编译,直接解释执行AST
3. 作用域栈: 管理变量的作用域
4. 错误处理: 支持事务回滚

---

## 📈 性能优化策略

### CTE优化
1. **内联优化**: 将CTE直接展开到主查询
2. **复用优化**: CTE被多次引用时缓存结果
3. **提前过滤**: 在CTE定义中尽早应用过滤条件

### 窗口函数优化
1. **增量计算**: 避免为每行重新计算整个窗口
2. **帧优化**: 优先使用ROW帧(比RANGE帧高效)
3. **分区缓存**: 缓存已排序的分区数据

### 子查询优化
1. **物化**: 子查询只执行一次
2. **去相关化**: 转换为JOIN
3. **下推**: 将过滤条件下推到子查询中

---

## ✅ 验收标准

### CTE支持
- [x] 支持简单WITH子句
- [x] 支持多个CTE
- [x] 支持嵌套CTE
- [x] 支持递归CTE(基础)
- [x] CTE优化规则生效
- [x] 测试通过

### 窗口函数
- [x] 支持ROW_NUMBER
- [x] 支持RANK
- [x] 支持DENSE_RANK
- [x] 支持LAG/LEAD
- [x] 支持聚合窗口函数
- [x] 支持PARTITION BY
- [x] 支持ORDER BY
- [x] 支持ROW帧
- [x] 测试通过

### 子查询
- [x] 支持标量子查询
- [x] 支持相关子查询
- [x] EXISTS子查询优化
- [x] IN子查询优化
- [x] 测试通过

### 存储过程
- [x] 支持CREATE PROCEDURE
- [x] 支持参数(IN/OUT/INOUT)
- [x] 支持变量(DECLARE/SET)
- [x] 支持流程控制(IF/WHILE)
- [x] 支持CALL
- [x] 测试通过

---

## 📚 参考资源

### CTE实现
- PostgreSQL CTE实现
- TiDB CTE优化
- DuckDB CTE执行

### 窗口函数
- PostgreSQL窗口函数文档
- Spark SQL窗口函数
- TiDB窗口函数实现

### 子查询优化
- MySQL子查询优化
- PostgreSQL子查询去相关化
- TiDB子查询重写

### 存储过程
- MySQL存储过程文档
- PostgreSQL PL/pgSQL
- TiDB存储过程支持

---

**文档版本**: 1.0  
**创建日期**: 2026年1月17日  
**作者**: AI Assistant
