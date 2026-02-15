# 查询优化器

SQLExec 内置基于代价的查询优化器，自动优化 SQL 查询的执行计划。

## 启用优化器

### 服务器模式

```json
{
  "optimizer": {
    "enabled": true
  }
}
```

### 嵌入式模式

```go
db, _ := api.NewDB(&api.DBConfig{
    UseEnhancedOptimizer: true,
})
```

## 优化规则

| 规则 | 说明 | 示例 |
|------|------|------|
| **谓词下推** | 将 WHERE 条件尽早下推到数据源 | `SELECT * FROM (SELECT * FROM t) s WHERE s.id > 10` → 条件推入子查询 |
| **列裁剪** | 只读取查询需要的列 | `SELECT name FROM t` → 不读取其他列 |
| **索引选择** | 根据代价选择最优索引 | 自动选择 B-Tree / Hash / 全文 / 向量索引 |
| **JOIN 重排序** | 优化多表 JOIN 顺序 | 小表驱动大表 |
| **子查询展平** | 将子查询转换为 JOIN | `WHERE id IN (SELECT id FROM ...)` → INNER JOIN |
| **子查询物化** | 缓存子查询结果避免重复计算 | 关联子查询 → 一次计算多次引用 |
| **JOIN 消除** | 移除不必要的 JOIN | 未使用的 LEFT JOIN 表可被消除 |
| **Max/Min 优化** | 利用索引快速获取 MAX/MIN | `SELECT MAX(id) FROM t` → 索引最后一个值 |
| **TopN 下推** | 将 LIMIT 下推到聚合之前 | 减少中间数据量 |
| **窗口函数优化** | 高效计算窗口函数 | 合并相同分区的窗口计算 |
| **全文索引利用** | 使用全文索引加速搜索 | `MATCH AGAINST` → 倒排索引 |
| **向量索引利用** | 使用向量索引加速 ANN 搜索 | `ORDER BY vec_distance() LIMIT K` → 向量索引 |

## 查看执行计划

使用 `EXPLAIN` 查看查询的执行计划：

```sql
EXPLAIN SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.age > 25
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 10;
```

### 嵌入式

```go
plan, err := session.Explain("SELECT * FROM users WHERE id > ?", 10)
fmt.Println(plan)
```

## 计划缓存

优化器会缓存已优化的查询计划，避免重复优化相同结构的查询：

- 基于 SQL 语句的哈希值匹配
- 参数化查询共享计划（`SELECT * FROM t WHERE id = ?`）
- DDL 操作自动使缓存失效
- 缓存大小和 TTL 通过 `cache.query_cache` 配置

```json
{
  "cache": {
    "query_cache": {
      "max_size": 1000,
      "ttl": "5m"
    }
  }
}
```
