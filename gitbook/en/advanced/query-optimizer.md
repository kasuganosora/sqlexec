# Query Optimizer

SQLExec includes a built-in cost-based query optimizer that automatically optimizes the execution plans of SQL queries.

## Enabling the Optimizer

### Server Mode

```json
{
  "optimizer": {
    "enabled": true
  }
}
```

### Embedded Mode

```go
db, _ := api.NewDB(&api.DBConfig{
    UseEnhancedOptimizer: true,
})
```

## Optimization Rules

| Rule | Description | Example |
|------|------|------|
| **Predicate Pushdown** | Push WHERE conditions down to the data source as early as possible | `SELECT * FROM (SELECT * FROM t) s WHERE s.id > 10` -> condition pushed into subquery |
| **Column Pruning** | Read only the columns needed by the query | `SELECT name FROM t` -> other columns are not read |
| **Index Selection** | Select the optimal index based on cost | Automatically selects B-Tree / Hash / Fulltext / Vector index |
| **JOIN Reordering** | Optimize the order of multi-table JOINs | Smaller tables drive larger tables |
| **Subquery Flattening** | Convert subqueries into JOINs | `WHERE id IN (SELECT id FROM ...)` -> INNER JOIN |
| **Subquery Materialization** | Cache subquery results to avoid redundant computation | Correlated subquery -> compute once, reference multiple times |
| **JOIN Elimination** | Remove unnecessary JOINs | Unused LEFT JOIN tables can be eliminated |
| **Max/Min Optimization** | Use indexes to quickly retrieve MAX/MIN values | `SELECT MAX(id) FROM t` -> last value in the index |
| **TopN Pushdown** | Push LIMIT down before aggregation | Reduces intermediate data volume |
| **Window Function Optimization** | Efficiently compute window functions | Merge window computations with the same partition |
| **Full-Text Index Utilization** | Use full-text indexes to accelerate search | `MATCH AGAINST` -> inverted index |
| **Vector Index Utilization** | Use vector indexes to accelerate ANN search | `ORDER BY vec_distance() LIMIT K` -> vector index |

## Viewing Execution Plans

Use `EXPLAIN` to view the execution plan of a query:

```sql
EXPLAIN SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.age > 25
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 10;
```

### Embedded

```go
plan, err := session.Explain("SELECT * FROM users WHERE id > ?", 10)
fmt.Println(plan)
```

## Plan Caching

The optimizer caches optimized query plans to avoid redundant optimization of queries with the same structure:

- Matching based on SQL statement hash values
- Parameterized queries share plans (`SELECT * FROM t WHERE id = ?`)
- DDL operations automatically invalidate the cache
- Cache size and TTL are configured via `cache.query_cache`

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
