# Index Management

Indexes are the core mechanism for accelerating database queries. SQLExec supports 5 index types, covering a range of scenarios from exact lookups to geospatial queries.

> **Note**: Currently only the Memory data source supports indexing. MySQL and PostgreSQL data sources use their native indexing mechanisms.

> **New Feature**: Composite (multi-column) indexes are now supported! You can create indexes on multiple columns for more efficient queries.

## Index Types

| Index Type | Applicable Queries | Operators | Characteristics |
|---------|---------|--------|------|
| B-Tree | Range queries, sorting | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN` | Default type, ordered storage, supports range scans |
| Hash | Exact match | `=` | Extremely fast point lookups, does not support range queries |
| Fulltext | Full-text search | `MATCH ... AGAINST` | BM25-based inverted index |
| Vector | Approximate nearest neighbor search | `vec_cosine_distance` etc. | ANN search for high-dimensional vectors |
| Spatial (R-Tree) | Geospatial queries | `ST_Contains`, `ST_Intersects` etc. | R-Tree index for geometry bounding boxes |

## Creating Indexes

### B-Tree Index (Default)

```sql
-- Create a B-Tree index by default
CREATE INDEX idx_user_name ON users(name);

-- Explicitly specify B-Tree
CREATE INDEX idx_user_age ON users(age) USING BTREE;
```

### Hash Index

```sql
CREATE INDEX idx_user_email ON users(email) USING HASH;
```

### Unique Index

```sql
CREATE UNIQUE INDEX idx_user_email ON users(email);
```

### Full-Text Index

```sql
CREATE FULLTEXT INDEX idx_article_content ON articles(content);

-- Specify a tokenizer
CREATE FULLTEXT INDEX idx_article_content ON articles(content)
    WITH (tokenizer = 'jieba');
```

### Vector Index

```sql
CREATE VECTOR INDEX idx_embedding ON documents(embedding)
    USING HNSW
    WITH (metric = 'cosine', m = 16, ef_construction = 200);
```

### Spatial Index (NEW)

```sql
CREATE SPATIAL INDEX idx_location ON cities(location);
```

The spatial index uses an R-Tree data structure to accelerate geospatial queries such as `ST_Contains`, `ST_Within`, and `ST_Intersects`. See [Spatial Index](spatial-index.md) for details.

## Composite Indexes (NEW)

B-Tree and Hash indexes support multi-column composite indexes, following the leftmost prefix matching principle:

```sql
-- Create a composite index on multiple columns
CREATE INDEX idx_user_name_age ON users(name, age);

-- Create a composite index on three columns
CREATE INDEX idx_order_customer_date ON orders(customer_id, order_date, status);

-- The following queries can utilize this index
SELECT * FROM users WHERE name = '张三';                    -- Uses index
SELECT * FROM users WHERE name = '张三' AND age > 20;       -- Uses index
SELECT * FROM users WHERE name = '张三' AND age = 25;       -- Uses index
SELECT * FROM users WHERE age > 20;                         -- Does not use this index (missing first column)
```

### Composite Index Benefits

- **More efficient filtering**: Multi-column conditions can be satisfied with a single index scan
- **Covering index**: If all selected columns are in the index, table lookup can be avoided
- **Sorted retrieval**: Composite indexes maintain ordering across all columns

### Limitations

- **Leftmost prefix rule**: Queries must include the first column to use the index
- **Full-text indexes**: Currently only support single-column indexes
- **Hash indexes**: Support composite indexes but only for exact equality matches on all columns

## Dropping Indexes

```sql
DROP INDEX idx_user_name ON users;
```

## Index Selection

SQLExec's query optimizer automatically selects the optimal index for queries:

- For `=` conditions, the Hash index is preferred (if available), otherwise the B-Tree index is selected
- For range conditions (`<`, `>`, `BETWEEN`), the B-Tree index is selected
- For `MATCH ... AGAINST`, the Fulltext index is selected
- For vector distance ordering, the Vector index is selected
- For spatial predicates (`ST_Contains`, `ST_Intersects`), the Spatial R-Tree index is selected
- If multiple indexes are available, the optimizer selects the best plan based on estimated cost

## Usage Recommendations

| Scenario | Recommended Index Type |
|------|-------------|
| Primary key lookup | B-Tree (automatically created) |
| Exact value lookup | Hash |
| Range queries, sorting | B-Tree |
| Text keyword search | Fulltext |
| Vector similarity search | Vector (HNSW) |
| Uniqueness constraint | UNIQUE (B-Tree) |
| Multi-column filtering | Composite B-Tree |
| Multi-column exact match | Composite Hash |
| Geospatial queries | Spatial (R-Tree) |

## Example

```sql
-- Create a table
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    description TEXT,
    feature_vector VECTOR(256)
);

-- Create a Hash index on the category column (exact match)
CREATE INDEX idx_category ON products(category) USING HASH;

-- Create a B-Tree index on the price column (range queries)
CREATE INDEX idx_price ON products(price);

-- Create a unique index on the name column
CREATE UNIQUE INDEX idx_name ON products(name);

-- Create a full-text index on the description column
CREATE FULLTEXT INDEX idx_desc ON products(description)
    WITH (tokenizer = 'jieba');

-- Create a vector index on the feature vector column
CREATE VECTOR INDEX idx_feature ON products(feature_vector)
    USING HNSW
    WITH (metric = 'cosine', m = 16, ef_construction = 200);

-- Create a spatial index on the geometry column
CREATE SPATIAL INDEX idx_loc ON products(location);

-- Create a composite index for multi-column queries
CREATE INDEX idx_category_price ON products(category, price);

-- The optimizer automatically selects indexes during queries
SELECT * FROM products WHERE category = '电子产品';           -- Uses Hash index
SELECT * FROM products WHERE price BETWEEN 100 AND 500;       -- Uses B-Tree index
SELECT * FROM products WHERE MATCH(description) AGAINST('蓝牙'); -- Uses Fulltext index
SELECT * FROM products WHERE category = '电子产品' AND price > 1000; -- Uses composite index
```
