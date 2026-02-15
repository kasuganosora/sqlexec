# Index Management

Indexes are the core mechanism for accelerating database queries. SQLExec supports 4 index types, covering a range of scenarios from exact lookups to semantic search.

> **Note**: Currently only the Memory data source supports indexing. MySQL and PostgreSQL data sources use their native indexing mechanisms.

## Index Types

| Index Type | Applicable Queries | Operators | Characteristics |
|---------|---------|--------|------|
| B-Tree | Range queries, sorting | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN` | Default type, ordered storage, supports range scans |
| Hash | Exact match | `=` | Extremely fast point lookups, does not support range queries |
| Fulltext | Full-text search | `MATCH ... AGAINST` | BM25-based inverted index |
| Vector | Approximate nearest neighbor search | `vec_cosine_distance` etc. | ANN search for high-dimensional vectors |

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

## Composite Indexes

B-Tree indexes support multi-column composite indexes, following the leftmost prefix matching principle:

```sql
-- Create a composite index
CREATE INDEX idx_user_name_age ON users(name, age);

-- The following queries can utilize this index
SELECT * FROM users WHERE name = '张三';                    -- Uses index
SELECT * FROM users WHERE name = '张三' AND age > 20;       -- Uses index
SELECT * FROM users WHERE age > 20;                         -- Does not use this index
```

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

-- The optimizer automatically selects indexes during queries
SELECT * FROM products WHERE category = '电子产品';           -- Uses Hash index
SELECT * FROM products WHERE price BETWEEN 100 AND 500;       -- Uses B-Tree index
SELECT * FROM products WHERE MATCH(description) AGAINST('蓝牙'); -- Uses Fulltext index
```
