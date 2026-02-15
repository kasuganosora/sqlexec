# 索引管理

索引是数据库查询加速的核心机制。SQLExec 支持 4 种索引类型，覆盖从精确查找到语义搜索的各种场景。

> **注意**：目前仅 Memory 数据源支持索引功能。MySQL 和 PostgreSQL 数据源使用其原生索引机制。

## 索引类型

| 索引类型 | 适用查询 | 操作符 | 特点 |
|---------|---------|--------|------|
| B-Tree | 范围查询、排序 | `=`, `<`, `>`, `<=`, `>=`, `BETWEEN` | 默认类型，有序存储，支持范围扫描 |
| Hash | 精确匹配 | `=` | 极快的点查询，不支持范围查询 |
| Fulltext | 全文搜索 | `MATCH ... AGAINST` | 基于 BM25 的倒排索引 |
| Vector | 近似最近邻搜索 | `vec_cosine_distance` 等 | 高维向量的 ANN 搜索 |

## 创建索引

### B-Tree 索引（默认）

```sql
-- 默认创建 B-Tree 索引
CREATE INDEX idx_user_name ON users(name);

-- 显式指定 B-Tree
CREATE INDEX idx_user_age ON users(age) USING BTREE;
```

### Hash 索引

```sql
CREATE INDEX idx_user_email ON users(email) USING HASH;
```

### 唯一索引

```sql
CREATE UNIQUE INDEX idx_user_email ON users(email);
```

### 全文索引

```sql
CREATE FULLTEXT INDEX idx_article_content ON articles(content);

-- 指定分词器
CREATE FULLTEXT INDEX idx_article_content ON articles(content)
    WITH (tokenizer = 'jieba');
```

### 向量索引

```sql
CREATE VECTOR INDEX idx_embedding ON documents(embedding)
    USING HNSW
    WITH (metric = 'cosine', m = 16, ef_construction = 200);
```

## 复合索引

B-Tree 索引支持多列复合索引，遵循最左前缀匹配原则：

```sql
-- 创建复合索引
CREATE INDEX idx_user_name_age ON users(name, age);

-- 以下查询可以利用该索引
SELECT * FROM users WHERE name = '张三';                    -- 使用索引
SELECT * FROM users WHERE name = '张三' AND age > 20;       -- 使用索引
SELECT * FROM users WHERE age > 20;                         -- 不使用该索引
```

## 删除索引

```sql
DROP INDEX idx_user_name ON users;
```

## 索引选择

SQLExec 的查询优化器会自动为查询选择最优索引：

- 对于 `=` 条件，优先选择 Hash 索引（如果存在），否则选择 B-Tree 索引
- 对于范围条件（`<`, `>`, `BETWEEN`），选择 B-Tree 索引
- 对于 `MATCH ... AGAINST`，选择 Fulltext 索引
- 对于向量距离排序，选择 Vector 索引
- 如果存在多个可用索引，优化器根据估算代价选择最优方案

## 使用建议

| 场景 | 推荐索引类型 |
|------|-------------|
| 主键查询 | B-Tree（自动创建） |
| 等值精确查找 | Hash |
| 范围查询、排序 | B-Tree |
| 文本关键词搜索 | Fulltext |
| 向量相似度搜索 | Vector (HNSW) |
| 唯一性约束 | UNIQUE (B-Tree) |

## 示例

```sql
-- 创建表
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    description TEXT,
    feature_vector VECTOR(256)
);

-- 为分类列创建 Hash 索引（精确匹配）
CREATE INDEX idx_category ON products(category) USING HASH;

-- 为价格列创建 B-Tree 索引（范围查询）
CREATE INDEX idx_price ON products(price);

-- 为名称创建唯一索引
CREATE UNIQUE INDEX idx_name ON products(name);

-- 为描述创建全文索引
CREATE FULLTEXT INDEX idx_desc ON products(description)
    WITH (tokenizer = 'jieba');

-- 为特征向量创建向量索引
CREATE VECTOR INDEX idx_feature ON products(feature_vector)
    USING HNSW
    WITH (metric = 'cosine', m = 16, ef_construction = 200);

-- 查询时优化器自动选择索引
SELECT * FROM products WHERE category = '电子产品';           -- 使用 Hash 索引
SELECT * FROM products WHERE price BETWEEN 100 AND 500;       -- 使用 B-Tree 索引
SELECT * FROM products WHERE MATCH(description) AGAINST('蓝牙'); -- 使用 Fulltext 索引
```
