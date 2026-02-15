# Vector Functions

SQLExec includes built-in vector distance calculation functions supporting multiple distance metrics, suitable for vector similarity search, recommendation systems, RAG (Retrieval-Augmented Generation), and other scenarios.

## Vector Format

Vectors are represented as strings using JSON array syntax:

```
'[0.1, 0.2, 0.3, 0.4]'
```

The two vectors involved in a calculation must have the same dimensions.

## Function List

| Function | Description | Return Range | Example |
|----------|-------------|--------------|---------|
| `VEC_COSINE_DISTANCE(v1, v2)` | Calculate cosine distance | 0.0 ~ 2.0 | `SELECT VEC_COSINE_DISTANCE('[1,0]', '[0,1]');` -- `1.0` |
| `VEC_L2_DISTANCE(v1, v2)` | Calculate Euclidean distance (L2) | 0.0 ~ +inf | `SELECT VEC_L2_DISTANCE('[0,0]', '[3,4]');` -- `5.0` |
| `VEC_INNER_PRODUCT(v1, v2)` | Calculate inner product (dot product) | -inf ~ +inf | `SELECT VEC_INNER_PRODUCT('[1,2]', '[3,4]');` -- `11.0` |
| `VEC_DISTANCE(v1, v2)` | Calculate default distance (equivalent to L2 distance) | 0.0 ~ +inf | `SELECT VEC_DISTANCE('[0,0]', '[3,4]');` -- `5.0` |

## Distance Metric Description

### Cosine Distance

Cosine distance = 1 - cosine similarity. It measures the directional difference between two vectors and is not affected by vector magnitude.

- **Value of 0**: The two vectors point in exactly the same direction
- **Value of 1**: The two vectors are orthogonal (unrelated)
- **Value of 2**: The two vectors point in exactly opposite directions

Suitable for text semantic search, document similarity, and other scenarios where vector magnitude is not a concern.

```sql
SELECT VEC_COSINE_DISTANCE('[1,1]', '[1,1]');    -- 0.0 (identical)
SELECT VEC_COSINE_DISTANCE('[1,0]', '[0,1]');    -- 1.0 (orthogonal)
SELECT VEC_COSINE_DISTANCE('[1,0]', '[-1,0]');   -- 2.0 (opposite)
```

### Euclidean Distance (L2 Distance)

Euclidean distance is the straight-line distance between two vectors in space. The smaller the value, the more similar the vectors are.

Suitable for scenarios that need to consider magnitude differences, such as image feature matching.

```sql
SELECT VEC_L2_DISTANCE('[0,0,0]', '[1,2,2]');   -- 3.0
SELECT VEC_L2_DISTANCE('[1,1]', '[4,5]');        -- 5.0
```

### Inner Product

The inner product (dot product) is the sum of the element-wise products of two vectors. A larger value indicates greater similarity (for normalized vectors).

Suitable for normalized vectors, commonly used in Maximum Inner Product Search (MIPS) scenarios.

```sql
SELECT VEC_INNER_PRODUCT('[1,2,3]', '[4,5,6]'); -- 32.0
SELECT VEC_INNER_PRODUCT('[0.5,0.5]', '[1,0]'); -- 0.5
```

## Usage Examples

### Basic Vector Search

```sql
-- Semantic search using cosine distance
SELECT id, title,
       VEC_COSINE_DISTANCE(embedding, '[0.12, -0.34, 0.56, 0.78, -0.91]') AS distance
FROM documents
ORDER BY distance
LIMIT 10;
```

### K-Nearest Neighbor Search (KNN)

```sql
-- Find the 10 most similar records
SELECT id, content,
       VEC_L2_DISTANCE(embedding, '[0.1, 0.2, 0.3, 0.4, 0.5]') AS distance
FROM docs
ORDER BY distance
LIMIT 10;
```

### RAG (Retrieval-Augmented Generation)

```sql
-- Retrieve relevant documents based on the vector representation of a user's question
SELECT id, content, source,
       VEC_COSINE_DISTANCE(embedding, '[0.15, -0.28, 0.44, ...]') AS score
FROM knowledge_base
ORDER BY score
LIMIT 5;
```

### Recommendation System

```sql
-- Find products most similar to a target product
SELECT p.id, p.name, p.category,
       VEC_COSINE_DISTANCE(p.feature_vector, t.feature_vector) AS distance
FROM products p, products t
WHERE t.id = 1001 AND p.id != 1001
ORDER BY distance
LIMIT 20;
```

### Combined with Filter Conditions

```sql
-- Vector search within a specific category
SELECT id, title,
       VEC_COSINE_DISTANCE(embedding, '[0.12, -0.34, 0.56, ...]') AS distance
FROM articles
WHERE category = 'technology'
  AND published = true
ORDER BY distance
LIMIT 10;
```

### Inner Product Search

```sql
-- Maximum similarity search using inner product (vectors are normalized)
SELECT id, title,
       VEC_INNER_PRODUCT(embedding, '[0.12, -0.34, 0.56, ...]') AS score
FROM documents
ORDER BY score DESC
LIMIT 10;
```

## Distance Metric Selection Guide

| Metric | Function | Use Case | Sort Order |
|--------|----------|----------|------------|
| Cosine Distance | `VEC_COSINE_DISTANCE` | Text semantic search, magnitude-independent | Ascending (smaller = more similar) |
| Euclidean Distance | `VEC_L2_DISTANCE` | Image features, spatial positions | Ascending (smaller = more similar) |
| Inner Product | `VEC_INNER_PRODUCT` | Normalized vectors, MIPS | Descending (larger = more similar) |
| Default Distance | `VEC_DISTANCE` | General purpose | Ascending (smaller = more similar) |

## Performance Tips

- For large-scale vector data, it is recommended to create vector indexes (such as HNSW indexes) to accelerate search.
- Higher vector dimensions result in greater computational overhead. Common dimension ranges are 128 to 1536.
- Apply scalar conditions to filter data first in your queries, then perform vector distance calculations. This can significantly improve performance.
- If vectors are normalized, cosine distance and inner product yield equivalent ranking results. Choose the one with faster computation.
