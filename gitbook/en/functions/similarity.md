# Similarity Functions

SQLExec provides multiple similarity calculation functions for fuzzy string matching and vector similarity computation, suitable for data cleaning, deduplication, fuzzy search, and other scenarios.

## Function List

| Function | Description | Return Range | Example |
|----------|-------------|--------------|---------|
| `LEVENSHTEIN(s1, s2)` | Calculate the edit distance between two strings | 0 ~ max(len(s1), len(s2)) | `SELECT LEVENSHTEIN('kitten', 'sitting');` -- `3` |
| `JARO_SIMILARITY(s1, s2)` | Calculate Jaro similarity | 0.0 ~ 1.0 | `SELECT JARO_SIMILARITY('martha', 'marhta');` -- `0.944` |
| `JARO_WINKLER(s1, s2)` | Calculate Jaro-Winkler similarity | 0.0 ~ 1.0 | `SELECT JARO_WINKLER('martha', 'marhta');` -- `0.961` |
| `COSINE_SIMILARITY(v1, v2)` | Calculate cosine similarity between two vectors | -1.0 ~ 1.0 | `SELECT COSINE_SIMILARITY('[1,0]', '[0,1]');` -- `0.0` |

## Detailed Description

### LEVENSHTEIN (Edit Distance)

The Levenshtein distance is the minimum number of single-character edit operations (insertions, deletions, substitutions) required to transform one string into another. The return value is a non-negative integer; the smaller the value, the more similar the two strings are.

```sql
-- Basic usage
SELECT LEVENSHTEIN('hello', 'hallo');  -- 1 (substitute e->a)
SELECT LEVENSHTEIN('abc', 'abcd');     -- 1 (insert d)
SELECT LEVENSHTEIN('cat', 'dog');      -- 3 (all substitutions)

-- Find similar names (edit distance <= 2)
SELECT name FROM customers
WHERE LEVENSHTEIN(name, '张三丰') <= 2;

-- Data cleaning: find potential duplicate records
SELECT a.id, a.name, b.id, b.name,
       LEVENSHTEIN(a.name, b.name) AS distance
FROM products a, products b
WHERE a.id < b.id
  AND LEVENSHTEIN(a.name, b.name) <= 3
ORDER BY distance;
```

### JARO_SIMILARITY (Jaro Similarity)

Jaro similarity measures the degree of matching between two strings, returning a floating-point number between 0.0 (completely different) and 1.0 (identical). It is particularly well-suited for comparing short strings.

```sql
-- Basic usage
SELECT JARO_SIMILARITY('martha', 'marhta');  -- 0.944
SELECT JARO_SIMILARITY('hello', 'world');    -- 0.467

-- Fuzzy matching query
SELECT * FROM contacts
WHERE JARO_SIMILARITY(name, '李明') > 0.8;
```

### JARO_WINKLER (Jaro-Winkler Similarity)

Jaro-Winkler is an improved version of Jaro that gives higher scores to strings sharing a common prefix. It returns a floating-point number between 0.0 and 1.0 and is particularly well-suited for name matching.

```sql
-- Basic usage
SELECT JARO_WINKLER('martha', 'marhta');    -- 0.961
SELECT JARO_WINKLER('dwayne', 'duane');     -- 0.840

-- Fuzzy name matching
SELECT name, JARO_WINKLER(name, '王晓明') AS similarity
FROM users
WHERE JARO_WINKLER(name, '王晓明') > 0.85
ORDER BY similarity DESC;

-- Address similarity matching
SELECT a.address, b.address,
       JARO_WINKLER(a.address, b.address) AS similarity
FROM addresses a, addresses b
WHERE a.id < b.id
  AND JARO_WINKLER(a.address, b.address) > 0.9;
```

### COSINE_SIMILARITY (Cosine Similarity)

Cosine similarity measures the directional consistency of two vectors, returning a floating-point number between -1.0 (completely opposite) and 1.0 (completely identical). A value of 0 indicates that the two vectors are orthogonal (unrelated). Vectors are represented as strings.

```sql
-- Basic usage
SELECT COSINE_SIMILARITY('[1,0,0]', '[0,1,0]');  -- 0.0 (orthogonal)
SELECT COSINE_SIMILARITY('[1,1]', '[1,1]');       -- 1.0 (identical)
SELECT COSINE_SIMILARITY('[1,0]', '[-1,0]');      -- -1.0 (opposite)

-- Document similarity query
SELECT id, title,
       COSINE_SIMILARITY(embedding, '[0.12, 0.45, 0.78, ...]') AS similarity
FROM documents
WHERE COSINE_SIMILARITY(embedding, '[0.12, 0.45, 0.78, ...]') > 0.8
ORDER BY similarity DESC
LIMIT 10;
```

## Use Cases

| Scenario | Recommended Function | Description |
|----------|---------------------|-------------|
| Spell correction | `LEVENSHTEIN` | Find candidates with the smallest edit distance |
| Name matching | `JARO_WINKLER` | More sensitive to names sharing a common prefix |
| Short string deduplication | `JARO_SIMILARITY` | Suitable for similarity comparison of short text |
| Semantic similarity | `COSINE_SIMILARITY` | Used in conjunction with text vectorization |
| Data cleaning | `LEVENSHTEIN` / `JARO_WINKLER` | Identify and merge similar records |

## Performance Tips

- Similarity calculations are typically expensive. Use other conditions in the `WHERE` clause to narrow the scope before calculating similarity.
- For fuzzy matching on large datasets, consider combining with indexes or pre-computation strategies.
- `COSINE_SIMILARITY` is suitable for vectors with moderate dimensions. For high-dimensional vectors, use the dedicated [Vector Functions](vector.md) with vector indexes.
