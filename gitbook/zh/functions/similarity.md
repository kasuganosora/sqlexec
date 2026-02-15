# 相似度函数

SQLExec 提供了多种相似度计算函数，用于字符串模糊匹配和向量相似度计算，适用于数据清洗、去重、模糊搜索等场景。

## 函数列表

| 函数 | 说明 | 返回值范围 | 示例 |
|------|------|------------|------|
| `LEVENSHTEIN(s1, s2)` | 计算两个字符串的编辑距离 | 0 ~ max(len(s1), len(s2)) | `SELECT LEVENSHTEIN('kitten', 'sitting');` -- `3` |
| `JARO_SIMILARITY(s1, s2)` | 计算 Jaro 相似度 | 0.0 ~ 1.0 | `SELECT JARO_SIMILARITY('martha', 'marhta');` -- `0.944` |
| `JARO_WINKLER(s1, s2)` | 计算 Jaro-Winkler 相似度 | 0.0 ~ 1.0 | `SELECT JARO_WINKLER('martha', 'marhta');` -- `0.961` |
| `COSINE_SIMILARITY(v1, v2)` | 计算两个向量的余弦相似度 | -1.0 ~ 1.0 | `SELECT COSINE_SIMILARITY('[1,0]', '[0,1]');` -- `0.0` |

## 详细说明

### LEVENSHTEIN（编辑距离）

Levenshtein 距离是将一个字符串转换为另一个字符串所需的最少单字符编辑操作次数（插入、删除、替换）。返回值为非负整数，值越小表示两个字符串越相似。

```sql
-- 基本用法
SELECT LEVENSHTEIN('hello', 'hallo');  -- 1（替换 e->a）
SELECT LEVENSHTEIN('abc', 'abcd');     -- 1（插入 d）
SELECT LEVENSHTEIN('cat', 'dog');      -- 3（全部替换）

-- 查找相似名称（编辑距离 <= 2）
SELECT name FROM customers
WHERE LEVENSHTEIN(name, '张三丰') <= 2;

-- 数据清洗：找出可能的重复记录
SELECT a.id, a.name, b.id, b.name,
       LEVENSHTEIN(a.name, b.name) AS distance
FROM products a, products b
WHERE a.id < b.id
  AND LEVENSHTEIN(a.name, b.name) <= 3
ORDER BY distance;
```

### JARO_SIMILARITY（Jaro 相似度）

Jaro 相似度衡量两个字符串之间的匹配程度，返回 0.0（完全不同）到 1.0（完全相同）之间的浮点数。特别适合短字符串的比较。

```sql
-- 基本用法
SELECT JARO_SIMILARITY('martha', 'marhta');  -- 0.944
SELECT JARO_SIMILARITY('hello', 'world');    -- 0.467

-- 模糊匹配查询
SELECT * FROM contacts
WHERE JARO_SIMILARITY(name, '李明') > 0.8;
```

### JARO_WINKLER（Jaro-Winkler 相似度）

Jaro-Winkler 是 Jaro 的改进版本，对前缀相同的字符串给予更高的评分。返回 0.0 到 1.0 之间的浮点数，特别适合人名匹配。

```sql
-- 基本用法
SELECT JARO_WINKLER('martha', 'marhta');    -- 0.961
SELECT JARO_WINKLER('dwayne', 'duane');     -- 0.840

-- 人名模糊匹配
SELECT name, JARO_WINKLER(name, '王晓明') AS similarity
FROM users
WHERE JARO_WINKLER(name, '王晓明') > 0.85
ORDER BY similarity DESC;

-- 地址相似度匹配
SELECT a.address, b.address,
       JARO_WINKLER(a.address, b.address) AS similarity
FROM addresses a, addresses b
WHERE a.id < b.id
  AND JARO_WINKLER(a.address, b.address) > 0.9;
```

### COSINE_SIMILARITY（余弦相似度）

余弦相似度衡量两个向量在方向上的一致性，返回 -1.0（完全相反）到 1.0（完全一致）之间的浮点数。值为 0 表示两个向量正交（无关）。向量以字符串形式表示。

```sql
-- 基本用法
SELECT COSINE_SIMILARITY('[1,0,0]', '[0,1,0]');  -- 0.0（正交）
SELECT COSINE_SIMILARITY('[1,1]', '[1,1]');       -- 1.0（完全一致）
SELECT COSINE_SIMILARITY('[1,0]', '[-1,0]');      -- -1.0（完全相反）

-- 文档相似度查询
SELECT id, title,
       COSINE_SIMILARITY(embedding, '[0.12, 0.45, 0.78, ...]') AS similarity
FROM documents
WHERE COSINE_SIMILARITY(embedding, '[0.12, 0.45, 0.78, ...]') > 0.8
ORDER BY similarity DESC
LIMIT 10;
```

## 使用场景

| 场景 | 推荐函数 | 说明 |
|------|----------|------|
| 拼写纠错 | `LEVENSHTEIN` | 找出编辑距离最小的候选词 |
| 人名匹配 | `JARO_WINKLER` | 对前缀相同的名称更敏感 |
| 短字符串去重 | `JARO_SIMILARITY` | 适合短文本的相似度比较 |
| 语义相似度 | `COSINE_SIMILARITY` | 配合文本向量化使用 |
| 数据清洗 | `LEVENSHTEIN` / `JARO_WINKLER` | 识别并合并相似记录 |

## 性能建议

- 相似度计算通常开销较大，应尽量在 `WHERE` 子句中先用其他条件缩小范围后再计算相似度。
- 对于大规模数据集的模糊匹配，建议结合索引或预计算策略。
- `COSINE_SIMILARITY` 适用于向量维度适中的场景；高维向量建议使用专用的[向量函数](vector.md)配合向量索引。
