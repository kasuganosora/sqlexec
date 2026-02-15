# 全文搜索

SQLExec 内置基于 BM25 算法的全文搜索引擎，通过倒排索引实现高效的文本检索，原生支持中文分词。

## 创建全文索引

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content);
```

指定分词器：

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content)
    WITH (tokenizer = 'jieba');
```

## 基本搜索

```sql
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库');
```

### 布尔模式

布尔模式支持更精确的搜索控制：

```sql
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 优化' IN BOOLEAN MODE);
```

## 分词器

SQLExec 提供 4 种分词器，适用于不同语言和场景：

| 分词器 | 说明 | 适用场景 |
|--------|------|---------|
| Standard | Unicode 标准分词，按空格和标点切分 | 通用文本，混合语言 |
| English | 英文分词，支持词干提取（stemming） | 英文文本 |
| Ngram | N-gram 切分，适用于 CJK 字符 | 中日韩文本，无需词典 |
| Jieba | 结巴中文分词，基于词典的精确切分 | 中文文本（推荐） |

指定分词器的方式：

```sql
-- 使用 Jieba 分词器（推荐用于中文）
CREATE FULLTEXT INDEX idx_cn ON articles(content)
    WITH (tokenizer = 'jieba');

-- 使用 Ngram 分词器
CREATE FULLTEXT INDEX idx_ngram ON articles(content)
    WITH (tokenizer = 'ngram');

-- 使用英文分词器
CREATE FULLTEXT INDEX idx_en ON articles(content)
    WITH (tokenizer = 'english');
```

## BM25 参数

BM25 算法有两个关键参数可调整：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `K1` | 1.2 | 词频饱和度参数，值越大词频的影响越大 |
| `B` | 0.75 | 文档长度归一化参数，值越大长文档的惩罚越重 |

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content)
    WITH (tokenizer = 'jieba', k1 = 1.5, b = 0.5);
```

## 查询类型

全文搜索支持丰富的查询语法：

| 查询类型 | 语法示例 | 说明 |
|---------|---------|------|
| 词条查询 | `'数据库'` | 匹配包含该词条的文档 |
| 短语查询 | `'"分布式数据库"'` | 精确匹配完整短语 |
| 布尔查询 | `'数据库 AND 优化'` | AND、OR 组合多个条件 |
| 模糊查询 | `'databse~1'` | 允许编辑距离为 1 的模糊匹配 |
| 通配符查询 | `'data*'` | 前缀匹配，匹配以 data 开头的词 |
| 范围查询 | `'[alpha TO omega]'` | 匹配字典序在范围内的词条 |

### 布尔查询示例

```sql
-- AND: 同时包含两个关键词
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 索引' IN BOOLEAN MODE);

-- OR: 包含任一关键词
SELECT * FROM articles
WHERE MATCH(content) AGAINST('MySQL OR PostgreSQL' IN BOOLEAN MODE);

-- 组合使用
SELECT * FROM articles
WHERE MATCH(content) AGAINST('(数据库 OR 存储) AND 优化' IN BOOLEAN MODE);
```

## 完整示例

```sql
-- 1. 创建文章表
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT
);

-- 2. 插入中文文档
INSERT INTO articles (id, title, content) VALUES
(1, '数据库索引原理', '数据库索引是提高查询性能的关键技术，B+树索引是最常用的索引结构。'),
(2, '全文搜索引擎', '全文搜索引擎使用倒排索引来快速定位包含特定词语的文档。'),
(3, '分布式系统设计', '分布式数据库需要处理数据分片、副本同步和一致性等核心问题。');

-- 3. 创建全文索引（使用 Jieba 分词器）
CREATE FULLTEXT INDEX idx_article_content ON articles(content)
    WITH (tokenizer = 'jieba');

-- 4. 搜索包含"数据库"的文章
SELECT id, title,
       MATCH(content) AGAINST('数据库') AS score
FROM articles
WHERE MATCH(content) AGAINST('数据库')
ORDER BY score DESC;

-- 5. 布尔模式搜索
SELECT id, title
FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 索引' IN BOOLEAN MODE);
```

## 中文搜索示例

Jieba 分词器能够准确切分中文文本：

```sql
-- 创建表并插入中文内容
CREATE TABLE news (
    id INT PRIMARY KEY,
    headline VARCHAR(500),
    body TEXT
);

INSERT INTO news (id, headline, body) VALUES
(1, '人工智能助力医疗诊断', '近年来，深度学习技术在医学影像分析领域取得了突破性进展，辅助医生进行更精准的疾病诊断。'),
(2, '新能源汽车市场持续增长', '电动汽车销量连续三年保持高速增长，充电基础设施建设也在加速推进。');

-- 创建 Jieba 全文索引
CREATE FULLTEXT INDEX idx_news_body ON news(body)
    WITH (tokenizer = 'jieba');

-- 搜索与深度学习相关的新闻
SELECT id, headline
FROM news
WHERE MATCH(body) AGAINST('深度学习');
```
