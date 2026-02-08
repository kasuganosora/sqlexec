# TiDB vs 当前实现向量索引语法对比

## TiDB 语法

### 标准语法
```sql
-- 1. 建表时创建索引
CREATE TABLE foo (
    id       INT PRIMARY KEY,
    embedding     VECTOR(5),
    VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding)))
);

-- 2. 建表后创建索引
CREATE VECTOR INDEX idx_embedding ON foo ((VEC_COSINE_DISTANCE(embedding)));

-- 3. ALTER TABLE 创建索引
ALTER TABLE foo ADD VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding)));

-- 4. 指定 USING HNSW
CREATE VECTOR INDEX idx_embedding ON foo ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;
ALTER TABLE foo ADD VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;
```

### 特点
- ✅ 使用函数表达式 `(VEC_COSINE_DISTANCE(embedding))`
- ✅ 距离度量通过函数名隐含（`VEC_COSINE_DISTANCE`）
- ✅ 使用标准的 `USING HNSW` 子句
- ✅ 支持 ALTER TABLE 语法

### 支持的距离函数
- `VEC_COSINE_DISTANCE` - 余弦距离
- `VEC_L2_DISTANCE` - L2 距离
- `VEC_INNER_PRODUCT` - 内积

---

## 当前实现语法

### 当前语法
```sql
-- 使用 COMMENT 参数传递配置
CREATE VECTOR INDEX idx_emb ON articles(embedding) 
USING HNSW 
COMMENT '{"metric":"cosine","dim":768,"M":8,"ef":96}';

-- 简化语法（默认参数）
CREATE VECTOR INDEX idx_vec ON products(features) 
USING ivf_flat;

-- 支持多种索引类型
CREATE VECTOR INDEX idx_pq ON docs(embedding) 
USING ivf_pq 
COMMENT '{"metric":"cosine","dim":512,"nlist":128,"nprobe":32,"M":16}';
```

### 特点
- ✅ 使用 `COMMENT` 传递 JSON 参数
- ✅ 支持更多索引类型（10种 vs TiDB 的 1种 HNSW）
- ✅ 灵活的参数配置
- ✅ 简洁的语法

---

## 详细对比

| 特性 | TiDB 语法 | 当前实现 | 优势 |
|-----|-----------|---------|------|
| **距离度量指定** | `VEC_COSINE_DISTANCE(embedding)` | `COMMENT '{"metric":"cosine"}'` | TiDB 更直观 |
| **维度指定** | 必须在表结构中 `VECTOR(5)` | `COMMENT '{"dim":768}'` | TiDB 更清晰 |
| **索引类型** | `USING HNSW` | `USING hnsw_sq, ivf_pq` 等 | 当前实现更丰富 |
| **参数配置** | 隐式（固定参数） | 显式 JSON 参数 | 当前实现更灵活 |
| **ALTER TABLE** | ✅ 支持 | ❌ 不支持 | TiDB 更完整 |
| **建表时索引** | ✅ 支持 | ❌ 不支持 | TiDB 更完整 |
| **标准 SQL 兼容性** | ⭐⭐⭐⭐ | ⭐⭐⭐ | TiDB 更标准 |

---

## 语法差异分析

### 1. 距离度量指定方式

**TiDB 方式：**
```sql
-- 通过函数名隐式指定
CREATE VECTOR INDEX idx ON foo ((VEC_COSINE_DISTANCE(embedding)));
-- VEC_L2_DISTANCE → L2 距离
-- VEC_INNER_PRODUCT → 内积
```

**当前实现：**
```sql
-- 通过 COMMENT 参数显式指定
CREATE VECTOR INDEX idx ON foo (embedding) 
USING HNSW 
COMMENT '{"metric":"cosine"}';
```

**优劣势：**
- TiDB：更符合 SQL 标准，通过函数调用表达意图
- 当前实现：更灵活，可以动态调整参数

### 2. 参数传递方式

**TiDB 方式：**
```sql
-- 参数隐式，通过 USING 指定索引类型
CREATE VECTOR INDEX idx ON foo ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;
```

**当前实现：**
```sql
-- 参数显式，通过 JSON 对象配置
CREATE VECTOR INDEX idx ON foo (embedding) 
USING hnsw_pq 
COMMENT '{
  "metric": "cosine",
  "dim": 512,
  "nlist": 128,
  "nprobe": 32,
  "M": 16,
  "nbits": 8
}';
```

**优劣势：**
- TiDB：简洁，但缺乏灵活性
- 当前实现：详细，支持高级调优

### 3. 索引类型支持

**TiDB：**
- 仅支持 HNSW 一种索引类型

**当前实现：**
- 支持 10 种索引类型
  - FLAT, HNSW
  - IVF_FLAT, IVF_SQ8, IVF_PQ
  - HNSW_SQ, HNSW_PQ
  - IVF_RABITQ, HNSW_PRQ, AISAQ

---

## 建议的改进方案

### 方案 1：兼容 TiDB 语法（推荐）

在保留当前功能的基础上，增加对 TiDB 语法的支持：

```go
// adapter.go
func (a *SQLAdapter) convertCreateIndexStmt(stmt *ast.CreateIndexStmt) (*CreateIndexStatement, error) {
    createIndexStmt := &CreateIndexStatement{
        IndexName: stmt.IndexName,
        TableName: stmt.Table.Name.String(),
        IndexType: strings.ToUpper(stmt.IndexOption.IndexType),
        Unique:    stmt.KeyType == IndexKeyTypeUnique,
    }
    
    // 检查是否为向量索引
    if stmt.KeyType == IndexKeyTypeVector {
        createIndexStmt.IsVectorIndex = true
        
        // 解析 IndexPartSpecification 中的函数
        if len(stmt.IndexPartSpecifications) > 0 {
            spec := stmt.IndexPartSpecifications[0]
            if spec.Expr != nil {
                // 检查是否为向量距离函数
                if funcName := extractFunctionName(spec.Expr); funcName != "" {
                    createIndexStmt.VectorMetric = convertFuncNameToMetric(funcName)
                    createIndexStmt.ColumnName = extractColumnName(spec.Expr)
                }
            } else if spec.Column != nil {
                // 简单列名，使用默认度量
                createIndexStmt.ColumnName = spec.Column.Name.String()
                createIndexStmt.VectorMetric = "cosine" // 默认
            }
        }
        
        // 解析 USING 子句
        if stmt.IndexOption != nil && stmt.IndexOption.ParserName.O != "" {
            createIndexStmt.VectorIndexType = strings.ToLower(stmt.IndexOption.ParserName.O)
        }
        
        // 解析维度（从表结构或 COMMENT）
        createIndexStmt.VectorDim = extractVectorDim(stmt)
    }
    
    return createIndexStmt, nil
}

// 距离函数名到度量类型的映射
func convertFuncNameToMetric(funcName string) string {
    switch strings.ToUpper(funcName) {
    case "VEC_COSINE_DISTANCE":
        return "cosine"
    case "VEC_L2_DISTANCE":
        return "l2"
    case "VEC_INNER_PRODUCT":
        return "inner_product"
    default:
        return "cosine"
    }
}
```

**优点：**
- ✅ 完全兼容 TiDB 语法
- ✅ 保留现有 COMMENT JSON 参数功能
- ✅ 更符合 SQL 标准
- ✅ 支持 ALTER TABLE

**缺点：**
- ⚠️ 需要修改 AST 解析器
- ⚠️ 需要支持表达式解析

### 方案 2：增强当前语法

保持当前语法，增强功能：

```sql
-- 增加标准 USING 子句支持
CREATE VECTOR INDEX idx_emb ON articles(embedding) 
USING HNSW WITH (metric='cosine', dim=768, M=8, ef=96);

-- 支持 ALTER TABLE
ALTER TABLE articles ADD VECTOR INDEX idx_emb (embedding) 
USING ivf_pq WITH (metric='l2', dim=128, nlist=128, nprobe=32);

-- 建表时创建索引
CREATE TABLE articles (
    id INT PRIMARY KEY,
    embedding VECTOR(768),
    VECTOR INDEX idx_emb USING HNSW WITH (metric='cosine', dim=768)
);
```

**优点：**
- ✅ 语法更标准（WITH 子句）
- ✅ 更直观的参数传递
- ✅ 不需要函数表达式解析

**缺点：**
- ⚠️ 仍然与 TiDB 不兼容
- ⚠️ 需要修改解析器

### 方案 3：混合方案（最佳）

同时支持两种语法：

```sql
-- TiDB 兼容语法
CREATE VECTOR INDEX idx ON foo ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;

-- 当前语法（保留）
CREATE VECTOR INDEX idx ON foo (embedding) 
USING HNSW 
COMMENT '{"metric":"cosine","dim":768}';

-- 增强语法（新增）
CREATE VECTOR INDEX idx ON foo (embedding) 
USING HNSW WITH (metric='cosine', dim=768, M=8, ef=96);
```

**实现优先级：**
1. 短期：增强当前语法（方案2）
   - 添加 WITH 子句支持
   - 支持 ALTER TABLE
   - 支持建表时索引

2. 中期：兼容 TiDB 语法（方案1）
   - 解析 `VEC_*_DISTANCE` 函数
   - 支持 `(expression)` 语法
   - 维持向后兼容

3. 长期：统一语法（方案3）
   - 提供两种语法支持
   - 文档说明各自的优劣势
   - 提供迁移工具

---

## 实现建议

### 当前可以做的改进

#### 1. 添加 WITH 子句支持

修改 `adapter.go` 解析 WITH 参数：

```go
// 在 convertCreateIndexStmt 中添加
if stmt.IndexOption != nil && stmt.IndexOption.Comment != "" {
    // 优先解析 WITH 子句
    if strings.HasPrefix(stmt.IndexOption.Comment, "WITH (") {
        params := parseWithClause(stmt.IndexOption.Comment)
        createIndexStmt.VectorParams = params
    } else {
        // 兼容 JSON 格式
        if json.Unmarshal([]byte(stmt.IndexOption.Comment), &params) == nil {
            createIndexStmt.VectorParams = params
        }
    }
}
```

#### 2. 支持 ALTER TABLE

修改 `adapter.go` 处理 ALTER TABLE ADD VECTOR INDEX：

```go
func (a *SQLAdapter) convertAlterTableStmt(stmt *ast.AlterTableStmt) (*AlterTableStatement, error) {
    alterStmt := &AlterTableStatement{
        TableName: stmt.Table.Name.String(),
    }
    
    for _, spec := range stmt.Specs {
        if spec.Tp == ast.AlterTableAddIndex {
            // 检查是否为向量索引
            if spec.IndexKey.KeyType == IndexKeyTypeVector {
                // 转换为向量索引创建
                alterStmt.VectorIndex = convertVectorIndex(spec.IndexKey)
            }
        }
    }
    
    return alterStmt, nil
}
```

---

## 总结

### 当前优势
1. ✅ 支持更多索引类型（10种 vs 1种）
2. ✅ 灵活的参数配置
3. ✅ 已完整实现并测试

### 需要改进
1. ❌ 语法与 TiDB 不兼容
2. ❌ 不支持 ALTER TABLE
3. ❌ 不支持建表时创建索引
4. ❌ 参数传递方式不够标准

### 推荐行动
1. **短期**：添加 WITH 子句支持，改善语法
2. **中期**：兼容 TiDB 语法，提高兼容性
3. **长期**：统一语法，提供最佳体验

## 参考资源
- [TiDB 向量能力上手指南](https://blog.csdn.net/TiDBer/article/details/156100627)
- [TiDB Vector 本地部署体验](https://blog.csdn.net/Java_fenxiang/article/details/145583296)
- [TiDB 源码分析](https://github.com/pingcap/tidb)
