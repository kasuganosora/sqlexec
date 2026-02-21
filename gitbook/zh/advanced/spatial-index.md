# 空间索引

SQLExec 支持 R-Tree 空间索引，用于高效的地理空间查询。R-Tree 索引加速边界框相交、包含和范围搜索等空间操作——这对于大数据集上的位置查询至关重要。

> **注意**：空间索引目前仅支持 Memory 数据源。

## R-Tree 工作原理

R-Tree 通过几何对象的边界框（MBR）将其组织在平衡的树结构中。每个内部节点存储一个包围其所有子节点的边界框。查询时，树可以快速裁剪不可能包含匹配结果的分支，从而大幅减少所需的比较次数。

SQLExec 的 R-Tree 实现特点：
- **二次分裂**算法实现均衡的节点分布
- **可配置的节点容量**（每个节点 2–8 个条目）
- **线程安全**，使用读写锁
- **自动 MBR 计算**，支持所有几何体类型

## 创建空间索引

```sql
CREATE SPATIAL INDEX idx_location ON cities(location);
```

空间索引会自动从列中每个几何值提取最小外接矩形（MBR），并将它们组织在 R-Tree 中。

## 支持的操作

R-Tree 空间索引加速以下类型的查询：

| 操作 | 说明 | 使用场景 |
|------|------|----------|
| 相交查询 | 查找边界框与查询区域重叠的几何体 | "哪些道路经过这个区域？" |
| 包含查询 | 查找完全在查询区域内的几何体 | "哪些城市在这个国家内？" |
| 精确匹配 | 查找边界框完全相同的几何体 | 按坐标查找点 |
| 范围搜索 | 查找在坐标范围内的几何体 | "纬度 35-36、经度 139-140 之间的所有兴趣点" |

## 查询示例

### 边界框相交查询

查找边界框与查询区域相交的所有要素：

```sql
-- 查找东京都市圈内的城市
SELECT name, ST_AsText(location)
FROM cities
WHERE ST_Intersects(
    location,
    ST_MakeEnvelope(139.5, 35.5, 140.0, 36.0)
);
```

### 包含查询

查找完全在某个区域内的所有要素：

```sql
-- 查找公园边界内的所有兴趣点
SELECT poi.name
FROM poi, parks
WHERE parks.name = '中央公园'
  AND ST_Contains(parks.boundary, poi.location);
```

### 基于距离的搜索（索引辅助）

虽然 R-Tree 索引不直接加速距离计算，但它可以大幅减少候选集：

```sql
-- 查找附近的餐厅（边界框预过滤 + 精确距离）
SELECT name,
       ST_Distance(location, ST_Point(139.7, 35.7)) AS distance
FROM restaurants
WHERE ST_Intersects(
    location,
    ST_MakeEnvelope(139.69, 35.69, 139.71, 35.71)
)
ORDER BY distance
LIMIT 10;
```

## 完整示例

```sql
-- 1. 创建包含几何列的表
CREATE TABLE landmarks (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    location GEOMETRY
);

-- 2. 创建空间索引
CREATE SPATIAL INDEX idx_landmark_loc ON landmarks(location);

-- 3. 插入数据
INSERT INTO landmarks VALUES (1, '东京塔', '塔', ST_Point(139.7454, 35.6586));
INSERT INTO landmarks VALUES (2, '晴空塔', '塔', ST_Point(139.8107, 35.7101));
INSERT INTO landmarks VALUES (3, '浅草寺', '寺庙', ST_Point(139.7966, 35.7148));
INSERT INTO landmarks VALUES (4, '明治神宫', '神社', ST_Point(139.6993, 35.6764));
INSERT INTO landmarks VALUES (5, '皇居', '宫殿', ST_Point(139.7528, 35.6852));

-- 4. 空间查询（R-Tree 索引加速）
-- 查找浅草地区的地标
SELECT name, category
FROM landmarks
WHERE ST_Contains(
    ST_MakeEnvelope(139.79, 35.71, 139.82, 35.72),
    location
);

-- 查找距离最近的地标
SELECT name, ST_Distance(location, ST_Point(139.75, 35.68)) AS dist
FROM landmarks
ORDER BY dist
LIMIT 3;
```

## 索引与几何体类型

空间索引适用于所有几何体类型。边界框（MBR）会自动计算：

| 几何体类型 | MBR 行为 |
|-----------|----------|
| Point | MBR 为该点坐标处的零面积框 |
| LineString | MBR 包围所有顶点 |
| Polygon | MBR 包围外环 |
| MultiPoint | MBR 包围所有点 |
| MultiLineString | MBR 包围所有线的顶点 |
| MultiPolygon | MBR 包围所有多边形的顶点 |
| GeometryCollection | MBR 包围所有组件几何体 |

## 与索引管理的集成

空间索引类型（`spatial_rtree`）与 SQLExec 的索引管理系统完全集成：

```sql
-- 查看表的所有索引（包括空间索引）
SHOW INDEX FROM landmarks;

-- 删除空间索引
DROP INDEX idx_landmark_loc ON landmarks;
```

空间索引还支持：
- 数据恢复时自动重建
- 通过 XML 持久化存储（几何体以 WKT 格式存储）
- 并发读写的线程安全访问

## 性能建议

- 在经常用于 `ST_Contains`、`ST_Within`、`ST_Intersects` 或空间 `WHERE` 子句的列上创建空间索引。
- 对于基于距离的查询，先使用 `ST_MakeEnvelope` 的边界框预过滤来利用索引，然后使用 `ST_Distance` 计算精确结果。
- R-Tree 索引在几何体空间分布均匀时最有效。高度聚集的数据可能导致更多重叠的边界框，查询速度较慢。
- 空间索引仅支持单列——不支持复合空间索引。
