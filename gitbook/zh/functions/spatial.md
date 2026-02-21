# 空间函数

SQLExec 提供了一整套地理空间函数，遵循 SQL/MM 和 OGC 简单要素标准。这些函数支持几何体构造、测量、空间关系判断和空间分析——全部使用纯 Go 实现，无外部依赖。

## 几何体格式

几何体使用 WKT（Well-Known Text）格式表示：

```
'POINT(139.6917 35.6895)'
'LINESTRING(0 0, 10 10, 20 25)'
'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'
```

支持的几何体类型：Point（点）、LineString（线）、Polygon（多边形）、MultiPoint（多点）、MultiLineString（多线）、MultiPolygon（多多边形）和 GeometryCollection（几何体集合）。

## 构造函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_Point(x, y)` | 根据 X/Y 坐标创建点 | Geometry | `SELECT ST_Point(139.69, 35.69);` |
| `ST_GeomFromText(wkt [,srid])` | 解析 WKT 字符串为几何体 | Geometry | `SELECT ST_GeomFromText('POINT(1 2)');` |
| `ST_MakeEnvelope(xmin, ymin, xmax, ymax)` | 根据边界创建矩形多边形 | Geometry | `SELECT ST_MakeEnvelope(0, 0, 10, 10);` |

## 输出函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_AsText(geom)` | 将几何体转换为 WKT 字符串 | string | `SELECT ST_AsText(location) FROM cities;` |
| `ST_AsWKT(geom)` | ST_AsText 的别名 | string | `SELECT ST_AsWKT(geom) FROM places;` |

## 访问函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_X(point)` | 获取点的 X 坐标 | float | `SELECT ST_X(ST_Point(3, 4));` -- `3` |
| `ST_Y(point)` | 获取点的 Y 坐标 | float | `SELECT ST_Y(ST_Point(3, 4));` -- `4` |
| `ST_SRID(geom)` | 获取空间参考 ID | int | `SELECT ST_SRID(geom) FROM cities;` |
| `ST_GeometryType(geom)` | 获取几何体类型名称 | string | `SELECT ST_GeometryType(ST_Point(1, 2));` -- `Point` |
| `ST_Dimension(geom)` | 获取拓扑维度 | int | `SELECT ST_Dimension(ST_Point(1, 2));` -- `0` |
| `ST_IsEmpty(geom)` | 检查几何体是否为空 | bool | `SELECT ST_IsEmpty(ST_GeomFromText('POINT EMPTY'));` |
| `ST_IsValid(geom)` | 验证几何体结构 | bool | `SELECT ST_IsValid(geom) FROM parcels;` |
| `ST_NumPoints(geom)` | 统计点的总数 | int | `SELECT ST_NumPoints(geom) FROM roads;` |

## 测量函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_Distance(g1, g2)` | 两个几何体之间的欧氏距离 | float | `SELECT ST_Distance(a.geom, b.geom) FROM ...;` |
| `ST_Area(geom)` | 多边形面积（鞋带公式） | float | `SELECT ST_Area(geom) FROM parcels;` |
| `ST_Length(geom)` | 线串长度 | float | `SELECT ST_Length(geom) FROM roads;` |
| `ST_Perimeter(geom)` | 多边形周长 | float | `SELECT ST_Perimeter(geom) FROM parcels;` |

## 空间关系函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_Contains(g1, g2)` | g1 是否完全包含 g2？ | bool | `SELECT ST_Contains(region, location) FROM ...;` |
| `ST_Within(g1, g2)` | g1 是否完全在 g2 内？ | bool | `SELECT * FROM cities WHERE ST_Within(location, region);` |
| `ST_Intersects(g1, g2)` | 两个几何体是否相交？ | bool | `SELECT ST_Intersects(road, boundary) FROM ...;` |
| `ST_Disjoint(g1, g2)` | 两个几何体是否不相交？ | bool | `SELECT ST_Disjoint(a.geom, b.geom) FROM ...;` |
| `ST_Equals(g1, g2)` | 两个几何体是否空间相等？ | bool | `SELECT ST_Equals(geom1, geom2);` |
| `ST_Touches(g1, g2)` | 边界是否相切（无重叠）？ | bool | `SELECT ST_Touches(parcel1, parcel2) FROM ...;` |
| `ST_Overlaps(g1, g2)` | 两个几何体是否部分重叠？ | bool | `SELECT ST_Overlaps(zone1, zone2) FROM ...;` |
| `ST_Crosses(g1, g2)` | 两个几何体是否交叉？ | bool | `SELECT ST_Crosses(road, river) FROM ...;` |

## 空间分析函数

| 函数 | 说明 | 返回类型 | 示例 |
|------|------|----------|------|
| `ST_Envelope(geom)` | 最小外接矩形（多边形） | Geometry | `SELECT ST_AsText(ST_Envelope(geom)) FROM roads;` |
| `ST_Centroid(geom)` | 几何体的质心 | Geometry | `SELECT ST_AsText(ST_Centroid(geom)) FROM parcels;` |
| `ST_Buffer(geom, distance)` | 几何体的缓冲区 | Geometry | `SELECT ST_Buffer(location, 0.01) FROM poi;` |
| `ST_Union(g1, g2)` | 两个几何体的并集 | Geometry | `SELECT ST_Union(zone1, zone2);` |

## 函数详细说明

### ST_Point — 构造点

根据 X（经度）和 Y（纬度）坐标创建点几何体。

```sql
SELECT ST_AsText(ST_Point(139.6917, 35.6895));
-- POINT(139.6917 35.6895)

INSERT INTO cities (name, location) VALUES
    ('东京', ST_Point(139.6917, 35.6895)),
    ('纽约', ST_Point(-74.006, 40.7128));
```

### ST_GeomFromText — 解析 WKT

将 WKT（Well-Known Text）字符串解析为几何体对象。可选传入 SRID 参数。

```sql
SELECT ST_GeomFromText('POINT(1 2)');
SELECT ST_GeomFromText('LINESTRING(0 0, 10 10, 20 25)', 4326);
SELECT ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))');

-- 多多边形
SELECT ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))');
```

### ST_Distance — 距离计算

计算两个几何体之间的欧氏距离。对于点到点，即直线距离。对于其他组合，计算边之间的最小距离。

```sql
-- 两点之间的距离
SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4));
-- 5.0

-- 城市之间的距离
SELECT a.name, b.name,
       ST_Distance(a.location, b.location) AS distance
FROM cities a, cities b
WHERE a.name < b.name
ORDER BY distance;
```

### ST_Contains / ST_Within — 包含判断

`ST_Contains(g1, g2)` 当 g1 完全包含 g2 时返回 true。`ST_Within(g1, g2)` 是反向操作——当 g1 完全在 g2 内时返回 true。

使用射线投射算法进行点在多边形内的判断。

```sql
-- 查找区域内的所有城市
SELECT name FROM cities
WHERE ST_Contains(
    ST_MakeEnvelope(130, 30, 145, 40),
    location
);

-- 使用 ST_Within 的等价写法
SELECT name FROM cities
WHERE ST_Within(
    location,
    ST_MakeEnvelope(130, 30, 145, 40)
);
```

### ST_Area — 多边形面积

使用鞋带公式计算多边形面积。对于有孔的多边形，会减去孔的面积。

```sql
-- 10x10 正方形的面积
SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
-- 100.0

-- 带孔多边形的面积
SELECT ST_Area(ST_GeomFromText(
    'POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))'
));
-- 300.0（外环 400 - 孔 100）
```

### ST_Buffer — 缓冲区

在几何体周围创建指定距离的缓冲区多边形。

```sql
-- 在点周围创建 1 个单位的缓冲区
SELECT ST_AsText(ST_Buffer(ST_Point(5, 5), 1));

-- 查找指定位置 0.01 度范围内的所有兴趣点
SELECT name FROM poi
WHERE ST_Intersects(
    location,
    ST_Buffer(ST_Point(139.69, 35.69), 0.01)
);
```

### ST_Centroid — 质心

计算几何体的质心（几何中心）。

```sql
SELECT ST_AsText(ST_Centroid(
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
));
-- POINT(5 5)
```

## 维度参考

| 几何体类型 | 维度 | 说明 |
|-----------|------|------|
| Point / MultiPoint | 0 | 零维 |
| LineString / MultiLineString | 1 | 一维 |
| Polygon / MultiPolygon | 2 | 二维 |
| GeometryCollection | 可变 | 组件的最大维度 |

## 使用示例

### 存储和查询地理数据

```sql
-- 创建带有空间列的表
CREATE TABLE places (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    location GEOMETRY
);

-- 插入位置数据
INSERT INTO places VALUES (1, '东京塔', '地标', ST_Point(139.7454, 35.6586));
INSERT INTO places VALUES (2, '中央公园', '公园', ST_Point(-73.9654, 40.7829));
INSERT INTO places VALUES (3, '埃菲尔铁塔', '地标', ST_Point(2.2945, 48.8584));

-- 使用空间函数查询
SELECT name, ST_AsText(location),
       ST_Distance(location, ST_Point(0, 0)) AS dist_from_origin
FROM places
ORDER BY dist_from_origin;
```

### 空间过滤

```sql
-- 查找边界框内的所有地点（例如日本区域）
SELECT name FROM places
WHERE ST_Contains(
    ST_MakeEnvelope(129, 30, 146, 46),
    location
);

-- 查找参考点 50 个单位内的地点
SELECT name, ST_Distance(location, ST_Point(139.7, 35.7)) AS distance
FROM places
WHERE ST_Distance(location, ST_Point(139.7, 35.7)) < 50
ORDER BY distance;
```

### 空间分析

```sql
-- 创建多边形边界表
CREATE TABLE regions (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    boundary GEOMETRY
);

INSERT INTO regions VALUES (1, '区域 A',
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO regions VALUES (2, '区域 B',
    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

-- 检查区域是否重叠
SELECT a.name, b.name, ST_Overlaps(a.boundary, b.boundary) AS overlaps
FROM regions a, regions b
WHERE a.id < b.id;

-- 计算面积和质心
SELECT name,
       ST_Area(boundary) AS area,
       ST_AsText(ST_Centroid(boundary)) AS center
FROM regions;
```

### 道路网络分析

```sql
CREATE TABLE roads (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    path GEOMETRY
);

INSERT INTO roads VALUES (1, '主街',
    ST_GeomFromText('LINESTRING(0 0, 5 0, 10 5)'));
INSERT INTO roads VALUES (2, '河边路',
    ST_GeomFromText('LINESTRING(0 5, 5 5, 10 5)'));

-- 计算道路长度
SELECT name, ST_Length(path) AS length FROM roads;

-- 检查道路是否相交
SELECT a.name, b.name
FROM roads a, roads b
WHERE a.id < b.id AND ST_Intersects(a.path, b.path);
```

## XML 持久化

使用 XML 持久化数据源存储时，空间数据会自动序列化为 WKT 格式，加载时自动反序列化为几何体对象。无需特殊配置。

```sql
-- 数据以 WKT 格式持久化在 XML 文件中
-- 列类型应为 GEOMETRY、POINT、LINESTRING、POLYGON 等
CREATE TABLE spatial_data (
    id INT PRIMARY KEY,
    geom GEOMETRY
);
```

## 注意事项

- 所有距离和面积计算使用欧氏（笛卡尔）坐标。对于地理（经纬度）数据，结果单位为度——在赤道处乘以约 111,320 可转换为近似米数。
- SRID 会被存储但不影响计算。可通过 `ST_GeomFromText(wkt, 4326)` 设置元数据。
- `ST_Buffer` 使用 32 边多边形近似圆形缓冲区。
- `ST_Union` 和 `ST_Intersection` 返回简化结果（GeometryCollection），而非执行完整的计算几何。
- 多边形环应闭合（首尾点相同）以确保正确结果。
