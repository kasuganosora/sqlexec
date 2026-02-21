# Spatial Index

SQLExec supports R-Tree spatial indexing for efficient geospatial queries. The R-Tree index accelerates spatial operations like bounding box intersection, containment, and range searches — essential for location-based queries on large datasets.

> **Note**: Spatial indexes are currently supported only on the Memory data source.

## How R-Tree Works

An R-Tree organizes spatial objects by their bounding boxes in a balanced tree structure. Each internal node stores a bounding box that encloses all of its children. When querying, the tree quickly prunes branches that cannot contain matching results, dramatically reducing the number of comparisons needed.

SQLExec's R-Tree implementation features:
- **Quadratic split** algorithm for balanced node distribution
- **Configurable node capacity** (2–8 entries per node)
- **Thread-safe** with read-write locking
- **Automatic MBR computation** from any geometry type

## Creating a Spatial Index

```sql
CREATE SPATIAL INDEX idx_location ON cities(location);
```

The spatial index automatically extracts the Minimum Bounding Rectangle (MBR) from each geometry value in the column and organizes them in the R-Tree.

## Supported Operations

The R-Tree spatial index accelerates these types of queries:

| Operation | Description | Use Case |
|-----------|-------------|----------|
| Intersect | Find geometries whose bounding boxes overlap a query region | "Which roads pass through this area?" |
| Contains | Find geometries fully within a query region | "Which cities are inside this country?" |
| Exact Match | Find geometries with identical bounding boxes | Point lookup by coordinates |
| Range Search | Find geometries within a coordinate range | "All POIs between lat 35-36, lon 139-140" |

## Query Examples

### Bounding Box Intersection

Find all features whose bounding boxes intersect a query region:

```sql
-- Find cities in the Tokyo metropolitan area
SELECT name, ST_AsText(location)
FROM cities
WHERE ST_Intersects(
    location,
    ST_MakeEnvelope(139.5, 35.5, 140.0, 36.0)
);
```

### Containment Query

Find all features fully contained within a region:

```sql
-- Find all POIs within a park boundary
SELECT poi.name
FROM poi, parks
WHERE parks.name = 'Central Park'
  AND ST_Contains(parks.boundary, poi.location);
```

### Distance-Based Search with Index Assist

While the R-Tree index doesn't directly accelerate distance calculations, it can dramatically reduce the candidate set:

```sql
-- Find nearby restaurants (bounding box pre-filter + exact distance)
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

## Complete Example

```sql
-- 1. Create a table with a geometry column
CREATE TABLE landmarks (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    location GEOMETRY
);

-- 2. Create a spatial index
CREATE SPATIAL INDEX idx_landmark_loc ON landmarks(location);

-- 3. Insert data
INSERT INTO landmarks VALUES (1, 'Tokyo Tower', 'tower', ST_Point(139.7454, 35.6586));
INSERT INTO landmarks VALUES (2, 'Sky Tree', 'tower', ST_Point(139.8107, 35.7101));
INSERT INTO landmarks VALUES (3, 'Senso-ji', 'temple', ST_Point(139.7966, 35.7148));
INSERT INTO landmarks VALUES (4, 'Meiji Shrine', 'shrine', ST_Point(139.6993, 35.6764));
INSERT INTO landmarks VALUES (5, 'Imperial Palace', 'palace', ST_Point(139.7528, 35.6852));

-- 4. Spatial queries (accelerated by R-Tree index)
-- Find landmarks in Asakusa area
SELECT name, category
FROM landmarks
WHERE ST_Contains(
    ST_MakeEnvelope(139.79, 35.71, 139.82, 35.72),
    location
);

-- Find all landmarks within a radius
SELECT name, ST_Distance(location, ST_Point(139.75, 35.68)) AS dist
FROM landmarks
ORDER BY dist
LIMIT 3;
```

## Index and Geometry Types

The spatial index works with all geometry types. The bounding box (MBR) is automatically computed:

| Geometry Type | MBR Behavior |
|---------------|-------------|
| Point | MBR is a zero-area box at the point's coordinates |
| LineString | MBR encloses all vertices |
| Polygon | MBR encloses the exterior ring |
| MultiPoint | MBR encloses all points |
| MultiLineString | MBR encloses all line vertices |
| MultiPolygon | MBR encloses all polygon vertices |
| GeometryCollection | MBR encloses all component geometries |

## Integration with Index Management

The spatial index type (`spatial_rtree`) is fully integrated with SQLExec's index management system:

```sql
-- View all indexes on a table (includes spatial indexes)
SHOW INDEX FROM landmarks;

-- Drop a spatial index
DROP INDEX idx_landmark_loc ON landmarks;
```

Spatial indexes are also:
- Automatically rebuilt during data recovery
- Persisted through XML persistence (geometries stored as WKT)
- Thread-safe for concurrent read/write access

## Performance Tips

- Create spatial indexes on columns that are frequently used in `ST_Contains`, `ST_Within`, `ST_Intersects`, or spatial `WHERE` clauses.
- For distance-based queries, use a bounding box pre-filter with `ST_MakeEnvelope` to leverage the index, then apply `ST_Distance` for exact results.
- The R-Tree index is most effective when geometries are spatially distributed. Highly clustered data may result in more overlapping bounding boxes and slower queries.
- Spatial indexes support single columns only — composite spatial indexes are not supported.
