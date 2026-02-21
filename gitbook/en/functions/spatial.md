# Spatial Functions

SQLExec provides a comprehensive set of geospatial functions following the SQL/MM and OGC Simple Features standards. These functions support geometry construction, measurement, spatial relationships, and spatial analysis — all implemented in pure Go with no external dependencies.

## Geometry Format

Geometries are represented using WKT (Well-Known Text) format:

```
'POINT(139.6917 35.6895)'
'LINESTRING(0 0, 10 10, 20 25)'
'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'
```

Supported geometry types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.

## Constructor Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_Point(x, y)` | Create a Point from X/Y coordinates | Geometry | `SELECT ST_Point(139.69, 35.69);` |
| `ST_GeomFromText(wkt [,srid])` | Parse a WKT string into a Geometry | Geometry | `SELECT ST_GeomFromText('POINT(1 2)');` |
| `ST_MakeEnvelope(xmin, ymin, xmax, ymax)` | Create a rectangular Polygon from bounds | Geometry | `SELECT ST_MakeEnvelope(0, 0, 10, 10);` |

## Output Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_AsText(geom)` | Convert Geometry to WKT string | string | `SELECT ST_AsText(location) FROM cities;` |
| `ST_AsWKT(geom)` | Alias for ST_AsText | string | `SELECT ST_AsWKT(geom) FROM places;` |

## Accessor Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_X(point)` | Get the X coordinate of a Point | float | `SELECT ST_X(ST_Point(3, 4));` -- `3` |
| `ST_Y(point)` | Get the Y coordinate of a Point | float | `SELECT ST_Y(ST_Point(3, 4));` -- `4` |
| `ST_SRID(geom)` | Get the Spatial Reference ID | int | `SELECT ST_SRID(geom) FROM cities;` |
| `ST_GeometryType(geom)` | Get the geometry type name | string | `SELECT ST_GeometryType(ST_Point(1, 2));` -- `Point` |
| `ST_Dimension(geom)` | Get the topological dimension | int | `SELECT ST_Dimension(ST_Point(1, 2));` -- `0` |
| `ST_IsEmpty(geom)` | Check if the geometry is empty | bool | `SELECT ST_IsEmpty(ST_GeomFromText('POINT EMPTY'));` |
| `ST_IsValid(geom)` | Validate geometry structure | bool | `SELECT ST_IsValid(geom) FROM parcels;` |
| `ST_NumPoints(geom)` | Count the total number of points | int | `SELECT ST_NumPoints(geom) FROM roads;` |

## Measurement Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_Distance(g1, g2)` | Euclidean distance between two geometries | float | `SELECT ST_Distance(a.geom, b.geom) FROM ...;` |
| `ST_Area(geom)` | Area of a polygon (Shoelace formula) | float | `SELECT ST_Area(geom) FROM parcels;` |
| `ST_Length(geom)` | Length of a LineString | float | `SELECT ST_Length(geom) FROM roads;` |
| `ST_Perimeter(geom)` | Perimeter of a polygon | float | `SELECT ST_Perimeter(geom) FROM parcels;` |

## Spatial Relationship Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_Contains(g1, g2)` | Does g1 fully contain g2? | bool | `SELECT ST_Contains(region, location) FROM ...;` |
| `ST_Within(g1, g2)` | Is g1 fully within g2? | bool | `SELECT * FROM cities WHERE ST_Within(location, region);` |
| `ST_Intersects(g1, g2)` | Do the geometries intersect? | bool | `SELECT ST_Intersects(road, boundary) FROM ...;` |
| `ST_Disjoint(g1, g2)` | Are the geometries disjoint? | bool | `SELECT ST_Disjoint(a.geom, b.geom) FROM ...;` |
| `ST_Equals(g1, g2)` | Are the geometries spatially equal? | bool | `SELECT ST_Equals(geom1, geom2);` |
| `ST_Touches(g1, g2)` | Do boundaries touch without overlap? | bool | `SELECT ST_Touches(parcel1, parcel2) FROM ...;` |
| `ST_Overlaps(g1, g2)` | Do the geometries partially overlap? | bool | `SELECT ST_Overlaps(zone1, zone2) FROM ...;` |
| `ST_Crosses(g1, g2)` | Do the geometries cross each other? | bool | `SELECT ST_Crosses(road, river) FROM ...;` |

## Spatial Analysis Functions

| Function | Description | Return Type | Example |
|----------|-------------|-------------|---------|
| `ST_Envelope(geom)` | Minimum bounding rectangle as Polygon | Geometry | `SELECT ST_AsText(ST_Envelope(geom)) FROM roads;` |
| `ST_Centroid(geom)` | Centroid point of a geometry | Geometry | `SELECT ST_AsText(ST_Centroid(geom)) FROM parcels;` |
| `ST_Buffer(geom, distance)` | Buffer zone around a geometry | Geometry | `SELECT ST_Buffer(location, 0.01) FROM poi;` |
| `ST_Union(g1, g2)` | Union of two geometries | Geometry | `SELECT ST_Union(zone1, zone2);` |

## Detailed Function Descriptions

### ST_Point — Construct a Point

Creates a Point geometry from X (longitude) and Y (latitude) coordinates.

```sql
SELECT ST_AsText(ST_Point(139.6917, 35.6895));
-- POINT(139.6917 35.6895)

INSERT INTO cities (name, location) VALUES
    ('Tokyo', ST_Point(139.6917, 35.6895)),
    ('New York', ST_Point(-74.006, 40.7128));
```

### ST_GeomFromText — Parse WKT

Parses a WKT (Well-Known Text) string into a Geometry object. Optionally accepts a SRID parameter.

```sql
SELECT ST_GeomFromText('POINT(1 2)');
SELECT ST_GeomFromText('LINESTRING(0 0, 10 10, 20 25)', 4326);
SELECT ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))');

-- MultiPolygon
SELECT ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))');
```

### ST_Distance — Distance Calculation

Calculates the Euclidean distance between two geometries. For Point-to-Point, this is the straight-line distance. For other combinations, it computes the minimum distance between edges.

```sql
-- Distance between two points
SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4));
-- 5.0

-- Distance between cities
SELECT a.name, b.name,
       ST_Distance(a.location, b.location) AS distance
FROM cities a, cities b
WHERE a.name < b.name
ORDER BY distance;
```

### ST_Contains / ST_Within — Containment Tests

`ST_Contains(g1, g2)` returns true if g1 fully contains g2. `ST_Within(g1, g2)` is the inverse — true if g1 is fully within g2.

Uses the ray casting algorithm for point-in-polygon tests.

```sql
-- Find all cities within a region
SELECT name FROM cities
WHERE ST_Contains(
    ST_MakeEnvelope(130, 30, 145, 40),
    location
);

-- Equivalent using ST_Within
SELECT name FROM cities
WHERE ST_Within(
    location,
    ST_MakeEnvelope(130, 30, 145, 40)
);
```

### ST_Area — Polygon Area

Calculates the area of a polygon using the Shoelace formula. For polygons with holes, the hole areas are subtracted.

```sql
-- Area of a square 10x10
SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
-- 100.0

-- Area of a polygon with a hole
SELECT ST_Area(ST_GeomFromText(
    'POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))'
));
-- 300.0 (400 outer - 100 hole)
```

### ST_Buffer — Buffer Zone

Creates a buffer polygon around a geometry at a specified distance.

```sql
-- Create a 1-unit buffer around a point
SELECT ST_AsText(ST_Buffer(ST_Point(5, 5), 1));

-- Find all POIs within 0.01 degrees of a location
SELECT name FROM poi
WHERE ST_Intersects(
    location,
    ST_Buffer(ST_Point(139.69, 35.69), 0.01)
);
```

### ST_Centroid — Center Point

Calculates the centroid (geometric center) of a geometry.

```sql
SELECT ST_AsText(ST_Centroid(
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
));
-- POINT(5 5)
```

## Dimension Reference

| Geometry Type | Dimension | Description |
|---------------|-----------|-------------|
| Point / MultiPoint | 0 | Zero-dimensional |
| LineString / MultiLineString | 1 | One-dimensional |
| Polygon / MultiPolygon | 2 | Two-dimensional |
| GeometryCollection | varies | Max dimension of components |

## Usage Examples

### Store and Query Geographic Data

```sql
-- Create a table with spatial columns
CREATE TABLE places (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    location GEOMETRY
);

-- Insert locations
INSERT INTO places VALUES (1, 'Tokyo Tower', 'landmark', ST_Point(139.7454, 35.6586));
INSERT INTO places VALUES (2, 'Central Park', 'park', ST_Point(-73.9654, 40.7829));
INSERT INTO places VALUES (3, 'Eiffel Tower', 'landmark', ST_Point(2.2945, 48.8584));

-- Query with spatial functions
SELECT name, ST_AsText(location),
       ST_Distance(location, ST_Point(0, 0)) AS dist_from_origin
FROM places
ORDER BY dist_from_origin;
```

### Spatial Filtering

```sql
-- Find all places within a bounding box (e.g., Japan region)
SELECT name FROM places
WHERE ST_Contains(
    ST_MakeEnvelope(129, 30, 146, 46),
    location
);

-- Find places within 50 units of a reference point
SELECT name, ST_Distance(location, ST_Point(139.7, 35.7)) AS distance
FROM places
WHERE ST_Distance(location, ST_Point(139.7, 35.7)) < 50
ORDER BY distance;
```

### Spatial Analysis

```sql
-- Create a table with polygon boundaries
CREATE TABLE regions (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    boundary GEOMETRY
);

INSERT INTO regions VALUES (1, 'Zone A',
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO regions VALUES (2, 'Zone B',
    ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

-- Check if zones overlap
SELECT a.name, b.name, ST_Overlaps(a.boundary, b.boundary) AS overlaps
FROM regions a, regions b
WHERE a.id < b.id;

-- Calculate area and centroid
SELECT name,
       ST_Area(boundary) AS area,
       ST_AsText(ST_Centroid(boundary)) AS center
FROM regions;
```

### Road Network Analysis

```sql
CREATE TABLE roads (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    path GEOMETRY
);

INSERT INTO roads VALUES (1, 'Main Street',
    ST_GeomFromText('LINESTRING(0 0, 5 0, 10 5)'));
INSERT INTO roads VALUES (2, 'River Road',
    ST_GeomFromText('LINESTRING(0 5, 5 5, 10 5)'));

-- Calculate road lengths
SELECT name, ST_Length(path) AS length FROM roads;

-- Check if roads intersect
SELECT a.name, b.name
FROM roads a, roads b
WHERE a.id < b.id AND ST_Intersects(a.path, b.path);
```

## XML Persistence

Spatial data is automatically serialized to WKT format when stored using the XML persistence data source, and deserialized back to Geometry objects when loaded. No special configuration is required.

```sql
-- Data persisted as WKT in XML files
-- Column type should be GEOMETRY, POINT, LINESTRING, POLYGON, etc.
CREATE TABLE spatial_data (
    id INT PRIMARY KEY,
    geom GEOMETRY
);
```

## Notes

- All distance and area calculations use Euclidean (Cartesian) coordinates. For geographic (lat/lon) data, results are in degrees — multiply by ~111,320 for approximate meters at the equator.
- SRID is stored but does not affect calculations. Set it for metadata purposes using `ST_GeomFromText(wkt, 4326)`.
- `ST_Buffer` approximates circular buffers using a 32-sided polygon.
- `ST_Union` and `ST_Intersection` return simplified results (GeometryCollection) rather than performing full computational geometry.
- Polygon rings should be closed (first point equals last point) for correct results.
