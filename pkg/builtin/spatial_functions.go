package builtin

import (
	"fmt"
	"math"
)

// CategorySpatial is the function category for spatial/geospatial functions.
const CategorySpatial FunctionCategory = "spatial"

func init() {
	registerSpatialFunctions()
}

func registerSpatialFunctions() {
	// ==================== Constructors ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_point",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Construct a Point geometry from X and Y coordinates",
		Signatures:  []FunctionSignature{{Name: "st_point", ReturnType: "geometry", ParamTypes: []string{"float", "float"}}},
		Handler:     stPointHandler,
		Example:     "SELECT ST_Point(1.0, 2.0)",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_geomfromtext",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Parse a WKT string into a Geometry object",
		Signatures:  []FunctionSignature{{Name: "st_geomfromtext", ReturnType: "geometry", ParamTypes: []string{"string"}, Variadic: true}},
		Handler:     stGeomFromTextHandler,
		Example:     "SELECT ST_GeomFromText('POINT(1 2)')",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_makeenvelope",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Create a rectangular Polygon from min/max coordinates",
		Signatures:  []FunctionSignature{{Name: "st_makeenvelope", ReturnType: "geometry", ParamTypes: []string{"float", "float", "float", "float"}}},
		Handler:     stMakeEnvelopeHandler,
		Example:     "SELECT ST_MakeEnvelope(0, 0, 10, 10)",
	})

	// ==================== Output ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_astext",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Convert a Geometry to its WKT string representation",
		Signatures:  []FunctionSignature{{Name: "st_astext", ReturnType: "string", ParamTypes: []string{"geometry"}}},
		Handler:     stAsTextHandler,
		Example:     "SELECT ST_AsText(location) FROM places",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_aswkt",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Convert a Geometry to its WKT string representation (alias for ST_AsText)",
		Signatures:  []FunctionSignature{{Name: "st_aswkt", ReturnType: "string", ParamTypes: []string{"geometry"}}},
		Handler:     stAsTextHandler,
		Example:     "SELECT ST_AsWKT(location) FROM places",
	})

	// ==================== Properties ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_x",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the X coordinate of a Point",
		Signatures:  []FunctionSignature{{Name: "st_x", ReturnType: "float", ParamTypes: []string{"geometry"}}},
		Handler:     stXHandler,
		Example:     "SELECT ST_X(ST_Point(1, 2))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_y",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the Y coordinate of a Point",
		Signatures:  []FunctionSignature{{Name: "st_y", ReturnType: "float", ParamTypes: []string{"geometry"}}},
		Handler:     stYHandler,
		Example:     "SELECT ST_Y(ST_Point(1, 2))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_srid",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the SRID of a Geometry",
		Signatures:  []FunctionSignature{{Name: "st_srid", ReturnType: "int", ParamTypes: []string{"geometry"}}},
		Handler:     stSRIDHandler,
		Example:     "SELECT ST_SRID(location) FROM places",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_geometrytype",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the type name of a Geometry",
		Signatures:  []FunctionSignature{{Name: "st_geometrytype", ReturnType: "string", ParamTypes: []string{"geometry"}}},
		Handler:     stGeometryTypeHandler,
		Example:     "SELECT ST_GeometryType(ST_Point(1, 2))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_dimension",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the topological dimension of a Geometry",
		Signatures:  []FunctionSignature{{Name: "st_dimension", ReturnType: "int", ParamTypes: []string{"geometry"}}},
		Handler:     stDimensionHandler,
		Example:     "SELECT ST_Dimension(ST_Point(1, 2))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_isempty",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether the Geometry is empty",
		Signatures:  []FunctionSignature{{Name: "st_isempty", ReturnType: "bool", ParamTypes: []string{"geometry"}}},
		Handler:     stIsEmptyHandler,
		Example:     "SELECT ST_IsEmpty(ST_GeomFromText('POINT EMPTY'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_isvalid",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether the Geometry is valid",
		Signatures:  []FunctionSignature{{Name: "st_isvalid", ReturnType: "bool", ParamTypes: []string{"geometry"}}},
		Handler:     stIsValidHandler,
		Example:     "SELECT ST_IsValid(ST_Point(1, 2))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_numpoints",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the number of points in a Geometry",
		Signatures:  []FunctionSignature{{Name: "st_numpoints", ReturnType: "int", ParamTypes: []string{"geometry"}}},
		Handler:     stNumPointsHandler,
		Example:     "SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_envelope",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the bounding box of a Geometry as a Polygon",
		Signatures:  []FunctionSignature{{Name: "st_envelope", ReturnType: "geometry", ParamTypes: []string{"geometry"}}},
		Handler:     stEnvelopeHandler,
		Example:     "SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0, 5 5)')))",
	})

	// ==================== Measurements ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_distance",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the Euclidean distance between two Geometries",
		Signatures:  []FunctionSignature{{Name: "st_distance", ReturnType: "float", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stDistanceHandler,
		Example:     "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_area",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the area of a Polygon or MultiPolygon",
		Signatures:  []FunctionSignature{{Name: "st_area", ReturnType: "float", ParamTypes: []string{"geometry"}}},
		Handler:     stAreaHandler,
		Example:     "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_length",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the length of a LineString or MultiLineString",
		Signatures:  []FunctionSignature{{Name: "st_length", ReturnType: "float", ParamTypes: []string{"geometry"}}},
		Handler:     stLengthHandler,
		Example:     "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_perimeter",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the perimeter of a Polygon",
		Signatures:  []FunctionSignature{{Name: "st_perimeter", ReturnType: "float", ParamTypes: []string{"geometry"}}},
		Handler:     stPerimeterHandler,
		Example:     "SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_centroid",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the centroid of a Geometry as a Point",
		Signatures:  []FunctionSignature{{Name: "st_centroid", ReturnType: "geometry", ParamTypes: []string{"geometry"}}},
		Handler:     stCentroidHandler,
		Example:     "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')))",
	})

	// ==================== Spatial Relationships ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_contains",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether the first Geometry contains the second",
		Signatures:  []FunctionSignature{{Name: "st_contains", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stContainsHandler,
		Example:     "SELECT ST_Contains(ST_MakeEnvelope(0, 0, 10, 10), ST_Point(5, 5))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_within",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether the first Geometry is within the second",
		Signatures:  []FunctionSignature{{Name: "st_within", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stWithinHandler,
		Example:     "SELECT ST_Within(ST_Point(5, 5), ST_MakeEnvelope(0, 0, 10, 10))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_intersects",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries intersect",
		Signatures:  []FunctionSignature{{Name: "st_intersects", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stIntersectsHandler,
		Example:     "SELECT ST_Intersects(ST_MakeEnvelope(0, 0, 5, 5), ST_MakeEnvelope(3, 3, 8, 8))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_disjoint",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries are disjoint (do not intersect)",
		Signatures:  []FunctionSignature{{Name: "st_disjoint", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stDisjointHandler,
		Example:     "SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(5, 5))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_equals",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries are spatially equal",
		Signatures:  []FunctionSignature{{Name: "st_equals", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stEqualsHandler,
		Example:     "SELECT ST_Equals(ST_Point(1, 2), ST_GeomFromText('POINT(1 2)'))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_touches",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries touch at their boundaries",
		Signatures:  []FunctionSignature{{Name: "st_touches", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stTouchesHandler,
		Example:     "SELECT ST_Touches(ST_MakeEnvelope(0, 0, 5, 5), ST_MakeEnvelope(5, 0, 10, 5))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_overlaps",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries overlap (partial intersection of same dimension)",
		Signatures:  []FunctionSignature{{Name: "st_overlaps", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stOverlapsHandler,
		Example:     "SELECT ST_Overlaps(ST_MakeEnvelope(0, 0, 5, 5), ST_MakeEnvelope(3, 3, 8, 8))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_crosses",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return whether two Geometries cross each other",
		Signatures:  []FunctionSignature{{Name: "st_crosses", ReturnType: "bool", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stCrossesHandler,
		Example:     "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(10 0, 0 10)'))",
	})

	// ==================== Operations ====================

	RegisterGlobal(&FunctionInfo{
		Name:        "st_buffer",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return a Geometry buffered by the given distance",
		Signatures:  []FunctionSignature{{Name: "st_buffer", ReturnType: "geometry", ParamTypes: []string{"geometry", "float"}}},
		Handler:     stBufferHandler,
		Example:     "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 5))",
	})

	RegisterGlobal(&FunctionInfo{
		Name:        "st_union",
		Type:        FunctionTypeScalar,
		Category:    "spatial",
		Description: "Return the union of two Geometries as a GeometryCollection",
		Signatures:  []FunctionSignature{{Name: "st_union", ReturnType: "geometry", ParamTypes: []string{"geometry", "geometry"}}},
		Handler:     stUnionHandler,
		Example:     "SELECT ST_AsText(ST_Union(ST_Point(0, 0), ST_Point(1, 1)))",
	})
}

// ==================== Handler Implementations ====================

func stPointHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Point requires exactly 2 arguments")
	}
	x, err := toFloat64Arg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Point: invalid X: %w", err)
	}
	y, err := toFloat64Arg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Point: invalid Y: %w", err)
	}
	return &GeoPoint{X: x, Y: y}, nil
}

func stGeomFromTextHandler(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ST_GeomFromText requires 1 or 2 arguments")
	}
	wkt, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("ST_GeomFromText: first argument must be a string")
	}
	geom, err := ParseWKT(wkt)
	if err != nil {
		return nil, fmt.Errorf("ST_GeomFromText: %w", err)
	}
	if len(args) == 2 {
		srid, err := toInt64Arg(args[1])
		if err != nil {
			return nil, fmt.Errorf("ST_GeomFromText: invalid SRID: %w", err)
		}
		setGeomSRID(geom, int(srid))
	}
	return geom, nil
}

func stMakeEnvelopeHandler(args []interface{}) (interface{}, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("ST_MakeEnvelope requires exactly 4 arguments")
	}
	xmin, err := toFloat64Arg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_MakeEnvelope: invalid xmin: %w", err)
	}
	ymin, err := toFloat64Arg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_MakeEnvelope: invalid ymin: %w", err)
	}
	xmax, err := toFloat64Arg(args[2])
	if err != nil {
		return nil, fmt.Errorf("ST_MakeEnvelope: invalid xmax: %w", err)
	}
	ymax, err := toFloat64Arg(args[3])
	if err != nil {
		return nil, fmt.Errorf("ST_MakeEnvelope: invalid ymax: %w", err)
	}
	return &GeoPolygon{
		Rings: [][]GeoPoint{{
			{X: xmin, Y: ymin},
			{X: xmax, Y: ymin},
			{X: xmax, Y: ymax},
			{X: xmin, Y: ymax},
			{X: xmin, Y: ymin},
		}},
	}, nil
}

func stAsTextHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_AsText requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_AsText: %w", err)
	}
	return geom.WKT(), nil
}

func stXHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_X requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_X: %w", err)
	}
	pt, ok := geom.(*GeoPoint)
	if !ok {
		return nil, fmt.Errorf("ST_X requires a Point geometry, got %s", geom.GeometryType())
	}
	return pt.X, nil
}

func stYHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Y requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Y: %w", err)
	}
	pt, ok := geom.(*GeoPoint)
	if !ok {
		return nil, fmt.Errorf("ST_Y requires a Point geometry, got %s", geom.GeometryType())
	}
	return pt.Y, nil
}

func stSRIDHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_SRID requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_SRID: %w", err)
	}
	return int64(geom.SRID()), nil
}

func stGeometryTypeHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_GeometryType requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_GeometryType: %w", err)
	}
	return geom.GeometryType(), nil
}

func stDimensionHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Dimension requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Dimension: %w", err)
	}
	return int64(geom.Dimension()), nil
}

func stIsEmptyHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_IsEmpty requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_IsEmpty: %w", err)
	}
	return geom.IsEmpty(), nil
}

func stIsValidHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_IsValid requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_IsValid: %w", err)
	}
	// Basic validity check
	switch g := geom.(type) {
	case *GeoPolygon:
		if !g.IsEmpty() {
			// Exterior ring must have at least 4 points (triangle + close)
			if len(g.Rings[0]) < 4 {
				return false, nil
			}
			// Rings must be closed
			for _, ring := range g.Rings {
				if len(ring) < 4 {
					return false, nil
				}
				first, last := ring[0], ring[len(ring)-1]
				if first.X != last.X || first.Y != last.Y {
					return false, nil
				}
			}
		}
	case *GeoLineString:
		if !g.IsEmpty() && len(g.Points) < 2 {
			return false, nil
		}
	}
	return true, nil
}

func stNumPointsHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_NumPoints requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_NumPoints: %w", err)
	}
	return int64(geom.NumPoints()), nil
}

func stEnvelopeHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Envelope requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Envelope: %w", err)
	}
	bb := geom.Envelope()
	return bb.ToPolygon(), nil
}

// ==================== Measurement Handlers ====================

func stDistanceHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Distance requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Distance: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Distance: arg2: %w", err)
	}
	return geometryDistance(g1, g2), nil
}

func stAreaHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Area requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Area: %w", err)
	}
	switch g := geom.(type) {
	case *GeoPolygon:
		return g.Area(), nil
	case *GeoMultiPolygon:
		total := 0.0
		for i := range g.Polygons {
			total += g.Polygons[i].Area()
		}
		return total, nil
	default:
		return 0.0, nil
	}
}

func stLengthHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Length requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Length: %w", err)
	}
	switch g := geom.(type) {
	case *GeoLineString:
		return g.Length(), nil
	case *GeoMultiLineString:
		total := 0.0
		for i := range g.Lines {
			total += g.Lines[i].Length()
		}
		return total, nil
	default:
		return 0.0, nil
	}
}

func stPerimeterHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Perimeter requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Perimeter: %w", err)
	}
	switch g := geom.(type) {
	case *GeoPolygon:
		return g.Perimeter(), nil
	case *GeoMultiPolygon:
		total := 0.0
		for i := range g.Polygons {
			total += g.Polygons[i].Perimeter()
		}
		return total, nil
	default:
		return 0.0, nil
	}
}

func stCentroidHandler(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ST_Centroid requires exactly 1 argument")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Centroid: %w", err)
	}
	return geometryCentroid(geom), nil
}

// ==================== Relationship Handlers ====================

func stContainsHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Contains requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Contains: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Contains: arg2: %w", err)
	}
	return geometryContains(g1, g2), nil
}

func stWithinHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Within requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Within: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Within: arg2: %w", err)
	}
	// ST_Within(a, b) = ST_Contains(b, a)
	return geometryContains(g2, g1), nil
}

func stIntersectsHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Intersects requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Intersects: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Intersects: arg2: %w", err)
	}
	return geometryIntersects(g1, g2), nil
}

func stDisjointHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Disjoint requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Disjoint: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Disjoint: arg2: %w", err)
	}
	return !geometryIntersects(g1, g2), nil
}

func stEqualsHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Equals requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Equals: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Equals: arg2: %w", err)
	}
	return g1.Equals(g2), nil
}

func stTouchesHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Touches requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Touches: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Touches: arg2: %w", err)
	}
	return geometryTouches(g1, g2), nil
}

func stOverlapsHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Overlaps requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Overlaps: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Overlaps: arg2: %w", err)
	}
	return geometryOverlaps(g1, g2), nil
}

func stCrossesHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Crosses requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Crosses: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Crosses: arg2: %w", err)
	}
	return geometryCrosses(g1, g2), nil
}

// ==================== Operation Handlers ====================

func stBufferHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Buffer requires exactly 2 arguments")
	}
	geom, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Buffer: arg1: %w", err)
	}
	dist, err := toFloat64Arg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Buffer: invalid distance: %w", err)
	}
	return geometryBuffer(geom, dist), nil
}

func stUnionHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ST_Union requires exactly 2 arguments")
	}
	g1, err := ParseGeometryArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("ST_Union: arg1: %w", err)
	}
	g2, err := ParseGeometryArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("ST_Union: arg2: %w", err)
	}
	return &GeoCollection{Geometries: []Geometry{g1, g2}}, nil
}

// ==================== Spatial Computation Functions ====================

// geometryDistance computes the minimum distance between two geometries.
func geometryDistance(g1, g2 Geometry) float64 {
	// Quick bounding box check
	bb1, bb2 := g1.Envelope(), g2.Envelope()
	if !bb1.Intersects(bb2) {
		// Minimum distance between bounding boxes as lower bound
		// Still need precise distance below
	}

	pts1 := extractPoints(g1)
	pts2 := extractPoints(g2)

	// Point-to-point distance
	if len(pts1) == 1 && len(pts2) == 1 {
		return pts1[0].DistanceTo(&pts2[0])
	}

	// Point to geometry distance
	if len(pts1) == 1 {
		return pointToGeometryDistance(pts1[0], g2)
	}
	if len(pts2) == 1 {
		return pointToGeometryDistance(pts2[0], g1)
	}

	// General: minimum distance between all point pairs and edges
	minDist := math.MaxFloat64
	edges1 := extractEdges(g1)
	edges2 := extractEdges(g2)

	// Point-to-edge distances
	for _, p := range pts1 {
		for _, e := range edges2 {
			d := PointToSegmentDistance(p.X, p.Y, e[0], e[1], e[2], e[3])
			if d < minDist {
				minDist = d
			}
		}
	}
	for _, p := range pts2 {
		for _, e := range edges1 {
			d := PointToSegmentDistance(p.X, p.Y, e[0], e[1], e[2], e[3])
			if d < minDist {
				minDist = d
			}
		}
	}

	// Check if one contains the other (distance = 0)
	if geometryContains(g1, g2) || geometryContains(g2, g1) {
		return 0
	}

	return minDist
}

func pointToGeometryDistance(p GeoPoint, g Geometry) float64 {
	switch geom := g.(type) {
	case *GeoPoint:
		return p.DistanceTo(geom)
	case *GeoLineString:
		minDist := math.MaxFloat64
		for i := 1; i < len(geom.Points); i++ {
			d := PointToSegmentDistance(p.X, p.Y,
				geom.Points[i-1].X, geom.Points[i-1].Y,
				geom.Points[i].X, geom.Points[i].Y)
			if d < minDist {
				minDist = d
			}
		}
		return minDist
	case *GeoPolygon:
		if geom.ContainsPoint(p.X, p.Y) {
			return 0
		}
		minDist := math.MaxFloat64
		for _, ring := range geom.Rings {
			for i := 1; i < len(ring); i++ {
				d := PointToSegmentDistance(p.X, p.Y,
					ring[i-1].X, ring[i-1].Y,
					ring[i].X, ring[i].Y)
				if d < minDist {
					minDist = d
				}
			}
		}
		return minDist
	default:
		pts := extractPoints(g)
		minDist := math.MaxFloat64
		for _, pt := range pts {
			d := p.DistanceTo(&pt)
			if d < minDist {
				minDist = d
			}
		}
		return minDist
	}
}

// geometryContains checks if g1 contains g2.
func geometryContains(g1, g2 Geometry) bool {
	// Bounding box pre-filter
	bb1, bb2 := g1.Envelope(), g2.Envelope()
	if !bb1.Contains(bb2) {
		return false
	}

	switch container := g1.(type) {
	case *GeoPolygon:
		pts := extractPoints(g2)
		for _, p := range pts {
			if !container.ContainsPoint(p.X, p.Y) {
				return false
			}
		}
		return len(pts) > 0
	case *GeoMultiPolygon:
		pts := extractPoints(g2)
		for _, p := range pts {
			contained := false
			for i := range container.Polygons {
				if container.Polygons[i].ContainsPoint(p.X, p.Y) {
					contained = true
					break
				}
			}
			if !contained {
				return false
			}
		}
		return len(pts) > 0
	case *GeoPoint:
		// A point only contains another point at the same location
		if p2, ok := g2.(*GeoPoint); ok {
			return container.X == p2.X && container.Y == p2.Y
		}
		return false
	default:
		return false
	}
}

// geometryIntersects checks if two geometries intersect.
func geometryIntersects(g1, g2 Geometry) bool {
	// Bounding box pre-filter
	bb1, bb2 := g1.Envelope(), g2.Envelope()
	if !bb1.Intersects(bb2) {
		return false
	}

	// Check containment
	if geometryContains(g1, g2) || geometryContains(g2, g1) {
		return true
	}

	// Check edge intersections
	edges1 := extractEdges(g1)
	edges2 := extractEdges(g2)
	for _, e1 := range edges1 {
		for _, e2 := range edges2 {
			if SegmentsIntersect(e1[0], e1[1], e1[2], e1[3], e2[0], e2[1], e2[2], e2[3]) {
				return true
			}
		}
	}

	// Check if any point of g1 is inside g2 or vice versa
	pts1 := extractPoints(g1)
	pts2 := extractPoints(g2)
	for _, p := range pts1 {
		if geometryContainsPoint(g2, p.X, p.Y) {
			return true
		}
	}
	for _, p := range pts2 {
		if geometryContainsPoint(g1, p.X, p.Y) {
			return true
		}
	}

	return false
}

// geometryTouches checks if two geometries touch (boundaries meet but interiors don't overlap).
func geometryTouches(g1, g2 Geometry) bool {
	if !geometryIntersects(g1, g2) {
		return false
	}
	// Touches means they intersect but interiors don't overlap
	// Simplified: intersect but no containment of interior points
	pts2 := extractPoints(g2)
	for _, p := range pts2 {
		if geometryContainsPointInterior(g1, p.X, p.Y) {
			return false
		}
	}
	pts1 := extractPoints(g1)
	for _, p := range pts1 {
		if geometryContainsPointInterior(g2, p.X, p.Y) {
			return false
		}
	}
	return true
}

// geometryOverlaps checks if two geometries of the same dimension partially overlap.
func geometryOverlaps(g1, g2 Geometry) bool {
	if g1.Dimension() != g2.Dimension() {
		return false
	}
	return geometryIntersects(g1, g2) && !geometryContains(g1, g2) && !geometryContains(g2, g1)
}

// geometryCrosses checks if two geometries cross.
func geometryCrosses(g1, g2 Geometry) bool {
	edges1 := extractEdges(g1)
	edges2 := extractEdges(g2)
	for _, e1 := range edges1 {
		for _, e2 := range edges2 {
			if SegmentsIntersect(e1[0], e1[1], e1[2], e1[3], e2[0], e2[1], e2[2], e2[3]) {
				return true
			}
		}
	}
	return false
}

// geometryBuffer creates a buffer polygon around a geometry.
func geometryBuffer(geom Geometry, distance float64) Geometry {
	if distance <= 0 {
		return geom
	}

	switch g := geom.(type) {
	case *GeoPoint:
		// Create a circular polygon (approximated as N-gon)
		const segments = 32
		ring := make([]GeoPoint, segments+1)
		for i := 0; i < segments; i++ {
			angle := 2 * math.Pi * float64(i) / float64(segments)
			ring[i] = GeoPoint{
				X: g.X + distance*math.Cos(angle),
				Y: g.Y + distance*math.Sin(angle),
			}
		}
		ring[segments] = ring[0] // close the ring
		return &GeoPolygon{Rings: [][]GeoPoint{ring}, Srid: g.Srid}
	default:
		// For other geometries, expand the bounding box
		bb := geom.Envelope()
		return &GeoPolygon{
			Rings: [][]GeoPoint{{
				{X: bb.MinX - distance, Y: bb.MinY - distance},
				{X: bb.MaxX + distance, Y: bb.MinY - distance},
				{X: bb.MaxX + distance, Y: bb.MaxY + distance},
				{X: bb.MinX - distance, Y: bb.MaxY + distance},
				{X: bb.MinX - distance, Y: bb.MinY - distance},
			}},
		}
	}
}

// geometryCentroid computes the centroid of a geometry.
func geometryCentroid(geom Geometry) Geometry {
	switch g := geom.(type) {
	case *GeoPoint:
		return g
	case *GeoLineString:
		if g.IsEmpty() {
			return &GeoPoint{X: math.NaN(), Y: math.NaN()}
		}
		sumX, sumY := 0.0, 0.0
		for _, p := range g.Points {
			sumX += p.X
			sumY += p.Y
		}
		n := float64(len(g.Points))
		return &GeoPoint{X: sumX / n, Y: sumY / n}
	case *GeoPolygon:
		if g.IsEmpty() {
			return &GeoPoint{X: math.NaN(), Y: math.NaN()}
		}
		return polygonCentroid(g.Rings[0])
	default:
		pts := extractPoints(geom)
		if len(pts) == 0 {
			return &GeoPoint{X: math.NaN(), Y: math.NaN()}
		}
		sumX, sumY := 0.0, 0.0
		for _, p := range pts {
			sumX += p.X
			sumY += p.Y
		}
		n := float64(len(pts))
		return &GeoPoint{X: sumX / n, Y: sumY / n}
	}
}

func polygonCentroid(ring []GeoPoint) *GeoPoint {
	n := len(ring)
	if n < 3 {
		return &GeoPoint{X: math.NaN(), Y: math.NaN()}
	}
	cx, cy, area := 0.0, 0.0, 0.0
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		cross := ring[i].X*ring[j].Y - ring[j].X*ring[i].Y
		cx += (ring[i].X + ring[j].X) * cross
		cy += (ring[i].Y + ring[j].Y) * cross
		area += cross
	}
	area /= 2.0
	if area == 0 {
		return &GeoPoint{X: ring[0].X, Y: ring[0].Y}
	}
	cx /= (6.0 * area)
	cy /= (6.0 * area)
	return &GeoPoint{X: cx, Y: cy}
}

// ==================== Geometry Helpers ====================

func extractPoints(g Geometry) []GeoPoint {
	switch geom := g.(type) {
	case *GeoPoint:
		return []GeoPoint{*geom}
	case *GeoLineString:
		return geom.Points
	case *GeoPolygon:
		var pts []GeoPoint
		for _, ring := range geom.Rings {
			pts = append(pts, ring...)
		}
		return pts
	case *GeoMultiPoint:
		return geom.Points
	case *GeoMultiLineString:
		var pts []GeoPoint
		for _, l := range geom.Lines {
			pts = append(pts, l.Points...)
		}
		return pts
	case *GeoMultiPolygon:
		var pts []GeoPoint
		for _, p := range geom.Polygons {
			for _, ring := range p.Rings {
				pts = append(pts, ring...)
			}
		}
		return pts
	case *GeoCollection:
		var pts []GeoPoint
		for _, child := range geom.Geometries {
			pts = append(pts, extractPoints(child)...)
		}
		return pts
	default:
		return nil
	}
}

func extractEdges(g Geometry) [][4]float64 {
	var edges [][4]float64
	switch geom := g.(type) {
	case *GeoLineString:
		for i := 1; i < len(geom.Points); i++ {
			edges = append(edges, [4]float64{
				geom.Points[i-1].X, geom.Points[i-1].Y,
				geom.Points[i].X, geom.Points[i].Y,
			})
		}
	case *GeoPolygon:
		for _, ring := range geom.Rings {
			for i := 1; i < len(ring); i++ {
				edges = append(edges, [4]float64{
					ring[i-1].X, ring[i-1].Y,
					ring[i].X, ring[i].Y,
				})
			}
		}
	case *GeoMultiLineString:
		for _, l := range geom.Lines {
			edges = append(edges, extractEdges(&l)...)
		}
	case *GeoMultiPolygon:
		for i := range geom.Polygons {
			edges = append(edges, extractEdges(&geom.Polygons[i])...)
		}
	case *GeoCollection:
		for _, child := range geom.Geometries {
			edges = append(edges, extractEdges(child)...)
		}
	}
	return edges
}

func geometryContainsPoint(g Geometry, x, y float64) bool {
	switch geom := g.(type) {
	case *GeoPoint:
		return geom.X == x && geom.Y == y
	case *GeoPolygon:
		return geom.ContainsPoint(x, y)
	case *GeoMultiPolygon:
		for i := range geom.Polygons {
			if geom.Polygons[i].ContainsPoint(x, y) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// geometryContainsPointInterior checks if a point is in the interior (not boundary) of a geometry.
func geometryContainsPointInterior(g Geometry, x, y float64) bool {
	switch geom := g.(type) {
	case *GeoPolygon:
		if !geom.ContainsPoint(x, y) {
			return false
		}
		// Check it's not on any edge
		for _, ring := range geom.Rings {
			for i := 1; i < len(ring); i++ {
				d := PointToSegmentDistance(x, y, ring[i-1].X, ring[i-1].Y, ring[i].X, ring[i].Y)
				if d < 1e-10 {
					return false // On boundary
				}
			}
		}
		return true
	default:
		return false
	}
}

func setGeomSRID(g Geometry, srid int) {
	switch geom := g.(type) {
	case *GeoPoint:
		geom.Srid = srid
	case *GeoLineString:
		geom.Srid = srid
	case *GeoPolygon:
		geom.Srid = srid
	case *GeoMultiPoint:
		geom.Srid = srid
	case *GeoMultiLineString:
		geom.Srid = srid
	case *GeoMultiPolygon:
		geom.Srid = srid
	case *GeoCollection:
		geom.Srid = srid
	}
}

// ==================== Argument conversion helpers ====================

func toFloat64Arg(arg interface{}) (float64, error) {
	switch v := arg.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case string:
		f, err := fmt.Sscanf(v, "%f", new(float64))
		if err != nil || f != 1 {
			return 0, fmt.Errorf("cannot convert %q to float64", v)
		}
		var val float64
		fmt.Sscanf(v, "%f", &val)
		return val, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", arg)
	}
}

func toInt64Arg(arg interface{}) (int64, error) {
	switch v := arg.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		var val int64
		_, err := fmt.Sscanf(v, "%d", &val)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to int64", v)
		}
		return val, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", arg)
	}
}
