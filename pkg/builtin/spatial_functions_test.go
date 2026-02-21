package builtin

import (
	"math"
	"testing"
)

// ==================== WKT Parse/Serialize Tests ====================

func TestParseWKT_Point(t *testing.T) {
	geom, err := ParseWKT("POINT(1.5 2.5)")
	if err != nil {
		t.Fatalf("ParseWKT POINT failed: %v", err)
	}
	pt, ok := geom.(*GeoPoint)
	if !ok {
		t.Fatalf("expected *GeoPoint, got %T", geom)
	}
	if pt.X != 1.5 || pt.Y != 2.5 {
		t.Errorf("expected (1.5, 2.5), got (%v, %v)", pt.X, pt.Y)
	}
}

func TestParseWKT_PointEmpty(t *testing.T) {
	geom, err := ParseWKT("POINT EMPTY")
	if err != nil {
		t.Fatalf("ParseWKT POINT EMPTY failed: %v", err)
	}
	if !geom.IsEmpty() {
		t.Error("expected empty point")
	}
}

func TestParseWKT_LineString(t *testing.T) {
	geom, err := ParseWKT("LINESTRING(0 0, 1 1, 2 2)")
	if err != nil {
		t.Fatalf("ParseWKT LINESTRING failed: %v", err)
	}
	ls, ok := geom.(*GeoLineString)
	if !ok {
		t.Fatalf("expected *GeoLineString, got %T", geom)
	}
	if len(ls.Points) != 3 {
		t.Errorf("expected 3 points, got %d", len(ls.Points))
	}
}

func TestParseWKT_Polygon(t *testing.T) {
	geom, err := ParseWKT("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	if err != nil {
		t.Fatalf("ParseWKT POLYGON failed: %v", err)
	}
	poly, ok := geom.(*GeoPolygon)
	if !ok {
		t.Fatalf("expected *GeoPolygon, got %T", geom)
	}
	if len(poly.Rings) != 1 {
		t.Errorf("expected 1 ring, got %d", len(poly.Rings))
	}
	if len(poly.Rings[0]) != 5 {
		t.Errorf("expected 5 points in ring, got %d", len(poly.Rings[0]))
	}
}

func TestParseWKT_PolygonWithHole(t *testing.T) {
	geom, err := ParseWKT("POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))")
	if err != nil {
		t.Fatalf("ParseWKT POLYGON with hole failed: %v", err)
	}
	poly := geom.(*GeoPolygon)
	if len(poly.Rings) != 2 {
		t.Errorf("expected 2 rings, got %d", len(poly.Rings))
	}
}

func TestParseWKT_MultiPoint(t *testing.T) {
	geom, err := ParseWKT("MULTIPOINT((0 0), (1 1), (2 2))")
	if err != nil {
		t.Fatalf("ParseWKT MULTIPOINT failed: %v", err)
	}
	mp, ok := geom.(*GeoMultiPoint)
	if !ok {
		t.Fatalf("expected *GeoMultiPoint, got %T", geom)
	}
	if len(mp.Points) != 3 {
		t.Errorf("expected 3 points, got %d", len(mp.Points))
	}
}

func TestParseWKT_MultiLineString(t *testing.T) {
	geom, err := ParseWKT("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))")
	if err != nil {
		t.Fatalf("ParseWKT MULTILINESTRING failed: %v", err)
	}
	mls, ok := geom.(*GeoMultiLineString)
	if !ok {
		t.Fatalf("expected *GeoMultiLineString, got %T", geom)
	}
	if len(mls.Lines) != 2 {
		t.Errorf("expected 2 lines, got %d", len(mls.Lines))
	}
}

func TestParseWKT_MultiPolygon(t *testing.T) {
	geom, err := ParseWKT("MULTIPOLYGON(((0 0, 5 0, 5 5, 0 5, 0 0)), ((10 10, 15 10, 15 15, 10 15, 10 10)))")
	if err != nil {
		t.Fatalf("ParseWKT MULTIPOLYGON failed: %v", err)
	}
	mp, ok := geom.(*GeoMultiPolygon)
	if !ok {
		t.Fatalf("expected *GeoMultiPolygon, got %T", geom)
	}
	if len(mp.Polygons) != 2 {
		t.Errorf("expected 2 polygons, got %d", len(mp.Polygons))
	}
}

func TestParseWKT_GeometryCollection(t *testing.T) {
	geom, err := ParseWKT("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))")
	if err != nil {
		t.Fatalf("ParseWKT GEOMETRYCOLLECTION failed: %v", err)
	}
	gc, ok := geom.(*GeoCollection)
	if !ok {
		t.Fatalf("expected *GeoCollection, got %T", geom)
	}
	if len(gc.Geometries) != 2 {
		t.Errorf("expected 2 geometries, got %d", len(gc.Geometries))
	}
}

func TestWKT_Roundtrip(t *testing.T) {
	tests := []string{
		"POINT(1 2)",
		"POINT(-3.14 2.718)",
		"LINESTRING(0 0, 1 1, 2 0)",
		"POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
		"POLYGON((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))",
		"MULTIPOINT((0 0), (1 1))",
		"MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
		"MULTIPOLYGON(((0 0, 5 0, 5 5, 0 5, 0 0)), ((10 10, 15 10, 15 15, 10 15, 10 10)))",
	}

	for _, wkt := range tests {
		geom, err := ParseWKT(wkt)
		if err != nil {
			t.Errorf("ParseWKT(%q) failed: %v", wkt, err)
			continue
		}
		output := geom.WKT()
		// Re-parse the output to verify roundtrip
		geom2, err := ParseWKT(output)
		if err != nil {
			t.Errorf("re-parse WKT(%q) failed: %v", output, err)
			continue
		}
		if !geom.Equals(geom2) {
			t.Errorf("roundtrip mismatch: %q -> %q", wkt, output)
		}
	}
}

func TestParseWKT_NegativeCoords(t *testing.T) {
	geom, err := ParseWKT("POINT(-74.006 40.7128)")
	if err != nil {
		t.Fatalf("ParseWKT negative coords failed: %v", err)
	}
	pt := geom.(*GeoPoint)
	if pt.X != -74.006 || pt.Y != 40.7128 {
		t.Errorf("expected (-74.006, 40.7128), got (%v, %v)", pt.X, pt.Y)
	}
}

func TestParseWKT_CaseInsensitive(t *testing.T) {
	geom, err := ParseWKT("point(1 2)")
	if err != nil {
		t.Fatalf("ParseWKT lowercase failed: %v", err)
	}
	if geom.GeometryType() != "Point" {
		t.Errorf("expected Point, got %s", geom.GeometryType())
	}
}

// ==================== ST_Point Tests ====================

func TestSTPoint(t *testing.T) {
	result, err := stPointHandler([]interface{}{1.0, 2.0})
	if err != nil {
		t.Fatalf("ST_Point failed: %v", err)
	}
	pt, ok := result.(*GeoPoint)
	if !ok {
		t.Fatalf("expected *GeoPoint, got %T", result)
	}
	if pt.X != 1.0 || pt.Y != 2.0 {
		t.Errorf("expected (1, 2), got (%v, %v)", pt.X, pt.Y)
	}
}

func TestSTPoint_IntArgs(t *testing.T) {
	result, err := stPointHandler([]interface{}{int64(3), int64(4)})
	if err != nil {
		t.Fatalf("ST_Point with int args failed: %v", err)
	}
	pt := result.(*GeoPoint)
	if pt.X != 3.0 || pt.Y != 4.0 {
		t.Errorf("expected (3, 4), got (%v, %v)", pt.X, pt.Y)
	}
}

// ==================== ST_GeomFromText Tests ====================

func TestSTGeomFromText(t *testing.T) {
	result, err := stGeomFromTextHandler([]interface{}{"POINT(5 10)"})
	if err != nil {
		t.Fatalf("ST_GeomFromText failed: %v", err)
	}
	pt, ok := result.(*GeoPoint)
	if !ok {
		t.Fatalf("expected *GeoPoint, got %T", result)
	}
	if pt.X != 5 || pt.Y != 10 {
		t.Errorf("expected (5, 10), got (%v, %v)", pt.X, pt.Y)
	}
}

func TestSTGeomFromText_WithSRID(t *testing.T) {
	result, err := stGeomFromTextHandler([]interface{}{"POINT(5 10)", int64(4326)})
	if err != nil {
		t.Fatalf("ST_GeomFromText with SRID failed: %v", err)
	}
	geom := result.(Geometry)
	if geom.SRID() != 4326 {
		t.Errorf("expected SRID 4326, got %d", geom.SRID())
	}
}

// ==================== ST_AsText Tests ====================

func TestSTAsText(t *testing.T) {
	pt := &GeoPoint{X: 1, Y: 2}
	result, err := stAsTextHandler([]interface{}{pt})
	if err != nil {
		t.Fatalf("ST_AsText failed: %v", err)
	}
	if result != "POINT(1 2)" {
		t.Errorf("expected 'POINT(1 2)', got %q", result)
	}
}

func TestSTAsText_FromWKT(t *testing.T) {
	result, err := stAsTextHandler([]interface{}{"POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"})
	if err != nil {
		t.Fatalf("ST_AsText from WKT failed: %v", err)
	}
	if result != "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))" {
		t.Errorf("unexpected result: %q", result)
	}
}

// ==================== ST_X, ST_Y Tests ====================

func TestSTXY(t *testing.T) {
	pt := &GeoPoint{X: 3.14, Y: 2.72}
	xResult, _ := stXHandler([]interface{}{pt})
	yResult, _ := stYHandler([]interface{}{pt})
	if xResult != 3.14 {
		t.Errorf("ST_X: expected 3.14, got %v", xResult)
	}
	if yResult != 2.72 {
		t.Errorf("ST_Y: expected 2.72, got %v", yResult)
	}
}

// ==================== Property Function Tests ====================

func TestSTGeometryType(t *testing.T) {
	tests := []struct {
		geom     Geometry
		expected string
	}{
		{&GeoPoint{X: 0, Y: 0}, "Point"},
		{&GeoLineString{Points: []GeoPoint{{0, 0, 0}, {1, 1, 0}}}, "LineString"},
		{&GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {1, 0, 0}, {1, 1, 0}, {0, 0, 0}}}}, "Polygon"},
	}
	for _, tt := range tests {
		result, err := stGeometryTypeHandler([]interface{}{tt.geom})
		if err != nil {
			t.Errorf("ST_GeometryType failed: %v", err)
		}
		if result != tt.expected {
			t.Errorf("expected %q, got %q", tt.expected, result)
		}
	}
}

func TestSTDimension(t *testing.T) {
	result, _ := stDimensionHandler([]interface{}{&GeoPoint{X: 0, Y: 0}})
	if result != int64(0) {
		t.Errorf("Point dimension: expected 0, got %v", result)
	}
	result, _ = stDimensionHandler([]interface{}{&GeoLineString{Points: []GeoPoint{{0, 0, 0}, {1, 1, 0}}}})
	if result != int64(1) {
		t.Errorf("LineString dimension: expected 1, got %v", result)
	}
}

func TestSTNumPoints(t *testing.T) {
	ls := &GeoLineString{Points: []GeoPoint{{0, 0, 0}, {1, 1, 0}, {2, 2, 0}}}
	result, _ := stNumPointsHandler([]interface{}{ls})
	if result != int64(3) {
		t.Errorf("expected 3 points, got %v", result)
	}
}

func TestSTIsValid(t *testing.T) {
	validPoly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	result, _ := stIsValidHandler([]interface{}{validPoly})
	if result != true {
		t.Error("expected valid polygon")
	}

	invalidPoly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {1, 1, 0}}}}
	result, _ = stIsValidHandler([]interface{}{invalidPoly})
	if result != false {
		t.Error("expected invalid polygon (too few points)")
	}
}

// ==================== Measurement Tests ====================

func TestSTDistance_Points(t *testing.T) {
	result, err := stDistanceHandler([]interface{}{
		&GeoPoint{X: 0, Y: 0},
		&GeoPoint{X: 3, Y: 4},
	})
	if err != nil {
		t.Fatalf("ST_Distance failed: %v", err)
	}
	dist := result.(float64)
	if math.Abs(dist-5.0) > 1e-10 {
		t.Errorf("expected distance 5.0, got %v", dist)
	}
}

func TestSTDistance_PointToLine(t *testing.T) {
	result, err := stDistanceHandler([]interface{}{
		&GeoPoint{X: 0, Y: 5},
		&GeoLineString{Points: []GeoPoint{{0, 0, 0}, {10, 0, 0}}},
	})
	if err != nil {
		t.Fatalf("ST_Distance point-to-line failed: %v", err)
	}
	dist := result.(float64)
	if math.Abs(dist-5.0) > 1e-10 {
		t.Errorf("expected distance 5.0, got %v", dist)
	}
}

func TestSTArea_Square(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	result, err := stAreaHandler([]interface{}{poly})
	if err != nil {
		t.Fatalf("ST_Area failed: %v", err)
	}
	area := result.(float64)
	if math.Abs(area-100.0) > 1e-10 {
		t.Errorf("expected area 100, got %v", area)
	}
}

func TestSTArea_WithHole(t *testing.T) {
	poly := &GeoPolygon{
		Rings: [][]GeoPoint{
			{{0, 0, 0}, {20, 0, 0}, {20, 20, 0}, {0, 20, 0}, {0, 0, 0}},
			{{5, 5, 0}, {15, 5, 0}, {15, 15, 0}, {5, 15, 0}, {5, 5, 0}},
		},
	}
	result, _ := stAreaHandler([]interface{}{poly})
	area := result.(float64)
	expected := 400.0 - 100.0 // 20x20 - 10x10
	if math.Abs(area-expected) > 1e-10 {
		t.Errorf("expected area %v, got %v", expected, area)
	}
}

func TestSTLength(t *testing.T) {
	ls := &GeoLineString{Points: []GeoPoint{{0, 0, 0}, {3, 4, 0}}}
	result, _ := stLengthHandler([]interface{}{ls})
	length := result.(float64)
	if math.Abs(length-5.0) > 1e-10 {
		t.Errorf("expected length 5.0, got %v", length)
	}
}

func TestSTPerimeter(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	result, _ := stPerimeterHandler([]interface{}{poly})
	peri := result.(float64)
	if math.Abs(peri-40.0) > 1e-10 {
		t.Errorf("expected perimeter 40, got %v", peri)
	}
}

func TestSTCentroid_Square(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	result, _ := stCentroidHandler([]interface{}{poly})
	pt := result.(*GeoPoint)
	if math.Abs(pt.X-5.0) > 1e-10 || math.Abs(pt.Y-5.0) > 1e-10 {
		t.Errorf("expected centroid (5, 5), got (%v, %v)", pt.X, pt.Y)
	}
}

// ==================== Spatial Relationship Tests ====================

func TestSTContains_PointInPolygon(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	inside := &GeoPoint{X: 5, Y: 5}
	outside := &GeoPoint{X: 15, Y: 5}

	result, _ := stContainsHandler([]interface{}{poly, inside})
	if result != true {
		t.Error("point (5,5) should be inside polygon")
	}

	result, _ = stContainsHandler([]interface{}{poly, outside})
	if result != false {
		t.Error("point (15,5) should be outside polygon")
	}
}

func TestSTWithin(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	pt := &GeoPoint{X: 5, Y: 5}

	result, _ := stWithinHandler([]interface{}{pt, poly})
	if result != true {
		t.Error("point should be within polygon")
	}
}

func TestSTIntersects_OverlappingPolygons(t *testing.T) {
	poly1 := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {5, 0, 0}, {5, 5, 0}, {0, 5, 0}, {0, 0, 0}}}}
	poly2 := &GeoPolygon{Rings: [][]GeoPoint{{{3, 3, 0}, {8, 3, 0}, {8, 8, 0}, {3, 8, 0}, {3, 3, 0}}}}

	result, _ := stIntersectsHandler([]interface{}{poly1, poly2})
	if result != true {
		t.Error("overlapping polygons should intersect")
	}
}

func TestSTDisjoint(t *testing.T) {
	pt1 := &GeoPoint{X: 0, Y: 0}
	pt2 := &GeoPoint{X: 10, Y: 10}

	result, _ := stDisjointHandler([]interface{}{pt1, pt2})
	if result != true {
		t.Error("separate points should be disjoint")
	}
}

func TestSTEquals(t *testing.T) {
	pt1 := &GeoPoint{X: 1, Y: 2}
	pt2 := &GeoPoint{X: 1, Y: 2}
	pt3 := &GeoPoint{X: 3, Y: 4}

	result, _ := stEqualsHandler([]interface{}{pt1, pt2})
	if result != true {
		t.Error("identical points should be equal")
	}

	result, _ = stEqualsHandler([]interface{}{pt1, pt3})
	if result != false {
		t.Error("different points should not be equal")
	}
}

func TestSTCrosses_Lines(t *testing.T) {
	line1 := &GeoLineString{Points: []GeoPoint{{0, 0, 0}, {10, 10, 0}}}
	line2 := &GeoLineString{Points: []GeoPoint{{10, 0, 0}, {0, 10, 0}}}

	result, _ := stCrossesHandler([]interface{}{line1, line2})
	if result != true {
		t.Error("crossing lines should return true")
	}
}

func TestSTOverlaps(t *testing.T) {
	poly1 := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {5, 0, 0}, {5, 5, 0}, {0, 5, 0}, {0, 0, 0}}}}
	poly2 := &GeoPolygon{Rings: [][]GeoPoint{{{3, 3, 0}, {8, 3, 0}, {8, 8, 0}, {3, 8, 0}, {3, 3, 0}}}}

	result, _ := stOverlapsHandler([]interface{}{poly1, poly2})
	if result != true {
		t.Error("partially overlapping polygons should return true")
	}
}

// ==================== Operation Tests ====================

func TestSTBuffer_Point(t *testing.T) {
	result, err := stBufferHandler([]interface{}{&GeoPoint{X: 0, Y: 0}, 5.0})
	if err != nil {
		t.Fatalf("ST_Buffer failed: %v", err)
	}
	poly, ok := result.(*GeoPolygon)
	if !ok {
		t.Fatalf("expected *GeoPolygon, got %T", result)
	}
	if poly.IsEmpty() {
		t.Error("buffer should not be empty")
	}
	// Buffer should create a circle-like polygon
	area := poly.Area()
	expectedArea := math.Pi * 25 // pi * r^2
	if math.Abs(area-expectedArea)/expectedArea > 0.05 {
		t.Errorf("buffer area %v should be close to %v (within 5%%)", area, expectedArea)
	}
}

func TestSTMakeEnvelope(t *testing.T) {
	result, err := stMakeEnvelopeHandler([]interface{}{0.0, 0.0, 10.0, 10.0})
	if err != nil {
		t.Fatalf("ST_MakeEnvelope failed: %v", err)
	}
	poly, ok := result.(*GeoPolygon)
	if !ok {
		t.Fatalf("expected *GeoPolygon, got %T", result)
	}
	area := poly.Area()
	if math.Abs(area-100.0) > 1e-10 {
		t.Errorf("expected area 100, got %v", area)
	}
}

func TestSTUnion(t *testing.T) {
	pt1 := &GeoPoint{X: 0, Y: 0}
	pt2 := &GeoPoint{X: 1, Y: 1}
	result, err := stUnionHandler([]interface{}{pt1, pt2})
	if err != nil {
		t.Fatalf("ST_Union failed: %v", err)
	}
	gc, ok := result.(*GeoCollection)
	if !ok {
		t.Fatalf("expected *GeoCollection, got %T", result)
	}
	if len(gc.Geometries) != 2 {
		t.Errorf("expected 2 geometries, got %d", len(gc.Geometries))
	}
}

func TestSTEnvelope(t *testing.T) {
	ls := &GeoLineString{Points: []GeoPoint{{1, 2, 0}, {5, 8, 0}, {3, 4, 0}}}
	result, err := stEnvelopeHandler([]interface{}{ls})
	if err != nil {
		t.Fatalf("ST_Envelope failed: %v", err)
	}
	poly := result.(*GeoPolygon)
	bb := poly.Envelope()
	if bb.MinX != 1 || bb.MinY != 2 || bb.MaxX != 5 || bb.MaxY != 8 {
		t.Errorf("unexpected envelope: %+v", bb)
	}
}

// ==================== BoundingBox Tests ====================

func TestBoundingBox_Intersects(t *testing.T) {
	bb1 := BoundingBox{MinX: 0, MinY: 0, MaxX: 5, MaxY: 5}
	bb2 := BoundingBox{MinX: 3, MinY: 3, MaxX: 8, MaxY: 8}
	bb3 := BoundingBox{MinX: 6, MinY: 6, MaxX: 10, MaxY: 10}

	if !bb1.Intersects(bb2) {
		t.Error("bb1 and bb2 should intersect")
	}
	if bb1.Intersects(bb3) {
		t.Error("bb1 and bb3 should not intersect")
	}
}

func TestBoundingBox_Contains(t *testing.T) {
	outer := BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	inner := BoundingBox{MinX: 2, MinY: 2, MaxX: 8, MaxY: 8}
	outside := BoundingBox{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15}

	if !outer.Contains(inner) {
		t.Error("outer should contain inner")
	}
	if outer.Contains(outside) {
		t.Error("outer should not contain outside")
	}
}

// ==================== ParseGeometryArg Tests ====================

func TestParseGeometryArg_Geometry(t *testing.T) {
	pt := &GeoPoint{X: 1, Y: 2}
	result, err := ParseGeometryArg(pt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != pt {
		t.Error("should return same geometry")
	}
}

func TestParseGeometryArg_String(t *testing.T) {
	result, err := ParseGeometryArg("POINT(1 2)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pt, ok := result.(*GeoPoint)
	if !ok {
		t.Fatalf("expected *GeoPoint, got %T", result)
	}
	if pt.X != 1 || pt.Y != 2 {
		t.Errorf("expected (1, 2), got (%v, %v)", pt.X, pt.Y)
	}
}

func TestParseGeometryArg_Invalid(t *testing.T) {
	_, err := ParseGeometryArg(42)
	if err == nil {
		t.Error("expected error for int argument")
	}
}

// ==================== IsSpatialColumnType Tests ====================

func TestIsSpatialColumnType(t *testing.T) {
	tests := []struct {
		colType  string
		expected bool
	}{
		{"GEOMETRY", true},
		{"POINT", true},
		{"LINESTRING", true},
		{"POLYGON", true},
		{"MULTIPOINT", true},
		{"MULTILINESTRING", true},
		{"MULTIPOLYGON", true},
		{"GEOMETRYCOLLECTION", true},
		{"geometry", true},
		{"point", true},
		{"INT", false},
		{"VARCHAR", false},
		{"FLOAT", false},
	}
	for _, tt := range tests {
		if got := IsSpatialColumnType(tt.colType); got != tt.expected {
			t.Errorf("IsSpatialColumnType(%q) = %v, want %v", tt.colType, got, tt.expected)
		}
	}
}

// ==================== Edge Cases ====================

func TestSTDistance_SamePoint(t *testing.T) {
	pt := &GeoPoint{X: 5, Y: 5}
	result, _ := stDistanceHandler([]interface{}{pt, pt})
	if result.(float64) != 0 {
		t.Errorf("distance from point to itself should be 0, got %v", result)
	}
}

func TestSTContains_PointOnEdge(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	edgePt := &GeoPoint{X: 5, Y: 0} // On edge

	// Point on edge may or may not be "contained" depending on ray-casting details
	// We mainly test it doesn't crash
	_, err := stContainsHandler([]interface{}{poly, edgePt})
	if err != nil {
		t.Fatalf("ST_Contains should not error for edge point: %v", err)
	}
}

func TestSTArea_Point(t *testing.T) {
	result, _ := stAreaHandler([]interface{}{&GeoPoint{X: 0, Y: 0}})
	if result.(float64) != 0 {
		t.Error("area of point should be 0")
	}
}

func TestSTLength_Point(t *testing.T) {
	result, _ := stLengthHandler([]interface{}{&GeoPoint{X: 0, Y: 0}})
	if result.(float64) != 0 {
		t.Error("length of point should be 0")
	}
}

func TestSTDistance_PointInsidePolygon(t *testing.T) {
	poly := &GeoPolygon{Rings: [][]GeoPoint{{{0, 0, 0}, {10, 0, 0}, {10, 10, 0}, {0, 10, 0}, {0, 0, 0}}}}
	pt := &GeoPoint{X: 5, Y: 5}

	result, _ := stDistanceHandler([]interface{}{pt, poly})
	if result.(float64) != 0 {
		t.Errorf("point inside polygon should have distance 0, got %v", result)
	}
}
