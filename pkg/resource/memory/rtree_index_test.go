package memory

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

func TestRTreeIndex_InsertAndFind(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	// Insert a point
	pt := &builtin.GeoPoint{X: 5, Y: 5}
	if err := idx.Insert(pt, []int64{1}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if idx.Size() != 1 {
		t.Errorf("expected size 1, got %d", idx.Size())
	}

	// Find by exact bbox
	results, found := idx.Find(pt)
	if !found {
		t.Error("expected to find the inserted point")
	}
	if len(results) != 1 || results[0] != 1 {
		t.Errorf("unexpected results: %v", results)
	}
}

func TestRTreeIndex_InsertMultiple(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	points := []struct {
		x, y  float64
		rowID int64
	}{
		{1, 1, 1},
		{5, 5, 2},
		{9, 9, 3},
		{3, 7, 4},
		{7, 3, 5},
	}

	for _, p := range points {
		pt := &builtin.GeoPoint{X: p.x, Y: p.y}
		if err := idx.Insert(pt, []int64{p.rowID}); err != nil {
			t.Fatalf("Insert failed for point (%v, %v): %v", p.x, p.y, err)
		}
	}

	if idx.Size() != 5 {
		t.Errorf("expected size 5, got %d", idx.Size())
	}
}

func TestRTreeIndex_SearchIntersects(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	// Insert points in a grid
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < 10; j++ {
			pt := &builtin.GeoPoint{X: float64(i), Y: float64(j)}
			if err := idx.Insert(pt, []int64{i*10 + j}); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}
	}

	if idx.Size() != 100 {
		t.Errorf("expected size 100, got %d", idx.Size())
	}

	// Search for points in a small area
	queryBBox := builtin.BoundingBox{MinX: 2, MinY: 2, MaxX: 4, MaxY: 4}
	results := idx.SearchIntersects(queryBBox)

	// Should find points at (2,2), (2,3), (2,4), (3,2), (3,3), (3,4), (4,2), (4,3), (4,4) = 9 points
	if len(results) != 9 {
		t.Errorf("expected 9 results in bbox [2,2]-[4,4], got %d", len(results))
	}
}

func TestRTreeIndex_SearchContains(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	// Insert some polygons with varying sizes
	poly1 := &builtin.GeoPolygon{Rings: [][]builtin.GeoPoint{{{X: 0, Y: 0}, {X: 2, Y: 0}, {X: 2, Y: 2}, {X: 0, Y: 2}, {X: 0, Y: 0}}}}
	poly2 := &builtin.GeoPolygon{Rings: [][]builtin.GeoPoint{{{X: 5, Y: 5}, {X: 7, Y: 5}, {X: 7, Y: 7}, {X: 5, Y: 7}, {X: 5, Y: 5}}}}
	poly3 := &builtin.GeoPolygon{Rings: [][]builtin.GeoPoint{{{X: 0, Y: 0}, {X: 10, Y: 0}, {X: 10, Y: 10}, {X: 0, Y: 10}, {X: 0, Y: 0}}}}

	idx.Insert(poly1, []int64{1})
	idx.Insert(poly2, []int64{2})
	idx.Insert(poly3, []int64{3})

	// Search for geometries contained within [0,0]-[10,10]
	searchBBox := builtin.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	results := idx.SearchContains(searchBBox)

	// All three should be contained
	if len(results) != 3 {
		t.Errorf("expected 3 results contained in [0,0]-[10,10], got %d", len(results))
	}

	// Search for geometries contained within [0,0]-[3,3]
	smallBBox := builtin.BoundingBox{MinX: 0, MinY: 0, MaxX: 3, MaxY: 3}
	results = idx.SearchContains(smallBBox)

	// Only poly1 should be contained
	if len(results) != 1 {
		t.Errorf("expected 1 result contained in [0,0]-[3,3], got %d", len(results))
	}
}

func TestRTreeIndex_Delete(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	pt1 := &builtin.GeoPoint{X: 1, Y: 1}
	pt2 := &builtin.GeoPoint{X: 5, Y: 5}
	pt3 := &builtin.GeoPoint{X: 9, Y: 9}

	idx.Insert(pt1, []int64{1})
	idx.Insert(pt2, []int64{2})
	idx.Insert(pt3, []int64{3})

	if idx.Size() != 3 {
		t.Errorf("expected size 3, got %d", idx.Size())
	}

	// Delete pt2
	if err := idx.Delete(pt2); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if idx.Size() != 2 {
		t.Errorf("expected size 2 after delete, got %d", idx.Size())
	}

	// pt2 should no longer be found
	results, found := idx.Find(pt2)
	if found && len(results) > 0 {
		t.Error("deleted point should not be found")
	}

	// pt1 and pt3 should still be found
	_, found = idx.Find(pt1)
	if !found {
		t.Error("pt1 should still be found")
	}
	_, found = idx.Find(pt3)
	if !found {
		t.Error("pt3 should still be found")
	}
}

func TestRTreeIndex_FindRange(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	for i := int64(0); i < 20; i++ {
		pt := &builtin.GeoPoint{X: float64(i), Y: float64(i)}
		idx.Insert(pt, []int64{i})
	}

	// FindRange with two bbox corners
	minBBox := builtin.BoundingBox{MinX: 5, MinY: 5, MaxX: 5, MaxY: 5}
	maxBBox := builtin.BoundingBox{MinX: 10, MinY: 10, MaxX: 10, MaxY: 10}

	results, err := idx.FindRange(minBBox, maxBBox)
	if err != nil {
		t.Fatalf("FindRange failed: %v", err)
	}

	// Should find points from 5..10 = 6 points
	if len(results) != 6 {
		t.Errorf("expected 6 results in range [5,5]-[10,10], got %d", len(results))
	}
}

func TestRTreeIndex_EmptyTree(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	results, found := idx.Find(&builtin.GeoPoint{X: 0, Y: 0})
	if found {
		t.Error("empty tree should not find anything")
	}
	if len(results) != 0 {
		t.Error("empty tree should return empty results")
	}

	intersects := idx.SearchIntersects(builtin.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10})
	if len(intersects) != 0 {
		t.Error("empty tree should return no intersections")
	}
}

func TestRTreeIndex_BulkInsert(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	// Insert many entries to test splitting
	n := 100
	for i := 0; i < n; i++ {
		pt := &builtin.GeoPoint{X: float64(i % 10), Y: float64(i / 10)}
		if err := idx.Insert(pt, []int64{int64(i)}); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	if idx.Size() != n {
		t.Errorf("expected size %d, got %d", n, idx.Size())
	}

	// Search should still work
	results := idx.SearchIntersects(builtin.BoundingBox{MinX: 0, MinY: 0, MaxX: 9, MaxY: 9})
	if len(results) != n {
		t.Errorf("expected all %d results, got %d", n, len(results))
	}
}

func TestRTreeIndex_GetIndexInfo(t *testing.T) {
	idx := NewRTreeIndex("cities", "location")
	info := idx.GetIndexInfo()

	if info.Name != "idx_sp_cities_location" {
		t.Errorf("expected name idx_sp_cities_location, got %s", info.Name)
	}
	if info.TableName != "cities" {
		t.Errorf("expected table cities, got %s", info.TableName)
	}
	if len(info.Columns) != 1 || info.Columns[0] != "location" {
		t.Errorf("unexpected columns: %v", info.Columns)
	}
	if info.Type != IndexTypeSpatialRTree {
		t.Errorf("expected type spatial_rtree, got %s", info.Type)
	}
}

func TestRTreeIndex_Reset(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	for i := 0; i < 10; i++ {
		pt := &builtin.GeoPoint{X: float64(i), Y: float64(i)}
		idx.Insert(pt, []int64{int64(i)})
	}

	if idx.Size() != 10 {
		t.Errorf("expected size 10, got %d", idx.Size())
	}

	idx.Reset()
	if idx.Size() != 0 {
		t.Errorf("expected size 0 after reset, got %d", idx.Size())
	}
}

func TestRTreeIndex_InsertWithBoundingBox(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	bbox := builtin.BoundingBox{MinX: 1, MinY: 1, MaxX: 5, MaxY: 5}
	if err := idx.Insert(bbox, []int64{1}); err != nil {
		t.Fatalf("Insert with BoundingBox failed: %v", err)
	}

	results, found := idx.Find(bbox)
	if !found {
		t.Error("expected to find inserted bbox")
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestRTreeIndex_LineStringEnvelope(t *testing.T) {
	idx := NewRTreeIndex("test_table", "geom_col")

	ls := &builtin.GeoLineString{Points: []builtin.GeoPoint{
		{X: 0, Y: 0}, {X: 10, Y: 10},
	}}
	idx.Insert(ls, []int64{1})

	// Search with a bbox that should intersect the linestring's envelope
	results := idx.SearchIntersects(builtin.BoundingBox{MinX: 3, MinY: 3, MaxX: 7, MaxY: 7})
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	// Search with a bbox that should NOT intersect
	results = idx.SearchIntersects(builtin.BoundingBox{MinX: 15, MinY: 15, MaxX: 20, MaxY: 20})
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
