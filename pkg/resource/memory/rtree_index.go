package memory

import (
	"fmt"
	"math"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

const (
	rtreeMinEntries = 2
	rtreeMaxEntries = 8
)

// RTreeIndex is a spatial index using the R-Tree data structure.
// It implements the Index interface for compatibility with the existing
// index management system.
type RTreeIndex struct {
	info *IndexInfo
	root *rtreeNode
	size int
	mu   sync.RWMutex
}

type rtreeNode struct {
	bbox     builtin.BoundingBox
	children []*rtreeNode  // non-nil for internal nodes
	entries  []rtreeEntry  // non-nil for leaf nodes
	isLeaf   bool
}

type rtreeEntry struct {
	bbox  builtin.BoundingBox
	rowID int64
}

// NewRTreeIndex creates a new R-Tree spatial index.
func NewRTreeIndex(tableName, columnName string) *RTreeIndex {
	return &RTreeIndex{
		info: &IndexInfo{
			Name:      fmt.Sprintf("idx_sp_%s_%s", tableName, columnName),
			TableName: tableName,
			Columns:   []string{columnName},
			Type:      IndexTypeSpatialRTree,
			Unique:    false,
		},
		root: &rtreeNode{isLeaf: true},
		size: 0,
	}
}

// Insert adds geometry entries to the R-Tree.
// The key can be a Geometry (uses its Envelope) or a BoundingBox.
func (idx *RTreeIndex) Insert(key interface{}, rowIDs []int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	bbox, err := toBoundingBox(key)
	if err != nil {
		return fmt.Errorf("RTreeIndex.Insert: %w", err)
	}

	for _, rowID := range rowIDs {
		entry := rtreeEntry{bbox: bbox, rowID: rowID}
		idx.root = idx.insert(idx.root, entry)
		idx.size++
	}
	return nil
}

// Delete removes entries matching the key from the R-Tree.
func (idx *RTreeIndex) Delete(key interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	bbox, err := toBoundingBox(key)
	if err != nil {
		return fmt.Errorf("RTreeIndex.Delete: %w", err)
	}

	idx.root, _ = idx.delete(idx.root, bbox)
	return nil
}

// Find returns row IDs with bounding boxes that exactly match the key's bounding box.
func (idx *RTreeIndex) Find(key interface{}) ([]int64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bbox, err := toBoundingBox(key)
	if err != nil {
		return nil, false
	}

	var results []int64
	idx.searchExact(idx.root, bbox, &results)
	return results, len(results) > 0
}

// FindRange returns row IDs whose bounding boxes intersect with the query range.
// min and max should be BoundingBox or Geometry values; the intersection of their
// envelopes defines the query window.
func (idx *RTreeIndex) FindRange(min, max interface{}) ([]int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	bboxMin, err := toBoundingBox(min)
	if err != nil {
		return nil, fmt.Errorf("RTreeIndex.FindRange: min: %w", err)
	}
	bboxMax, err := toBoundingBox(max)
	if err != nil {
		return nil, fmt.Errorf("RTreeIndex.FindRange: max: %w", err)
	}

	queryBBox := bboxMin.Expand(bboxMax)
	var results []int64
	idx.searchIntersects(idx.root, queryBBox, &results)
	return results, nil
}

// SearchIntersects returns all row IDs whose bounding boxes intersect with the given bbox.
func (idx *RTreeIndex) SearchIntersects(bbox builtin.BoundingBox) []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []int64
	idx.searchIntersects(idx.root, bbox, &results)
	return results
}

// SearchContains returns all row IDs whose bounding boxes are fully contained
// within the given bbox.
func (idx *RTreeIndex) SearchContains(bbox builtin.BoundingBox) []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []int64
	idx.searchContains(idx.root, bbox, &results)
	return results
}

// GetIndexInfo returns the index metadata.
func (idx *RTreeIndex) GetIndexInfo() *IndexInfo {
	return idx.info
}

// Size returns the number of entries in the index.
func (idx *RTreeIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.size
}

// Reset clears the R-Tree for rebuilding.
func (idx *RTreeIndex) Reset() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.root = &rtreeNode{isLeaf: true}
	idx.size = 0
}

// ==================== R-Tree Core Operations ====================

// insert adds an entry to the tree, potentially splitting nodes and returning a new root.
func (idx *RTreeIndex) insert(node *rtreeNode, entry rtreeEntry) *rtreeNode {
	if node.isLeaf {
		node.entries = append(node.entries, entry)
		node.bbox = recalcBBox(node)
		if len(node.entries) > rtreeMaxEntries {
			left, right := idx.splitLeaf(node)
			newRoot := &rtreeNode{
				isLeaf:   false,
				children: []*rtreeNode{left, right},
			}
			newRoot.bbox = recalcBBox(newRoot)
			return newRoot
		}
		return node
	}

	// Choose the child that needs the least enlargement
	bestIdx := 0
	bestEnlargement := math.MaxFloat64
	bestArea := math.MaxFloat64
	for i, child := range node.children {
		enlargement := child.bbox.Enlargement(entry.bbox)
		area := child.bbox.Area()
		if enlargement < bestEnlargement || (enlargement == bestEnlargement && area < bestArea) {
			bestEnlargement = enlargement
			bestArea = area
			bestIdx = i
		}
	}

	node.children[bestIdx] = idx.insert(node.children[bestIdx], entry)
	node.bbox = recalcBBox(node)

	if len(node.children) > rtreeMaxEntries {
		left, right := idx.splitInternal(node)
		newRoot := &rtreeNode{
			isLeaf:   false,
			children: []*rtreeNode{left, right},
		}
		newRoot.bbox = recalcBBox(newRoot)
		return newRoot
	}
	return node
}

// delete removes entries matching the bbox from the tree.
func (idx *RTreeIndex) delete(node *rtreeNode, bbox builtin.BoundingBox) (*rtreeNode, int) {
	if node == nil {
		return nil, 0
	}

	if node.isLeaf {
		removed := 0
		newEntries := node.entries[:0]
		for _, e := range node.entries {
			if e.bbox.MinX == bbox.MinX && e.bbox.MinY == bbox.MinY &&
				e.bbox.MaxX == bbox.MaxX && e.bbox.MaxY == bbox.MaxY {
				removed++
				idx.size--
			} else {
				newEntries = append(newEntries, e)
			}
		}
		node.entries = newEntries
		if len(node.entries) == 0 {
			return nil, removed
		}
		node.bbox = recalcBBox(node)
		return node, removed
	}

	// Internal node: recurse into children that might contain the bbox
	totalRemoved := 0
	newChildren := node.children[:0]
	for _, child := range node.children {
		if child.bbox.Intersects(bbox) {
			child, removed := idx.delete(child, bbox)
			totalRemoved += removed
			if child != nil {
				newChildren = append(newChildren, child)
			}
		} else {
			newChildren = append(newChildren, child)
		}
	}
	node.children = newChildren

	if len(node.children) == 0 {
		return nil, totalRemoved
	}
	if len(node.children) == 1 {
		return node.children[0], totalRemoved
	}
	node.bbox = recalcBBox(node)
	return node, totalRemoved
}

// searchExact finds entries with exactly matching bounding boxes.
func (idx *RTreeIndex) searchExact(node *rtreeNode, bbox builtin.BoundingBox, results *[]int64) {
	if node == nil {
		return
	}

	if node.isLeaf {
		for _, e := range node.entries {
			if e.bbox.MinX == bbox.MinX && e.bbox.MinY == bbox.MinY &&
				e.bbox.MaxX == bbox.MaxX && e.bbox.MaxY == bbox.MaxY {
				*results = append(*results, e.rowID)
			}
		}
		return
	}

	for _, child := range node.children {
		if child.bbox.Intersects(bbox) {
			idx.searchExact(child, bbox, results)
		}
	}
}

// searchIntersects finds all entries whose bounding boxes intersect with the query bbox.
func (idx *RTreeIndex) searchIntersects(node *rtreeNode, bbox builtin.BoundingBox, results *[]int64) {
	if node == nil {
		return
	}

	if node.isLeaf {
		for _, e := range node.entries {
			if e.bbox.Intersects(bbox) {
				*results = append(*results, e.rowID)
			}
		}
		return
	}

	for _, child := range node.children {
		if child.bbox.Intersects(bbox) {
			idx.searchIntersects(child, bbox, results)
		}
	}
}

// searchContains finds all entries whose bounding boxes are fully within the query bbox.
func (idx *RTreeIndex) searchContains(node *rtreeNode, bbox builtin.BoundingBox, results *[]int64) {
	if node == nil {
		return
	}

	if node.isLeaf {
		for _, e := range node.entries {
			if bbox.Contains(e.bbox) {
				*results = append(*results, e.rowID)
			}
		}
		return
	}

	for _, child := range node.children {
		if child.bbox.Intersects(bbox) {
			idx.searchContains(child, bbox, results)
		}
	}
}

// ==================== Node Splitting (Quadratic Split) ====================

func (idx *RTreeIndex) splitLeaf(node *rtreeNode) (*rtreeNode, *rtreeNode) {
	entries := node.entries
	seed1, seed2 := pickSeedsEntries(entries)

	left := &rtreeNode{isLeaf: true, entries: []rtreeEntry{entries[seed1]}}
	right := &rtreeNode{isLeaf: true, entries: []rtreeEntry{entries[seed2]}}
	left.bbox = entries[seed1].bbox
	right.bbox = entries[seed2].bbox

	// Distribute remaining entries
	for i, e := range entries {
		if i == seed1 || i == seed2 {
			continue
		}
		// Ensure minimum entries
		if len(left.entries)+len(entries)-i-1 <= rtreeMinEntries {
			left.entries = append(left.entries, e)
			left.bbox = left.bbox.Expand(e.bbox)
			continue
		}
		if len(right.entries)+len(entries)-i-1 <= rtreeMinEntries {
			right.entries = append(right.entries, e)
			right.bbox = right.bbox.Expand(e.bbox)
			continue
		}

		enlargeLeft := left.bbox.Enlargement(e.bbox)
		enlargeRight := right.bbox.Enlargement(e.bbox)
		if enlargeLeft < enlargeRight {
			left.entries = append(left.entries, e)
			left.bbox = left.bbox.Expand(e.bbox)
		} else if enlargeRight < enlargeLeft {
			right.entries = append(right.entries, e)
			right.bbox = right.bbox.Expand(e.bbox)
		} else if left.bbox.Area() <= right.bbox.Area() {
			left.entries = append(left.entries, e)
			left.bbox = left.bbox.Expand(e.bbox)
		} else {
			right.entries = append(right.entries, e)
			right.bbox = right.bbox.Expand(e.bbox)
		}
	}

	return left, right
}

func (idx *RTreeIndex) splitInternal(node *rtreeNode) (*rtreeNode, *rtreeNode) {
	children := node.children
	seed1, seed2 := pickSeedsChildren(children)

	left := &rtreeNode{isLeaf: false, children: []*rtreeNode{children[seed1]}}
	right := &rtreeNode{isLeaf: false, children: []*rtreeNode{children[seed2]}}
	left.bbox = children[seed1].bbox
	right.bbox = children[seed2].bbox

	for i, child := range children {
		if i == seed1 || i == seed2 {
			continue
		}
		if len(left.children)+len(children)-i-1 <= rtreeMinEntries {
			left.children = append(left.children, child)
			left.bbox = left.bbox.Expand(child.bbox)
			continue
		}
		if len(right.children)+len(children)-i-1 <= rtreeMinEntries {
			right.children = append(right.children, child)
			right.bbox = right.bbox.Expand(child.bbox)
			continue
		}

		enlargeLeft := left.bbox.Enlargement(child.bbox)
		enlargeRight := right.bbox.Enlargement(child.bbox)
		if enlargeLeft < enlargeRight {
			left.children = append(left.children, child)
			left.bbox = left.bbox.Expand(child.bbox)
		} else if enlargeRight < enlargeLeft {
			right.children = append(right.children, child)
			right.bbox = right.bbox.Expand(child.bbox)
		} else if left.bbox.Area() <= right.bbox.Area() {
			left.children = append(left.children, child)
			left.bbox = left.bbox.Expand(child.bbox)
		} else {
			right.children = append(right.children, child)
			right.bbox = right.bbox.Expand(child.bbox)
		}
	}

	return left, right
}

// pickSeedsEntries selects two entries that would waste the most area if grouped together.
func pickSeedsEntries(entries []rtreeEntry) (int, int) {
	maxWaste := -math.MaxFloat64
	s1, s2 := 0, 1
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			combined := entries[i].bbox.Expand(entries[j].bbox)
			waste := combined.Area() - entries[i].bbox.Area() - entries[j].bbox.Area()
			if waste > maxWaste {
				maxWaste = waste
				s1, s2 = i, j
			}
		}
	}
	return s1, s2
}

// pickSeedsChildren selects two children that would waste the most area.
func pickSeedsChildren(children []*rtreeNode) (int, int) {
	maxWaste := -math.MaxFloat64
	s1, s2 := 0, 1
	for i := 0; i < len(children); i++ {
		for j := i + 1; j < len(children); j++ {
			combined := children[i].bbox.Expand(children[j].bbox)
			waste := combined.Area() - children[i].bbox.Area() - children[j].bbox.Area()
			if waste > maxWaste {
				maxWaste = waste
				s1, s2 = i, j
			}
		}
	}
	return s1, s2
}

// ==================== Helpers ====================

func recalcBBox(node *rtreeNode) builtin.BoundingBox {
	if node.isLeaf {
		if len(node.entries) == 0 {
			return builtin.BoundingBox{}
		}
		bb := node.entries[0].bbox
		for _, e := range node.entries[1:] {
			bb = bb.Expand(e.bbox)
		}
		return bb
	}

	if len(node.children) == 0 {
		return builtin.BoundingBox{}
	}
	bb := node.children[0].bbox
	for _, c := range node.children[1:] {
		bb = bb.Expand(c.bbox)
	}
	return bb
}

func toBoundingBox(key interface{}) (builtin.BoundingBox, error) {
	switch v := key.(type) {
	case builtin.BoundingBox:
		return v, nil
	case *builtin.BoundingBox:
		return *v, nil
	case builtin.Geometry:
		return v.Envelope(), nil
	case *builtin.GeoPoint:
		return v.Envelope(), nil
	case *builtin.GeoLineString:
		return v.Envelope(), nil
	case *builtin.GeoPolygon:
		return v.Envelope(), nil
	case *builtin.GeoMultiPoint:
		return v.Envelope(), nil
	case *builtin.GeoMultiLineString:
		return v.Envelope(), nil
	case *builtin.GeoMultiPolygon:
		return v.Envelope(), nil
	case *builtin.GeoCollection:
		return v.Envelope(), nil
	default:
		return builtin.BoundingBox{}, fmt.Errorf("cannot convert %T to BoundingBox", key)
	}
}
