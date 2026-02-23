package builtin

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ==================== Geometry Interface ====================

// Geometry is the interface all spatial types implement.
type Geometry interface {
	GeometryType() string  // "Point", "LineString", "Polygon", etc.
	Dimension() int        // 0=Point, 1=Line, 2=Polygon
	SRID() int             // Spatial Reference ID (0=Cartesian, 4326=WGS84)
	IsEmpty() bool         // Whether the geometry has no coordinates
	Envelope() BoundingBox // Minimum Bounding Rectangle
	WKT() string           // Serialize to Well-Known Text
	Equals(other Geometry) bool
	NumPoints() int // Total number of coordinate points
}

// ==================== Bounding Box ====================

// BoundingBox represents a minimum bounding rectangle for spatial queries.
type BoundingBox struct {
	MinX, MinY, MaxX, MaxY float64
}

// Intersects returns true if this box overlaps with another box.
func (b BoundingBox) Intersects(other BoundingBox) bool {
	return b.MinX <= other.MaxX && b.MaxX >= other.MinX &&
		b.MinY <= other.MaxY && b.MaxY >= other.MinY
}

// Contains returns true if this box fully contains another box.
func (b BoundingBox) Contains(other BoundingBox) bool {
	return b.MinX <= other.MinX && b.MaxX >= other.MaxX &&
		b.MinY <= other.MinY && b.MaxY >= other.MaxY
}

// ContainsPoint returns true if this box contains a point.
func (b BoundingBox) ContainsPoint(x, y float64) bool {
	return x >= b.MinX && x <= b.MaxX && y >= b.MinY && y <= b.MaxY
}

// Expand returns a new BoundingBox that contains both this and the other box.
func (b BoundingBox) Expand(other BoundingBox) BoundingBox {
	return BoundingBox{
		MinX: math.Min(b.MinX, other.MinX),
		MinY: math.Min(b.MinY, other.MinY),
		MaxX: math.Max(b.MaxX, other.MaxX),
		MaxY: math.Max(b.MaxY, other.MaxY),
	}
}

// Area returns the area of this bounding box.
func (b BoundingBox) Area() float64 {
	return (b.MaxX - b.MinX) * (b.MaxY - b.MinY)
}

// Enlargement returns the additional area needed to include the other box.
func (b BoundingBox) Enlargement(other BoundingBox) float64 {
	return b.Expand(other).Area() - b.Area()
}

// IsZero returns true if the bounding box is uninitialized.
func (b BoundingBox) IsZero() bool {
	return b.MinX == 0 && b.MinY == 0 && b.MaxX == 0 && b.MaxY == 0
}

// ToPolygon converts the bounding box to a Polygon geometry.
func (b BoundingBox) ToPolygon() *GeoPolygon {
	return &GeoPolygon{
		Rings: [][]GeoPoint{
			{
				{X: b.MinX, Y: b.MinY},
				{X: b.MaxX, Y: b.MinY},
				{X: b.MaxX, Y: b.MaxY},
				{X: b.MinX, Y: b.MaxY},
				{X: b.MinX, Y: b.MinY},
			},
		},
	}
}

// ==================== Point ====================

// GeoPoint represents a 2D point.
type GeoPoint struct {
	X, Y float64
	Srid int
}

func (p *GeoPoint) GeometryType() string { return "Point" }
func (p *GeoPoint) Dimension() int       { return 0 }
func (p *GeoPoint) SRID() int            { return p.Srid }
func (p *GeoPoint) IsEmpty() bool        { return math.IsNaN(p.X) && math.IsNaN(p.Y) }
func (p *GeoPoint) NumPoints() int       { return 1 }

func (p *GeoPoint) Envelope() BoundingBox {
	return BoundingBox{MinX: p.X, MinY: p.Y, MaxX: p.X, MaxY: p.Y}
}

func (p *GeoPoint) WKT() string {
	if p.IsEmpty() {
		return "POINT EMPTY"
	}
	return fmt.Sprintf("POINT(%s %s)", formatCoord(p.X), formatCoord(p.Y))
}

func (p *GeoPoint) Equals(other Geometry) bool {
	o, ok := other.(*GeoPoint)
	if !ok {
		return false
	}
	return p.X == o.X && p.Y == o.Y
}

// DistanceTo calculates the Euclidean distance to another point.
func (p *GeoPoint) DistanceTo(other *GeoPoint) float64 {
	dx := p.X - other.X
	dy := p.Y - other.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// ==================== LineString ====================

// GeoLineString represents an ordered sequence of points forming a line.
type GeoLineString struct {
	Points []GeoPoint
	Srid   int
}

func (l *GeoLineString) GeometryType() string { return "LineString" }
func (l *GeoLineString) Dimension() int       { return 1 }
func (l *GeoLineString) SRID() int            { return l.Srid }
func (l *GeoLineString) IsEmpty() bool        { return len(l.Points) == 0 }
func (l *GeoLineString) NumPoints() int       { return len(l.Points) }

func (l *GeoLineString) Envelope() BoundingBox {
	if l.IsEmpty() {
		return BoundingBox{}
	}
	bb := BoundingBox{
		MinX: l.Points[0].X, MinY: l.Points[0].Y,
		MaxX: l.Points[0].X, MaxY: l.Points[0].Y,
	}
	for _, p := range l.Points[1:] {
		bb.MinX = math.Min(bb.MinX, p.X)
		bb.MinY = math.Min(bb.MinY, p.Y)
		bb.MaxX = math.Max(bb.MaxX, p.X)
		bb.MaxY = math.Max(bb.MaxY, p.Y)
	}
	return bb
}

func (l *GeoLineString) WKT() string {
	if l.IsEmpty() {
		return "LINESTRING EMPTY"
	}
	return "LINESTRING(" + formatPointList(l.Points) + ")"
}

func (l *GeoLineString) Equals(other Geometry) bool {
	o, ok := other.(*GeoLineString)
	if !ok || len(l.Points) != len(o.Points) {
		return false
	}
	for i := range l.Points {
		if l.Points[i].X != o.Points[i].X || l.Points[i].Y != o.Points[i].Y {
			return false
		}
	}
	return true
}

// Length returns the total length of the line string.
func (l *GeoLineString) Length() float64 {
	length := 0.0
	for i := 1; i < len(l.Points); i++ {
		length += l.Points[i-1].DistanceTo(&l.Points[i])
	}
	return length
}

// ==================== Polygon ====================

// GeoPolygon represents a polygon with an exterior ring and optional holes.
type GeoPolygon struct {
	Rings [][]GeoPoint // Rings[0] = exterior, Rings[1:] = holes
	Srid  int
}

func (p *GeoPolygon) GeometryType() string { return "Polygon" }
func (p *GeoPolygon) Dimension() int       { return 2 }
func (p *GeoPolygon) SRID() int            { return p.Srid }
func (p *GeoPolygon) IsEmpty() bool        { return len(p.Rings) == 0 || len(p.Rings[0]) == 0 }

func (p *GeoPolygon) NumPoints() int {
	n := 0
	for _, ring := range p.Rings {
		n += len(ring)
	}
	return n
}

func (p *GeoPolygon) Envelope() BoundingBox {
	if p.IsEmpty() {
		return BoundingBox{}
	}
	ring := p.Rings[0]
	bb := BoundingBox{
		MinX: ring[0].X, MinY: ring[0].Y,
		MaxX: ring[0].X, MaxY: ring[0].Y,
	}
	for _, pt := range ring[1:] {
		bb.MinX = math.Min(bb.MinX, pt.X)
		bb.MinY = math.Min(bb.MinY, pt.Y)
		bb.MaxX = math.Max(bb.MaxX, pt.X)
		bb.MaxY = math.Max(bb.MaxY, pt.Y)
	}
	return bb
}

func (p *GeoPolygon) WKT() string {
	if p.IsEmpty() {
		return "POLYGON EMPTY"
	}
	rings := make([]string, len(p.Rings))
	for i, ring := range p.Rings {
		rings[i] = "(" + formatPointList(ring) + ")"
	}
	return "POLYGON(" + strings.Join(rings, ", ") + ")"
}

func (p *GeoPolygon) Equals(other Geometry) bool {
	o, ok := other.(*GeoPolygon)
	if !ok || len(p.Rings) != len(o.Rings) {
		return false
	}
	for i := range p.Rings {
		if len(p.Rings[i]) != len(o.Rings[i]) {
			return false
		}
		for j := range p.Rings[i] {
			if p.Rings[i][j].X != o.Rings[i][j].X || p.Rings[i][j].Y != o.Rings[i][j].Y {
				return false
			}
		}
	}
	return true
}

// Area computes the area using the Shoelace formula.
// Holes are subtracted from the exterior ring area.
func (p *GeoPolygon) Area() float64 {
	if p.IsEmpty() {
		return 0
	}
	area := math.Abs(ringArea(p.Rings[0]))
	for _, hole := range p.Rings[1:] {
		area -= math.Abs(ringArea(hole))
	}
	return math.Abs(area)
}

// Perimeter returns the total perimeter of the polygon (exterior + holes).
func (p *GeoPolygon) Perimeter() float64 {
	total := 0.0
	for _, ring := range p.Rings {
		for i := 1; i < len(ring); i++ {
			total += ring[i-1].DistanceTo(&ring[i])
		}
	}
	return total
}

// ContainsPoint tests if a point is inside the polygon using ray casting.
func (p *GeoPolygon) ContainsPoint(x, y float64) bool {
	if p.IsEmpty() {
		return false
	}
	// Must be inside exterior ring
	if !pointInRing(x, y, p.Rings[0]) {
		return false
	}
	// Must not be inside any hole
	for _, hole := range p.Rings[1:] {
		if pointInRing(x, y, hole) {
			return false
		}
	}
	return true
}

// ==================== MultiPoint ====================

// GeoMultiPoint is a collection of points.
type GeoMultiPoint struct {
	Points []GeoPoint
	Srid   int
}

func (m *GeoMultiPoint) GeometryType() string { return "MultiPoint" }
func (m *GeoMultiPoint) Dimension() int       { return 0 }
func (m *GeoMultiPoint) SRID() int            { return m.Srid }
func (m *GeoMultiPoint) IsEmpty() bool        { return len(m.Points) == 0 }
func (m *GeoMultiPoint) NumPoints() int       { return len(m.Points) }

func (m *GeoMultiPoint) Envelope() BoundingBox {
	if m.IsEmpty() {
		return BoundingBox{}
	}
	bb := BoundingBox{
		MinX: m.Points[0].X, MinY: m.Points[0].Y,
		MaxX: m.Points[0].X, MaxY: m.Points[0].Y,
	}
	for _, p := range m.Points[1:] {
		bb.MinX = math.Min(bb.MinX, p.X)
		bb.MinY = math.Min(bb.MinY, p.Y)
		bb.MaxX = math.Max(bb.MaxX, p.X)
		bb.MaxY = math.Max(bb.MaxY, p.Y)
	}
	return bb
}

func (m *GeoMultiPoint) WKT() string {
	if m.IsEmpty() {
		return "MULTIPOINT EMPTY"
	}
	parts := make([]string, len(m.Points))
	for i, p := range m.Points {
		parts[i] = fmt.Sprintf("(%s %s)", formatCoord(p.X), formatCoord(p.Y))
	}
	return "MULTIPOINT(" + strings.Join(parts, ", ") + ")"
}

func (m *GeoMultiPoint) Equals(other Geometry) bool {
	o, ok := other.(*GeoMultiPoint)
	if !ok || len(m.Points) != len(o.Points) {
		return false
	}
	for i := range m.Points {
		if m.Points[i].X != o.Points[i].X || m.Points[i].Y != o.Points[i].Y {
			return false
		}
	}
	return true
}

// ==================== MultiLineString ====================

// GeoMultiLineString is a collection of line strings.
type GeoMultiLineString struct {
	Lines []GeoLineString
	Srid  int
}

func (m *GeoMultiLineString) GeometryType() string { return "MultiLineString" }
func (m *GeoMultiLineString) Dimension() int       { return 1 }
func (m *GeoMultiLineString) SRID() int            { return m.Srid }
func (m *GeoMultiLineString) IsEmpty() bool        { return len(m.Lines) == 0 }

func (m *GeoMultiLineString) NumPoints() int {
	n := 0
	for i := range m.Lines {
		n += m.Lines[i].NumPoints()
	}
	return n
}

func (m *GeoMultiLineString) Envelope() BoundingBox {
	if m.IsEmpty() {
		return BoundingBox{}
	}
	bb := m.Lines[0].Envelope()
	for _, l := range m.Lines[1:] {
		bb = bb.Expand(l.Envelope())
	}
	return bb
}

func (m *GeoMultiLineString) WKT() string {
	if m.IsEmpty() {
		return "MULTILINESTRING EMPTY"
	}
	parts := make([]string, len(m.Lines))
	for i, l := range m.Lines {
		parts[i] = "(" + formatPointList(l.Points) + ")"
	}
	return "MULTILINESTRING(" + strings.Join(parts, ", ") + ")"
}

func (m *GeoMultiLineString) Equals(other Geometry) bool {
	o, ok := other.(*GeoMultiLineString)
	if !ok || len(m.Lines) != len(o.Lines) {
		return false
	}
	for i := range m.Lines {
		if !m.Lines[i].Equals(&o.Lines[i]) {
			return false
		}
	}
	return true
}

// ==================== MultiPolygon ====================

// GeoMultiPolygon is a collection of polygons.
type GeoMultiPolygon struct {
	Polygons []GeoPolygon
	Srid     int
}

func (m *GeoMultiPolygon) GeometryType() string { return "MultiPolygon" }
func (m *GeoMultiPolygon) Dimension() int       { return 2 }
func (m *GeoMultiPolygon) SRID() int            { return m.Srid }
func (m *GeoMultiPolygon) IsEmpty() bool        { return len(m.Polygons) == 0 }

func (m *GeoMultiPolygon) NumPoints() int {
	n := 0
	for i := range m.Polygons {
		n += m.Polygons[i].NumPoints()
	}
	return n
}

func (m *GeoMultiPolygon) Envelope() BoundingBox {
	if m.IsEmpty() {
		return BoundingBox{}
	}
	bb := m.Polygons[0].Envelope()
	for _, p := range m.Polygons[1:] {
		bb = bb.Expand(p.Envelope())
	}
	return bb
}

func (m *GeoMultiPolygon) WKT() string {
	if m.IsEmpty() {
		return "MULTIPOLYGON EMPTY"
	}
	parts := make([]string, len(m.Polygons))
	for i, poly := range m.Polygons {
		rings := make([]string, len(poly.Rings))
		for j, ring := range poly.Rings {
			rings[j] = "(" + formatPointList(ring) + ")"
		}
		parts[i] = "(" + strings.Join(rings, ", ") + ")"
	}
	return "MULTIPOLYGON(" + strings.Join(parts, ", ") + ")"
}

func (m *GeoMultiPolygon) Equals(other Geometry) bool {
	o, ok := other.(*GeoMultiPolygon)
	if !ok || len(m.Polygons) != len(o.Polygons) {
		return false
	}
	for i := range m.Polygons {
		if !m.Polygons[i].Equals(&o.Polygons[i]) {
			return false
		}
	}
	return true
}

// ==================== GeometryCollection ====================

// GeoCollection is a heterogeneous collection of geometries.
type GeoCollection struct {
	Geometries []Geometry
	Srid       int
}

func (c *GeoCollection) GeometryType() string { return "GeometryCollection" }
func (c *GeoCollection) Dimension() int {
	maxDim := -1
	for _, g := range c.Geometries {
		if d := g.Dimension(); d > maxDim {
			maxDim = d
		}
	}
	if maxDim < 0 {
		return 0
	}
	return maxDim
}
func (c *GeoCollection) SRID() int     { return c.Srid }
func (c *GeoCollection) IsEmpty() bool { return len(c.Geometries) == 0 }

func (c *GeoCollection) NumPoints() int {
	n := 0
	for _, g := range c.Geometries {
		n += g.NumPoints()
	}
	return n
}

func (c *GeoCollection) Envelope() BoundingBox {
	if c.IsEmpty() {
		return BoundingBox{}
	}
	bb := c.Geometries[0].Envelope()
	for _, g := range c.Geometries[1:] {
		bb = bb.Expand(g.Envelope())
	}
	return bb
}

func (c *GeoCollection) WKT() string {
	if c.IsEmpty() {
		return "GEOMETRYCOLLECTION EMPTY"
	}
	parts := make([]string, len(c.Geometries))
	for i, g := range c.Geometries {
		parts[i] = g.WKT()
	}
	return "GEOMETRYCOLLECTION(" + strings.Join(parts, ", ") + ")"
}

func (c *GeoCollection) Equals(other Geometry) bool {
	o, ok := other.(*GeoCollection)
	if !ok || len(c.Geometries) != len(o.Geometries) {
		return false
	}
	for i := range c.Geometries {
		if !c.Geometries[i].Equals(o.Geometries[i]) {
			return false
		}
	}
	return true
}

// ==================== WKT Parser ====================

// ParseWKT parses a Well-Known Text string into a Geometry.
func ParseWKT(wkt string) (Geometry, error) {
	wkt = strings.TrimSpace(wkt)
	if wkt == "" {
		return nil, fmt.Errorf("empty WKT string")
	}

	p := &wktParser{input: wkt, pos: 0}
	geom, err := p.parseGeometry()
	if err != nil {
		return nil, fmt.Errorf("WKT parse error: %w", err)
	}
	return geom, nil
}

type wktParser struct {
	input string
	pos   int
}

func (p *wktParser) parseGeometry() (Geometry, error) {
	p.skipWhitespace()
	typeName := p.readWord()
	upperType := strings.ToUpper(typeName)

	p.skipWhitespace()

	// Check for EMPTY
	if p.peekWord() == "EMPTY" {
		p.readWord() // consume "EMPTY"
		return p.emptyGeometry(upperType)
	}

	switch upperType {
	case "POINT":
		return p.parsePoint()
	case "LINESTRING":
		return p.parseLineString()
	case "POLYGON":
		return p.parsePolygon()
	case "MULTIPOINT":
		return p.parseMultiPoint()
	case "MULTILINESTRING":
		return p.parseMultiLineString()
	case "MULTIPOLYGON":
		return p.parseMultiPolygon()
	case "GEOMETRYCOLLECTION":
		return p.parseGeometryCollection()
	default:
		return nil, fmt.Errorf("unknown geometry type: %s", typeName)
	}
}

func (p *wktParser) emptyGeometry(typeName string) (Geometry, error) {
	switch typeName {
	case "POINT":
		return &GeoPoint{X: math.NaN(), Y: math.NaN()}, nil
	case "LINESTRING":
		return &GeoLineString{}, nil
	case "POLYGON":
		return &GeoPolygon{}, nil
	case "MULTIPOINT":
		return &GeoMultiPoint{}, nil
	case "MULTILINESTRING":
		return &GeoMultiLineString{}, nil
	case "MULTIPOLYGON":
		return &GeoMultiPolygon{}, nil
	case "GEOMETRYCOLLECTION":
		return &GeoCollection{}, nil
	default:
		return nil, fmt.Errorf("unknown geometry type: %s", typeName)
	}
}

func (p *wktParser) parsePoint() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}
	x, y, err := p.readCoordinate()
	if err != nil {
		return nil, err
	}
	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoPoint{X: x, Y: y}, nil
}

func (p *wktParser) parseLineString() (Geometry, error) {
	points, err := p.readPointList()
	if err != nil {
		return nil, err
	}
	if len(points) < 2 {
		return nil, fmt.Errorf("LINESTRING requires at least 2 points")
	}
	return &GeoLineString{Points: points}, nil
}

func (p *wktParser) parsePolygon() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var rings [][]GeoPoint
	for {
		ring, err := p.readPointList()
		if err != nil {
			return nil, err
		}
		rings = append(rings, ring)
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
			p.skipWhitespace()
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoPolygon{Rings: rings}, nil
}

func (p *wktParser) parseMultiPoint() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var points []GeoPoint
	for {
		p.skipWhitespace()
		// MultiPoint supports both (x y) and x y notation
		if p.pos < len(p.input) && p.input[p.pos] == '(' {
			p.pos++ // consume '('
			x, y, err := p.readCoordinate()
			if err != nil {
				return nil, err
			}
			points = append(points, GeoPoint{X: x, Y: y})
			if err := p.expect(')'); err != nil {
				return nil, err
			}
		} else {
			x, y, err := p.readCoordinate()
			if err != nil {
				return nil, err
			}
			points = append(points, GeoPoint{X: x, Y: y})
		}

		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoMultiPoint{Points: points}, nil
}

func (p *wktParser) parseMultiLineString() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var lines []GeoLineString
	for {
		pts, err := p.readPointList()
		if err != nil {
			return nil, err
		}
		lines = append(lines, GeoLineString{Points: pts})
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
			p.skipWhitespace()
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoMultiLineString{Lines: lines}, nil
}

func (p *wktParser) parseMultiPolygon() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var polygons []GeoPolygon
	for {
		p.skipWhitespace()
		if err := p.expect('('); err != nil {
			return nil, err
		}

		var rings [][]GeoPoint
		for {
			ring, err := p.readPointList()
			if err != nil {
				return nil, err
			}
			rings = append(rings, ring)
			p.skipWhitespace()
			if p.pos < len(p.input) && p.input[p.pos] == ',' {
				p.pos++
				p.skipWhitespace()
			} else {
				break
			}
		}

		if err := p.expect(')'); err != nil {
			return nil, err
		}
		polygons = append(polygons, GeoPolygon{Rings: rings})

		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
			p.skipWhitespace()
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoMultiPolygon{Polygons: polygons}, nil
}

func (p *wktParser) parseGeometryCollection() (Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var geoms []Geometry
	for {
		p.skipWhitespace()
		g, err := p.parseGeometry()
		if err != nil {
			return nil, err
		}
		geoms = append(geoms, g)
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return &GeoCollection{Geometries: geoms}, nil
}

// readPointList reads "(x1 y1, x2 y2, ...)"
func (p *wktParser) readPointList() ([]GeoPoint, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var points []GeoPoint
	for {
		x, y, err := p.readCoordinate()
		if err != nil {
			return nil, err
		}
		points = append(points, GeoPoint{X: x, Y: y})
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ',' {
			p.pos++
			p.skipWhitespace()
		} else {
			break
		}
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}
	return points, nil
}

func (p *wktParser) readCoordinate() (float64, float64, error) {
	p.skipWhitespace()
	xStr := p.readNumber()
	if xStr == "" {
		return 0, 0, fmt.Errorf("expected X coordinate at position %d", p.pos)
	}
	x, err := strconv.ParseFloat(xStr, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid X coordinate %q: %w", xStr, err)
	}

	p.skipWhitespace()
	yStr := p.readNumber()
	if yStr == "" {
		return 0, 0, fmt.Errorf("expected Y coordinate at position %d", p.pos)
	}
	y, err := strconv.ParseFloat(yStr, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Y coordinate %q: %w", yStr, err)
	}

	return x, y, nil
}

func (p *wktParser) readNumber() string {
	p.skipWhitespace()
	start := p.pos
	if p.pos < len(p.input) && (p.input[p.pos] == '-' || p.input[p.pos] == '+') {
		p.pos++
	}
	for p.pos < len(p.input) && (p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.' || p.input[p.pos] == 'e' || p.input[p.pos] == 'E' || p.input[p.pos] == '-' || p.input[p.pos] == '+') {
		// Handle scientific notation: after 'e'/'E', allow one more sign
		if p.input[p.pos] == '-' || p.input[p.pos] == '+' {
			if p.pos > start+1 && p.input[p.pos-1] != 'e' && p.input[p.pos-1] != 'E' {
				break
			}
		}
		p.pos++
	}
	return p.input[start:p.pos]
}

func (p *wktParser) readWord() string {
	p.skipWhitespace()
	start := p.pos
	for p.pos < len(p.input) && (unicode.IsLetter(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
		p.pos++
	}
	return p.input[start:p.pos]
}

func (p *wktParser) peekWord() string {
	saved := p.pos
	w := p.readWord()
	p.pos = saved
	return strings.ToUpper(w)
}

func (p *wktParser) skipWhitespace() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t' || p.input[p.pos] == '\n' || p.input[p.pos] == '\r') {
		p.pos++
	}
}

func (p *wktParser) expect(ch byte) error {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return fmt.Errorf("expected '%c' but reached end of input", ch)
	}
	if p.input[p.pos] != ch {
		return fmt.Errorf("expected '%c' but got '%c' at position %d", ch, p.input[p.pos], p.pos)
	}
	p.pos++
	return nil
}

// ==================== Spatial Computation Helpers ====================

// ringArea computes the signed area of a ring using the Shoelace formula.
func ringArea(ring []GeoPoint) float64 {
	n := len(ring)
	if n < 3 {
		return 0
	}
	area := 0.0
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		area += ring[i].X * ring[j].Y
		area -= ring[j].X * ring[i].Y
	}
	return area / 2.0
}

// pointInRing tests if a point is inside a ring using the ray casting algorithm.
func pointInRing(x, y float64, ring []GeoPoint) bool {
	n := len(ring)
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		xi, yi := ring[i].X, ring[i].Y
		xj, yj := ring[j].X, ring[j].Y
		if ((yi > y) != (yj > y)) && (x < (xj-xi)*(y-yi)/(yj-yi)+xi) {
			inside = !inside
		}
		j = i
	}
	return inside
}

// PointToSegmentDistance returns the shortest distance from a point to a line segment.
func PointToSegmentDistance(px, py, ax, ay, bx, by float64) float64 {
	dx := bx - ax
	dy := by - ay
	if dx == 0 && dy == 0 {
		// Segment is a point
		return math.Sqrt((px-ax)*(px-ax) + (py-ay)*(py-ay))
	}
	t := ((px-ax)*dx + (py-ay)*dy) / (dx*dx + dy*dy)
	t = math.Max(0, math.Min(1, t))
	projX := ax + t*dx
	projY := ay + t*dy
	return math.Sqrt((px-projX)*(px-projX) + (py-projY)*(py-projY))
}

// SegmentsIntersect tests whether two line segments (p1-p2) and (p3-p4) intersect.
func SegmentsIntersect(p1x, p1y, p2x, p2y, p3x, p3y, p4x, p4y float64) bool {
	d1 := direction(p3x, p3y, p4x, p4y, p1x, p1y)
	d2 := direction(p3x, p3y, p4x, p4y, p2x, p2y)
	d3 := direction(p1x, p1y, p2x, p2y, p3x, p3y)
	d4 := direction(p1x, p1y, p2x, p2y, p4x, p4y)

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}

	if d1 == 0 && onSegment(p3x, p3y, p4x, p4y, p1x, p1y) {
		return true
	}
	if d2 == 0 && onSegment(p3x, p3y, p4x, p4y, p2x, p2y) {
		return true
	}
	if d3 == 0 && onSegment(p1x, p1y, p2x, p2y, p3x, p3y) {
		return true
	}
	if d4 == 0 && onSegment(p1x, p1y, p2x, p2y, p4x, p4y) {
		return true
	}
	return false
}

func direction(ax, ay, bx, by, cx, cy float64) float64 {
	return (cx-ax)*(by-ay) - (cy-ay)*(bx-ax)
}

func onSegment(ax, ay, bx, by, cx, cy float64) bool {
	return math.Min(ax, bx) <= cx && cx <= math.Max(ax, bx) &&
		math.Min(ay, by) <= cy && cy <= math.Max(ay, by)
}

// ==================== Formatting Helpers ====================

func formatCoord(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	return s
}

func formatPointList(points []GeoPoint) string {
	parts := make([]string, len(points))
	for i, p := range points {
		parts[i] = formatCoord(p.X) + " " + formatCoord(p.Y)
	}
	return strings.Join(parts, ", ")
}

// ==================== Geometry Parsing Helper ====================

// ParseGeometryArg converts an arbitrary argument to a Geometry.
// It accepts Geometry objects directly, or WKT strings.
func ParseGeometryArg(arg interface{}) (Geometry, error) {
	switch v := arg.(type) {
	case Geometry:
		return v, nil
	case *GeoPoint:
		return v, nil
	case *GeoLineString:
		return v, nil
	case *GeoPolygon:
		return v, nil
	case *GeoMultiPoint:
		return v, nil
	case *GeoMultiLineString:
		return v, nil
	case *GeoMultiPolygon:
		return v, nil
	case *GeoCollection:
		return v, nil
	case string:
		return ParseWKT(v)
	default:
		return nil, fmt.Errorf("cannot convert %T to Geometry", arg)
	}
}

// IsSpatialColumnType returns true if the column type string is a spatial type.
func IsSpatialColumnType(colType string) bool {
	switch strings.ToUpper(colType) {
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON",
		"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return true
	}
	return false
}
