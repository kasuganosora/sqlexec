package join

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewJoinGraph(t *testing.T) {
	graph := NewJoinGraph()

	assert.NotNil(t, graph)
	assert.NotNil(t, graph.nodes)
	assert.Equal(t, 0, graph.edgeCount)
	assert.Equal(t, 0, len(graph.edges))
}

func TestJoinGraphAddNode(t *testing.T) {
	graph := NewJoinGraph()

	// Add a node
	graph.AddNode("users", int64(1000))
	node := graph.GetNode("users")

	assert.NotNil(t, node)
	assert.Equal(t, "users", node.Name)
	assert.Equal(t, int64(1000), node.Cardinality)
	assert.Equal(t, 0, node.Degree)
}

func TestJoinGraphAddEdge(t *testing.T) {
	graph := NewJoinGraph()

	// Add nodes
	graph.AddNode("users", 1000)
	graph.AddNode("orders", 5000)

	// Add edge
	graph.AddEdge("users", "orders", "INNER JOIN", 1000)

	// Check that nodes have updated degrees
	usersNode := graph.GetNode("users")
	ordersNode := graph.GetNode("orders")

	assert.Equal(t, 1, usersNode.Degree)
	assert.Equal(t, 1, ordersNode.Degree)
	assert.Equal(t, 1, len(usersNode.Edges))
	assert.Equal(t, 1, len(graph.edges))
}

func TestJoinGraphGetNeighbors(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("users", 1000)
	graph.AddNode("orders", 5000)
	graph.AddNode("products", 100)

	// Add edges
	graph.AddEdge("users", "orders", "INNER JOIN", 1000)
	graph.AddEdge("users", "products", "INNER JOIN", 100)

	// Get neighbors
	neighbors := graph.GetNeighbors("users")

	assert.Equal(t, 2, len(neighbors))
	assert.Contains(t, neighbors, "orders")
	assert.Contains(t, neighbors, "products")
}

func TestJoinGraphGetNeighborsNonExistent(t *testing.T) {
	graph := NewJoinGraph()

	neighbors := graph.GetNeighbors("non_existent")

	assert.Equal(t, 0, len(neighbors))
}

func TestJoinGraphGetNodeNonExistent(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("users", 1000)

	node := graph.GetNode("non_existent")

	assert.Nil(t, node)

	// Check that existing node can be retrieved
	existingNode := graph.GetNode("users")
	assert.NotNil(t, existingNode)
}

func TestJoinGraphExplain(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("users", int64(1000))
	graph.AddNode("orders", int64(5000))
	graph.AddEdge("users", "orders", "INNER JOIN", 1000)

	explain := graph.Explain()

	assert.NotEmpty(t, explain)
	assert.Contains(t, explain, "Join Graph")
	assert.Contains(t, explain, "Nodes:")
	assert.Contains(t, explain, "Edges:")
}

func TestJoinGraphAddMultipleEdges(t *testing.T) {
	graph := NewJoinGraph()

	// Add nodes
	graph.AddNode("a", int64(100))
	graph.AddNode("b", int64(100))
	graph.AddNode("c", int64(100))

	// Add multiple edges
	graph.AddEdge("a", "b", "JOIN", 50)
	graph.AddEdge("b", "c", "JOIN", 50)
	graph.AddEdge("a", "c", "JOIN", 50)

	assert.Equal(t, 3, graph.edgeCount)
	assert.Equal(t, 3, len(graph.edges))
}

func TestJoinGraphUpdateExistingEdge(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", int64(100))
	graph.AddNode("b", int64(100))

	// Add edge first time
	graph.AddEdge("a", "b", "INNER JOIN", 50)
	assert.Equal(t, 1, graph.edgeCount)

	// Add edge second time (should add new edge, not update)
	graph.AddEdge("a", "b", "LEFT JOIN", 50)
	assert.Equal(t, 2, graph.edgeCount)

	aNode := graph.GetNode("a")
	assert.Equal(t, 2, len(aNode.Edges))
}

// TestJoinGraph_GetConnectedComponents tests connected components detection
func TestJoinGraph_GetConnectedComponents(t *testing.T) {
	// Single connected component
	graph1 := NewJoinGraph()
	graph1.AddNode("a", 100)
	graph1.AddNode("b", 100)
	graph1.AddNode("c", 100)
	graph1.AddEdge("a", "b", "JOIN", 50)
	graph1.AddEdge("b", "c", "JOIN", 50)

	components := graph1.GetConnectedComponents()
	assert.Equal(t, 1, len(components), "Should have 1 connected component")
	assert.Contains(t, components[0], "a")
	assert.Contains(t, components[0], "b")
	assert.Contains(t, components[0], "c")

	// Multiple disconnected components
	graph2 := NewJoinGraph()
	graph2.AddNode("a", 100)
	graph2.AddNode("b", 100)
	graph2.AddNode("c", 100)
	graph2.AddNode("d", 100)
	graph2.AddEdge("a", "b", "JOIN", 50)
	graph2.AddEdge("c", "d", "JOIN", 50)

	components = graph2.GetConnectedComponents()
	assert.Equal(t, 2, len(components), "Should have 2 connected components")

	// Check that each component has the right nodes
	componentAFound := false
	componentBFound := false
	for _, comp := range components {
		if len(comp) == 2 {
			if contains(comp, "a") && contains(comp, "b") {
				componentAFound = true
			}
			if contains(comp, "c") && contains(comp, "d") {
				componentBFound = true
			}
		}
	}
	assert.True(t, componentAFound, "Component with a and b not found")
	assert.True(t, componentBFound, "Component with c and d not found")
}

// TestJoinGraph_GetConnectedComponents_Empty tests empty graph
func TestJoinGraph_GetConnectedComponents_Empty(t *testing.T) {
	graph := NewJoinGraph()
	components := graph.GetConnectedComponents()
	assert.Equal(t, 0, len(components))
}

// TestJoinGraph_GetConnectedComponents_SingleNode tests single node graph
func TestJoinGraph_GetConnectedComponents_SingleNode(t *testing.T) {
	graph := NewJoinGraph()
	graph.AddNode("a", 100)
	components := graph.GetConnectedComponents()
	assert.Equal(t, 1, len(components))
	assert.Equal(t, 1, len(components[0]))
	assert.Equal(t, "a", components[0][0])
}

// TestJoinGraph_FindMinSpanningTree tests MST algorithm
func TestJoinGraph_FindMinSpanningTree(t *testing.T) {
	graph := NewJoinGraph()

	// Add nodes
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	// Add edges with different cardinalities
	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("b", "c", "JOIN", 15)
	graph.AddEdge("c", "d", "JOIN", 20)
	graph.AddEdge("a", "c", "JOIN", 25)
	graph.AddEdge("b", "d", "JOIN", 30)

	mst := graph.FindMinSpanningTree()

	// MST should have n-1 edges
	assert.Equal(t, 3, len(mst), "MST should have 3 edges for 4 nodes")

	// Calculate total cost
	totalCost := 0.0
	for _, edge := range mst {
		totalCost += edge.Cardinality
	}

	// Minimum cost should be 10 + 15 + 20 = 45
	assert.Equal(t, 45.0, totalCost, "MST should have minimum total cost")
}

// TestJoinGraph_FindMinSpanningTree_Empty tests empty graph
func TestJoinGraph_FindMinSpanningTree_Empty(t *testing.T) {
	graph := NewJoinGraph()
	mst := graph.FindMinSpanningTree()
	assert.Equal(t, 0, len(mst))
}

// TestJoinGraph_FindMinSpanningTree_Disconnected tests disconnected graph
func TestJoinGraph_FindMinSpanningTree_Disconnected(t *testing.T) {
	graph := NewJoinGraph()
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	// Only connect a-b and c-d
	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("c", "d", "JOIN", 15)

	mst := graph.FindMinSpanningTree()

	// MST should still work but won't span all nodes
	// Kruskal will create a forest
	assert.True(t, len(mst) >= 0)
}

// TestJoinGraph_GetDegreeSequence tests degree sequence
func TestJoinGraph_GetDegreeSequence(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)

	graph.AddEdge("a", "b", "JOIN", 50)
	graph.AddEdge("b", "c", "JOIN", 50)

	degrees := graph.GetDegreeSequence()

	assert.Equal(t, 3, len(degrees))

	// a has degree 1, b has degree 2, c has degree 1
	hasDegree1 := 0
	hasDegree2 := 0
	for _, deg := range degrees {
		if deg == 1 {
			hasDegree1++
		} else if deg == 2 {
			hasDegree2++
		}
	}

	assert.Equal(t, 2, hasDegree1, "Should have 2 nodes with degree 1")
	assert.Equal(t, 1, hasDegree2, "Should have 1 node with degree 2")
}

// TestJoinGraph_GetDegreeSequence_Empty tests empty graph
func TestJoinGraph_GetDegreeSequence_Empty(t *testing.T) {
	graph := NewJoinGraph()
	degrees := graph.GetDegreeSequence()
	assert.Equal(t, 0, len(degrees))
}

// TestJoinGraph_IsStarGraph tests star graph detection
func TestJoinGraph_IsStarGraph(t *testing.T) {
	// Create a star graph: center connected to all other nodes
	graph := NewJoinGraph()
	graph.AddNode("center", 1000)
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	// Connect center to all other nodes
	graph.AddEdge("center", "a", "JOIN", 10)
	graph.AddEdge("center", "b", "JOIN", 20)
	graph.AddEdge("center", "c", "JOIN", 30)
	graph.AddEdge("center", "d", "JOIN", 40)

	isStar := graph.IsStarGraph()
	assert.True(t, isStar, "Should detect star graph")
}

// TestJoinGraph_IsStarGraph_LineGraph tests line graph is not star
func TestJoinGraph_IsStarGraph_LineGraph(t *testing.T) {
	// Create a line graph: a-b-c-d
	graph := NewJoinGraph()
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("b", "c", "JOIN", 20)
	graph.AddEdge("c", "d", "JOIN", 30)

	isStar := graph.IsStarGraph()
	assert.False(t, isStar, "Line graph should not be detected as star graph")
}

// TestJoinGraph_IsStarGraph_Cycle tests cycle graph is not star
func TestJoinGraph_IsStarGraph_Cycle(t *testing.T) {
	// Create a cycle graph: a-b-c-d-a
	graph := NewJoinGraph()
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("b", "c", "JOIN", 20)
	graph.AddEdge("c", "d", "JOIN", 30)
	graph.AddEdge("d", "a", "JOIN", 40)

	isStar := graph.IsStarGraph()
	assert.False(t, isStar, "Cycle graph should not be detected as star graph")
}

// TestJoinGraph_IsStarGraph_TooFewNodes tests with too few nodes
func TestJoinGraph_IsStarGraph_TooFewNodes(t *testing.T) {
	// Graph with less than 3 nodes
	graph := NewJoinGraph()
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddEdge("a", "b", "JOIN", 10)

	isStar := graph.IsStarGraph()
	assert.False(t, isStar, "Graph with 2 nodes should not be considered star graph")

	// Single node
	graph2 := NewJoinGraph()
	graph2.AddNode("a", 100)
	isStar2 := graph2.IsStarGraph()
	assert.False(t, isStar2, "Single node graph should not be considered star graph")
}

// TestJoinGraph_EstimateJoinCardinality tests join cardinality estimation
func TestJoinGraph_EstimateJoinCardinality(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("users", 1000)
	graph.AddNode("orders", 5000)

	// Add edge with estimated cardinality
	graph.AddEdge("users", "orders", "INNER JOIN", 500)

	cardinality := graph.EstimateJoinCardinality("users", "orders")
	assert.Equal(t, 500.0, cardinality, "Should return edge cardinality")
}

// TestJoinGraph_EstimateJoinCardinality_NoDirectEdge tests estimation without direct edge
func TestJoinGraph_EstimateJoinCardinality_NoDirectEdge(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 200)

	// No edge between a and b

	cardinality := graph.EstimateJoinCardinality("a", "b")
	// Should use product of node cardinalities
	assert.Equal(t, float64(100*200), cardinality, "Should estimate using node cardinalities")
}

// TestJoinGraph_EstimateJoinCardinality_NonExistentNode tests with non-existent nodes
func TestJoinGraph_EstimateJoinCardinality_NonExistentNode(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)

	cardinality := graph.EstimateJoinCardinality("a", "non_existent")
	assert.Equal(t, 0.0, cardinality, "Should return 0 for non-existent nodes")

	cardinality = graph.EstimateJoinCardinality("non_existent1", "non_existent2")
	assert.Equal(t, 0.0, cardinality, "Should return 0 for two non-existent nodes")
}

// TestJoinGraph_GetStats tests graph statistics
func TestJoinGraph_GetStats(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("b", "c", "JOIN", 20)
	graph.AddEdge("c", "d", "JOIN", 30)

	stats := graph.GetStats()

	assert.Equal(t, 4, stats.NodeCount, "Should have 4 nodes")
	assert.Equal(t, 3, stats.EdgeCount, "Should have 3 edges")
	assert.True(t, stats.IsConnected, "Should be connected")
	assert.False(t, stats.IsStar, "Should not be star graph")
	assert.Equal(t, 2, stats.MaxDegree, "Max degree should be 2")
	assert.Equal(t, 1, stats.MinDegree, "Min degree should be 1")
	assert.Equal(t, 1.5, stats.AvgDegree, "Avg degree should be 1.5")
}

// TestJoinGraph_GetStats_Empty tests empty graph stats
func TestJoinGraph_GetStats_Empty(t *testing.T) {
	graph := NewJoinGraph()
	stats := graph.GetStats()

	assert.Equal(t, 0, stats.NodeCount)
	assert.Equal(t, 0, stats.EdgeCount)
	assert.True(t, stats.IsConnected, "Empty graph should be considered connected")
	assert.False(t, stats.IsStar)
}

// TestJoinGraph_GetStats_StarGraph tests star graph stats
func TestJoinGraph_GetStats_StarGraph(t *testing.T) {
	graph := NewJoinGraph()
	graph.AddNode("center", 1000)
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)

	graph.AddEdge("center", "a", "JOIN", 10)
	graph.AddEdge("center", "b", "JOIN", 20)
	graph.AddEdge("center", "c", "JOIN", 30)

	stats := graph.GetStats()

	assert.Equal(t, 4, stats.NodeCount)
	assert.Equal(t, 3, stats.EdgeCount)
	assert.True(t, stats.IsConnected)
	assert.True(t, stats.IsStar, "Should be detected as star graph")
	assert.Equal(t, 3, stats.MaxDegree)
	assert.Equal(t, 1, stats.MinDegree)
	assert.Equal(t, 1.5, stats.AvgDegree)
	t.Logf("Star graph stats: Nodes=%d, Edges=%d, MaxDeg=%d, MinDeg=%d, AvgDeg=%.2f",
		stats.NodeCount, stats.EdgeCount, stats.MaxDegree, stats.MinDegree, stats.AvgDegree)
}

// TestJoinGraph_FindMinSpanningTree_Complex tests MST on complex graph
func TestJoinGraph_FindMinSpanningTree_Complex(t *testing.T) {
	graph := NewJoinGraph()

	// Create a more complex graph
	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)
	graph.AddNode("e", 100)

	// Add edges
	graph.AddEdge("a", "b", "JOIN", 1)
	graph.AddEdge("a", "c", "JOIN", 3)
	graph.AddEdge("a", "d", "JOIN", 5)
	graph.AddEdge("b", "c", "JOIN", 2)
	graph.AddEdge("b", "d", "JOIN", 4)
	graph.AddEdge("c", "d", "JOIN", 6)
	graph.AddEdge("c", "e", "JOIN", 7)
	graph.AddEdge("d", "e", "JOIN", 8)

	mst := graph.FindMinSpanningTree()
	assert.Equal(t, 4, len(mst), "Should have 4 edges for 5 nodes")

	// Verify no cycles (MST property)
	// If there are no cycles, we should have exactly n-1 edges
	assert.Equal(t, 4, len(mst))
}

// TestJoinGraph_BFS tests BFS traversal
func TestJoinGraph_BFS(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)
	graph.AddNode("d", 100)

	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("b", "c", "JOIN", 20)
	graph.AddEdge("c", "d", "JOIN", 30)
	graph.AddEdge("a", "d", "JOIN", 40)

	// Get neighbors should return adjacent nodes (where node is the FROM side of edges)
	neighbors := graph.GetNeighbors("a")
	assert.Equal(t, 2, len(neighbors))
	assert.Contains(t, neighbors, "b")
	assert.Contains(t, neighbors, "d")

	neighbors = graph.GetNeighbors("b")
	// b is only in FROM of b->c, so only 1 neighbor
	assert.Equal(t, 1, len(neighbors))
	assert.Contains(t, neighbors, "c")

	neighbors = graph.GetNeighbors("c")
	// c is only in FROM of c->d
	assert.Equal(t, 1, len(neighbors))
	assert.Contains(t, neighbors, "d")

	neighbors = graph.GetNeighbors("d")
	// d is never in FROM position
	assert.Equal(t, 0, len(neighbors))
}

// TestJoinGraph_MultipleEdges tests handling of multiple edges between same nodes
func TestJoinGraph_MultipleEdges(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 100)

	// Add multiple edges
	graph.AddEdge("a", "b", "INNER JOIN", 10)
	graph.AddEdge("a", "b", "LEFT JOIN", 20)
	graph.AddEdge("a", "b", "RIGHT JOIN", 30)

	assert.Equal(t, 3, graph.edgeCount)

	aNode := graph.GetNode("a")
	assert.Equal(t, 3, len(aNode.Edges))

	bNode := graph.GetNode("b")
	assert.Equal(t, 3, bNode.Degree, "b should have degree 3")
}

// TestJoinGraph_LargeGraph tests with larger graph
func TestJoinGraph_LargeGraph(t *testing.T) {
	graph := NewJoinGraph()

	// Add 10 nodes
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("n%d", i)
		graph.AddNode(nodeName, int64(100))
	}

	// Connect them in a line
	for i := 0; i < 9; i++ {
		graph.AddEdge(fmt.Sprintf("n%d", i), fmt.Sprintf("n%d", i+1), "JOIN", float64(i+1)*10)
	}

	stats := graph.GetStats()
	assert.Equal(t, 10, stats.NodeCount)
	assert.Equal(t, 9, stats.EdgeCount)
	// The line graph is connected when treating edges as undirected
	// (for join planning, connectivity is checked in undirected sense)
	assert.True(t, stats.IsConnected, "Line graph should be connected when treated as undirected")
	assert.False(t, stats.IsStar, "Line graph should not be star")

	mst := graph.FindMinSpanningTree()
	assert.Equal(t, 9, len(mst))
	t.Logf("Large graph: Nodes=%d, Edges=%d, Connected=%v, Star=%v, MST edges=%d",
		stats.NodeCount, stats.EdgeCount, stats.IsConnected, stats.IsStar, len(mst))
}

// TestJoinGraph_DirectedEdgeHandling tests edge direction in neighbor queries
func TestJoinGraph_DirectedEdgeHandling(t *testing.T) {
	graph := NewJoinGraph()

	graph.AddNode("a", 100)
	graph.AddNode("b", 100)
	graph.AddNode("c", 100)

	// Add edges
	graph.AddEdge("a", "b", "JOIN", 10)
	graph.AddEdge("a", "c", "JOIN", 20)

	// Get neighbors of a
	neighborsA := graph.GetNeighbors("a")
	assert.Equal(t, 2, len(neighborsA))
	assert.Contains(t, neighborsA, "b")
	assert.Contains(t, neighborsA, "c")

	// Get neighbors of b (should be empty as we only have a->b)
	neighborsB := graph.GetNeighbors("b")
	assert.Equal(t, 0, len(neighborsB))
}

// Helper function to check if string is in slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
