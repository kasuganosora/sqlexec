package join

import (
	"fmt"
)

// JoinGraph JOIN关系图
// 用于表示表之间的连接关系，辅助JOIN重排序
type JoinGraph struct {
	nodes      map[string]*JoinNode      // table_name -> node
	edges      []*JoinEdge              // 所有边
	edgeCount  int                      // 边数
}

// JoinNode JOIN图节点
type JoinNode struct {
	Name       string            // 表名
	Degree     int                // 度数（连接的边数）
	Edges      []*JoinEdge        // 连接的边
	Cardinality int64              // 表基数（可选）
}

// JoinEdge JOIN图边
type JoinEdge struct {
	From      string                // 起始表
	To        string                // 目标表
	JoinType  string                // 连接类型
	Cardinality float64             // 估算的基数
}

// NewJoinGraph 创建JOIN图
func NewJoinGraph() *JoinGraph {
	return &JoinGraph{
		nodes:     make(map[string]*JoinNode),
		edges:     []*JoinEdge{},
		edgeCount: 0,
	}
}

// AddNode 添加节点
func (jg *JoinGraph) AddNode(name string, cardinality int64) {
	if _, exists := jg.nodes[name]; !exists {
		jg.nodes[name] = &JoinNode{
			Name:       name,
			Degree:     0,
			Edges:      []*JoinEdge{},
			Cardinality: cardinality,
		}
	}
}

// AddEdge 添加边（连接关系）
func (jg *JoinGraph) AddEdge(from, to, joinType string, cardinality float64) {
	// 添加边
	edge := &JoinEdge{
		From:      from,
		To:        to,
		JoinType:  joinType,
		Cardinality: cardinality,
	}
	jg.edges = append(jg.edges, edge)
	jg.edgeCount++

	// 更新节点度数
	if fromNode, exists := jg.nodes[from]; exists {
		fromNode.Degree++
		fromNode.Edges = append(fromNode.Edges, edge)
	}

	if toNode, exists := jg.nodes[to]; exists {
		toNode.Degree++
	}
}

// GetNode 获取节点
func (jg *JoinGraph) GetNode(name string) *JoinNode {
	return jg.nodes[name]
}

// GetNeighbors 获取节点的邻居
func (jg *JoinGraph) GetNeighbors(name string) []string {
	node := jg.GetNode(name)
	if node == nil {
		return []string{}
	}

	neighbors := make([]string, 0, len(node.Edges))
	for _, edge := range node.Edges {
		if edge.From == name {
			neighbors = append(neighbors, edge.To)
		}
	}

	return neighbors
}

// GetConnectedComponents 获取连通分量
func (jg *JoinGraph) GetConnectedComponents() [][]string {
	visited := make(map[string]bool)
	components := [][]string{}

	for name := range jg.nodes {
		if !visited[name] {
			component := jg.bfs(name, visited)
			if len(component) > 0 {
				components = append(components, component)
			}
		}
	}

	return components
}

// BFS 广度优先搜索
func (jg *JoinGraph) bfs(start string, visited map[string]bool) []string {
	queue := []string{start}
	visited[start] = true
	component := []string{}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		component = append(component, current)

		// 遍历邻居
		for _, neighbor := range jg.GetNeighbors(current) {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}

	return component
}

// FindMinSpanningTree 查找最小生成树（Kruskal算法）
func (jg *JoinGraph) FindMinSpanningTree() []*JoinEdge {
	// 按基数排序边
	sortedEdges := make([]*JoinEdge, len(jg.edges))
	copy(sortedEdges, jg.edges)

	// 简化的冒泡排序（实际应该使用更高效的算法）
	for i := 0; i < len(sortedEdges)-1; i++ {
		for j := i + 1; j < len(sortedEdges); j++ {
			if sortedEdges[i].Cardinality > sortedEdges[j].Cardinality {
				sortedEdges[i], sortedEdges[j] = sortedEdges[j], sortedEdges[i]
			}
		}
	}

	// Kruskal算法
	parent := make(map[string]string)
	for name := range jg.nodes {
		parent[name] = name
	}

	mst := []*JoinEdge{}
	for _, edge := range sortedEdges {
		// 检查是否形成环
		rootFrom := jg.findParent(edge.From, parent)
		rootTo := jg.findParent(edge.To, parent)

		if rootFrom != rootTo {
			mst = append(mst, edge)
			parent[rootFrom] = rootTo
		}

		// 如果MST包含所有节点，停止
		if len(mst) == len(jg.nodes)-1 {
			break
		}
	}

	return mst
}

// findParent 查找根节点（用于并查集）
func (jg *JoinGraph) findParent(name string, parent map[string]string) string {
	for parent[name] != name {
		name = parent[name]
	}
	return name
}

// GetDegreeSequence 获取度数序列（用于判断是否是星型图）
func (jg *JoinGraph) GetDegreeSequence() []int {
	degrees := make([]int, 0, len(jg.nodes))
	for _, node := range jg.nodes {
		degrees = append(degrees, node.Degree)
	}
	return degrees
}

// IsStarGraph 判断是否是星型图（一个中心节点连接所有其他节点）
func (jg *JoinGraph) IsStarGraph() bool {
	if len(jg.nodes) < 3 {
		return false
	}

	// 找度数最大的节点
	maxDegree := 0
	centerNodes := []string{}
	for _, node := range jg.nodes {
		if node.Degree > maxDegree {
			maxDegree = node.Degree
			centerNodes = []string{node.Name}
		} else if node.Degree == maxDegree {
			centerNodes = append(centerNodes, node.Name)
		}
	}

	// 检查是否所有其他节点都连接到中心节点
	if len(centerNodes) == 1 {
		center := centerNodes[0]
		neighbors := jg.GetNeighbors(center)
		
		// 检查度数是否等于节点数-1
		if len(neighbors) == len(jg.nodes)-1 {
			return true
		}
	}

	return false
}

// EstimateJoinCardinality 估算JOIN的基数（基于图）
func (jg *JoinGraph) EstimateJoinCardinality(from, to string) float64 {
	fromNode := jg.GetNode(from)
	toNode := jg.GetNode(to)

	if fromNode == nil || toNode == nil {
		return 0
	}

	// 查找连接这两个节点的边
	for _, edge := range fromNode.Edges {
		if edge.To == to {
			return edge.Cardinality
		}
	}

	// 没有找到边，使用节点基数估算
	return float64(fromNode.Cardinality * toNode.Cardinality)
}

// GetStats 获取图的统计信息
func (jg *JoinGraph) GetStats() GraphStats {
	stats := GraphStats{
		NodeCount:    len(jg.nodes),
		EdgeCount:    jg.edgeCount,
		IsConnected:   jg.isConnected(),
		IsStar:       jg.IsStarGraph(),
	}

	if len(jg.nodes) > 0 {
		stats.MaxDegree = jg.getMaxDegree()
		stats.MinDegree = jg.getMinDegree()
		stats.AvgDegree = float64(jg.edgeCount * 2) / float64(len(jg.nodes))
	}

	return stats
}

// isConnected 检查图是否连通
func (jg *JoinGraph) isConnected() bool {
	if len(jg.nodes) == 0 {
		return true
	}

	// 从任意节点开始BFS
	start := ""
	for name := range jg.nodes {
		start = name
		break
	}

	visited := make(map[string]bool)
	jg.bfs(start, visited)

	return len(visited) == len(jg.nodes)
}

// getMaxDegree 获取最大度数
func (jg *JoinGraph) getMaxDegree() int {
	maxDegree := 0
	for _, node := range jg.nodes {
		if node.Degree > maxDegree {
			maxDegree = node.Degree
		}
	}
	return maxDegree
}

// getMinDegree 获取最小度数
func (jg *JoinGraph) getMinDegree() int {
	minDegree := int(^uint(0) >> 1)
	for _, node := range jg.nodes {
		if node.Degree < minDegree {
			minDegree = node.Degree
		}
	}
	return minDegree
}

// GraphStats 图统计信息
type GraphStats struct {
	NodeCount   int
	EdgeCount   int
	MaxDegree   int
	MinDegree   int
	AvgDegree   float64
	IsConnected bool
	IsStar      bool
}

// Explain 返回图的说明
func (jg *JoinGraph) Explain() string {
	stats := jg.GetStats()
	
	return fmt.Sprintf(
		"=== Join Graph ===\n"+
		"Nodes: %d\n"+
		"Edges: %d\n"+
		"Max Degree: %d\n"+
		"Min Degree: %d\n"+
		"Avg Degree: %.2f\n"+
		"Connected: %v\n"+
		"Star Graph: %v\n",
		stats.NodeCount,
		stats.EdgeCount,
		stats.MaxDegree,
		stats.MinDegree,
		stats.AvgDegree,
		stats.IsConnected,
		stats.IsStar,
	)
}
