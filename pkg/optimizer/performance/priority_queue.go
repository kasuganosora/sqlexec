package performance

import (
	"container/heap"
)

// PriorityQueue 优先队列（用于JOIN重排序等优化）
type PriorityQueue []*PlanNode

// PlanNode 计划节点
type PlanNode struct {
	Plan     interface{} // LogicalPlan 在 optimizer 包中定义
	Cost     float64
	Priority int
	Index    int
}

// Len 实现 heap.Interface
func (pq PriorityQueue) Len() int { return len(pq) }

// Less 实现 heap.Interface
func (pq PriorityQueue) Less(i, j int) bool {
	// 优先级高的在前（成本低的优先）
	if pq[i].Priority == pq[j].Priority {
		return pq[i].Cost < pq[j].Cost
	}
	return pq[i].Priority > pq[j].Priority
}

// Swap 实现 heap.Interface
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 实现 heap.Interface
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	node := x.(*PlanNode)
	node.Index = n
	*pq = append(*pq, node)
}

// Pop 实现 heap.Interface
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	node := old[n-1]
	old[n-1] = nil
	node.Index = -1
	*pq = old[0 : n-1]
	return node
}

// NewPriorityQueue 创建优先队列
func NewPriorityQueue() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &pq
}
