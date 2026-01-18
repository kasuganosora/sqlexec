package reliability

import (
	"context"
	"errors"
	"sync"
	"time"
)

// NodeStatus 节点状�?
type NodeStatus int

const (
	NodeStatusHealthy NodeStatus = iota
	NodeStatusDegraded
	NodeStatusUnhealthy
	NodeStatusOffline
)

// Node 节点
type Node struct {
	ID       string
	Address  string
	Weight   int
	Status   NodeStatus
	LastPing time.Time
	Load     float64
}

// HealthChecker 健康检查器接口
type HealthChecker interface {
	Check(node *Node) error
}

// FailoverManager 故障转移管理�?
type FailoverManager struct {
	nodes          []*Node
	activeNode     *Node
	checker        HealthChecker
	checkInterval  time.Duration
	failureTimeout time.Duration
	stopChan       chan struct{}
	mu             sync.RWMutex
}

// NewFailoverManager 创建故障转移管理�?
func NewFailoverManager(checker HealthChecker, checkInterval, failureTimeout time.Duration) *FailoverManager {
	return &FailoverManager{
		nodes:          make([]*Node, 0),
		checker:        checker,
		checkInterval:  checkInterval,
		failureTimeout: failureTimeout,
		stopChan:       make(chan struct{}),
	}
}

// AddNode 添加节点
func (fm *FailoverManager) AddNode(id, address string, weight int) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, node := range fm.nodes {
		if node.ID == id {
			return errors.New("node already exists")
		}
	}

	node := &Node{
		ID:       id,
		Address:  address,
		Weight:   weight,
		Status:   NodeStatusHealthy,
		LastPing: time.Now(),
		Load:     0,
	}

	fm.nodes = append(fm.nodes, node)

	// 如果没有活跃节点，设置这个为活跃节点
	if fm.activeNode == nil {
		fm.activeNode = node
	}

	return nil
}

// RemoveNode 移除节点
func (fm *FailoverManager) RemoveNode(id string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for i, node := range fm.nodes {
		if node.ID == id {
			if fm.activeNode != nil && fm.activeNode.ID == id {
				// 需要切换到其他节点
				fm.activeNode = nil
				fm.selectActiveNode()
			}
			fm.nodes = append(fm.nodes[:i], fm.nodes[i+1:]...)
			return nil
		}
	}

	return errors.New("node not found")
}

// GetActiveNode 获取活跃节点
func (fm *FailoverManager) GetActiveNode() *Node {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.activeNode
}

// selectActiveNode 选择活跃节点
func (fm *FailoverManager) selectActiveNode() {
	if len(fm.nodes) == 0 {
		fm.activeNode = nil
		return
	}

	// 选择健康且权重最高的节点
	var bestNode *Node
	maxWeight := 0

	for _, node := range fm.nodes {
		if node.Status == NodeStatusHealthy && node.Weight > maxWeight {
			bestNode = node
			maxWeight = node.Weight
		}
	}

	fm.activeNode = bestNode
}

// Start 启动故障转移管理�?
func (fm *FailoverManager) Start() {
	go fm.runHealthCheck()
}

// Stop 停止故障转移管理�?
func (fm *FailoverManager) Stop() {
	close(fm.stopChan)
}

// runHealthCheck 运行健康检�?
func (fm *FailoverManager) runHealthCheck() {
	ticker := time.NewTicker(fm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fm.checkAllNodes()
		case <-fm.stopChan:
			return
		}
	}
}

// checkAllNodes 检查所有节�?
func (fm *FailoverManager) checkAllNodes() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	now := time.Now()
	needsFailover := false

	for _, node := range fm.nodes {
		err := fm.checker.Check(node)
		if err != nil {
			// 节点健康检查失�?
			if node.Status != NodeStatusUnhealthy {
				node.Status = NodeStatusUnhealthy
				if fm.activeNode != nil && fm.activeNode.ID == node.ID {
					needsFailover = true
				}
			}
		} else {
			// 节点健康
			if node.Status == NodeStatusUnhealthy {
				node.Status = NodeStatusHealthy
			}
		}
		node.LastPing = now
	}

	if needsFailover {
		fm.selectActiveNode()
	}
}

// GetNodeStatus 获取节点状�?
func (fm *FailoverManager) GetNodeStatus(id string) (*Node, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	for _, node := range fm.nodes {
		if node.ID == id {
			return node, nil
		}
	}

	return nil, errors.New("node not found")
}

// GetAllNodes 获取所有节�?
func (fm *FailoverManager) GetAllNodes() []*Node {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	nodes := make([]*Node, len(fm.nodes))
	copy(nodes, fm.nodes)
	return nodes
}

// UpdateNodeLoad 更新节点负载
func (fm *FailoverManager) UpdateNodeLoad(id string, load float64) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, node := range fm.nodes {
		if node.ID == id {
			node.Load = load

			// 根据负载调整状�?
			if load > 0.8 && node.Status == NodeStatusHealthy {
				node.Status = NodeStatusDegraded
			} else if load <= 0.8 && node.Status == NodeStatusDegraded {
				node.Status = NodeStatusHealthy
			}

			return nil
		}
	}

	return errors.New("node not found")
}

// ManualFailover 手动故障转移
func (fm *FailoverManager) ManualFailover(targetNodeID string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// 查找目标节点
	var targetNode *Node
	for _, node := range fm.nodes {
		if node.ID == targetNodeID {
			targetNode = node
			break
		}
	}

	if targetNode == nil {
		return errors.New("target node not found")
	}

	if targetNode.Status == NodeStatusUnhealthy {
		return errors.New("target node is unhealthy")
	}

	fm.activeNode = targetNode
	return nil
}

// LoadBalancer 负载均衡�?
type LoadBalancer struct {
	nodes    []*Node
	current  int
	mu       sync.RWMutex
}

// NewLoadBalancer 创建负载均衡�?
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		nodes:   make([]*Node, 0),
		current: 0,
	}
}

// AddNode 添加节点
func (lb *LoadBalancer) AddNode(node *Node) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.nodes = append(lb.nodes, node)
}

// NextNode 获取下一个节点（轮询�?
func (lb *LoadBalancer) NextNode() *Node {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.nodes) == 0 {
		return nil
	}

	// 跳过不健康的节点
	healthyNodes := make([]*Node, 0, len(lb.nodes))
	for _, node := range lb.nodes {
		if node.Status == NodeStatusHealthy || node.Status == NodeStatusDegraded {
			healthyNodes = append(healthyNodes, node)
		}
	}

	if len(healthyNodes) == 0 {
		return nil
	}

	node := healthyNodes[lb.current]
	lb.current = (lb.current + 1) % len(healthyNodes)

	return node
}

// LeastLoadedNode 获取负载最低的节点
func (lb *LoadBalancer) LeastLoadedNode() *Node {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if len(lb.nodes) == 0 {
		return nil
	}

	var bestNode *Node
	minLoad := 1.0

	for _, node := range lb.nodes {
		if (node.Status == NodeStatusHealthy || node.Status == NodeStatusDegraded) &&
			node.Load < minLoad {
			bestNode = node
			minLoad = node.Load
		}
	}

	return bestNode
}

// ExecuteWithRetryAndFailover 使用重试和故障转移执行操�?
func ExecuteWithRetryAndFailover(ctx context.Context, fm *FailoverManager, lb *LoadBalancer, fn func(*Node) error) error {
	maxRetries := 3
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 获取节点
		node := lb.NextNode()
		if node == nil {
			node = fm.GetActiveNode()
		}
		if node == nil {
			return errors.New("no available nodes")
		}

		// 执行操作
		err := fn(node)
		if err == nil {
			// 更新负载（假设操作成功）
			fm.UpdateNodeLoad(node.ID, node.Load*0.9)
			return nil
		}

		lastErr = err

		// 更新负载（假设操作失败）
		fm.UpdateNodeLoad(node.ID, node.Load*1.1)

		// 如果是最后一次重试，尝试故障转移
		if i == maxRetries-1 {
			if activeNode := fm.GetActiveNode(); activeNode != nil && activeNode.ID != node.ID {
				err := fn(activeNode)
				if err == nil {
					return nil
				}
				lastErr = err
			}
		}
	}

	return lastErr
}
