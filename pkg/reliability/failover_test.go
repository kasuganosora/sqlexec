package reliability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockHealthChecker 模拟健康检查器
type MockHealthChecker struct {
	healthy bool
}

func (m *MockHealthChecker) Check(node *Node) error {
	if m.healthy {
		return nil
	}
	return errors.New("node unhealthy")
}

func TestNewFailoverManager(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.nodes)
	assert.Equal(t, 5*time.Second, manager.checkInterval)
	assert.Equal(t, 30*time.Second, manager.failureTimeout)
}

func TestFailoverManager_AddNode(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	err := manager.AddNode("node1", "localhost:3306", 10)
	require.NoError(t, err)

	// Verify node was added
	nodes := manager.GetAllNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node1", nodes[0].ID)
	assert.Equal(t, "localhost:3306", nodes[0].Address)
	assert.Equal(t, 10, nodes[0].Weight)
	assert.Equal(t, NodeStatusHealthy, nodes[0].Status)

	// Active node should be set
	activeNode := manager.GetActiveNode()
	assert.NotNil(t, activeNode)
	assert.Equal(t, "node1", activeNode.ID)
}

func TestFailoverManager_AddNode_Duplicate(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	err := manager.AddNode("node1", "localhost:3306", 10)
	require.NoError(t, err)

	err = manager.AddNode("node1", "localhost:3307", 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestFailoverManager_RemoveNode(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	// Add nodes
	manager.AddNode("node1", "localhost:3306", 10)
	manager.AddNode("node2", "localhost:3307", 5)

	// Remove node1
	err := manager.RemoveNode("node1")
	require.NoError(t, err)

	nodes := manager.GetAllNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node2", nodes[0].ID)
}

func TestFailoverManager_RemoveNode_NotFound(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	err := manager.RemoveNode("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestFailoverManager_GetActiveNode(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	// Add multiple nodes with different weights
	manager.AddNode("node1", "localhost:3306", 5)
	manager.AddNode("node2", "localhost:3307", 10)
	manager.AddNode("node3", "localhost:3308", 8)

	// Active node should be the one with highest weight
	activeNode := manager.GetActiveNode()
	assert.NotNil(t, activeNode)
	assert.Equal(t, "node2", activeNode.ID)
	assert.Equal(t, 10, activeNode.Weight)
}

func TestFailoverManager_ManualFailover(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)
	manager.AddNode("node2", "localhost:3307", 5)

	// Manual failover to node2
	err := manager.ManualFailover("node2")
	require.NoError(t, err)

	activeNode := manager.GetActiveNode()
	assert.Equal(t, "node2", activeNode.ID)
}

func TestFailoverManager_ManualFailover_Unhealthy(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)
	manager.AddNode("node2", "localhost:3307", 5)

	// Mark node2 as unhealthy
	nodes := manager.GetAllNodes()
	for _, node := range nodes {
		if node.ID == "node2" {
			node.Status = NodeStatusUnhealthy
		}
	}

	err := manager.ManualFailover("node2")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unhealthy")
}

func TestFailoverManager_ManualFailover_NotFound(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	err := manager.ManualFailover("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestFailoverManager_GetNodeStatus(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)

	node, err := manager.GetNodeStatus("node1")
	require.NoError(t, err)
	assert.Equal(t, "node1", node.ID)
	assert.Equal(t, "localhost:3306", node.Address)
}

func TestFailoverManager_GetNodeStatus_NotFound(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	_, err := manager.GetNodeStatus("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestFailoverManager_UpdateNodeLoad(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)

	// Update load
	err := manager.UpdateNodeLoad("node1", 0.9)
	require.NoError(t, err)

	node, _ := manager.GetNodeStatus("node1")
	assert.Equal(t, 0.9, node.Load)
	assert.Equal(t, NodeStatusDegraded, node.Status)

	// Reduce load
	err = manager.UpdateNodeLoad("node1", 0.6)
	require.NoError(t, err)

	node, _ = manager.GetNodeStatus("node1")
	assert.Equal(t, 0.6, node.Load)
	assert.Equal(t, NodeStatusHealthy, node.Status)
}

func TestFailoverManager_UpdateNodeLoad_NotFound(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 5*time.Second, 30*time.Second)

	err := manager.UpdateNodeLoad("nonexistent", 0.5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestLoadBalancer(t *testing.T) {
	lb := NewLoadBalancer()
	assert.NotNil(t, lb)

	// Add nodes
	node1 := &Node{ID: "node1", Address: "localhost:3306", Status: NodeStatusHealthy, Load: 0.3}
	node2 := &Node{ID: "node2", Address: "localhost:3307", Status: NodeStatusHealthy, Load: 0.5}
	node3 := &Node{ID: "node3", Address: "localhost:3308", Status: NodeStatusUnhealthy, Load: 0.2}

	lb.AddNode(node1)
	lb.AddNode(node2)
	lb.AddNode(node3)

	// Round robin should skip unhealthy nodes
	nodes := make([]string, 0, 10)
	for i := 0; i < 4; i++ {
		node := lb.NextNode()
		if node != nil {
			nodes = append(nodes, node.ID)
		}
	}

	assert.NotContains(t, nodes, "node3")
}

func TestLoadBalancer_LeastLoadedNode(t *testing.T) {
	lb := NewLoadBalancer()

	node1 := &Node{ID: "node1", Address: "localhost:3306", Status: NodeStatusHealthy, Load: 0.7}
	node2 := &Node{ID: "node2", Address: "localhost:3307", Status: NodeStatusHealthy, Load: 0.3}
	node3 := &Node{ID: "node3", Address: "localhost:3308", Status: NodeStatusHealthy, Load: 0.5}

	lb.AddNode(node1)
	lb.AddNode(node2)
	lb.AddNode(node3)

	leastLoaded := lb.LeastLoadedNode()
	assert.NotNil(t, leastLoaded)
	assert.Equal(t, "node2", leastLoaded.ID)
	assert.Equal(t, 0.3, leastLoaded.Load)
}

func TestExecuteWithRetryAndFailover(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	fm := NewFailoverManager(checker, 5*time.Second, 30*time.Second)
	lb := NewLoadBalancer()

	// Add nodes
	fm.AddNode("node1", "localhost:3306", 10)
	lb.AddNode(&Node{ID: "node1", Address: "localhost:3306", Status: NodeStatusHealthy, Load: 0.3})

	// Execute successfully
	ctx := context.Background()
	called := 0
	err := ExecuteWithRetryAndFailover(ctx, fm, lb, func(node *Node) error {
		called++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, called)
}

func TestExecuteWithRetryAndFailover_NoNodes(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	fm := NewFailoverManager(checker, 5*time.Second, 30*time.Second)
	lb := NewLoadBalancer()

	ctx := context.Background()
	err := ExecuteWithRetryAndFailover(ctx, fm, lb, func(node *Node) error {
		return nil
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no available nodes")
}

func TestFailoverManager_StartStop(t *testing.T) {
	checker := &MockHealthChecker{healthy: true}
	manager := NewFailoverManager(checker, 100*time.Millisecond, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)

	// Start the manager
	manager.Start()

	// Wait a bit for health checks to run
	time.Sleep(150 * time.Millisecond)

	// Stop the manager
	manager.Stop()

	// Verify node was checked
	node, err := manager.GetNodeStatus("node1")
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), node.LastPing, 1*time.Second)
}

func TestFailoverManager_HealthCheckFailure(t *testing.T) {
	checker := &MockHealthChecker{healthy: false}
	manager := NewFailoverManager(checker, 100*time.Millisecond, 30*time.Second)

	manager.AddNode("node1", "localhost:3306", 10)
	manager.AddNode("node2", "localhost:3307", 5)

	// Start health checks
	manager.Start()
	time.Sleep(150 * time.Millisecond)
	manager.Stop()

	// Node1 should be marked as unhealthy
	node1, err := manager.GetNodeStatus("node1")
	require.NoError(t, err)
	assert.Equal(t, NodeStatusUnhealthy, node1.Status)
}

func TestLoadBalancer_NoHealthyNodes(t *testing.T) {
	lb := NewLoadBalancer()

	node1 := &Node{ID: "node1", Address: "localhost:3306", Status: NodeStatusUnhealthy, Load: 0.3}
	node2 := &Node{ID: "node2", Address: "localhost:3307", Status: NodeStatusOffline, Load: 0.5}

	lb.AddNode(node1)
	lb.AddNode(node2)

	node := lb.NextNode()
	assert.Nil(t, node)
}

func TestLoadBalancer_Empty(t *testing.T) {
	lb := NewLoadBalancer()

	node := lb.NextNode()
	assert.Nil(t, node)

	node = lb.LeastLoadedNode()
	assert.Nil(t, node)
}

func TestNodeStatus(t *testing.T) {
	assert.Equal(t, 0, int(NodeStatusHealthy))
	assert.Equal(t, 1, int(NodeStatusDegraded))
	assert.Equal(t, 2, int(NodeStatusUnhealthy))
	assert.Equal(t, 3, int(NodeStatusOffline))
}

func TestCircuitState(t *testing.T) {
	assert.Equal(t, 0, int(StateClosed))
	assert.Equal(t, 1, int(StateOpen))
	assert.Equal(t, 2, int(StateHalfOpen))
}

