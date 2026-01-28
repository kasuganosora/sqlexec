package reliability

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFailoverManager(t *testing.T) {
	manager := NewFailoverManager()

	assert.NotNil(t, manager)
}

func TestAddNode(t *testing.T) {
	manager := NewFailoverManager()

	err := manager.AddNode(&FailoverNode{
		ID:     "node1",
		Address: "localhost:3306",
		Healthy: true,
		Load:    0,
	})
	assert.NoError(t, err)

	// Verify node was added
	node := manager.nodes["node1"]
	assert.NotNil(t, node)
}

func TestGetActiveNode(t *testing.T) {
	manager := NewFailoverManager()

	// Add a healthy node
	_ = manager.AddNode(&FailoverNode{
		ID:     "node1",
		Address: "localhost:3306",
		Healthy: true,
		Load:    0,
	})

	// Get active node
	node := manager.GetActiveNode()
	assert.NotNil(t, node)
	assert.True(t, node.Healthy)
}
