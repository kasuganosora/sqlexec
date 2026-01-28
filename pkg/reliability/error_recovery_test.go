package reliability

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewErrorRecoveryManager(t *testing.T) {
	manager := NewErrorRecoveryManager()

	assert.NotNil(t, manager)
}

func TestExecuteWithRetry_Success(t *testing.T) {
	manager := NewErrorRecoveryManager()

	attempts := 0
	err := manager.ExecuteWithRetry(3, func() error {
		attempts++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestExecuteWithRetry_RetryOnFailure(t *testing.T) {
	manager := NewErrorRecoveryManager()

	attempts := 0
	_ = manager.ExecuteWithRetry(3, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

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
