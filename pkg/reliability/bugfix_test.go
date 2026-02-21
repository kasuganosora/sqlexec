package reliability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyChecker is a no-op health checker for testing
type dummyChecker struct{}

func (d *dummyChecker) Check(node *Node) error { return nil }

// ==========================================================================
// Bug 3 (P1): selectActiveNode uses maxWeight=0 with strict >
// When all healthy nodes have weight 0, none are selected, leaving
// activeNode nil even though healthy nodes exist.
// ==========================================================================

func TestBug3_SelectActiveNode_WeightZero(t *testing.T) {
	fm := NewFailoverManager(&dummyChecker{}, 0, 0)

	// Add two nodes with weight 0
	require.NoError(t, fm.AddNode("node1", "addr1", 0))
	require.NoError(t, fm.AddNode("node2", "addr2", 0))

	// node1 should be active (first added, set directly)
	active := fm.GetActiveNode()
	require.NotNil(t, active)
	assert.Equal(t, "node1", active.ID)

	// Remove node1 — should trigger selectActiveNode()
	require.NoError(t, fm.RemoveNode("node1"))

	// node2 should now be active (it's healthy, weight 0)
	active = fm.GetActiveNode()
	assert.NotNil(t, active,
		"selectActiveNode should select healthy weight-0 nodes, not return nil")
	if active != nil {
		assert.Equal(t, "node2", active.ID)
	}
}

func TestBug3_SelectActiveNode_AllSameWeight(t *testing.T) {
	fm := NewFailoverManager(&dummyChecker{}, 0, 0)

	require.NoError(t, fm.AddNode("a", "addr_a", 5))
	require.NoError(t, fm.AddNode("b", "addr_b", 5))
	require.NoError(t, fm.AddNode("c", "addr_c", 5))

	// Remove active node
	active := fm.GetActiveNode()
	require.NotNil(t, active)
	activeID := active.ID

	require.NoError(t, fm.RemoveNode(activeID))

	// Another node should become active
	newActive := fm.GetActiveNode()
	assert.NotNil(t, newActive,
		"after removing active node, another same-weight node should be selected")
}

// ==========================================================================
// Bug 4 (P1): LeastLoadedNode uses minLoad=1.0
// If all healthy nodes have load >= 1.0, the function returns nil
// even though healthy nodes exist. Should return the least-loaded one.
// ==========================================================================

func TestBug4_LeastLoadedNode_AllFullyLoaded(t *testing.T) {
	lb := NewLoadBalancer()

	lb.AddNode(&Node{ID: "n1", Status: NodeStatusHealthy, Load: 1.0})
	lb.AddNode(&Node{ID: "n2", Status: NodeStatusHealthy, Load: 1.5})
	lb.AddNode(&Node{ID: "n3", Status: NodeStatusHealthy, Load: 2.0})

	node := lb.LeastLoadedNode()
	assert.NotNil(t, node,
		"LeastLoadedNode should return a node even when all loads >= 1.0")
	if node != nil {
		assert.Equal(t, "n1", node.ID,
			"should return the node with lowest load (1.0)")
	}
}

func TestBug4_LeastLoadedNode_NormalCase(t *testing.T) {
	lb := NewLoadBalancer()

	lb.AddNode(&Node{ID: "n1", Status: NodeStatusHealthy, Load: 0.8})
	lb.AddNode(&Node{ID: "n2", Status: NodeStatusHealthy, Load: 0.3})
	lb.AddNode(&Node{ID: "n3", Status: NodeStatusHealthy, Load: 0.6})

	node := lb.LeastLoadedNode()
	require.NotNil(t, node)
	assert.Equal(t, "n2", node.ID,
		"should return node with lowest load")
}

// ==========================================================================
// Bug 5 (P1): Backup metadata race — fields modified outside lock
// The metadata is inserted into the map early, then modified without
// holding the lock, creating a data race with concurrent readers.
// ==========================================================================

func TestBug5_BackupMetadata_ConsistentOnError(t *testing.T) {
	bm := NewBackupManager(t.TempDir())

	// A nil data value is rejected early, so use a channel which can't be marshaled
	id, err := bm.Backup(BackupTypeFull, []string{"users"}, make(chan int))
	require.Error(t, err, "should fail to marshal channel type")

	if id == "" {
		// If no ID returned, nothing to check
		return
	}

	// If metadata is stored, it should be in a consistent state
	metadata, getErr := bm.GetBackup(id)
	if getErr != nil {
		// Not stored at all is also acceptable (deferred insert)
		return
	}

	// If stored, status must reflect failure
	assert.Equal(t, BackupStatusFailed, metadata.Status,
		"failed backup metadata should have Failed status")
	assert.NotEmpty(t, metadata.Error,
		"failed backup metadata should have error message")
}
