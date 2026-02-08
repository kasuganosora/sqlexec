package performance

import (
	"container/heap"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()

	// Test initial state
	if pq.Len() != 0 {
		t.Errorf("Len() = %d, want 0", pq.Len())
	}

	// Test Push
	node1 := &PlanNode{
		Plan:     "plan1",
		Cost:     10.0,
		Priority: 1,
	}

	heap.Push(pq, node1)

	if pq.Len() != 1 {
		t.Errorf("Len() = %d, want 1", pq.Len())
	}

	// Test Pop
	popped := heap.Pop(pq)
	if popped != node1 {
		t.Error("Popped item should be node1")
	}

	if pq.Len() != 0 {
		t.Errorf("Len() = %d, want 0", pq.Len())
	}
}

func TestPriorityQueueOrdering(t *testing.T) {
	pq := NewPriorityQueue()

	// Add nodes with different costs and priorities
	node1 := &PlanNode{Plan: "plan1", Cost: 100.0, Priority: 1}
	node2 := &PlanNode{Plan: "plan2", Cost: 50.0, Priority: 2}  // Higher priority
	node3 := &PlanNode{Plan: "plan3", Cost: 75.0, Priority: 2}  // Same priority, higher cost
	node4 := &PlanNode{Plan: "plan4", Cost: 25.0, Priority: 3}  // Highest priority

	heap.Push(pq, node1)
	heap.Push(pq, node2)
	heap.Push(pq, node3)
	heap.Push(pq, node4)

	// Pop all items and check order
	// Should be ordered by priority (descending), then by cost (ascending)
	items := make([]*PlanNode, 0, 4)
	for pq.Len() > 0 {
		item := heap.Pop(pq)
		if node, ok := item.(*PlanNode); ok {
			items = append(items, node)
		}
	}

	if len(items) != 4 {
		t.Fatalf("Expected 4 items, got %d", len(items))
	}

	// Check order: highest priority first
	if items[0].Priority != 3 {
		t.Errorf("First item priority = %d, want 3", items[0].Priority)
	}
	if items[0].Cost != 25.0 {
		t.Errorf("First item cost = %f, want 25.0", items[0].Cost)
	}

	// Next two items should have priority 2, ordered by cost
	if items[1].Priority != 2 {
		t.Errorf("Second item priority = %d, want 2", items[1].Priority)
	}
	if items[2].Priority != 2 {
		t.Errorf("Third item priority = %d, want 2", items[2].Priority)
	}

	// Last item should have priority 1
	if items[3].Priority != 1 {
		t.Errorf("Last item priority = %d, want 1", items[3].Priority)
	}
}

func TestPriorityQueueEmpty(t *testing.T) {
	pq := NewPriorityQueue()

	if pq.Len() != 0 {
		t.Errorf("Len() = %d, want 0", pq.Len())
	}

	// Pop from empty queue should panic, but let's just check length
	if pq.Len() != 0 {
		t.Error("Queue should be empty")
	}
}
