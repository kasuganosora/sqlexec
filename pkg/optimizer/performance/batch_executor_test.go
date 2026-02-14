package performance

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchExecutorAdd(t *testing.T) {
	flushCalled := false
	var flushedItems []interface{}

	flushFunc := func(items []interface{}) error {
		flushCalled = true
		flushedItems = items
		return nil
	}

	// Use a longer interval to avoid timer interference
	be := NewBatchExecutor(3, 1*time.Hour, flushFunc)
	defer be.Close()

	// Test Add - first item
	err := be.Add("item1")
	if err != nil {
		t.Errorf("Add() error = %v", err)
	}

	if flushCalled {
		t.Error("Flush should not be called after first item")
	}

	// Add second item
	err = be.Add("item2")
	if err != nil {
		t.Errorf("Add() error = %v", err)
	}

	// Should not flush yet (only 2 items)
	if flushCalled {
		t.Error("Flush should not be called after second item")
	}

	// Add third item, should trigger flush
	err = be.Add("item3")
	if err != nil {
		t.Errorf("Add() error = %v", err)
	}

	// Check if flush was called
	if !flushCalled {
		t.Error("Flush should be called after 3 items")
	}

	if len(flushedItems) != 3 {
		t.Errorf("Flushed items count = %v, want 3", len(flushedItems))
	}
}

func TestBatchExecutorManualFlush(t *testing.T) {
	flushCalled := false
	var flushedItems []interface{}

	flushFunc := func(items []interface{}) error {
		flushCalled = true
		flushedItems = items
		return nil
	}

	be := NewBatchExecutor(10, 100*time.Millisecond, flushFunc)
	defer be.Close()

	// Add items
	be.Add("item1")
	be.Add("item2")

	// Manual flush
	err := be.Flush()
	if err != nil {
		t.Errorf("Flush() error = %v", err)
	}

	if !flushCalled {
		t.Error("Flush should be called")
	}

	if len(flushedItems) != 2 {
		t.Errorf("Flushed items count = %v, want 2", len(flushedItems))
	}
}

func TestBatchExecutorTimerFlush(t *testing.T) {
	var flushCalled atomic.Bool

	flushFunc := func(items []interface{}) error {
		flushCalled.Store(true)
		return nil
	}

	be := NewBatchExecutor(10, 50*time.Millisecond, flushFunc)
	defer be.Close()

	// Add one item
	be.Add("item1")

	// Wait for timer to trigger
	time.Sleep(100 * time.Millisecond)

	if !flushCalled.Load() {
		t.Error("Flush should be called by timer")
	}
}

func TestBatchExecutorClose(t *testing.T) {
	flushCalled := false

	flushFunc := func(items []interface{}) error {
		flushCalled = true
		return nil
	}

	be := NewBatchExecutor(10, 100*time.Millisecond, flushFunc)

	be.Add("item1")
	be.Add("item2")

	err := be.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !flushCalled {
		t.Error("Flush should be called on Close")
	}
}
