package mock

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMockSession_SequenceID_InitialValue tests initial sequence ID
func TestMockSession_SequenceID_InitialValue(t *testing.T) {
	sess := NewMockSession()
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "Initial sequence ID should be 0")
}

// TestMockSession_GetNextSequenceID_SingleCall tests single call to get next sequence ID
func TestMockSession_GetNextSequenceID_SingleCall(t *testing.T) {
	sess := NewMockSession()

	// When: Get next sequence ID
	seqID := sess.GetNextSequenceID()

	// Then: Verify returns 0
	assert.Equal(t, uint8(0), seqID, "First call should return 0")

	// And: Verify internal sequence ID increments to 1
	assert.Equal(t, uint8(1), sess.GetSequenceID(), "Internal sequence ID should increment to 1")
}

// TestMockSession_GetNextSequenceID_MultipleCalls tests multiple calls increment sequence ID
func TestMockSession_GetNextSequenceID_MultipleCalls(t *testing.T) {
	sess := NewMockSession()

	// When: Get sequence IDs multiple times
	sequences := make([]uint8, 10)
	for i := 0; i < 10; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: Verify sequence IDs increment correctly
	expected := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, expected, sequences, "Sequence IDs should increment from 0")

	// And: Verify current sequence ID is 10
	assert.Equal(t, uint8(10), sess.GetSequenceID(), "Current sequence ID should be 10")
}

// TestMockSession_ResetSequenceID tests sequence ID reset
func TestMockSession_ResetSequenceID(t *testing.T) {
	sess := NewMockSession()

	// Given: Get several sequence IDs
	for i := 0; i < 5; i++ {
		sess.GetNextSequenceID()
	}
	assert.Equal(t, uint8(5), sess.GetSequenceID())

	// When: Reset sequence ID
	sess.ResetSequenceID()

	// Then: Verify sequence ID is 0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "After reset sequence ID should be 0")

	// And: Verify next sequence ID is 0
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(0), nextSeqID, "First call after reset should return 0")
}

// TestMockSession_SetSequenceID tests setting sequence ID
func TestMockSession_SetSequenceID(t *testing.T) {
	sess := NewMockSession()

	// When: Set sequence ID to 100
	sess.SetSequenceID(100)

	// Then: Verify sequence ID is set
	assert.Equal(t, uint8(100), sess.GetSequenceID(), "Sequence ID should be set to 100")

	// And: Verify next sequence ID is 100
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(100), nextSeqID, "Next sequence ID should be 100")
}

// TestMockSession_SequenceID_Overflow255 tests 255 overflow wraps to 0
func TestMockSession_SequenceID_Overflow255(t *testing.T) {
	sess := NewMockSession()

	// Given: Set sequence ID to 255
	sess.SetSequenceID(255)

	// When: Get next sequence ID
	seqID := sess.GetNextSequenceID()

	// Then: Verify returns 255
	assert.Equal(t, uint8(255), seqID, "Should return 255")

	// And: Verify sequence ID wraps to 0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "After 255 should wrap to 0")

	// And: Verify next sequence ID is 0
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(0), nextSeqID, "After wrap should return 0")
}

// TestMockSession_SequenceID_OverflowMultipleTimes tests multiple overflows
func TestMockSession_SequenceID_OverflowMultipleTimes(t *testing.T) {
	sess := NewMockSession()

	// When: Call 300 times (exceeds 255)
	sequences := make([]uint8, 300)
	for i := 0; i < 300; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: Verify sequence IDs wrap correctly
	// First 256 sequence IDs should be 0-255
	for i := 0; i < 256; i++ {
		assert.Equal(t, uint8(i), sequences[i], "Sequence ID %d should be %d", i, i)
	}

	// 257th sequence ID should be 0
	assert.Equal(t, uint8(0), sequences[256], "257th sequence ID should be 0")

	// 258th sequence ID should be 1
	assert.Equal(t, uint8(1), sequences[257], "258th sequence ID should be 1")

	// 300th sequence ID should be 43 ((300-1) % 256 = 299 % 256 = 43)
	assert.Equal(t, uint8(43), sequences[299], "300th sequence ID should be 43")
}

// TestMockSession_SequenceID_Exact255 tests exactly 255 case
func TestMockSession_SequenceID_Exact255(t *testing.T) {
	sess := NewMockSession()

	// Given: Get 255 sequence IDs
	sequences := make([]uint8, 255)
	for i := 0; i < 255; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: Verify 254th sequence ID is 254
	assert.Equal(t, uint8(254), sequences[254], "254th sequence ID should be 254")

	// And: Verify current sequence ID is 255
	assert.Equal(t, uint8(255), sess.GetSequenceID(), "Current sequence ID should be 255")
}

// TestMockSession_SequenceID_ConcurrentAccess tests concurrent access to sequence ID
func TestMockSession_SequenceID_ConcurrentAccess(t *testing.T) {
	sess := NewMockSession()

	var wg sync.WaitGroup
	numGoroutines := 100
	numCallsPerGoroutine := 10

	// When: Concurrent calls to GetNextSequenceID
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numCallsPerGoroutine; j++ {
				sess.GetNextSequenceID()
			}
		}()
	}

	wg.Wait()

	// Then: Verify total sequence IDs count is correct
	expectedTotal := numGoroutines * numCallsPerGoroutine
	assert.Equal(t, uint8(expectedTotal%256), sess.GetSequenceID(), "Sequence ID should be %d", expectedTotal%256)
}

// TestMockSession_SequenceID_ConcurrentReset tests concurrent reset and increment
func TestMockSession_SequenceID_ConcurrentReset(t *testing.T) {
	sess := NewMockSession()

	var wg sync.WaitGroup

	// When: Concurrent reset and increment
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			sess.GetNextSequenceID()
		}()
		go func() {
			defer wg.Done()
			sess.ResetSequenceID()
		}()
	}

	wg.Wait()

	// Then: Verify no crash, sequence ID in valid range
	seqID := sess.GetSequenceID()
	assert.GreaterOrEqual(t, seqID, uint8(0), "Sequence ID should be >= 0")
	assert.LessOrEqual(t, seqID, uint8(255), "Sequence ID should be <= 255")
}

// TestMockSession_SequenceID_GetDoesNotModify tests GetSequenceID does not modify internal state
func TestMockSession_SequenceID_GetDoesNotModify(t *testing.T) {
	sess := NewMockSession()

	// When: Multiple calls to GetSequenceID (not GetNextSequenceID)
	for i := 0; i < 10; i++ {
		_ = sess.GetSequenceID()
	}

	// Then: Verify sequence ID is still 0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "GetSequenceID should not modify internal state")
}

// TestMockSession_SequenceID_AfterReset tests behavior after reset
func TestMockSession_SequenceID_AfterReset(t *testing.T) {
	sess := NewMockSession()

	// Given: Get some sequence IDs
	for i := 0; i < 10; i++ {
		sess.GetNextSequenceID()
	}

	// When: Reset and get sequence IDs
	sess.ResetSequenceID()
	sequences := make([]uint8, 5)
	for i := 0; i < 5; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: Verify sequence IDs start from 0 after reset
	expected := []uint8{0, 1, 2, 3, 4}
	assert.Equal(t, expected, sequences, "After reset sequence IDs should start from 0")
}

// TestMockSession_Clone_PreservesSequenceID tests Clone preserves sequence ID
func TestMockSession_Clone_PreservesSequenceID(t *testing.T) {
	sess := NewMockSession()

	// Given: Set sequence ID
	for i := 0; i < 5; i++ {
		sess.GetNextSequenceID()
	}

	// When: Clone session
	clone := sess.Clone()

	// Then: Verify cloned sequence ID is same
	assert.Equal(t, sess.GetSequenceID(), clone.GetSequenceID(), "Clone should preserve sequence ID")

	// And: Verify clone independence
	sess.GetNextSequenceID()
	assert.NotEqual(t, sess.GetSequenceID(), clone.GetSequenceID(), "Clone should be independent")
}
