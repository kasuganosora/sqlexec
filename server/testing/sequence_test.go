package testing

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMockSession_SequenceID_InitialValue 测试初始序列号
func TestMockSession_SequenceID_InitialValue(t *testing.T) {
	sess := NewMockSession()
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "初始序列号应该是0")
}

// TestMockSession_GetNextSequenceID_SingleCall 测试单次获取序列号
func TestMockSession_GetNextSequenceID_SingleCall(t *testing.T) {
	sess := NewMockSession()

	// When: 获取下一个序列号
	seqID := sess.GetNextSequenceID()

	// Then: 验证返回0
	assert.Equal(t, uint8(0), seqID, "第一次调用应该返回0")

	// And: 验证内部序列号递增为1
	assert.Equal(t, uint8(1), sess.GetSequenceID(), "内部序列号应该递增为1")
}

// TestMockSession_GetNextSequenceID_MultipleCalls 测试多次调用序列号递增
func TestMockSession_GetNextSequenceID_MultipleCalls(t *testing.T) {
	sess := NewMockSession()

	// When: 多次获取序列号
	sequences := make([]uint8, 10)
	for i := 0; i < 10; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: 验证序列号正确递增
	expected := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, expected, sequences, "序列号应该从0开始递增")

	// And: 验证当前序列号为10
	assert.Equal(t, uint8(10), sess.GetSequenceID(), "当前序列号应该是10")
}

// TestMockSession_ResetSequenceID 测试序列号重置
func TestMockSession_ResetSequenceID(t *testing.T) {
	sess := NewMockSession()

	// Given: 获取几个序列号
	for i := 0; i < 5; i++ {
		sess.GetNextSequenceID()
	}
	assert.Equal(t, uint8(5), sess.GetSequenceID())

	// When: 重置序列号
	sess.ResetSequenceID()

	// Then: 验证序列号为0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "重置后序列号应该是0")

	// And: 验证下一个序列号是0
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(0), nextSeqID, "重置后第一次调用应该返回0")
}

// TestMockSession_SetSequenceID 测试设置序列号
func TestMockSession_SetSequenceID(t *testing.T) {
	sess := NewMockSession()

	// When: 设置序列号为100
	sess.SetSequenceID(100)

	// Then: 验证序列号被设置
	assert.Equal(t, uint8(100), sess.GetSequenceID(), "序列号应该被设置为100")

	// And: 验证下一个序列号是100
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(100), nextSeqID, "下一个序列号应该是100")
}

// TestMockSession_SequenceID_Overflow255 测试255溢出后回绕到0
func TestMockSession_SequenceID_Overflow255(t *testing.T) {
	sess := NewMockSession()

	// Given: 设置序列号为255
	sess.SetSequenceID(255)

	// When: 获取下一个序列号
	seqID := sess.GetNextSequenceID()

	// Then: 验证返回255
	assert.Equal(t, uint8(255), seqID, "应该返回255")

	// And: 验证序列号回绕到0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "255之后应该回绕到0")

	// And: 验证下一个序列号是0
	nextSeqID := sess.GetNextSequenceID()
	assert.Equal(t, uint8(0), nextSeqID, "回绕后应该返回0")
}

// TestMockSession_SequenceID_OverflowMultipleTimes 测试多次溢出
func TestMockSession_SequenceID_OverflowMultipleTimes(t *testing.T) {
	sess := NewMockSession()

	// When: 调用300次（超过255）
	sequences := make([]uint8, 300)
	for i := 0; i < 300; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: 验证序列号正确回绕
	// 前256个序列号应该是0-255
	for i := 0; i < 256; i++ {
		assert.Equal(t, uint8(i), sequences[i], "序列号%d应该是%d", i, i)
	}

	// 第257个序列号应该是0
	assert.Equal(t, uint8(0), sequences[256], "第257个序列号应该是0")

	// 第258个序列号应该是1
	assert.Equal(t, uint8(1), sequences[257], "第258个序列号应该是1")

	// 第300个序列号应该是43（(300-1) % 256 = 299 % 256 = 43）
	assert.Equal(t, uint8(43), sequences[299], "第300个序列号应该是43")
}

// TestMockSession_SequenceID_Exact255 测试恰好为255的情况
func TestMockSession_SequenceID_Exact255(t *testing.T) {
	sess := NewMockSession()

	// Given: 获取255个序列号
	sequences := make([]uint8, 255)
	for i := 0; i < 255; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: 验证第254个序列号是254
	assert.Equal(t, uint8(254), sequences[254], "第254个序列号应该是254")

	// And: 验证当前序列号是255
	assert.Equal(t, uint8(255), sess.GetSequenceID(), "当前序列号应该是255")
}

// TestMockSession_SequenceID_ConcurrentAccess 测试并发访问序列号
func TestMockSession_SequenceID_ConcurrentAccess(t *testing.T) {
	sess := NewMockSession()

	var wg sync.WaitGroup
	numGoroutines := 100
	numCallsPerGoroutine := 10

	// When: 并发调用GetNextSequenceID
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

	// Then: 验证序列号总数正确
	expectedTotal := numGoroutines * numCallsPerGoroutine
	assert.Equal(t, uint8(expectedTotal%256), sess.GetSequenceID(), "序列号应该是%d", expectedTotal%256)
}

// TestMockSession_SequenceID_ConcurrentReset 测试并发重置和递增
func TestMockSession_SequenceID_ConcurrentReset(t *testing.T) {
	sess := NewMockSession()

	var wg sync.WaitGroup

	// When: 并发重置和递增
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

	// Then: 验证不会崩溃，序列号在有效范围内
	seqID := sess.GetSequenceID()
	assert.GreaterOrEqual(t, seqID, uint8(0), "序列号应该>=0")
	assert.LessOrEqual(t, seqID, uint8(255), "序列号应该<=255")
}

// TestMockSession_SequenceID_GetDoesNotModify 测试GetSequenceID不修改内部状态
func TestMockSession_SequenceID_GetDoesNotModify(t *testing.T) {
	sess := NewMockSession()

	// When: 多次调用GetSequenceID（不是GetNextSequenceID）
	for i := 0; i < 10; i++ {
		_ = sess.GetSequenceID()
	}

	// Then: 验证序列号仍然是0
	assert.Equal(t, uint8(0), sess.GetSequenceID(), "GetSequenceID不应该修改内部状态")
}

// TestMockSession_SequenceID_AfterReset 测试重置后的行为
func TestMockSession_SequenceID_AfterReset(t *testing.T) {
	sess := NewMockSession()

	// Given: 获取一些序列号
	for i := 0; i < 10; i++ {
		sess.GetNextSequenceID()
	}

	// When: 重置并获取序列号
	sess.ResetSequenceID()
	sequences := make([]uint8, 5)
	for i := 0; i < 5; i++ {
		sequences[i] = sess.GetNextSequenceID()
	}

	// Then: 验证重置后的序列号从0开始
	expected := []uint8{0, 1, 2, 3, 4}
	assert.Equal(t, expected, sequences, "重置后序列号应该从0开始")
}

// TestMockSession_Clone_PreservesSequenceID 测试Clone保留序列号
func TestMockSession_Clone_PreservesSequenceID(t *testing.T) {
	sess := NewMockSession()

	// Given: 设置序列号
	for i := 0; i < 5; i++ {
		sess.GetNextSequenceID()
	}

	// When: 克隆Session
	clone := sess.Clone()

	// Then: 验证克隆的序列号相同
	assert.Equal(t, sess.GetSequenceID(), clone.GetSequenceID(), "克隆应该保留序列号")

	// And: 验证克隆的独立性
	sess.GetNextSequenceID()
	assert.NotEqual(t, sess.GetSequenceID(), clone.GetSequenceID(), "克隆应该独立")
}
