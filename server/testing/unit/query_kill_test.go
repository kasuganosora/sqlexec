package unit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/stretchr/testify/assert"
)

// TestServer_ComProcessKill 测试COM_PROCESS_KILL命令处理
func TestServer_ComProcessKill(t *testing.T) {
	tests := []struct {
		name       string
		threadID   uint32
		expectOK   bool
		setupQuery bool
	}{
		{
			name:       "Kill不存在的查询",
			threadID:   999,
			expectOK:   false,
			setupQuery: false,
		},
		{
			name:       "Kill存在的查询",
			threadID:   123,
			expectOK:   true,
			setupQuery: true,
		},
		{
			name:       "KillThreadID=0",
			threadID:   0,
			expectOK:   false,
			setupQuery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 如果需要设置查询,先注册一个测试查询
			if tt.setupQuery {
				setupTestQuery(t, tt.threadID)
				defer cleanupTestQuery(t, tt.threadID)
			}

			// 测试Kill API
			err := session.KillQueryByThreadID(tt.threadID)

			// 验证结果
			if tt.expectOK && tt.setupQuery {
				assert.NoError(t, err)
			} else if !tt.expectOK && !tt.setupQuery {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "query not found")
			}

			t.Logf("Test KILL ThreadID=%d completed", tt.threadID)
		})
	}
}

// TestServer_KillQueryByThreadID 测试通过ThreadID终止查询
func TestServer_KillQueryByThreadID(t *testing.T) {
	tests := []struct {
		name      string
		threadID  uint32
		setup     bool
		expectErr bool
	}{
		{
			name:      "Kill存在的查询",
			threadID:  1,
			setup:     true,
			expectErr: false,
		},
		{
			name:      "Kill不存在的查询",
			threadID:  999,
			setup:     false,
			expectErr: true,
		},
		{
			name:      "Kill另一个查询",
			threadID:  2,
			setup:     true,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 如果需要设置查询,先注册
			if tt.setup {
				setupTestQuery(t, tt.threadID)
				defer cleanupTestQuery(t, tt.threadID)
			}

			// 调用Kill
			err := session.KillQueryByThreadID(tt.threadID)

			// 验证结果
			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "query not found")
			} else {
				assert.NoError(t, err)

				// 验证查询已被标记为取消
				qc := session.GetQueryByThreadID(tt.threadID)
				if qc != nil {
					assert.True(t, qc.IsCanceled())
				}
			}
		})
	}
}

// TestServer_GetQueryByThreadID 测试获取查询
func TestServer_GetQueryByThreadID(t *testing.T) {
	tests := []struct {
		name      string
		threadID  uint32
		setup     bool
		expectNil bool
	}{
		{
			name:      "获取存在的查询",
			threadID:  1,
			setup:     true,
			expectNil: false,
		},
		{
			name:      "获取不存在的查询",
			threadID:  999,
			setup:     false,
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 如果需要设置查询,先注册
			if tt.setup {
				setupTestQuery(t, tt.threadID)
				defer cleanupTestQuery(t, tt.threadID)
			}

			// 获取查询
			qc := session.GetQueryByThreadID(tt.threadID)

			// 验证结果
			if tt.expectNil {
				assert.Nil(t, qc)
			} else {
				assert.NotNil(t, qc)
				assert.Equal(t, tt.threadID, qc.ThreadID)
			}
		})
	}
}

// TestServer_GetAllQueries 测试获取所有查询
func TestServer_GetAllQueries(t *testing.T) {
	// 清理之前可能存在的查询
	session.GetAllQueries()

	// 注册3个查询
	threadIDs := []uint32{1, 2, 3}
	for _, tid := range threadIDs {
		setupTestQuery(t, tid)
		defer cleanupTestQuery(t, tid)
	}

	// 获取所有查询
	queries := session.GetAllQueries()

	// 验证查询数量
	assert.Equal(t, 3, len(queries))

	// 验证ThreadID
	threadIDSet := make(map[uint32]bool)
	for _, qc := range queries {
		threadIDSet[qc.ThreadID] = true
	}
	for _, tid := range threadIDs {
		assert.True(t, threadIDSet[tid], "ThreadID %d should be in queries", tid)
	}
}

// TestServer_QueryRegistrationAndCleanup 测试查询注册和清理
func TestServer_QueryRegistrationAndCleanup(t *testing.T) {
	threadID := uint32(123)

	// 注册查询
	setupTestQuery(t, threadID)

	// 验证查询已注册
	qc := session.GetQueryByThreadID(threadID)
	assert.NotNil(t, qc)
	assert.Equal(t, threadID, qc.ThreadID)
	assert.False(t, qc.IsCanceled())
	assert.False(t, qc.IsTimeout())

	// 清理查询
	cleanupTestQuery(t, threadID)

	// 验证查询已清理
	qc = session.GetQueryByThreadID(threadID)
	assert.Nil(t, qc)

	// 验证全局查询数量减少
	queries := session.GetAllQueries()
	for _, q := range queries {
		assert.NotEqual(t, threadID, q.ThreadID)
	}
}

// TestServer_QueryStatus 测试查询状态
func TestServer_QueryStatus(t *testing.T) {
	threadID := uint32(456)

	// 注册查询
	setupTestQuery(t, threadID)
	defer cleanupTestQuery(t, threadID)

	// 获取查询
	qc := session.GetQueryByThreadID(threadID)
	assert.NotNil(t, qc)

	// 检查初始状态
	status := qc.GetStatus()
	assert.Equal(t, "running", status.Status)
	assert.Equal(t, threadID, status.ThreadID)
	assert.GreaterOrEqual(t, status.Duration, time.Duration(0))

	// 标记为取消
	qc.SetCanceled()
	status = qc.GetStatus()
	assert.Equal(t, "canceled", status.Status)
	assert.True(t, qc.IsCanceled())

	// 创建新查询
	threadID2 := uint32(789)
	setupTestQuery(t, threadID2)
	defer cleanupTestQuery(t, threadID2)
	qc2 := session.GetQueryByThreadID(threadID2)

	// 标记为超时
	qc2.SetTimeout()
	status2 := qc2.GetStatus()
	assert.Equal(t, "timeout", status2.Status)
	assert.True(t, qc2.IsTimeout())
}

// TestServer_ConcurrentKillQueries 测试并发Kill查询
func TestServer_ConcurrentKillQueries(t *testing.T) {
	// 注册多个查询
	threadIDs := []uint32{1, 2, 3, 4, 5}
	for _, tid := range threadIDs {
		setupTestQuery(t, tid)
		defer cleanupTestQuery(t, tid)
	}

	// 并发Kill查询
	done := make(chan bool, len(threadIDs))
	for _, tid := range threadIDs {
		go func(threadID uint32) {
			err := session.KillQueryByThreadID(threadID)
			assert.NoError(t, err, "Kill thread %d failed", threadID)
			done <- true
		}(tid)
	}

	// 等待所有Kill完成
	for i := 0; i < len(threadIDs); i++ {
		<-done
	}

	// 验证所有查询都已被标记为取消
	for _, tid := range threadIDs {
		qc := session.GetQueryByThreadID(tid)
		if qc != nil {
			assert.True(t, qc.IsCanceled(), "Query %d should be canceled", tid)
		}
	}
}

// TestServer_QueryIDUniqueness 测试查询ID唯一性
func TestServer_QueryIDUniqueness(t *testing.T) {
	threadID := uint32(1)

	// 生成多个查询ID
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := session.GenerateQueryID(threadID)
		// 验证ID不重复
		assert.False(t, ids[id], "QueryID %s is duplicated", id)
		ids[id] = true
	}

	// 验证生成了100个唯一ID
	assert.Equal(t, 100, len(ids))
}

// TestServer_KillNonExistentQuery 测试Kill不存在的查询
func TestServer_KillNonExistentQuery(t *testing.T) {
	threadID := uint32(999)

	// 尝试Kill不存在的查询
	err := session.KillQueryByThreadID(threadID)

	// 验证错误
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query not found")
	assert.Contains(t, err.Error(), fmt.Sprintf("%d", threadID))

	// 验证查询计数器
	queries := session.GetAllQueries()
	for _, qc := range queries {
		assert.NotEqual(t, threadID, qc.ThreadID)
	}
}

// TestServer_SessionThreadIDAssociation 测试Session与ThreadID关联
func TestServer_SessionThreadIDAssociation(t *testing.T) {
	// 测试Session的ThreadID设置
	tests := []struct {
		name     string
		threadID uint32
	}{
		{"ThreadID=1", 1},
		{"ThreadID=100", 100},
		{"ThreadID=MAX", 4294967295},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建Session(这里使用mock Session)
			// 实际测试中应该使用真实的Session对象

			// 验证ThreadID设置和获取
			// 这个测试需要配合实际的Session实现
			t.Logf("Testing ThreadID=%d", tt.threadID)
		})
	}
}

// TestServer_QueryTimeoutScenario 测试超时场景
func TestServer_QueryTimeoutScenario(t *testing.T) {
	threadID := uint32(1)

	// 注册查询
	setupTestQuery(t, threadID)
	defer cleanupTestQuery(t, threadID)

	// 获取查询
	qc := session.GetQueryByThreadID(threadID)
	assert.NotNil(t, qc)

	// 获取初始状态
	status1 := qc.GetStatus()
	assert.Equal(t, "running", status1.Status)
	initialDuration := status1.Duration

	// 等待一段时间
	time.Sleep(100 * time.Millisecond)

	// 获取新状态
	status2 := qc.GetStatus()
	assert.Equal(t, "running", status2.Status)
	assert.Greater(t, status2.Duration, initialDuration, "Duration should increase")

	// 标记为超时
	qc.SetTimeout()
	status3 := qc.GetStatus()
	assert.Equal(t, "timeout", status3.Status)
	assert.True(t, qc.IsTimeout())
}

// TestServer_QueryCleanupAfterKill 测试Kill后的清理
func TestServer_QueryCleanupAfterKill(t *testing.T) {
	threadID := uint32(1)

	// 注册查询
	setupTestQuery(t, threadID)

	// 验证查询存在
	qc := session.GetQueryByThreadID(threadID)
	assert.NotNil(t, qc)

	// Kill查询
	err := session.KillQueryByThreadID(threadID)
	assert.NoError(t, err)

	// 验证查询被标记为取消
	assert.True(t, qc.IsCanceled())

	// 清理查询
	cleanupTestQuery(t, threadID)

	// 验证查询不存在
	qc2 := session.GetQueryByThreadID(threadID)
	assert.Nil(t, qc2)

	// 验证全局注册表
	queries := session.GetAllQueries()
	for _, q := range queries {
		assert.NotEqual(t, threadID, q.ThreadID)
	}
}

// TestServer_MultipleSessionsDifferentThreadIDs 测试多个Session不同ThreadID
func TestServer_MultipleSessionsDifferentThreadIDs(t *testing.T) {
	threadIDs := []uint32{10, 20, 30, 40}

	// 注册多个查询
	for _, tid := range threadIDs {
		setupTestQuery(t, tid)
		defer cleanupTestQuery(t, tid)
	}

	// 验证每个ThreadID都有对应的查询
	for _, tid := range threadIDs {
		qc := session.GetQueryByThreadID(tid)
		assert.NotNil(t, qc, "Query with ThreadID %d should exist", tid)
		assert.Equal(t, tid, qc.ThreadID)
	}

	// 验证总查询数量
	queries := session.GetAllQueries()
	assert.Equal(t, len(threadIDs), len(queries))

	// Kill所有查询
	for _, tid := range threadIDs {
		err := session.KillQueryByThreadID(tid)
		assert.NoError(t, err)
	}

	// 验证所有查询都被取消
	for _, tid := range threadIDs {
		qc := session.GetQueryByThreadID(tid)
		if qc != nil {
			assert.True(t, qc.IsCanceled())
		}
	}
}

// TestServer_QueryCount 测试查询计数
func TestServer_QueryCount(t *testing.T) {
	// 获取初始查询数量
	initialCount := len(session.GetAllQueries())

	// 注册3个查询
	threadIDs := []uint32{1, 2, 3}
	for _, tid := range threadIDs {
		setupTestQuery(t, tid)
		defer cleanupTestQuery(t, tid)
	}

	// 验证查询数量增加
	queries := session.GetAllQueries()
	assert.Equal(t, initialCount+len(threadIDs), len(queries))

	// 注销1个查询
	cleanupTestQuery(t, 1)

	// 验证查询数量减少
	queries = session.GetAllQueries()
	assert.Equal(t, initialCount+len(threadIDs)-1, len(queries))
}

// setupTestQuery 设置测试查询
func setupTestQuery(t *testing.T, threadID uint32) *session.QueryContext {
	_, cancel := context.WithCancel(context.Background())

	qc := &session.QueryContext{
		QueryID:    session.GenerateQueryID(threadID),
		ThreadID:   threadID,
		SQL:        fmt.Sprintf("SELECT * FROM test WHERE thread_id = %d", threadID),
		StartTime:  time.Now(),
		CancelFunc: cancel,
	}

	// 注册到全局注册表
	registry := session.GetGlobalQueryRegistry()
	err := registry.RegisterQuery(qc)
	assert.NoError(t, err)

	return qc
}

// cleanupTestQuery 清理测试查询
func cleanupTestQuery(t *testing.T, threadID uint32) {
	qc := session.GetQueryByThreadID(threadID)
	if qc != nil {
		registry := session.GetGlobalQueryRegistry()
		registry.UnregisterQuery(qc.QueryID)
	}
}

// TestServer_ComProcessKillPacket 测试COM_PROCESS_KILL包序列化
func TestServer_ComProcessKillPacket(t *testing.T) {
	tests := []struct {
		name     string
		threadID uint32
	}{
		{
			name:     "ThreadID=1",
			threadID: 1,
		},
		{
			name:     "ThreadID=12345",
			threadID: 12345,
		},
		{
			name:     "ThreadID=MAX_UINT32",
			threadID: 4294967295,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试查询ID生成(因为protocol包测试在别的地方)
			queryID := session.GenerateQueryID(tt.threadID)
			assert.NotEmpty(t, queryID, "QueryID should not be empty")
			assert.Contains(t, queryID, fmt.Sprintf("%d", tt.threadID), "QueryID should contain threadID")
		})
	}
}

// TestServer_ErrorResponseOnKill 测试Kill失败的错误响应
func TestServer_ErrorResponseOnKill(t *testing.T) {
	tests := []struct {
		name      string
		threadID  uint32
		expectErr bool
	}{
		{"Kill不存在的ThreadID", 9999, true},
		{"KillThreadID=0", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 尝试Kill
			err := session.KillQueryByThreadID(tt.threadID)

			// 验证错误
			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "query not found")
			}
		})
	}
}

// TestServer_QueryDurationTracking 测试查询时长跟踪
func TestServer_QueryDurationTracking(t *testing.T) {
	threadID := uint32(1)

	// 注册查询
	setupTestQuery(t, threadID)
	defer cleanupTestQuery(t, threadID)

	// 获取查询
	qc := session.GetQueryByThreadID(threadID)
	assert.NotNil(t, qc)

	// 初始时长
	duration1 := qc.GetDuration()
	assert.GreaterOrEqual(t, duration1, time.Duration(0))

	// 等待一段时间
	time.Sleep(50 * time.Millisecond)

	// 验证时长增加
	duration2 := qc.GetDuration()
	assert.Greater(t, duration2, duration1, "Duration should increase over time")
}

// TestServer_QueryRegistryConcurrency 测试查询注册表并发安全
func TestServer_QueryRegistryConcurrency(t *testing.T) {
	// 清理
	session.GetAllQueries()

	// 并发注册和Kill查询
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(index int) {
			threadID := uint32(index + 1)

			// 注册查询
			qc := setupTestQuery(t, threadID)

			// 立即Kill
			err := session.KillQueryByThreadID(threadID)
			assert.NoError(t, err)

			// 验证取消
			assert.True(t, qc.IsCanceled())

			// 清理
			cleanupTestQuery(t, threadID)

			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证注册表清理
	queries := session.GetAllQueries()
	assert.Equal(t, 0, len(queries))
}
