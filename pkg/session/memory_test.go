package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemoryDriver(t *testing.T) {
	driver := NewMemoryDriver()

	assert.NotNil(t, driver)
	assert.NotNil(t, driver.SessionMap)
	assert.NotNil(t, driver.Values)
	// driver.Mutex is a sync.RWMutex value type — zero value is valid, no nil check needed
}

func TestMemoryDriver_CreateSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)

	assert.NoError(t, err)
	assert.NotNil(t, driver.SessionMap[sess.ID])
	assert.Equal(t, sess.ID, driver.SessionMap[sess.ID].ID)
	assert.NotNil(t, driver.Values[sess.ID])
}

func TestMemoryDriver_GetSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	retrievedSess, err := driver.GetSession(ctx, "test_session_id")

	assert.NoError(t, err)
	assert.NotNil(t, retrievedSess)
	assert.Equal(t, sess.ID, retrievedSess.ID)
}

func TestMemoryDriver_GetSession_NotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess, err := driver.GetSession(ctx, "non_existent_id")

	assert.Error(t, err)
	assert.Nil(t, sess)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetSessions(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	// 创建多个会话
	sess1 := &Session{ID: "sess1", RemoteIP: "127.0.0.1", RemotePort: "3306", Created: time.Now(), LastUsed: time.Now()}
	sess2 := &Session{ID: "sess2", RemoteIP: "127.0.0.2", RemotePort: "3306", Created: time.Now(), LastUsed: time.Now()}
	sess3 := &Session{ID: "sess3", RemoteIP: "127.0.0.3", RemotePort: "3306", Created: time.Now(), LastUsed: time.Now()}

	err := driver.CreateSession(ctx, sess1)
	require.NoError(t, err)
	err = driver.CreateSession(ctx, sess2)
	require.NoError(t, err)
	err = driver.CreateSession(ctx, sess3)
	require.NoError(t, err)

	sessions, err := driver.GetSessions(ctx)

	assert.NoError(t, err)
	assert.Len(t, sessions, 3)
}

func TestMemoryDriver_DeleteSession(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	err = driver.DeleteSession(ctx, "test_session_id")
	assert.NoError(t, err)

	// 验证会话已被删除
	_, err = driver.GetSession(ctx, "test_session_id")
	assert.Error(t, err)

	// 验证值也被删除
	_, ok := driver.Values["test_session_id"]
	assert.False(t, ok)
}

func TestMemoryDriver_SetKey(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	err = driver.SetKey(ctx, "test_session_id", "username", "testuser")
	assert.NoError(t, err)

	assert.Equal(t, "testuser", driver.Values["test_session_id"]["username"])
}

func TestMemoryDriver_SetKey_SessionNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	err := driver.SetKey(ctx, "non_existent_session", "key", "value")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetKey(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	driver.Values["test_session_id"]["test_key"] = "test_value"

	val, err := driver.GetKey(ctx, "test_session_id", "test_key")

	assert.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

func TestMemoryDriver_GetKey_SessionNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	val, err := driver.GetKey(ctx, "non_existent_session", "key")

	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetKey_KeyNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	val, err := driver.GetKey(ctx, "test_session_id", "non_existent_key")

	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Contains(t, err.Error(), "key not found")
}

func TestMemoryDriver_DeleteKey(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	driver.Values["test_session_id"]["test_key"] = "test_value"

	err = driver.DeleteKey(ctx, "test_session_id", "test_key")
	assert.NoError(t, err)

	// 验证键已被删除
	_, ok := driver.Values["test_session_id"]["test_key"]
	assert.False(t, ok)
}

func TestMemoryDriver_DeleteKey_SessionNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	err := driver.DeleteKey(ctx, "non_existent_session", "key")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetAllKeys(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	// 设置多个键
	driver.Values["test_session_id"]["key1"] = "value1"
	driver.Values["test_session_id"]["key2"] = "value2"
	driver.Values["test_session_id"]["key3"] = "value3"

	keys, err := driver.GetAllKeys(ctx, "test_session_id")

	assert.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
	assert.Contains(t, keys, "key3")
}

func TestMemoryDriver_GetAllKeys_SessionNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	keys, err := driver.GetAllKeys(ctx, "non_existent_session")

	assert.Error(t, err)
	assert.Nil(t, keys)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetAllKeys_Empty(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	keys, err := driver.GetAllKeys(ctx, "test_session_id")

	assert.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestMemoryDriver_Touch(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	oldLastUsed := driver.SessionMap["test_session_id"].LastUsed

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	err = driver.Touch(ctx, "test_session_id")
	assert.NoError(t, err)

	// 验证LastUsed已更新
	assert.True(t, driver.SessionMap["test_session_id"].LastUsed.After(oldLastUsed))
}

func TestMemoryDriver_Touch_SessionNotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	err := driver.Touch(ctx, "non_existent_session")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session not found")
}

func TestMemoryDriver_GetThreadId(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		ThreadID:   456,
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	// 先创建 session
	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	err = driver.SetThreadId(ctx, 456, sess)
	require.NoError(t, err)

	threadID, err := driver.GetThreadId(ctx, 456)

	assert.NoError(t, err)
	assert.Equal(t, uint32(456), threadID)
}

func TestMemoryDriver_GetThreadId_NotFound(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	threadID, err := driver.GetThreadId(ctx, 999)

	assert.Error(t, err)
	assert.Equal(t, uint32(0), threadID)
	assert.Contains(t, err.Error(), "thread id not found")
}

func TestMemoryDriver_DeleteThreadId(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		ThreadID:   789,
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.SetThreadId(ctx, 789, sess)
	require.NoError(t, err)

	err = driver.DeleteThreadId(ctx, 789)
	assert.NoError(t, err)

	// 验证threadId已被删除
	_, err = driver.GetThreadId(ctx, 789)
	assert.Error(t, err)
}

func TestMemoryDriver_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	// 并发写入
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := "key" + string(rune('0'+i))
			driver.SetKey(ctx, "test_session_id", key, i)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有键都已设置
	keys, err := driver.GetAllKeys(ctx, "test_session_id")
	assert.NoError(t, err)
	assert.Len(t, keys, 10)
}

func TestMemoryDriver_GetKey_UpdatesLastUsed(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	driver.Values["test_session_id"]["test_key"] = "test_value"

	oldLastUsed := driver.SessionMap["test_session_id"].LastUsed

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	_, _ = driver.GetKey(ctx, "test_session_id", "test_key")

	// 验证LastUsed已更新
	assert.True(t, driver.SessionMap["test_session_id"].LastUsed.After(oldLastUsed))
}

func TestMemoryDriver_SetKey_UpdatesLastUsed(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	oldLastUsed := driver.SessionMap["test_session_id"].LastUsed

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	_ = driver.SetKey(ctx, "test_session_id", "test_key", "test_value")

	// 验证LastUsed已更新
	assert.True(t, driver.SessionMap["test_session_id"].LastUsed.After(oldLastUsed))
}

func TestMemoryDriver_DeleteKey_UpdatesLastUsed(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "test_session_id",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	driver.Values["test_session_id"]["test_key"] = "test_value"

	oldLastUsed := driver.SessionMap["test_session_id"].LastUsed

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	_ = driver.DeleteKey(ctx, "test_session_id", "test_key")

	// 验证LastUsed已更新
	assert.True(t, driver.SessionMap["test_session_id"].LastUsed.After(oldLastUsed))
}
