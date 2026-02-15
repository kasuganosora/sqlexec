package mock

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// === MockConnection Tests ===

func TestMockConnection_NewMockConnection(t *testing.T) {
	conn := NewMockConnection()
	assert.NotNil(t, conn)
	assert.False(t, conn.IsClosed())
	assert.Empty(t, conn.GetWrittenData())
}

func TestMockConnection_WriteRead(t *testing.T) {
	conn := NewMockConnection()

	// Write data
	data := []byte("hello world")
	n, err := conn.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	// Verify written data
	written := conn.GetWrittenData()
	assert.Len(t, written, 1)
	assert.Equal(t, data, written[0])
}

func TestMockConnection_MultipleWrites(t *testing.T) {
	conn := NewMockConnection()

	conn.Write([]byte("first"))
	conn.Write([]byte("second"))
	conn.Write([]byte("third"))

	written := conn.GetWrittenData()
	assert.Len(t, written, 3)
	assert.Equal(t, []byte("first"), written[0])
	assert.Equal(t, []byte("second"), written[1])
	assert.Equal(t, []byte("third"), written[2])
}

func TestMockConnection_GetWrittenDataBytes(t *testing.T) {
	conn := NewMockConnection()
	conn.Write([]byte("hello"))
	conn.Write([]byte(" world"))

	combined := conn.GetWrittenDataBytes()
	assert.Equal(t, []byte("hello world"), combined)
}

func TestMockConnection_ClearWrittenData(t *testing.T) {
	conn := NewMockConnection()
	conn.Write([]byte("data"))
	assert.Len(t, conn.GetWrittenData(), 1)

	conn.ClearWrittenData()
	assert.Empty(t, conn.GetWrittenData())
}

func TestMockConnection_ReadFromQueue(t *testing.T) {
	conn := NewMockConnection()
	conn.AddReadData([]byte("packet1"))

	buf := make([]byte, 100)
	n, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, "packet1", string(buf[:n]))
}

func TestMockConnection_ReadMultiplePackets(t *testing.T) {
	conn := NewMockConnection()
	conn.AddReadData([]byte("first"))
	conn.AddReadData([]byte("second"))

	buf := make([]byte, 100)
	n, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, "first", string(buf[:n]))

	n, err = conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, "second", string(buf[:n]))
}

func TestMockConnection_ReadEmptyQueue(t *testing.T) {
	conn := NewMockConnection()
	buf := make([]byte, 100)
	_, err := conn.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestMockConnection_ReadAfterClose(t *testing.T) {
	conn := NewMockConnection()
	conn.AddReadData([]byte("data"))
	conn.Close()

	buf := make([]byte, 100)
	_, err := conn.Read(buf)
	assert.Equal(t, io.EOF, err)
}

func TestMockConnection_WriteAfterClose(t *testing.T) {
	conn := NewMockConnection()
	conn.Close()

	_, err := conn.Write([]byte("data"))
	assert.Equal(t, io.EOF, err)
}

func TestMockConnection_SetReadError(t *testing.T) {
	conn := NewMockConnection()
	testErr := errors.New("read error")
	conn.SetReadError(testErr)

	buf := make([]byte, 100)
	_, err := conn.Read(buf)
	assert.Equal(t, testErr, err)
}

func TestMockConnection_SetWriteError(t *testing.T) {
	conn := NewMockConnection()
	testErr := errors.New("write error")
	conn.SetWriteError(testErr)

	_, err := conn.Write([]byte("data"))
	assert.Equal(t, testErr, err)
}

func TestMockConnection_Addresses(t *testing.T) {
	conn := NewMockConnection()

	local := conn.LocalAddr()
	assert.Equal(t, "tcp", local.Network())
	assert.Equal(t, "127.0.0.1:3306", local.String())

	remote := conn.RemoteAddr()
	assert.Equal(t, "tcp", remote.Network())
	assert.Equal(t, "127.0.0.1:12345", remote.String())
}

func TestMockConnection_Deadlines(t *testing.T) {
	conn := NewMockConnection()
	deadline := time.Now().Add(5 * time.Second)

	assert.NoError(t, conn.SetDeadline(deadline))
	assert.NoError(t, conn.SetReadDeadline(deadline))
	assert.NoError(t, conn.SetWriteDeadline(deadline))
}

func TestMockConnection_Close(t *testing.T) {
	conn := NewMockConnection()
	assert.False(t, conn.IsClosed())

	err := conn.Close()
	assert.NoError(t, err)
	assert.True(t, conn.IsClosed())
}

func TestMockConnection_WriteDataIsolation(t *testing.T) {
	conn := NewMockConnection()
	data := []byte("original")
	conn.Write(data)

	// Mutate original slice
	data[0] = 'X'

	// Written data should be independent copy
	written := conn.GetWrittenData()
	assert.Equal(t, byte('o'), written[0][0])
}

// === MockLogger Tests ===

func TestMockLogger_NewMockLogger(t *testing.T) {
	logger := NewMockLogger()
	assert.NotNil(t, logger)
	assert.Equal(t, 0, logger.GetLogCount())
}

func TestMockLogger_Printf(t *testing.T) {
	logger := NewMockLogger()
	logger.Printf("hello %s, count=%d", "world", 42)

	logs := logger.GetLogs()
	assert.Len(t, logs, 1)
	assert.Equal(t, "hello world, count=42", logs[0])
}

func TestMockLogger_MultipleLogs(t *testing.T) {
	logger := NewMockLogger()
	logger.Printf("first")
	logger.Printf("second")
	logger.Printf("third")

	assert.Equal(t, 3, logger.GetLogCount())
}

func TestMockLogger_ContainsLog(t *testing.T) {
	logger := NewMockLogger()
	logger.Printf("error: something went wrong")
	logger.Printf("info: all is well")

	assert.True(t, logger.ContainsLog("error"))
	assert.True(t, logger.ContainsLog("all is well"))
	assert.False(t, logger.ContainsLog("warning"))
}

func TestMockLogger_ClearLogs(t *testing.T) {
	logger := NewMockLogger()
	logger.Printf("log1")
	logger.Printf("log2")
	assert.Equal(t, 2, logger.GetLogCount())

	logger.ClearLogs()
	assert.Equal(t, 0, logger.GetLogCount())
	assert.Empty(t, logger.GetLogs())
}

func TestMockLogger_GetLastLog(t *testing.T) {
	logger := NewMockLogger()

	// Empty logger
	assert.Equal(t, "", logger.GetLastLog())

	logger.Printf("first")
	logger.Printf("last")
	assert.Equal(t, "last", logger.GetLastLog())
}

func TestMockLogger_DisableEnable(t *testing.T) {
	logger := NewMockLogger()
	logger.Printf("before disable")

	logger.Disable()
	logger.Printf("while disabled")
	assert.Equal(t, 1, logger.GetLogCount()) // Only "before disable"

	logger.Enable()
	logger.Printf("after enable")
	assert.Equal(t, 2, logger.GetLogCount())
}

// === MockSession Tests ===

func TestMockSession_NewMockSession(t *testing.T) {
	sess := NewMockSession()
	assert.NotNil(t, sess)
	assert.Equal(t, "mock-session-1", sess.GetID())
	assert.Equal(t, uint32(1), sess.GetThreadID())
	assert.Equal(t, "", sess.GetUser())
	assert.False(t, sess.IsClosed())
}

func TestMockSession_SetGetID(t *testing.T) {
	sess := NewMockSession()
	sess.SetID("new-id")
	assert.Equal(t, "new-id", sess.GetID())
}

func TestMockSession_SetGetThreadID(t *testing.T) {
	sess := NewMockSession()
	sess.SetThreadID(42)
	assert.Equal(t, uint32(42), sess.GetThreadID())
}

func TestMockSession_SetGetUser(t *testing.T) {
	sess := NewMockSession()
	sess.SetUser("admin")
	assert.Equal(t, "admin", sess.GetUser())
}

func TestMockSession_DataGetSetDelete(t *testing.T) {
	sess := NewMockSession()

	// Get non-existent key
	assert.Nil(t, sess.Get("missing"))

	// Set and get
	sess.Set("key1", "value1")
	sess.Set("key2", 42)
	assert.Equal(t, "value1", sess.Get("key1"))
	assert.Equal(t, 42, sess.Get("key2"))

	// Delete
	sess.Delete("key1")
	assert.Nil(t, sess.Get("key1"))
	assert.Equal(t, 42, sess.Get("key2"))
}

func TestMockSession_APISession(t *testing.T) {
	sess := NewMockSession()
	assert.Nil(t, sess.GetAPISession())

	sess.SetAPISession("mock-api-session")
	assert.Equal(t, "mock-api-session", sess.GetAPISession())
}

func TestMockSession_Close(t *testing.T) {
	sess := NewMockSession()
	assert.False(t, sess.IsClosed())

	err := sess.Close()
	assert.NoError(t, err)
	assert.True(t, sess.IsClosed())
}

func TestMockSession_Clone(t *testing.T) {
	sess := NewMockSession()
	sess.SetUser("user1")
	sess.Set("key", "value")
	sess.SetAPISession("api")

	clone := sess.Clone()

	// Verify clone has same values
	assert.Equal(t, sess.GetID(), clone.GetID())
	assert.Equal(t, sess.GetThreadID(), clone.GetThreadID())
	assert.Equal(t, sess.GetUser(), clone.GetUser())
	assert.Equal(t, "value", clone.Get("key"))

	// Verify independence
	sess.SetUser("changed")
	assert.Equal(t, "user1", clone.GetUser())

	sess.Set("key", "changed")
	assert.Equal(t, "value", clone.Get("key"))
}

func TestMockSession_ClonePreservesClosed(t *testing.T) {
	sess := NewMockSession()
	sess.Close()

	clone := sess.Clone()
	assert.True(t, clone.IsClosed())
}

// === MockAddr Tests ===

func TestMockAddr(t *testing.T) {
	addr := &MockAddr{addr: "192.168.1.1:3306"}
	assert.Equal(t, "tcp", addr.Network())
	assert.Equal(t, "192.168.1.1:3306", addr.String())
}

func TestNewMockAPISession(t *testing.T) {
	apiSess := NewMockAPISession()
	assert.Nil(t, apiSess)
}
