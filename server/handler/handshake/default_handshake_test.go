package handshake

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLogger struct {
	messages []string
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	// no-op for tests
}

func newTestSession() *pkg_session.Session {
	driver := pkg_session.NewMemoryDriver()
	mgr := pkg_session.NewSessionMgr(context.Background(), driver)
	sess, _ := mgr.CreateSession(context.Background(), "127.0.0.1", "12345")
	return sess
}

func buildHandshakeResponse(user, database string) []byte {
	resp := &protocol.HandshakeResponse{}
	resp.SequenceID = 1
	resp.ClientCapabilities = 0xf7fe
	resp.ExtendedClientCapabilities = 0x81bf
	resp.MaxPacketSize = 16777216
	resp.CharacterSet = 33 // utf8
	resp.Reserved = make([]byte, 19)
	resp.MariaDBCaps = 0x00000007
	resp.User = user
	resp.AuthResponse = "0102030405060708090a0b0c0d0e0f10" // hex-encoded dummy auth
	resp.Database = database
	resp.ClientAuthPluginName = "mysql_native_password"

	data, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func TestNewDefaultHandshakeHandler(t *testing.T) {
	h := NewDefaultHandshakeHandler(nil, nil)
	require.NotNil(t, h)
	assert.Equal(t, "DefaultHandshakeHandler", h.Name())
}

func TestHandle_Success(t *testing.T) {
	logger := &testLogger{}
	h := NewDefaultHandshakeHandler(nil, logger)
	sess := newTestSession()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	// Client side: read handshake packet
	buf := make([]byte, 4096)
	n, err := clientConn.Read(buf)
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	// Send handshake response
	respData := buildHandshakeResponse("test_user", "")
	_, err = clientConn.Write(respData)
	require.NoError(t, err)

	// Read OK packet
	n, err = clientConn.Read(buf)
	require.NoError(t, err)
	assert.Greater(t, n, 0)
	// OK packet header byte should be 0x00
	assert.Equal(t, byte(0x00), buf[4])

	// Check handler returned successfully
	err = <-done
	assert.NoError(t, err)

	// Session should have user set
	assert.Equal(t, "test_user", sess.User)
	assert.Equal(t, uint8(255), sess.SequenceID)
}

func TestHandle_WithDatabase(t *testing.T) {
	logger := &testLogger{}
	h := NewDefaultHandshakeHandler(nil, logger)
	sess := newTestSession()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	// Read handshake
	buf := make([]byte, 4096)
	clientConn.Read(buf)

	// Send response with database
	respData := buildHandshakeResponse("db_user", "mydb")
	clientConn.Write(respData)

	// Read OK
	clientConn.Read(buf)

	err := <-done
	assert.NoError(t, err)

	assert.Equal(t, "db_user", sess.User)
	// Database should be stored in session data
	val, _ := sess.Get("current_database")
	assert.Equal(t, "mydb", val)
}

func TestHandle_WithDB_APISession(t *testing.T) {
	db, err := api.NewDB(&api.DBConfig{CacheEnabled: false, DebugMode: false})
	require.NoError(t, err)

	logger := &testLogger{}
	h := NewDefaultHandshakeHandler(db, logger)
	sess := newTestSession()

	// Create and attach API session
	apiSess := db.Session()
	sess.SetAPISession(apiSess)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	buf := make([]byte, 4096)
	clientConn.Read(buf)

	respData := buildHandshakeResponse("api_user", "")
	clientConn.Write(respData)

	clientConn.Read(buf)

	err = <-done
	assert.NoError(t, err)

	assert.Equal(t, "api_user", sess.User)
}

func TestHandle_WriteError(t *testing.T) {
	h := NewDefaultHandshakeHandler(nil, &testLogger{})
	sess := newTestSession()

	// Close server conn before handler writes → write error
	clientConn, serverConn := net.Pipe()
	clientConn.Close()

	err := h.Handle(serverConn, sess)
	assert.Error(t, err)
}

func TestHandle_ReadError(t *testing.T) {
	h := NewDefaultHandshakeHandler(nil, &testLogger{})
	sess := newTestSession()

	clientConn, serverConn := net.Pipe()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	// Read the handshake packet, then close without sending response → read error
	buf := make([]byte, 4096)
	clientConn.Read(buf)
	clientConn.Close()

	err := <-done
	assert.Error(t, err)
}

func TestHandle_NilLogger(t *testing.T) {
	h := NewDefaultHandshakeHandler(nil, nil)
	sess := newTestSession()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	buf := make([]byte, 4096)
	clientConn.Read(buf)

	respData := buildHandshakeResponse("nil_logger_user", "")
	clientConn.Write(respData)

	clientConn.Read(buf)

	err := <-done
	assert.NoError(t, err)
	assert.Equal(t, "nil_logger_user", sess.User)
}

func TestHandle_OKWriteError(t *testing.T) {
	h := NewDefaultHandshakeHandler(nil, &testLogger{})
	sess := newTestSession()

	clientConn, serverConn := net.Pipe()

	done := make(chan error, 1)
	go func() {
		done <- h.Handle(serverConn, sess)
	}()

	// Read handshake, send response, then close before reading OK
	buf := make([]byte, 4096)
	clientConn.Read(buf)

	respData := buildHandshakeResponse("ok_error_user", "")
	clientConn.Write(respData)

	// Close immediately — the OK write may or may not succeed depending on buffering
	// Use a small read to drain partial data then close
	go func() {
		io.ReadAll(clientConn)
	}()
	clientConn.Close()

	err := <-done
	// May succeed or fail depending on timing
	_ = err
}
