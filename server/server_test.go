package server

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHandshakeHandler skips the handshake and just sets the user
type mockHandshakeHandler struct{}

func (m *mockHandshakeHandler) Handle(conn net.Conn, sess *pkg_session.Session) error {
	sess.SetUser("test_user")
	sess.SequenceID = 255
	return nil
}

func (m *mockHandshakeHandler) Name() string {
	return "MockHandshakeHandler"
}

var _ handler.HandshakeHandler = (*mockHandshakeHandler)(nil)

func TestNewServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	ctx := context.Background()
	s := NewServer(ctx, listener, nil)
	require.NotNil(t, s)

	assert.NotNil(t, s.GetDB())
	assert.NotEmpty(t, s.GetConfigDir())
	assert.NotNil(t, s.handlerRegistry)
	assert.NotNil(t, s.parserRegistry)
	assert.NotNil(t, s.handshakeHandler)
	assert.NotNil(t, s.logger)
	assert.NotNil(t, s.sessionMgr)
}

func TestNewServer_WithConfig(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	cfg := config.DefaultConfig()
	s := NewServer(context.Background(), listener, cfg)
	require.NotNil(t, s)
	assert.Equal(t, cfg, s.config)
}

func TestServer_SetDB(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)

	newDB, err := api.NewDB(&api.DBConfig{
		CacheEnabled: false,
		DebugMode:    false,
	})
	require.NoError(t, err)

	s.SetDB(newDB)
	assert.Equal(t, newDB, s.GetDB())
}

func TestServer_GetConfigDir(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	assert.Equal(t, ".", s.GetConfigDir())
}

func TestServer_Start_ContextCancel(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(ctx, listener, nil)
	require.NotNil(t, s)

	done := make(chan error, 1)
	go func() {
		done <- s.Start()
	}()

	cancel()
	err = <-done
	assert.ErrorIs(t, err, context.Canceled)
}

func TestServer_Start_ListenerClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)

	done := make(chan error, 1)
	go func() {
		done <- s.Start()
	}()

	// Close listener to cause Accept() error
	listener.Close()
	err = <-done
	assert.Error(t, err)
}

func TestNewServer_WithDatasources(t *testing.T) {
	// Create a temp dir with datasources.json and chdir to it
	tmpDir := t.TempDir()

	origDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	// Create datasources.json with a memory datasource
	dsJSON := `[{"name":"test_ds","type":"memory","writable":true}]`
	require.NoError(t, os.WriteFile("datasources.json", []byte(dsJSON), 0644))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	assert.NotNil(t, s.GetDB())
}

func TestServerLogger(t *testing.T) {
	l := &serverLogger{logger: log.New(os.Stdout, "[TEST] ", 0)}
	// Should not panic
	l.Printf("test message %d", 42)
}

func TestServer_HandleConnection_NilSessionMgr(t *testing.T) {
	s := &Server{
		logger: &serverLogger{logger: log.New(os.Stdout, "[TEST] ", 0)},
	}
	// Use a pipe for connection
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	err := s.handleConnection(serverConn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sessionMgr is nil")
}

func TestServer_HandleConnection_ClosedConn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)

	// Use a pipe; close client side immediately to trigger EOF on read
	clientConn, serverConn := net.Pipe()
	clientConn.Close()

	err = s.handleConnection(serverConn)
	// Should error during handshake or packet read
	assert.Error(t, err)
}

func TestServer_HandleConnection_QuitCommand(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	// Replace handshake handler with mock to skip the handshake protocol
	s.handshakeHandler = &mockHandshakeHandler{}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- s.handleConnection(serverConn)
	}()

	// Send QUIT packet: 4-byte header + 1-byte COM_QUIT (0x01)
	// Length=1, SequenceID=0, Payload=COM_QUIT
	quitPacket := []byte{0x01, 0x00, 0x00, 0x00, 0x01}
	_, err = clientConn.Write(quitPacket)
	require.NoError(t, err)

	select {
	case err := <-done:
		assert.NoError(t, err) // QUIT should return nil
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection did not return in time")
	}
}

func TestServer_HandleConnection_PingThenQuit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	s.handshakeHandler = &mockHandshakeHandler{}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- s.handleConnection(serverConn)
	}()

	// Send PING packet: Length=1, SeqID=0, Payload=COM_PING (0x0e)
	pingPacket := []byte{0x01, 0x00, 0x00, 0x00, 0x0e}
	_, err = clientConn.Write(pingPacket)
	require.NoError(t, err)

	// Read OK response (variable length, just read what's available)
	buf := make([]byte, 1024)
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := clientConn.Read(buf)
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	// Verify OK packet header byte (5th byte should be 0x00)
	assert.Equal(t, byte(0x00), buf[4])

	// Now send QUIT
	quitPacket := []byte{0x01, 0x00, 0x00, 0x00, 0x01}
	_, err = clientConn.Write(quitPacket)
	require.NoError(t, err)

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection did not return in time")
	}
}

func TestServer_HandleConnection_HandlerError_Continue(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	s.handshakeHandler = &mockHandshakeHandler{}
	// Set db to nil so no API session is created, causing query handler to error
	s.db = nil

	clientConn, serverConn := net.Pipe()

	done := make(chan error, 1)
	go func() {
		done <- s.handleConnection(serverConn)
	}()

	// Drain all server responses in background to avoid pipe deadlock
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	// Send COM_QUERY "SELECT 1" — handler will return error (no API session), then continue
	sql := "SELECT 1"
	payload := append([]byte{0x03}, []byte(sql)...) // COM_QUERY = 0x03
	pktLen := len(payload)
	queryPacket := append([]byte{byte(pktLen), byte(pktLen >> 8), byte(pktLen >> 16), 0x00}, payload...)
	_, err = clientConn.Write(queryPacket)
	require.NoError(t, err)

	// Wait for error processing, then send QUIT
	time.Sleep(200 * time.Millisecond)
	quitPacket := []byte{0x01, 0x00, 0x00, 0x00, 0x01}
	_, err = clientConn.Write(quitPacket)
	require.NoError(t, err)

	select {
	case err := <-done:
		assert.NoError(t, err) // QUIT should return nil
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection did not return in time")
	}
	clientConn.Close()
}

func TestServer_HandleConnection_UnknownCommand(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s := NewServer(context.Background(), listener, nil)
	require.NotNil(t, s)
	s.handshakeHandler = &mockHandshakeHandler{}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- s.handleConnection(serverConn)
	}()

	// Send unknown command (0xFF is not a registered command)
	// Parse error causes handleConnection to return immediately
	unknownPacket := []byte{0x01, 0x00, 0x00, 0x00, 0xFF}
	_, err = clientConn.Write(unknownPacket)
	require.NoError(t, err)

	select {
	case err := <-done:
		assert.Error(t, err) // Parse error for unknown command
	case <-time.After(5 * time.Second):
		t.Fatal("handleConnection did not return in time")
	}
}
