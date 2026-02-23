package pkg

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// countingListener wraps a net.Listener and counts Accept calls after close.
type countingListener struct {
	net.Listener
	acceptAfterClose atomic.Int64
	closed           atomic.Bool
}

func (cl *countingListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	if err != nil && cl.closed.Load() {
		cl.acceptAfterClose.Add(1)
	}
	return conn, err
}

func (cl *countingListener) Close() error {
	cl.closed.Store(true)
	return cl.Listener.Close()
}

// TestBug_Start_AcceptLoopExitsOnListenerClose verifies that the accept loop
// in Server.Start exits when the listener is closed, instead of spinning
// in an infinite error loop.
func TestBug_Start_AcceptLoopExitsOnListenerClose(t *testing.T) {
	server := NewServer(nil)
	defer server.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	cl := &countingListener{Listener: ln}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = server.Start(ctx, cl)
	assert.NoError(t, err)

	// Give the goroutine time to reach Accept()
	time.Sleep(50 * time.Millisecond)

	// Close the listener - this should cause Accept() to return an error
	cl.Close()

	// Wait for the goroutine to notice and exit
	time.Sleep(200 * time.Millisecond)

	// After the listener is closed, the accept loop should have exited.
	// With the bug, it keeps calling Accept() in a tight loop.
	// Allow at most 2 calls (one to detect the error, one retry).
	acceptCalls := cl.acceptAfterClose.Load()
	assert.LessOrEqual(t, acceptCalls, int64(2),
		"accept loop should exit after listener close, but Accept was called %d times", acceptCalls)
}

// ==========================================================================
// Performance optimization tests
// ==========================================================================

func TestCountParams_ByteIteration(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect uint16
	}{
		{"no params", "SELECT * FROM users", 0},
		{"one param", "SELECT * FROM users WHERE id = ?", 1},
		{"three params", "SELECT * FROM users WHERE a = ? AND b = ? OR c = ?", 3},
		{"empty string", "", 0},
		{"unicode with params", "SELECT * FROM users WHERE name = ? AND comment = '你好?'", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, countParams(tt.query))
		})
	}
}

func TestFormatValue_Strconv(t *testing.T) {
	server := NewServer(nil)

	tests := []struct {
		name   string
		input  interface{}
		expect string
	}{
		{"nil", nil, "NULL"},
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"int64", int64(123456789), "123456789"},
		{"int32", int32(-100), "-100"},
		{"float64", float64(3.14), "3.14"},
		{"float64 integer", float64(100), "100"},
		{"float32", float32(2.5), "2.5"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
		{"bytes", []byte("data"), "data"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, server.formatValue(tt.input))
		})
	}
}

func BenchmarkCountParams_Byte(b *testing.B) {
	query := "SELECT * FROM users WHERE a = ? AND b = ? AND c = ? AND d = ? AND e = ?" +
		strings.Repeat(" AND x = ?", 50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		countParams(query)
	}
}

func BenchmarkFormatValue_Strconv(b *testing.B) {
	server := NewServer(nil)
	values := []interface{}{int64(12345), "hello world", float64(3.14159), true, nil}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.formatValue(values[i%len(values)])
	}
}
