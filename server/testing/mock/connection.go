package mock

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

// MockConnection implements net.Conn interface for testing
type MockConnection struct {
	mu          sync.Mutex
	writtenData [][]byte
	readQueue   [][]byte
	closed      bool
	readError   error
	writeError  error
	localAddr   net.Addr
	remoteAddr  net.Addr
	deadline    time.Time
}

// NewMockConnection creates a new mock connection
func NewMockConnection() *MockConnection {
	return &MockConnection{
		writtenData: make([][]byte, 0),
		readQueue:   make([][]byte, 0),
		closed:      false,
		localAddr:   &MockAddr{addr: "127.0.0.1:3306"},
		remoteAddr:  &MockAddr{addr: "127.0.0.1:12345"},
	}
}

// Write implements io.Writer interface, records written data
func (m *MockConnection) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeError != nil {
		return 0, m.writeError
	}

	if m.closed {
		return 0, io.EOF
	}

	// Record written data
	data := make([]byte, len(b))
	copy(data, b)
	m.writtenData = append(m.writtenData, data)

	return len(b), nil
}

// Read implements io.Reader interface, reads data from queue
func (m *MockConnection) Read(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.readError != nil {
		return 0, m.readError
	}

	if m.closed {
		return 0, io.EOF
	}

	if len(m.readQueue) == 0 {
		// No data to read, return EOF
		return 0, io.EOF
	}

	// Take a packet from the queue head
	packet := m.readQueue[0]
	m.readQueue = m.readQueue[1:]

	n = copy(b, packet)
	return n, nil
}

// Close closes the connection
func (m *MockConnection) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// LocalAddress returns local address
func (m *MockConnection) LocalAddr() net.Addr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.localAddr
}

// RemoteAddress returns remote address
func (m *MockConnection) RemoteAddr() net.Addr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remoteAddr
}

// SetDeadline sets read/write timeout
func (m *MockConnection) SetDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// SetReadDeadline sets read timeout
func (m *MockConnection) SetReadDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// SetWriteDeadline sets write timeout
func (m *MockConnection) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// AddReadData adds mock read data (for testing)
func (m *MockConnection) AddReadData(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	packet := make([]byte, len(data))
	copy(packet, data)
	m.readQueue = append(m.readQueue, packet)
}

// GetWrittenData returns all written data
func (m *MockConnection) GetWrittenData() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]byte, len(m.writtenData))
	copy(result, m.writtenData)
	return result
}

// GetWrittenDataBytes returns all written byte data
func (m *MockConnection) GetWrittenDataBytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var buf bytes.Buffer
	for _, data := range m.writtenData {
		buf.Write(data)
	}
	return buf.Bytes()
}

// ClearWrittenData clears written data records
func (m *MockConnection) ClearWrittenData() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writtenData = make([][]byte, 0)
}

// IsClosed checks if connection is closed
func (m *MockConnection) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// SetReadError sets read error (for testing error scenarios)
func (m *MockConnection) SetReadError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readError = err
}

// SetWriteError sets write error (for testing error scenarios)
func (m *MockConnection) SetWriteError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeError = err
}

// MockAddr implements net.Addr interface
type MockAddr struct {
	addr string
}

// Network returns network type
func (m *MockAddr) Network() string {
	return "tcp"
}

// String returns address string
func (m *MockAddr) String() string {
	return m.addr
}
