package testing

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"
)

// MockConnection 实现net.Conn接口用于测试
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

// NewMockConnection 创建一个新的Mock连接
func NewMockConnection() *MockConnection {
	return &MockConnection{
		writtenData: make([][]byte, 0),
		readQueue:   make([][]byte, 0),
		closed:      false,
		localAddr:   &MockAddr{addr: "127.0.0.1:3306"},
		remoteAddr:  &MockAddr{addr: "127.0.0.1:12345"},
	}
}

// Write 实现io.Writer接口，记录写入的数据
func (m *MockConnection) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeError != nil {
		return 0, m.writeError
	}

	if m.closed {
		return 0, io.EOF
	}

	// 记录写入的数据
	data := make([]byte, len(b))
	copy(data, b)
	m.writtenData = append(m.writtenData, data)

	return len(b), nil
}

// Read 实现io.Reader接口，从队列中读取数据
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
		// 没有数据可读，等待或返回EOF
		return 0, io.EOF
	}

	// 从队列头部取出一个包
	packet := m.readQueue[0]
	m.readQueue = m.readQueue[1:]

	n = copy(b, packet)
	return n, nil
}

// Close 关闭连接
func (m *MockConnection) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// LocalAddress 返回本地地址
func (m *MockConnection) LocalAddr() net.Addr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.localAddr
}

// RemoteAddress 返回远程地址
func (m *MockConnection) RemoteAddr() net.Addr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remoteAddr
}

// SetDeadline 设置读写超时
func (m *MockConnection) SetDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// SetReadDeadline 设置读超时
func (m *MockConnection) SetReadDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// SetWriteDeadline 设置写超时
func (m *MockConnection) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadline = t
	return nil
}

// AddReadData 添加模拟的读取数据（用于测试）
func (m *MockConnection) AddReadData(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	packet := make([]byte, len(data))
	copy(packet, data)
	m.readQueue = append(m.readQueue, packet)
}

// GetWrittenData 获取所有写入的数据
func (m *MockConnection) GetWrittenData() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]byte, len(m.writtenData))
	copy(result, m.writtenData)
	return result
}

// GetWrittenDataBytes 获取所有写入的字节数据
func (m *MockConnection) GetWrittenDataBytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var buf bytes.Buffer
	for _, data := range m.writtenData {
		buf.Write(data)
	}
	return buf.Bytes()
}

// ClearWrittenData 清除写入的数据记录
func (m *MockConnection) ClearWrittenData() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writtenData = make([][]byte, 0)
}

// IsClosed 检查连接是否已关闭
func (m *MockConnection) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// SetReadError 设置读取错误（用于测试错误场景）
func (m *MockConnection) SetReadError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readError = err
}

// SetWriteError 设置写入错误（用于测试错误场景）
func (m *MockConnection) SetWriteError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeError = err
}

// MockAddr 实现net.Addr接口
type MockAddr struct {
	addr string
}

// Network 返回网络类型
func (m *MockAddr) Network() string {
	return "tcp"
}

// String 返回地址字符串
func (m *MockAddr) String() string {
	return m.addr
}
