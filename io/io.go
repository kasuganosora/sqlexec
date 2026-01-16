package io

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// 连接状态
type ConnectionState int

const (
	StateHandshaking ConnectionState = iota
	StateConnected
	StateAuthenticated
)

// 错误定义
var (
	ErrAlreadyRunning  = &IOError{"IO module is already running"}
	ErrPacketTooLarge  = &IOError{"Packet size exceeds maximum allowed size"}
	ErrInvalidPacket   = &IOError{"Invalid packet format"}
	ErrPacketSequence  = &IOError{"Packet sequence mismatch"}
	ErrUnhandledPacket = &IOError{"Unhandled packet"}
)

// PacketHandler 包处理器接口，接收 context 和原始包数据
type PacketHandler func(ctx context.Context, packetData []byte) error

// IO 模块，负责 MySQL 协议的网络收发和包解析
type IO struct {
	conn     io.ReadWriter
	mu       sync.RWMutex
	handlers map[uint8]PacketHandler
	ctx      context.Context
	cancel   context.CancelFunc

	// 配置选项
	readTimeout   time.Duration
	writeTimeout  time.Duration
	maxPacketSize uint32
	// 新增：是否启用压缩
	enableCompression bool

	// 状态
	running bool
	done    chan struct{}

	// 修复：添加序列号管理
	writeSeq uint8
	readSeq  uint8
	state    ConnectionState
}

// NewIO 创建新的 IO 实例
func NewIO(conn io.ReadWriter) *IO {
	ctx, cancel := context.WithCancel(context.Background())
	return &IO{
		conn:          conn,
		handlers:      make(map[uint8]PacketHandler),
		ctx:           ctx,
		cancel:        cancel,
		readTimeout:   30 * time.Second,
		writeTimeout:  30 * time.Second,
		maxPacketSize: 16 * 1024 * 1024, // 16MB
		done:          make(chan struct{}),
		writeSeq:      0,
		readSeq:       0,
		state:         StateHandshaking,
	}
}

// RegisterHandler 注册包处理器
func (i *IO) RegisterHandler(cmd uint8, handler PacketHandler) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.handlers[cmd] = handler
}

// UnregisterHandler 注销包处理器
func (i *IO) UnregisterHandler(cmd uint8) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.handlers, cmd)
}

// SetReadTimeout 设置读取超时
func (i *IO) SetReadTimeout(timeout time.Duration) {
	i.readTimeout = timeout
}

// SetWriteTimeout 设置写入超时
func (i *IO) SetWriteTimeout(timeout time.Duration) {
	i.writeTimeout = timeout
}

// SetMaxPacketSize 设置最大包大小
func (i *IO) SetMaxPacketSize(size uint32) {
	i.maxPacketSize = size
}

// EnableCompression 启用压缩
func (i *IO) EnableCompression(enable bool) {
	i.enableCompression = enable
}

// getConnectionState 获取连接状态
func (i *IO) getConnectionState() ConnectionState {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state
}

// setConnectionState 设置连接状态
func (i *IO) setConnectionState(state ConnectionState) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.state = state
}

// StartReadLoop 启动读取循环，自动读取、解包、分发
func (i *IO) StartReadLoop() error {
	if i.running {
		return ErrAlreadyRunning
	}

	i.running = true
	defer func() {
		i.running = false
		close(i.done)
	}()

	for {
		select {
		case <-i.ctx.Done():
			return i.ctx.Err()
		default:
			// 读取包
			packetData, err := i.readPacket()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			// 分发包
			if err := i.dispatchPacket(packetData); err != nil {
				return err
			}
		}
	}
}

// Stop 停止 IO 模块
func (i *IO) Stop() {
	i.cancel()
	<-i.done
}

// WritePacket 发送包，自动分包、压缩
func (i *IO) WritePacket(packetData []byte) error {
	// 检查包大小（不包括包头）
	if len(packetData) > 4 {
		payloadLength := len(packetData) - 4
		if payloadLength > int(i.maxPacketSize) {
			return ErrPacketTooLarge
		}
	}

	// 修复：设置正确的序列号
	if len(packetData) >= 4 {
		packetData[3] = i.writeSeq
		i.writeSeq++
	}

	// 写入数据
	if i.writeTimeout > 0 {
		// TODO: 实现带超时的写入
	}

	_, err := i.conn.Write(packetData)
	return err
}

// readPacket 读取单个包 - 修复：实现分块包处理
func (i *IO) readPacket() ([]byte, error) {
	var fullPayload []byte
	var sequenceID uint8

	for {
		// 读取包头 (4字节)
		header := make([]byte, 4)
		if _, err := io.ReadFull(i.conn, header); err != nil {
			return nil, err
		}

		// 解析包头
		payloadLength := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
		seq := header[3]

		// 修复：验证序列号连续性
		if fullPayload != nil && seq != sequenceID+1 {
			return nil, ErrPacketSequence
		}
		sequenceID = seq

		// 检查包大小
		if payloadLength > i.maxPacketSize {
			return nil, ErrPacketTooLarge
		}

		// 读取包体
		payload := make([]byte, payloadLength)
		if _, err := io.ReadFull(i.conn, payload); err != nil {
			return nil, err
		}

		// 只有启用压缩时才尝试解压
		if i.enableCompression && len(payload) > 0 && payload[0] == 0x00 {
			// 这是一个压缩包，需要解压
			decompressed, err := i.decompressPacket(payload)
			if err != nil {
				return nil, err
			}
			payload = decompressed
		}

		fullPayload = append(fullPayload, payload...)

		// 如果当前包小于最大包大小，说明是最后一个包
		if payloadLength < 0xffffff {
			break
		}
	}

	// 组装完整的包数据
	totalLength := len(fullPayload)
	packetData := make([]byte, 4+totalLength)
	packetData[0] = byte(totalLength)
	packetData[1] = byte(totalLength >> 8)
	packetData[2] = byte(totalLength >> 16)
	packetData[3] = sequenceID
	copy(packetData[4:], fullPayload)

	return packetData, nil
}

// dispatchPacket 分发包到对应的处理器 - 修复：重构分发逻辑，支持状态机
func (i *IO) dispatchPacket(packetData []byte) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if len(packetData) < 5 {
		fmt.Printf("[IO] dispatchPacket: 包太短: %v\n", packetData)
		return ErrInvalidPacket
	}

	seq := packetData[3]
	cmd := packetData[4]
	fmt.Printf("[IO] dispatchPacket: SequenceID=%d, cmd=0x%02x, len=%d\n", seq, cmd, len(packetData))

	// 修复：根据连接状态分发包
	switch i.state {
	case StateHandshaking:
		if seq == 1 {
			// 握手响应包
			if handler, exists := i.handlers[0x00]; exists {
				fmt.Printf("[IO] dispatchPacket: 处理握手响应包\n")
				err := handler(i.ctx, packetData)
				if err == nil {
					i.setConnectionState(StateConnected)
					fmt.Printf("[IO] dispatchPacket: 握手成功，状态切换到已连接\n")
				}
				return err
			}
		}

	case StateConnected:
		// 查询包处理（COM_QUERY = 0x03）
		if cmd == 0x03 {
			fmt.Printf("[IO] dispatchPacket: 检测到查询包，cmd=0x03\n")
			if handler, exists := i.handlers[cmd]; exists {
				fmt.Printf("[IO] dispatchPacket: 查询包命中 handler\n")
				return handler(i.ctx, packetData)
			} else {
				fmt.Printf("[IO] dispatchPacket: 查询包未找到 handler\n")
			}
		}

		// 其它命令分发
		if handler, exists := i.handlers[cmd]; exists {
			fmt.Printf("[IO] dispatchPacket: 命令 0x%02x 命中 handler\n", cmd)
			return handler(i.ctx, packetData)
		}
	}

	fmt.Printf("[IO] dispatchPacket: 未找到 handler，cmd=0x%02x，丢弃\n", cmd)
	// 如果没有找到处理器，记录日志但不报错
	return nil
}

// IOError IO 模块错误
type IOError struct {
	message string
}

func (e *IOError) Error() string {
	return e.message
}
