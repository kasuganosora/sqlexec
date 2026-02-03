package handler

import (
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/server/protocol"
)

// PacketParser 命令包解析器接口
type PacketParser interface {
	// Command 返回命令类型
	Command() uint8

	// Name 返回解析器名称
	Name() string

	// Parse 解析命令包
	Parse(packet *protocol.Packet) (interface{}, error)
}

// PacketParserRegistry 命令包解析器注册中心（并发安全）
type PacketParserRegistry struct {
	parsers map[uint8]PacketParser
	mu      sync.RWMutex
	logger  Logger
}

// NewPacketParserRegistry 创建包解析器注册中心
func NewPacketParserRegistry(logger Logger) *PacketParserRegistry {
	return &PacketParserRegistry{
		parsers: make(map[uint8]PacketParser),
		logger:  logger,
	}
}

// Register 注册解析器（并发安全）
func (r *PacketParserRegistry) Register(parser PacketParser) error {
	if parser == nil {
		return fmt.Errorf("cannot register nil parser")
	}

	cmd := parser.Command()
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.parsers[cmd]; exists {
		return fmt.Errorf("parser for command 0x%02x already registered", cmd)
	}
	r.parsers[cmd] = parser
	if r.logger != nil {
		r.logger.Printf("Registered parser: %s (0x%02x)", parser.Name(), cmd)
	}
	return nil
}

// Get 获取解析器（并发安全）
func (r *PacketParserRegistry) Get(commandType uint8) (PacketParser, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	parser, exists := r.parsers[commandType]
	return parser, exists
}

// Parse 解析命令包
func (r *PacketParserRegistry) Parse(commandType uint8, packet *protocol.Packet) (interface{}, error) {
	parser, exists := r.Get(commandType)
	if !exists {
		if r.logger != nil {
			r.logger.Printf("[ERROR] No parser registered for command 0x%02x", commandType)
		}
		return nil, fmt.Errorf("no parser registered for command 0x%02x", commandType)
	}
	return parser.Parse(packet)
}

// List 列出所有注册的解析器
func (r *PacketParserRegistry) List() []PacketParser {
	r.mu.RLock()
	defer r.mu.RUnlock()

	parsers := make([]PacketParser, 0, len(r.parsers))
	for _, p := range r.parsers {
		parsers = append(parsers, p)
	}
	return parsers
}

// Count 获取已注册的解析器数量
func (r *PacketParserRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.parsers)
}
