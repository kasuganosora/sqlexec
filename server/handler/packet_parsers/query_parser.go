package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// QueryPacketParser QUERY 命令包解析器
type QueryPacketParser struct{}

// NewQueryPacketParser 创建 QUERY 命令包解析器
func NewQueryPacketParser() handler.PacketParser {
	return &QueryPacketParser{}
}

// Command 返回命令类型
func (p *QueryPacketParser) Command() uint8 {
	return protocol.COM_QUERY
}

// Name 返回解析器名称
func (p *QueryPacketParser) Name() string {
	return "COM_QUERY"
}

// Parse 解析命令包
func (p *QueryPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComQueryPacket{}
	cmd.Packet = *packet
	// ComQueryPacket.Unmarshal 会自动从 Payload 中提取 Query 字段
	// 因为 cmd.Packet 已经被赋值，所以不需要再次 Unmarshal
	// Query 字段会在访问时自动提取
	return cmd, nil
}
