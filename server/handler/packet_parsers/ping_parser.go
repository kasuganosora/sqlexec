package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// PingPacketParser PING 命令包解析器
type PingPacketParser struct{}

// NewPingPacketParser 创建 PING 命令包解析器
func NewPingPacketParser() handler.PacketParser {
	return &PingPacketParser{}
}

// Command 返回命令类型
func (p *PingPacketParser) Command() uint8 {
	return protocol.COM_PING
}

// Name 返回解析器名称
func (p *PingPacketParser) Name() string {
	return "COM_PING"
}

// Parse 解析命令包
func (p *PingPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComPingPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
