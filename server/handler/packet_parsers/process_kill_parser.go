package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// ProcessKillPacketParser PROCESS_KILL 命令包解析器
type ProcessKillPacketParser struct{}

// NewProcessKillPacketParser 创建 PROCESS_KILL 命令包解析器
func NewProcessKillPacketParser() handler.PacketParser {
	return &ProcessKillPacketParser{}
}

// Command 返回命令类型
func (p *ProcessKillPacketParser) Command() uint8 {
	return protocol.COM_PROCESS_KILL
}

// Name 返回解析器名称
func (p *ProcessKillPacketParser) Name() string {
	return "COM_PROCESS_KILL"
}

// Parse 解析命令包
func (p *ProcessKillPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComProcessKillPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
