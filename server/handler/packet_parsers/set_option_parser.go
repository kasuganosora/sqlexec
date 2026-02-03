package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// SetOptionPacketParser SET_OPTION 命令包解析器
type SetOptionPacketParser struct{}

// NewSetOptionPacketParser 创建 SET_OPTION 命令包解析器
func NewSetOptionPacketParser() handler.PacketParser {
	return &SetOptionPacketParser{}
}

// Command 返回命令类型
func (p *SetOptionPacketParser) Command() uint8 {
	return protocol.COM_SET_OPTION
}

// Name 返回解析器名称
func (p *SetOptionPacketParser) Name() string {
	return "COM_SET_OPTION"
}

// Parse 解析命令包
func (p *SetOptionPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComSetOptionPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
