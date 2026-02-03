package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// FieldListPacketParser FIELD_LIST 命令包解析器
type FieldListPacketParser struct{}

// NewFieldListPacketParser 创建 FIELD_LIST 命令包解析器
func NewFieldListPacketParser() handler.PacketParser {
	return &FieldListPacketParser{}
}

// Command 返回命令类型
func (p *FieldListPacketParser) Command() uint8 {
	return protocol.COM_FIELD_LIST
}

// Name 返回解析器名称
func (p *FieldListPacketParser) Name() string {
	return "COM_FIELD_LIST"
}

// Parse 解析命令包
func (p *FieldListPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComFieldListPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
