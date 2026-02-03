package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// QuitPacketParser QUIT 命令包解析器
type QuitPacketParser struct{}

// NewQuitPacketParser 创建 QUIT 命令包解析器
func NewQuitPacketParser() handler.PacketParser {
	return &QuitPacketParser{}
}

// Command 返回命令类型
func (p *QuitPacketParser) Command() uint8 {
	return protocol.COM_QUIT
}

// Name 返回解析器名称
func (p *QuitPacketParser) Name() string {
	return "COM_QUIT"
}

// Parse 解析命令包
func (p *QuitPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComQuitPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
