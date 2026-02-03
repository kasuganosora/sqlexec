package packet_parsers

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// InitDBPacketParser INIT_DB 命令包解析器
type InitDBPacketParser struct{}

// NewInitDBPacketParser 创建 INIT_DB 命令包解析器
func NewInitDBPacketParser() handler.PacketParser {
	return &InitDBPacketParser{}
}

// Command 返回命令类型
func (p *InitDBPacketParser) Command() uint8 {
	return protocol.COM_INIT_DB
}

// Name 返回解析器名称
func (p *InitDBPacketParser) Name() string {
	return "COM_INIT_DB"
}

// Parse 解析命令包
func (p *InitDBPacketParser) Parse(packet *protocol.Packet) (interface{}, error) {
	cmd := &protocol.ComInitDBPacket{}
	cmd.Packet = *packet
	return cmd, nil
}
