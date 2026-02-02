package response

import (
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// EOFBuilder EOF 包构建器
type EOFBuilder struct{}

// NewEOFBuilder 创建 EOF 构建器
func NewEOFBuilder() *EOFBuilder {
	return &EOFBuilder{}
}

// Build 构建 EOF 包
func (b *EOFBuilder) Build(sequenceID uint8, warnings uint16, statusFlags uint16) *protocol.EofPacket {
	packet := &protocol.EofPacket{}
	packet.SequenceID = sequenceID
	packet.Header = 0xFE // EOF 包头
	packet.Warnings = warnings
	packet.StatusFlags = statusFlags
	return packet
}
