package response

import (
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// OKBuilder OK 包构建器
type OKBuilder struct{}

// NewOKBuilder 创建 OK 构建器
func NewOKBuilder() *OKBuilder {
	return &OKBuilder{}
}

// Build 构建 OK 包
func (b *OKBuilder) Build(sequenceID uint8, affectedRows, lastInsertID uint64, warnings uint16) *protocol.OkPacket {
	packet := &protocol.OkPacket{}
	packet.SequenceID = sequenceID
	packet.OkInPacket.Header = 0x00
	packet.OkInPacket.AffectedRows = affectedRows
	packet.OkInPacket.LastInsertId = lastInsertID
	packet.OkInPacket.Warnings = warnings
	packet.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	return packet
}
