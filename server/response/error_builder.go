package response

import (
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// ErrorBuilder 错误包构建器
type ErrorBuilder struct{}

// NewErrorBuilder 创建错误构建器
func NewErrorBuilder() *ErrorBuilder {
	return &ErrorBuilder{}
}

// Build 构建错误包
func (b *ErrorBuilder) Build(sequenceID uint8, errorCode uint16, sqlState string, errorMessage string) *protocol.ErrorPacket {
	packet := &protocol.ErrorPacket{}
	packet.SequenceID = sequenceID
	packet.ErrorCode = errorCode
	packet.SqlState = sqlState
	if sqlState != "" {
		packet.SqlStateMarker = "#"
	}
	packet.ErrorMessage = errorMessage
	return packet
}
