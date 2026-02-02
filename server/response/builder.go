package response

import (
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// ResponseBuilder 响应构建器接口
type ResponseBuilder interface {
	// BuildOK 构建OK包
	BuildOK(sequenceID uint8, affectedRows, lastInsertID uint64, warnings uint16) *protocol.OkPacket

	// BuildError 构建错误包
	BuildError(sequenceID uint8, errorCode uint16, sqlState string, errorMessage string) *protocol.ErrorPacket

	// BuildResultSet 构建结果集
	BuildResultSet(sequenceID uint8, columns []protocol.FieldMeta, rows [][]string) []interface{}

	// BuildColumnCount 构建列数包
	BuildColumnCount(sequenceID uint8, count uint64) []byte

	// BuildEOF 构建EOF包
	BuildEOF(sequenceID uint8, warnings uint16, statusFlags uint16) *protocol.EofPacket
}
