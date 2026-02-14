package simple

import (
	"bytes"

	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// StatisticsHandler STATISTICS 命令处理器
type StatisticsHandler struct{}

// NewStatisticsHandler 创建 STATISTICS 处理器
func NewStatisticsHandler() *StatisticsHandler {
	return &StatisticsHandler{}
}

// Handle 处理 COM_STATISTICS 命令
func (h *StatisticsHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	ctx.Log("处理 COM_STATISTICS")
	ctx.ResetSequenceID()

	// COM_STATISTICS response is a string wrapped in a MySQL packet header
	stats := "Uptime: 3600  Threads: 1  Questions: 10  Slow queries: 0  Opens: 5  Flush tables: 1  Open tables: 4  Queries per second avg: 0.003"
	payload := []byte(stats)

	// Build proper MySQL packet: 3-byte length (LE) + 1-byte sequence ID + payload
	packetBuf := new(bytes.Buffer)
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	packetBuf.WriteByte(ctx.GetNextSequenceID())
	packetBuf.Write(payload)

	_, err := ctx.Connection.Write(packetBuf.Bytes())
	return err
}

// Command 返回命令类型
func (h *StatisticsHandler) Command() uint8 {
	return protocol.COM_STATISTICS
}

// Name 返回处理器名称
func (h *StatisticsHandler) Name() string {
	return "COM_STATISTICS"
}
