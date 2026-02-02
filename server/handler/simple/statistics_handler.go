package simple

import (
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
	// 返回统计信息字符串
	stats := "Uptime: 3600  Threads: 1  Questions: 10  Slow queries: 0  Opens: 5  Flush tables: 1  Open tables: 4  Queries per second avg: 0.003"
	_, err := ctx.Connection.Write([]byte(stats))
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
