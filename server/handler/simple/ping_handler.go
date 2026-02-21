package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
)

// PingHandler PING 命令处理器
type PingHandler struct {
	okBuilder *response.OKBuilder
}

// NewPingHandler 创建 PING 处理器
func NewPingHandler(okBuilder *response.OKBuilder) *PingHandler {
	if okBuilder == nil {
		okBuilder = response.NewOKBuilder()
	}
	return &PingHandler{
		okBuilder: okBuilder,
	}
}

// Handle 处理 COM_PING 命令
func (h *PingHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	ctx.Log("处理 COM_PING")

	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	// 简化实现，直接返回 OK
	ctx.DebugLog("About to call SendOK()")
	return ctx.SendOK()
}

// Command 返回命令类型
func (h *PingHandler) Command() uint8 {
	return protocol.COM_PING
}

// Name 返回处理器名称
func (h *PingHandler) Name() string {
	return "COM_PING"
}
