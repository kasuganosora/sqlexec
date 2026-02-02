package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
)

// RefreshHandler REFRESH 命令处理器
type RefreshHandler struct {
	okBuilder *response.OKBuilder
}

// NewRefreshHandler 创建 REFRESH 处理器
func NewRefreshHandler(okBuilder *response.OKBuilder) *RefreshHandler {
	if okBuilder == nil {
		okBuilder = response.NewOKBuilder()
	}
	return &RefreshHandler{
		okBuilder: okBuilder,
	}
}

// Handle 处理 COM_REFRESH 命令
func (h *RefreshHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	ctx.Log("处理 COM_REFRESH")

	// 简化实现，直接返回 OK
	return ctx.SendOK()
}

// Command 返回命令类型
func (h *RefreshHandler) Command() uint8 {
	return protocol.COM_REFRESH
}

// Name 返回处理器名称
func (h *RefreshHandler) Name() string {
	return "COM_REFRESH"
}
