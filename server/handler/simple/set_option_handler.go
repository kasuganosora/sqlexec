package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
)

// SetOptionHandler SET_OPTION 命令处理器
type SetOptionHandler struct {
	okBuilder *response.OKBuilder
}

// NewSetOptionHandler 创建 SET_OPTION 处理器
func NewSetOptionHandler(okBuilder *response.OKBuilder) *SetOptionHandler {
	if okBuilder == nil {
		okBuilder = response.NewOKBuilder()
	}
	return &SetOptionHandler{
		okBuilder: okBuilder,
	}
}

// Handle 处理 COM_SET_OPTION 命令
func (h *SetOptionHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	cmd, ok := packet.(*protocol.ComSetOptionPacket)
	if !ok {
		return ctx.SendError(handler.NewHandlerError("Invalid packet type for COM_SET_OPTION"))
	}

	ctx.Log("处理 COM_SET_OPTION: option=%d", cmd.OptionOperation)

	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	// 简化实现，直接返回 OK
	return ctx.SendOK()
}

// Command 返回命令类型
func (h *SetOptionHandler) Command() uint8 {
	return protocol.COM_SET_OPTION
}

// Name 返回处理器名称
func (h *SetOptionHandler) Name() string {
	return "COM_SET_OPTION"
}
