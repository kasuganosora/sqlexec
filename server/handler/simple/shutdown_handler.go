package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// ShutdownHandler SHUTDOWN 命令处理器
type ShutdownHandler struct{}

// NewShutdownHandler 创建 SHUTDOWN 处理器
func NewShutdownHandler() *ShutdownHandler {
	return &ShutdownHandler{}
}

// Handle 处理 COM_SHUTDOWN 命令
func (h *ShutdownHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	ctx.Log("处理 COM_SHUTDOWN")
	// 简化实现，不实际关闭服务器
	return nil
}

// Command 返回命令类型
func (h *ShutdownHandler) Command() uint8 {
	return protocol.COM_SHUTDOWN
}

// Name 返回处理器名称
func (h *ShutdownHandler) Name() string {
	return "COM_SHUTDOWN"
}
