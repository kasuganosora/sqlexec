package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// DebugHandler DEBUG 命令处理器
type DebugHandler struct{}

// NewDebugHandler 创建 DEBUG 处理器
func NewDebugHandler() *DebugHandler {
	return &DebugHandler{}
}

// Handle 处理 COM_DEBUG 命令
func (h *DebugHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	ctx.Log("处理 COM_DEBUG")
	// 简化实现，不执行任何操作
	return nil
}

// Command 返回命令类型
func (h *DebugHandler) Command() uint8 {
	return protocol.COM_DEBUG
}

// Name 返回处理器名称
func (h *DebugHandler) Name() string {
	return "COM_DEBUG"
}
