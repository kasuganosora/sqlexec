package simple

import (
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// QuitHandler QUIT 命令处理器
type QuitHandler struct{}

// NewQuitHandler 创建 QUIT 处理器
func NewQuitHandler() *QuitHandler {
	return &QuitHandler{}
}

// Handle 处理 COM_QUIT 命令
func (h *QuitHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	ctx.Log("处理 COM_QUIT: 不发送响应")
	// COM_QUIT 不发送响应包，直接返回 nil
	// 调用方需要关闭连接
	return nil
}

// Command 返回命令类型
func (h *QuitHandler) Command() uint8 {
	return protocol.COM_QUIT
}

// Name 返回处理器名称
func (h *QuitHandler) Name() string {
	return "COM_QUIT"
}
