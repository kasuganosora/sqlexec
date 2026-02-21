package process

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
)

// ProcessKillHandler PROCESS_KILL 命令处理器
type ProcessKillHandler struct {
	okBuilder *response.OKBuilder
}

// NewProcessKillHandler 创建 PROCESS_KILL 处理器
func NewProcessKillHandler(okBuilder *response.OKBuilder) *ProcessKillHandler {
	if okBuilder == nil {
		okBuilder = response.NewOKBuilder()
	}
	return &ProcessKillHandler{
		okBuilder: okBuilder,
	}
}

// Handle 处理 COM_PROCESS_KILL 命令
func (h *ProcessKillHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	cmd, ok := packet.(*protocol.ComProcessKillPacket)
	if !ok {
		return ctx.SendError(fmt.Errorf("invalid packet type for COM_PROCESS_KILL"))
	}

	threadID := cmd.ProcessID
	ctx.Log("处理 COM_PROCESS_KILL, threadID=%d", threadID)

	// 调用全局 Kill 功能
	err := pkg_session.KillQueryByThreadID(threadID)
	if err != nil {
		ctx.Log("Kill查询失败: %v", err)
		// 返回错误包
		return ctx.SendError(fmt.Errorf("Unknown thread id: %d", threadID))
	}

	ctx.Log("成功 killed thread %d", threadID)
	return ctx.SendOK()
}

// Command 返回命令类型
func (h *ProcessKillHandler) Command() uint8 {
	return protocol.COM_PROCESS_KILL
}

// Name 返回处理器名称
func (h *ProcessKillHandler) Name() string {
	return "COM_PROCESS_KILL"
}
