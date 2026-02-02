package query

import (
	"fmt"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
)

// FieldListHandler FIELD_LIST 命令处理器
type FieldListHandler struct {
	eofBuilder *response.EOFBuilder
}

// NewFieldListHandler 创建 FIELD_LIST 处理器
func NewFieldListHandler(eofBuilder *response.EOFBuilder) *FieldListHandler {
	if eofBuilder == nil {
		eofBuilder = response.NewEOFBuilder()
	}
	return &FieldListHandler{
		eofBuilder: eofBuilder,
	}
}

// Handle 处理 COM_FIELD_LIST 命令
func (h *FieldListHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	// 从 session 获取序列号
	seqID := ctx.GetNextSequenceID()

	cmd, ok := packet.(*protocol.ComFieldListPacket)
	if !ok {
		return fmt.Errorf("invalid packet type for COM_FIELD_LIST")
	}

	ctx.Log("处理 COM_FIELD_LIST: table=%s, wildcard=%s", cmd.Table, cmd.Wildcard)

	// 发送 EOF 包（简化实现，不返回实际字段列表）
	eofPacket := h.eofBuilder.Build(seqID, 0, protocol.SERVER_STATUS_AUTOCOMMIT)
	data, err := eofPacket.Marshal()
	if err != nil {
		return err
	}

	_, err = ctx.Connection.Write(data)
	return err
}

// Command 返回命令类型
func (h *FieldListHandler) Command() uint8 {
	return protocol.COM_FIELD_LIST
}

// Name 返回处理器名称
func (h *FieldListHandler) Name() string {
	return "COM_FIELD_LIST"
}
