package query

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
)

// InitDBHandler INIT_DB 命令处理器
type InitDBHandler struct {
	okBuilder *response.OKBuilder
}

// NewInitDBHandler 创建 INIT_DB 处理器
func NewInitDBHandler(okBuilder *response.OKBuilder) *InitDBHandler {
	if okBuilder == nil {
		okBuilder = response.NewOKBuilder()
	}
	return &InitDBHandler{
		okBuilder: okBuilder,
	}
}

// Handle 处理 COM_INIT_DB 命令
func (h *InitDBHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	// 从 session 获取序列号
	seqID := ctx.GetNextSequenceID()

	cmd, ok := packet.(*protocol.ComInitDBPacket)
	if !ok {
		return ctx.SendError(fmt.Errorf("invalid packet type for COM_INIT_DB"))
	}

	// ComInitDBPacket 的 SchemaName 字段是通过 tag 定义的
	// 我们需要手动从 Payload 中提取，因为 tag 解析需要 Unmarshal 方法
	var dbName string
	if len(cmd.Payload) > 1 {
		// 跳过第一个字节（命令字节），读取数据库名
		dbName = string(cmd.Payload[1:])
	}

	ctx.Log("处理 COM_INIT_DB: %s", dbName)

	// 如果数据库名为空，从 session 中读取
	if dbName == "" {
		if val, err := ctx.Session.Get("current_database"); err == nil && val != nil {
			if strVal, ok := val.(string); ok {
				dbName = strVal
				ctx.Log("从 session 中读取到数据库名: %s", dbName)
			}
		}
	}

	// 如果仍然为空，跳过处理
	if dbName == "" {
		ctx.Log("COM_INIT_DB 数据库名为空且无法从 session 获取，跳过处理")
		return h.sendOK(ctx, seqID)
	}

	// 设置数据库名
	ctx.Session.Set("current_database", dbName)

	// 获取 API Session 并更新当前数据库
	apiSessIntf := ctx.Session.GetAPISession()
	if apiSessIntf != nil {
		if apiSess, ok := apiSessIntf.(*api.Session); ok {
			ctx.Log("更新 API Session 当前数据库: %s", dbName)
			apiSess.SetCurrentDB(dbName)
		} else {
			ctx.Log("API Session 类型断言失败")
		}
	} else {
		ctx.Log("API Session 未初始化，无法更新当前数据库")
	}

	return h.sendOK(ctx, seqID)
}

// sendOK 发送 OK 包
func (h *InitDBHandler) sendOK(ctx *handler.HandlerContext, seqID uint8) error {
	okPacket := h.okBuilder.Build(seqID, 0, 0, 0)
	data, err := okPacket.Marshal()
	if err != nil {
		return err
	}

	_, err = ctx.Connection.Write(data)
	return err
}

// Command 返回命令类型
func (h *InitDBHandler) Command() uint8 {
	return protocol.COM_INIT_DB
}

// Name 返回处理器名称
func (h *InitDBHandler) Name() string {
	return "COM_INIT_DB"
}
