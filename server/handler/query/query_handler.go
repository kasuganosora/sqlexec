package query

import (
	"fmt"
	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
	"strings"
)

// QueryHandler QUERY 命令处理器
type QueryHandler struct {
	resultSetBuilder *response.ResultSetBuilder
}

// NewQueryHandler 创建 QUERY 处理器
func NewQueryHandler() *QueryHandler {
	return &QueryHandler{
		resultSetBuilder: response.NewResultSetBuilder(),
	}
}

// Handle 处理 COM_QUERY 命令
func (h *QueryHandler) Handle(ctx *handler.HandlerContext, packet interface{}) error {
	// 每个命令开始时重置序列号
	ctx.ResetSequenceID()

	cmd, ok := packet.(*protocol.ComQueryPacket)
	if !ok {
		return ctx.SendError(handler.NewHandlerError("Invalid packet type for COM_QUERY"))
	}

	// 从 Packet.Payload 中提取查询字符串 (跳过第一个字节的 Command)
	var query string
	if len(cmd.Payload) > 1 {
		query = strings.TrimSpace(string(cmd.Payload[1:]))
	}

	ctx.Log("处理 COM_QUERY: %s", query)

	// 使用 API Session 执行查询
	// 所有查询（包括 SELECT DATABASE()、SELECT 1、@@变量查询）都通过 API 执行
	// 这些查询会在 OptimizedExecutor 中处理（包括 handleNoFromQuery）
	apiSessIntf := ctx.Session.GetAPISession()
	if apiSessIntf == nil {
		ctx.Log("API Session 未初始化")
		err := fmt.Errorf("database not initialized")
		return ctx.SendError(err)
	}

	apiSess, ok := apiSessIntf.(*api.Session)
	if !ok {
		ctx.Log("API Session 类型断言失败")
		err := fmt.Errorf("invalid session type")
		return ctx.SendError(err)
	}

	// 执行查询
	queryObj, err := apiSess.Query(query)
	if err != nil {
		ctx.Log("查询失败: %v", err)
		return ctx.SendError(err)
	}
	defer queryObj.Close()

	// 获取列信息
	columns := queryObj.Columns()
	if len(columns) == 0 {
		// 空结果集，返回 OK
		ctx.Log("查询返回空列，发送 OK")
		return ctx.SendOK()
	}

	// 收集行数据
	var rows []domain.Row
	rowCount := 0
	for queryObj.Next() {
		row := queryObj.Row()
		ctx.Log("  Query 返回的行 %d: %+v", rowCount, row)
		rows = append(rows, row)
		rowCount++
	}

	ctx.Log("总共收集到 %d 行数据", rowCount)

	// 发送结果集
	return h.sendQueryResult(ctx, columns, rows)
}

// sendQueryResult 发送查询结果
func (h *QueryHandler) sendQueryResult(ctx *handler.HandlerContext, columns []domain.ColumnInfo, rows []domain.Row) error {
	// 获取序列号
	seqID := ctx.GetNextSequenceID()

	// 发送列数包
	columnCountPacket, err := response.BuildColumnCountPacket(seqID, uint64(len(columns)))
	if err != nil {
		return err
	}
	if _, err := ctx.Connection.Write(columnCountPacket); err != nil {
		return err
	}

	// 发送每个列的定义
	for _, col := range columns {
		seqID = ctx.GetNextSequenceID()
		fieldPacket := h.buildFieldPacket(seqID, col)
		data, err := fieldPacket.Marshal(0)
		if err != nil {
			return err
		}
		if _, err := ctx.Connection.Write(data); err != nil {
			return err
		}
	}

	// 发送 EOF 包
	eofBuilder := response.NewEOFBuilder()
	eofPacket := eofBuilder.Build(ctx.GetNextSequenceID(), 0, protocol.SERVER_STATUS_AUTOCOMMIT)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := ctx.Connection.Write(eofData); err != nil {
		return err
	}

	// 发送行数据
	for _, row := range rows {
		rowPacket := h.buildRowPacket(ctx.GetNextSequenceID(), columns, row)
		if _, err := ctx.Connection.Write(rowPacket); err != nil {
			return err
		}
	}

	// 发送最后的 EOF 包
	eofPacket2 := eofBuilder.Build(ctx.GetNextSequenceID(), 0, protocol.SERVER_STATUS_AUTOCOMMIT)
	eofData2, err := eofPacket2.Marshal()
	if err != nil {
		return err
	}
	_, err = ctx.Connection.Write(eofData2)
	return err
}

// buildFieldPacket 构建列定义包
func (h *QueryHandler) buildFieldPacket(sequenceID uint8, col domain.ColumnInfo) *protocol.FieldMetaPacket {
	packet := &protocol.FieldMetaPacket{}
	packet.SequenceID = sequenceID
	packet.Catalog = "def"
	packet.Schema = ""
	packet.Table = ""
	packet.OrgTable = ""
	packet.Name = col.Name
	packet.OrgName = col.Name
	packet.CharacterSet = 0x21
	packet.ColumnLength = 255
	packet.Type = h.mapMySQLType(col.Type)
	packet.Flags = 0
	packet.Decimals = 0
	return packet
}

// buildRowPacket 构建行数据包
func (h *QueryHandler) buildRowPacket(sequenceID uint8, columns []domain.ColumnInfo, row domain.Row) []byte {
	packet := &protocol.RowDataPacket{}
	packet.SequenceID = sequenceID

	// 构建列值数组
	values := make([]string, len(columns))
	for i, col := range columns {
		if val, exists := row[col.Name]; exists {
			values[i] = h.formatValue(val)
		} else {
			values[i] = "___SQL_EXEC_NULL___" // NULL 值标记
		}
	}
	packet.RowData = values

	// 序列化为字节
	data, err := packet.Marshal()
	if err != nil {
		return nil
	}
	return data
}

// formatValue 格式化值为字符串
func (h *QueryHandler) formatValue(value interface{}) string {
	if value == nil {
		return "___SQL_EXEC_NULL___" // NULL 值标记
	}
	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		if v == float32(int(v)) {
			return fmt.Sprintf("%d", int(v))
		}
		return fmt.Sprintf("%g", v)
	case float64:
		if v == float64(int(v)) {
			return fmt.Sprintf("%d", int(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// mapMySQLType 映射数据类型
func (h *QueryHandler) mapMySQLType(typeStr string) byte {
	switch {
	case typeStr == "int", typeStr == "integer":
		return protocol.MYSQL_TYPE_LONG
	case typeStr == "tinyint":
		return protocol.MYSQL_TYPE_TINY
	case typeStr == "smallint":
		return protocol.MYSQL_TYPE_SHORT
	case typeStr == "bigint":
		return protocol.MYSQL_TYPE_LONGLONG
	case typeStr == "float":
		return protocol.MYSQL_TYPE_FLOAT
	case typeStr == "double":
		return protocol.MYSQL_TYPE_DOUBLE
	case typeStr == "decimal", typeStr == "numeric":
		return protocol.MYSQL_TYPE_DECIMAL
	case typeStr == "date":
		return protocol.MYSQL_TYPE_DATE
	case typeStr == "datetime":
		return protocol.MYSQL_TYPE_DATETIME
	case typeStr == "timestamp":
		return protocol.MYSQL_TYPE_TIMESTAMP
	case typeStr == "time":
		return protocol.MYSQL_TYPE_TIME
	case typeStr == "text", typeStr == "string":
		return protocol.MYSQL_TYPE_VAR_STRING
	case typeStr == "boolean", typeStr == "bool":
		return protocol.MYSQL_TYPE_TINY
	default:
		return protocol.MYSQL_TYPE_VAR_STRING
	}
}

// Command 返回命令类型
func (h *QueryHandler) Command() uint8 {
	return protocol.COM_QUERY
}

// Name 返回处理器名称
func (h *QueryHandler) Name() string {
	return "COM_QUERY"
}
