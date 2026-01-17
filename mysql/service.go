package mysql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mysql-proxy/mysql/protocol"
	"net"
	"strings"
	"time"
)

// 定义 context key
type contextKey string

const (
	// 连接状态相关的 key
	keyHandshakeDone contextKey = "handshake_done"
)

// 设置握手完成状态
func withHandshakeDone(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyHandshakeDone, true)
}

// 检查握手是否完成
func isHandshakeDone(ctx context.Context) bool {
	done, _ := ctx.Value(keyHandshakeDone).(bool)
	return done
}

// HandleConn 用于处理MYSQL的链接
func HandleConn(ctx context.Context, conn net.Conn) (err error) {
	// 设置连接超时
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// 检查是否已经完成握手
	if !isHandshakeDone(ctx) {
	// 发送握手包
	handshake := protocol.NewHandshakePacket()
	log.Printf("准备发送握手包: ProtocolVersion=%d, ServerVersion=%s, ConnectionID=%d, AuthPluginName=%s",
		handshake.ProtocolVersion,
		handshake.ServerVersion,
		handshake.ThreadID,
		handshake.AuthPluginName)

	data, err := handshake.Marshal()
	if err != nil {
		log.Printf("序列化握手包失败: %v", err)
		return fmt.Errorf("序列化握手包失败: %w", err)
	}

	if _, err := conn.Write(data); err != nil {
		log.Printf("发送握手包失败: %v", err)
		return fmt.Errorf("发送握手包失败: %w", err)
	}
	log.Printf("已发送握手包")

	// 读取客户端的认证包
	log.Printf("等待客户端认证包...")
	authPacket, err := protocol.ReadPacket(conn)
	if err != nil {
		log.Printf("读取认证包失败: %v", err)
		return fmt.Errorf("读取认证包失败: %w", err)
	}

	// 打印认证包信息
	log.Printf("收到认证包: 长度=%d, SequenceID=%d", authPacket.PayloadLength, authPacket.SequenceID)
	if len(authPacket.Payload) > 0 {
		log.Printf("认证包命令类型: %d", authPacket.Payload[0])
	}

	// 发送 OK 包表示认证成功
	log.Printf("发送认证成功包...")
	if err := protocol.SendOK(conn, authPacket.SequenceID+1); err != nil {
		log.Printf("发送认证成功包失败: %v", err)
		return fmt.Errorf("发送认证成功包失败: %w", err)
	}
	log.Printf("已发送认证成功包")

		// 标记握手完成
		ctx = withHandshakeDone(ctx)
	}

	// 处理后续的命令包
	for {
		log.Printf("等待命令包...")
		packet, err := protocol.ReadPacket(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("客户端断开连接")
				return nil
			}
			log.Printf("读取命令包失败: %v", err)
			protocol.SendError(conn, err)
			return fmt.Errorf("读取命令包失败: %w", err)
		}

		// 打印封包信息用于调试
		log.Printf("收到命令包: 长度=%d, SequenceID=%d", packet.PayloadLength, packet.SequenceID)

		// 解析封包的类型
		packetType := packet.GetCommandType()
		switch packetType {
		case protocol.COM_QUIT:
			log.Printf("收到退出命令")
			return nil
		case protocol.COM_QUERY:
			if err := handleQuery(ctx, conn, packet); err != nil {
				return err
			}
		case protocol.COM_INIT_DB:
			if err := handleInitDB(ctx, conn, packet); err != nil {
				return err
			}
		case protocol.COM_PING:
			if err := handlePing(ctx, conn); err != nil {
				return err
			}
		case protocol.COM_STMT_PREPARE:
			if err := handleStmtPrepare(ctx, conn, packet); err != nil {
				return err
			}
		case protocol.COM_STMT_EXECUTE:
			if err := handleStmtExecute(ctx, conn, packet); err != nil {
				return err
			}
		case protocol.COM_STMT_CLOSE:
			if err := handleStmtClose(ctx, conn, packet); err != nil {
				return err
			}
		default:
			log.Printf("不支持的命令类型: %d", packetType)
			protocol.SendError(conn, fmt.Errorf("不支持的命令类型: %d", packetType))
		}
	}
}

func handleQuery(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	log.Printf("handling query: %s", string(packet.Payload[1:]))
	return protocol.SendOK(conn, packet.SequenceID+1)
}

func handleInitDB(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	dbName := string(packet.Payload[1:])
	log.Printf("switching to database: %s", dbName)
	return protocol.SendOK(conn, packet.SequenceID+1)
}

func handlePing(ctx context.Context, conn net.Conn) error {
	log.Printf("handling ping")
	return protocol.SendOK(conn, 1)
}

// handleStmtPrepare 处理 COM_STMT_PREPARE 命令
func handleStmtPrepare(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	// 解析 COM_STMT_PREPARE 包
	stmtPreparePacket := &protocol.ComStmtPreparePacket{}
	if err := stmtPreparePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_PREPARE 包失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}

	log.Printf("handling COM_STMT_PREPARE: query='%s'", stmtPreparePacket.Query)

	// 这里应该解析SQL语句，提取参数和列信息
	// 简化实现：返回一个基本的响应包

	// 示例：分析查询中的参数占位符
	paramCount := uint16(0)
	if strings.Contains(stmtPreparePacket.Query, "?") {
		paramCount = 1 // 简化：假设只有一个参数
	}

	// 示例：分析查询中的列数
	columnCount := uint16(1) // 简化：假设只有一列

	// 创建 Prepare 响应包
	response := &protocol.StmtPrepareResponsePacket{
		Packet: protocol.Packet{
			SequenceID: packet.SequenceID + 1,
		},
		StatementID:  1, // 生成唯一的语句ID
		ColumnCount:   columnCount,
		ParamCount:    paramCount,
		Reserved:      0,
		WarningCount:  0,
		Params:        make([]protocol.FieldMeta, paramCount),
		Columns:       make([]protocol.FieldMeta, columnCount),
	}

	// 填充参数元数据（如果有）
	if paramCount > 0 {
		response.Params[0] = protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "",
			Table:                     "",
			OrgTable:                  "",
			Name:                      "?",
			OrgName:                   "",
			LengthOfFixedLengthFields:  12,
			CharacterSet:              33, // utf8_general_ci
			ColumnLength:              255,
			Type:                      0xfd, // VAR_STRING
			Flags:                     0,
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		}
	}

	// 填充列元数据（如果有）
	if columnCount > 0 {
		response.Columns[0] = protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "test",
			Table:                     "users",
			OrgTable:                  "users",
			Name:                      "id",
			OrgName:                   "id",
			LengthOfFixedLengthFields:  12,
			CharacterSet:              33, // utf8_general_ci
			ColumnLength:              11,
			Type:                      0x03, // LONG (INT)
			Flags:                     0x81, // NOT_NULL_FLAG | PRI_KEY_FLAG
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		}
	}

	// 发送响应
	data, err := response.Marshal()
	if err != nil {
		log.Printf("序列化 COM_STMT_PREPARE 响应失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}

	_, err = conn.Write(data)
	if err != nil {
		log.Printf("发送 COM_STMT_PREPARE 响应失败: %v", err)
		return err
	}

	log.Printf("已发送 COM_STMT_PREPARE 响应: statement_id=%d, params=%d, columns=%d",
		response.StatementID, response.ParamCount, response.ColumnCount)
	return nil
}

// handleStmtExecute 处理 COM_STMT_EXECUTE 命令
func handleStmtExecute(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	// 解析 COM_STMT_EXECUTE 包
	stmtExecutePacket := &protocol.ComStmtExecutePacket{}
	if err := stmtExecutePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_EXECUTE 包失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}

	log.Printf("handling COM_STMT_EXECUTE: statement_id=%d, params=%v",
		stmtExecutePacket.StatementID, stmtExecutePacket.ParamValues)

	// 这里应该执行预处理语句并返回结果集
	// 简化实现：返回一个基本的结果集

	// 发送列数包（简化：假设只有1列）
	columnCountData := []byte{
		0x01, 0x00, 0x00, // 列数 = 1
		stmtExecutePacket.SequenceID + 1,
	}
	_, err := conn.Write(columnCountData)
	if err != nil {
		return err
	}

	// 发送列元数据包
	fieldMeta := protocol.FieldMetaPacket{
		Packet: protocol.Packet{
			SequenceID: stmtExecutePacket.SequenceID + 2,
		},
		FieldMeta: protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "test",
			Table:                     "users",
			OrgTable:                  "users",
			Name:                      "id",
			OrgName:                   "id",
			LengthOfFixedLengthFields:  12,
			CharacterSet:              33,
			ColumnLength:              11,
			Type:                      0x03,
			Flags:                     0x81,
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		},
	}
	fieldMetaData, err := fieldMeta.MarshalDefault()
	if err != nil {
		log.Printf("序列化列元数据失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}
	_, err = conn.Write(fieldMetaData)
	if err != nil {
		return err
	}

	// 发送列结束包（OK包）
	eofPacket := protocol.CreateEofPacketWithStatus(stmtExecutePacket.SequenceID+3, true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.Write(eofData)
	if err != nil {
		return err
	}

	// 发送数据行（简化：假设只有一行）
	rowData := protocol.RowDataPacket{
		Packet: protocol.Packet{
			SequenceID: stmtExecutePacket.SequenceID + 4,
		},
		RowData: []string{"1"},
	}
	rowDataBytes, err := rowData.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.Write(rowDataBytes)
	if err != nil {
		return err
	}

	// 发送结束包
	finalEof := protocol.CreateEofPacketWithStatus(stmtExecutePacket.SequenceID+5, true, false)
	finalEofData, err := finalEof.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.Write(finalEofData)
	if err != nil {
		return err
	}

	log.Printf("已发送 COM_STMT_EXECUTE 响应: statement_id=%d", stmtExecutePacket.StatementID)
	return nil
}

// handleStmtClose 处理 COM_STMT_CLOSE 命令
func handleStmtClose(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	// 解析 COM_STMT_CLOSE 包
	stmtClosePacket := &protocol.ComStmtClosePacket{}
	if err := stmtClosePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_CLOSE 包失败: %v", err)
		return err
	}

	log.Printf("handling COM_STMT_CLOSE: statement_id=%d", stmtClosePacket.StatementID)

	// 这里应该释放预处理语句资源
	// 简化实现：只记录日志

	// COM_STMT_CLOSE 不需要发送响应
	log.Printf("已关闭预处理语句: statement_id=%d", stmtClosePacket.StatementID)
	return nil
}
