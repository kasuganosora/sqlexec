package mysql

import (
	"context"
	"fmt"
	"io"
	"log"
	"mysql-proxy/mysql/protocol"
	"net"
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
			handshake.ConnectionID,
			handshake.AuthPluginName)

		if err := handshake.Write(conn); err != nil {
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
		log.Printf("收到认证包: %s", authPacket.Dump())
		if len(authPacket.Payload) > 0 {
			log.Printf("认证包命令类型: %d", authPacket.Payload[0])
		}

		// 发送 OK 包表示认证成功
		log.Printf("发送认证成功包...")
		if err := protocol.SendOK(conn, authPacket.SequenceID[0]+1); err != nil {
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
		log.Printf("收到命令包: %s", packet.Dump())

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
		default:
			log.Printf("不支持的命令类型: %d", packetType)
			protocol.SendError(conn, fmt.Errorf("不支持的命令类型: %d", packetType))
		}
	}
}

func handleQuery(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	log.Printf("handling query: %s", string(packet.Payload[1:]))
	return protocol.SendOK(conn, packet.SequenceID[0]+1)
}

func handleInitDB(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	dbName := string(packet.Payload[1:])
	log.Printf("switching to database: %s", dbName)
	return protocol.SendOK(conn, packet.SequenceID[0]+1)
}

func handlePing(ctx context.Context, conn net.Conn) error {
	log.Printf("handling ping")
	return protocol.SendOK(conn, 1)
}
