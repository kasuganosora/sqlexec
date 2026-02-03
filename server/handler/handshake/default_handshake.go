package handshake

import (
	"net"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
)

// DefaultHandshakeHandler 默认握手处理器
type DefaultHandshakeHandler struct {
	db *api.DB
	logger handler.Logger
}

// NewDefaultHandshakeHandler 创建默认握手处理器
func NewDefaultHandshakeHandler(db *api.DB, logger handler.Logger) handler.HandshakeHandler {
	return &DefaultHandshakeHandler{
		db:     db,
		logger: logger,
	}
}

// Handle 处理握手流程
func (h *DefaultHandshakeHandler) Handle(conn net.Conn, sess *pkg_session.Session) error {
	// 发送握手包 (序列号为0)
	handshakePacket := &protocol.HandshakeV10Packet{}
	handshakePacket.Packet.SequenceID = 0
	handshakePacket.ProtocolVersion = 10
	handshakePacket.ServerVersion = "10.11.4-MariaDB"
	handshakePacket.ThreadID = sess.ThreadID
	handshakePacket.AuthPluginDataPart = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	handshakePacket.AuthPluginDataPart2 = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	handshakePacket.CapabilityFlags1 = 0xf7fe
	handshakePacket.CharacterSet = 8
	handshakePacket.StatusFlags = 0x0002
	handshakePacket.CapabilityFlags2 = 0x81bf
	handshakePacket.MariaDBCaps = 0x00000007
	handshakePacket.AuthPluginName = "mysql_native_password"

	handshakeData, err := handshakePacket.Marshal()
	if err != nil {
		return err
	}

	if _, err := conn.Write(handshakeData); err != nil {
		return err
	}
	if h.logger != nil {
		h.logger.Printf("已发送握手包, ThreadID: %d", handshakePacket.ThreadID)
	}

	// 计算完整的能力标志 (32位)
	serverCapabilities := (uint32(handshakePacket.CapabilityFlags2) << 16) | uint32(handshakePacket.CapabilityFlags1)

	// 读取握手响应
	handshakeResponse := &protocol.HandshakeResponse{}
	if err := handshakeResponse.Unmarshal(conn, serverCapabilities); err != nil {
		if h.logger != nil {
			h.logger.Printf("解析认证包失败: %v", err)
		}
		return err
	}

	if h.logger != nil {
		h.logger.Printf("收到认证包: User=%s, Database=%s, CharacterSet=%d",
			handshakeResponse.User, handshakeResponse.Database, handshakeResponse.CharacterSet)
	}

	// 更新 session 信息
	sess.SetUser(handshakeResponse.User)

	// 同时设置 API 层 Session 的用户
	if h.db != nil {
		if apiSessIntf := sess.GetAPISession(); apiSessIntf != nil {
			if apiSess, ok := apiSessIntf.(*api.Session); ok {
				apiSess.SetUser(handshakeResponse.User)
				if h.logger != nil {
					h.logger.Printf("已设置 API Session 用户: %s", handshakeResponse.User)
				}
			}
		}
	}

	if handshakeResponse.Database != "" {
		// 简化实现，不调用 SetCurrentDB
		sess.Set("current_database", handshakeResponse.Database)
	}

	// MySQL握手阶段序列号是连续的：
	// - 握手包（服务器->客户端）：序列号0
	// - 认证响应（客户端->服务器）：序列号1
	// - OK包（服务器->客户端）：序列号2
	// 握手完成后，准备接收新命令，序列号重置为255（GetNextSequenceID后为0）
	// 参考MariaDB: net_new_transaction重置序列号
	sess.SequenceID = 255

	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = 2
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 0
	okPacket.OkInPacket.LastInsertId = 0
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	okData, err := okPacket.Marshal()
	if err != nil {
		return err
	}

	_, err = conn.Write(okData)
	if err != nil {
		return err
	}
	if h.logger != nil {
		h.logger.Printf("已发送认证成功包")
	}

	return nil
}

// Name 返回处理器名称
func (h *DefaultHandshakeHandler) Name() string {
	return "DefaultHandshakeHandler"
}
