package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mysql-proxy/mysql/protocol"
	"mysql-proxy/mysql/session"
	"net"
)

type Server struct {
	ctx        context.Context
	listener   net.Listener
	sessionMgr *session.SessionMgr
}

func NewServer(ctx context.Context, listener net.Listener) *Server {
	s := &Server{
		listener:   listener,
		ctx:        ctx,
		sessionMgr: session.NewSessionMgr(ctx, session.NewMemoryDriver()),
	}

	return s
}

func (s *Server) Start() (err error) {

	for {
		// 判断是否退出
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go func(ctx context.Context, conn net.Conn) {
			err = s.Handle(ctx, conn)
			if err != nil {
				fmt.Printf("handle error: %+v\n	", err)
				conn.Close()
			}
		}(s.ctx, conn)
	}
}

func (s *Server) Handle(ctx context.Context, conn net.Conn) (err error) {
	// 处理session， 通过 远端IP+端口 作为sessionID
	sess, err := s.sessionMgr.GetOrCreateSession(ctx, conn.RemoteAddr().String(), conn.RemoteAddr().String())
	if err != nil {
		return err
	}

	// 如果没获取到session，则为一个新链接，需要发送握手包给客户端
	if len(sess.User) == 0 {
		//构建握手包
		handshakePacket := &protocol.HandshakeV10Packet{}
		handshakePacket.ProtocolVersion = 10
		handshakePacket.ServerVersion = "5.5.5-10.3.12-MariaDB"
		handshakePacket.ThreadID = sess.ThreadID
		handshakePacket.AuthPluginDataPart = []byte(RandomString(8))   // Salt
		handshakePacket.AuthPluginDataPart2 = []byte(RandomString(12)) // Salt 后半段
		handshakePacket.CapabilityFlags1 = 0xf7fe
		handshakePacket.CharacterSet = 8     // utf8mb4_general_ci (utf8mb4)
		handshakePacket.StatusFlags = 0x0002 //AutoCommit
		handshakePacket.CapabilityFlags2 = 0x81bf
		handshakePacket.MariaDBCaps = 0x00000007
		handshakePacket.AuthPluginName = "mysql_native_password"
		var packBytes []byte
		packBytes, err = handshakePacket.Marshal()
		if err != nil {
			return err
		}
		_, err = io.Copy(conn, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}

		// 把盐保存到session
		sess.Set("salt", handshakePacket.AuthPluginDataPart)
		sess.Set("salt2", handshakePacket.AuthPluginDataPart2)
	}

	// 持续处理包，直到连接关闭
	for {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 按 MySQL 协议包格式读取数据
		packContent, err := s.readMySQLPacket(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// 连接正常关闭
				return nil
			}
			return err
		}

		// 这个时候应该是收到 auth requesrt 包
		if len(sess.User) == 0 && len(packContent) > 0 {
			authRequestPacket := &protocol.HandshakeResponse{}
			err = authRequestPacket.Unmarshal(bytes.NewReader(packContent), 0)
			if err != nil {
				return err
			}

			// 检查用户名密码
			// TODO 先跳过，后面再实现

			sess.SetUser(authRequestPacket.User)
			sess.Set("capability", authRequestPacket.ClientCapabilities)
			sess.Set("extended_capability", authRequestPacket.ExtendedClientCapabilities)
			sess.Set("max_packet_size", authRequestPacket.MaxPacketSize)
			sess.Set("character_set", authRequestPacket.CharacterSet)
			sess.Set("maria_db_caps", authRequestPacket.MariaDBCaps)
			sess.Set("auth_response", authRequestPacket.AuthResponse)
			sess.Set("database", authRequestPacket.Database)
			sess.Set("connection_attributes", authRequestPacket.ConnectionAttributes)
			sess.Set("zstd_compression_level", authRequestPacket.ZstdCompressionLevel)

			// 然后发送 ok 包 告诉客户端登录成功
			okPacket := &protocol.OkPacket{}
			okPacket.SequenceID = sess.GetNextSequenceID()
			okPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
			okPacket.Packet.SequenceID = authRequestPacket.SequenceID + 1
			var packBytes []byte
			packBytes, err = okPacket.Marshal()
			if err != nil {
				return err
			}
			_, err = io.Copy(conn, bytes.NewReader(packBytes))
			if err != nil {
				return err
			}
			continue // 继续等待下一个包
		}

		// 这里应该是收到其他包了 通过读取第4个字节来判断
		var commandType uint8
		var commadPack any
		commandType = packContent[4]

		switch commandType {
		case protocol.COM_QUIT:
			ComQuitPacket := &protocol.ComQuitPacket{}
			err = ComQuitPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComQuitPacket
		case protocol.COM_PING:
			ComPingPacket := &protocol.ComPingPacket{}
			err = ComPingPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComPingPacket
		case protocol.COM_QUERY:
			ComQueryPacket := &protocol.ComQueryPacket{}
			err = ComQueryPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComQueryPacket
		case protocol.COM_INIT_DB:
			ComInitDBPacket := &protocol.ComInitDBPacket{}
			err = ComInitDBPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComInitDBPacket
		case protocol.COM_SET_OPTION:
			ComSetOptionPacket := &protocol.ComSetOptionPacket{}
			err = ComSetOptionPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComSetOptionPacket
			// 备注参数占位符的包需要在各自包中处理
		// case protocol.COM_STMT_PREPARE:
		// 	ComStmtPreparePacket := &protocol.ComStmtPreparePacket{}
		// 	err = ComStmtPreparePacket.Unmarshal(bytes.NewReader(packContent))
		// 	if err != nil {
		// 		return err
		// 	}
		// 	commadPack = ComStmtPreparePacket
		case protocol.COM_STMT_EXECUTE:
			ComStmtExecutePacket := &protocol.ComStmtExecutePacket{}
			err = ComStmtExecutePacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComStmtExecutePacket
		case protocol.COM_STMT_CLOSE:
			ComStmtClosePacket := &protocol.ComStmtClosePacket{}
			err = ComStmtClosePacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComStmtClosePacket
		case protocol.COM_STMT_SEND_LONG_DATA:
			ComStmtSendLongDataPacket := &protocol.ComStmtSendLongDataPacket{}
			err = ComStmtSendLongDataPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComStmtSendLongDataPacket
		case protocol.COM_STMT_RESET:
			ComStmtResetPacket := &protocol.ComStmtResetPacket{}
			err = ComStmtResetPacket.Unmarshal(bytes.NewReader(packContent))
			if err != nil {
				return err
			}
			commadPack = ComStmtResetPacket
		default:
			// 发送错误包
			errPacket := &protocol.ErrorPacket{}
			errPacket.ErrorCode = 1045
			errPacket.ErrorMessage = fmt.Sprintf("protocol error: command not implemented: %s", protocol.GetCommandName(commandType))
			var packBytes []byte
			packBytes, err = errPacket.Marshal()
			if err != nil {
				return err
			}
			_, err = io.Copy(conn, bytes.NewReader(packBytes))
			if err != nil {
				return err
			}
			continue // 继续等待下一个包
		}

		// 分发到各个包中处理
		err = s.handCommand(ctx, sess, conn, commadPack)
		if err != nil {
			return err
		}

		// 如果是 QUIT 命令，处理完后退出循环
		if commandType == protocol.COM_QUIT {
			return nil
		}
	}
}

// readMySQLPacket 按 MySQL 协议包格式读取数据
func (s *Server) readMySQLPacket(conn net.Conn) ([]byte, error) {
	// 读取包长度（3字节）和序列号（1字节）
	header := make([]byte, 4)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	// 解析包长度（小端序，3字节）
	packetLength := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16

	// 读取包体
	packetBody := make([]byte, packetLength)
	_, err = io.ReadFull(conn, packetBody)
	if err != nil {
		return nil, err
	}

	// 返回完整的包（包括头部）
	fullPacket := make([]byte, 4+packetLength)
	copy(fullPacket[:4], header)
	copy(fullPacket[4:], packetBody)

	return fullPacket, nil
}

// handCommand 处理命令
func (s *Server) handCommand(ctx context.Context, sess *session.Session, conn net.Conn, commadPack any) (err error) {
	switch commadPack.(type) {
	case *protocol.ComQuitPacket:
		return s.handComQuit(ctx, sess, conn, commadPack.(*protocol.ComQuitPacket))
	case *protocol.ComPingPacket:
		return s.handComPing(ctx, sess, conn, commadPack.(*protocol.ComPingPacket))
	case *protocol.ComInitDBPacket:
		return s.handComInitDB(ctx, sess, conn, commadPack.(*protocol.ComInitDBPacket))
	case *protocol.ComSetOptionPacket:
		return s.handComSetOption(ctx, sess, conn, commadPack.(*protocol.ComSetOptionPacket))
	case *protocol.ComStmtSendLongDataPacket:
		return s.handComStmtSendLongData(ctx, sess, conn, commadPack.(*protocol.ComStmtSendLongDataPacket))
	case *protocol.ComStmtResetPacket:
		return s.handComStmtReset(ctx, sess, conn, commadPack.(*protocol.ComStmtResetPacket))
	case *protocol.ComQueryPacket:
		return s.handComQuery(ctx, sess, conn, commadPack.(*protocol.ComQueryPacket))
	case *protocol.ComStmtPreparePacket:
		return s.handComStmtPrepare(ctx, sess, conn, commadPack.(*protocol.ComStmtPreparePacket))
	case *protocol.ComStmtExecutePacket:
		return s.handComStmtExecute(ctx, sess, conn, commadPack.(*protocol.ComStmtExecutePacket))
	case *protocol.ComStmtClosePacket:
		return s.handComStmtClose(ctx, sess, conn, commadPack.(*protocol.ComStmtClosePacket))
	}
	return nil
}

// handComQuit 处理 COM_QUIT 命令
func (s *Server) handComQuit(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComQuitPacket) (err error) {
	// 删除session
	err = s.sessionMgr.DeleteSession(ctx, sess.ID)
	if err != nil {
		return err
	}
	return
}

// handComPing 处理 COM_PING 命令
func (s *Server) handComPing(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComPingPacket) (err error) {
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = sess.GetNextSequenceID()
	okPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComInitDB(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComInitDBPacket) (err error) {
	// 设置数据库
	sess.Set("database", commadPack.SchemaName)
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = sess.GetNextSequenceID()
	okPacket.SetAutoCommit(true)
	okPacket.SetSessionStateChanged(true)
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComSetOption(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComSetOptionPacket) (err error) {
	// 设置选项
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	okPacket.SetAutoCommit(true)
	okPacket.SetSessionStateChanged(true)
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComStmtExecute(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComStmtExecutePacket) (err error) {
	// 执行语句
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	okPacket.SetAutoCommit(true)
	okPacket.SetSessionStateChanged(true)
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComStmtClose(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComStmtClosePacket) (err error) {
	// 关闭语句
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComStmtSendLongData(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComStmtSendLongDataPacket) (err error) {
	// 发送长数据
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComStmtReset(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComStmtResetPacket) (err error) {
	// 重置语句
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	packBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComQuery(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComQueryPacket) (err error) {
	// 打印查询语句
	fmt.Printf("query: %s\n", commadPack.Query)

	// 先手动处理 select @@version_comment limit 1
	if commadPack.Query == "select @@version_comment limit 1" {
		sendBuffer := bytes.NewBuffer(nil)
		// 构造结果
		var packNum uint8 = 1
	//  column count 包
	columnCountPacket := &protocol.ColumnCountPacket{}
	columnCountPacket.ColumnCount = 1
	columnCountPacket.Packet.SequenceID = packNum
	packBytes, err := columnCountPacket.MarshalDefault()
		if err != nil {
			return err
		}
		_, err = io.Copy(sendBuffer, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}
		packNum++

	//  field 包
	fieldPacket := &protocol.FieldMetaPacket{}
	fieldPacket.Packet.SequenceID = packNum
	fieldPacket.Catalog = "def"
	fieldPacket.Name = "@@version_comment"
	fieldPacket.CharacterSet = 28
	fieldPacket.ColumnLength = 62
	fieldPacket.Decimals = 39
	fieldPacket.Type = 253
	fieldPacket.Flags = 0
	packBytes, err = fieldPacket.MarshalDefault()
		if err != nil {
			return err
		}
		_, err = io.Copy(sendBuffer, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}
		packNum++

		// intermediate eof 包
		intermediateEofPacket := &protocol.EofPacket{}
		intermediateEofPacket.Packet.SequenceID = packNum
		intermediateEofPacket.EofInPacket.Header = protocol.EOF_MARKER
		intermediateEofPacket.EofInPacket.SetAutoCommit(true)
		packBytes, err = intermediateEofPacket.Marshal()
		if err != nil {
			return err
		}
		_, err = io.Copy(sendBuffer, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}
		packNum++

		// row 包
		rowDataPacket := &protocol.RowDataPacket{}
		rowDataPacket.Packet.SequenceID = packNum
		rowDataPacket.RowData = []string{"mariadb.org binary distribution"}
		packBytes, err = rowDataPacket.Marshal()
		if err != nil {
			return err
		}
		_, err = io.Copy(sendBuffer, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}
		packNum++

		// response eof 包
		finalEofPacket := &protocol.EofPacket{}
		finalEofPacket.Packet.SequenceID = packNum
		finalEofPacket.EofInPacket.Header = protocol.EOF_MARKER
		finalEofPacket.EofInPacket.SetAutoCommit(true)
		packBytes, err = finalEofPacket.Marshal()
		if err != nil {
			return err
		}
		_, err = io.Copy(sendBuffer, bytes.NewReader(packBytes))
		if err != nil {
			return err
		}

		_, err = io.Copy(conn, sendBuffer)
		if err != nil {
			return err
		}
		return nil
	}

	// 执行查询
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	packBytes, err := okPacket.Marshal()

	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func (s *Server) handComStmtPrepare(ctx context.Context, sess *session.Session, conn net.Conn, commadPack *protocol.ComStmtPreparePacket) (err error) {
	// 准备语句
	// TODO 先跳过，后面再实现
	// 回复 ok
	okPacket := &protocol.OkPacket{}
	packBytes, err := okPacket.Marshal()

	if err != nil {
		return err
	}
	_, err = io.Copy(conn, bytes.NewReader(packBytes))
	return
}

func RandomString(n int) string {
	letters := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-/!@#$%^&*()_~`"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
