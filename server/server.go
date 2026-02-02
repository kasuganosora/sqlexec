package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/server/acl"
	"github.com/kasuganosora/sqlexec/server/handler"
	simpleHandlers "github.com/kasuganosora/sqlexec/server/handler/simple"
	queryHandlers "github.com/kasuganosora/sqlexec/server/handler/query"
	processHandlers "github.com/kasuganosora/sqlexec/server/handler/process"
	"github.com/kasuganosora/sqlexec/server/protocol"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
)

type Server struct {
	ctx            context.Context
	listener       net.Listener
	sessionMgr     *pkg_session.SessionMgr
	config         *config.Config
	db             *api.DB
	aclManager     *acl.ACLManager
	handlerRegistry *handler.HandlerRegistry
	logger         Logger
}

type Logger interface {
	Printf(format string, v ...interface{})
}

type serverLogger struct {
	logger *log.Logger
}

func (l *serverLogger) Printf(format string, v ...interface{}) {
	l.logger.Printf(format, v...)
}

func NewServer(ctx context.Context, listener net.Listener, cfg *config.Config) *Server {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// 初始化 API DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	if err != nil {
		log.Printf("初始化 API DB 失败: %v", err)
	}

	// 创建并注册 MVCC 数据源
	memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	})

	if db != nil {
		// 连接数据源
		if err := memoryDS.Connect(ctx); err != nil {
			log.Printf("连接内存数据源失败: %v", err)
		}

		// 注册数据源到 API DB
		if err := db.RegisterDataSource("default", memoryDS); err != nil {
			log.Printf("注册数据源失败: %v", err)
		} else {
			log.Printf("已注册数据源: default")
		}
	}

	// 初始化 ACL Manager
	// 使用服务器启动目录作为数据目录
	dataDir := "."
	aclManager, err := acl.NewACLManager(dataDir)
	if err != nil {
		log.Printf("初始化 ACL Manager 失败: %v", err)
		// 继续使用未初始化的 ACL（无权限控制）
		aclManager = nil
	}

	// 注册进程列表提供者（用于 SHOW PROCESSLIST）
	optimizer.RegisterProcessListProvider(pkg_session.GetProcessListForOptimizer)

	s := &Server{
		listener:       listener,
		ctx:            ctx,
		sessionMgr:     pkg_session.NewSessionMgr(ctx, pkg_session.NewMemoryDriver()),
		config:         cfg,
		db:             db,
		aclManager:     aclManager,
		handlerRegistry: handler.NewHandlerRegistry(&serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)}),
		logger:         &serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)},
	}

	// 注册所有处理器
	s.registerHandlers()

	return s
}

// registerHandlers 注册所有命令处理器
func (s *Server) registerHandlers() {
	// 注册简单处理器
	s.handlerRegistry.Register(simpleHandlers.NewPingHandler(nil))
	s.handlerRegistry.Register(simpleHandlers.NewQuitHandler())
	s.handlerRegistry.Register(simpleHandlers.NewSetOptionHandler(nil))
	s.handlerRegistry.Register(simpleHandlers.NewRefreshHandler(nil))
	s.handlerRegistry.Register(simpleHandlers.NewStatisticsHandler())
	s.handlerRegistry.Register(simpleHandlers.NewDebugHandler())
	s.handlerRegistry.Register(simpleHandlers.NewShutdownHandler())

	// 注册查询处理器
	s.handlerRegistry.Register(queryHandlers.NewQueryHandler())
	s.handlerRegistry.Register(queryHandlers.NewInitDBHandler(nil))
	s.handlerRegistry.Register(queryHandlers.NewFieldListHandler(nil))

	// 注册进程控制处理器
	s.handlerRegistry.Register(processHandlers.NewProcessKillHandler(nil))

	if s.logger != nil {
		s.logger.Printf("已注册 %d 个命令处理器", s.handlerRegistry.Count())
	}
}

// SetDB 设置服务器的 DB 实例（用于测试）
func (s *Server) SetDB(db *api.DB) {
	s.db = db
}

func (s *Server) Start() (err error) {
	acceptChan := make(chan net.Conn)
	errChan := make(chan error, 1)

	// 启动监听协程
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				errChan <- err
				return
			}
			acceptChan <- conn
		}
	}()

	// 主循环
	for {
		select {
		case conn := <-acceptChan:
			go s.handleConnection(conn)
		case err := <-errChan:
			return err
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) (err error) {
	defer conn.Close()

	// 调试：检查 server 的关键字段是否为 nil
	if s == nil {
		log.Printf("[严重错误] server 为 nil!")
		return fmt.Errorf("server is nil")
	}
	if s.sessionMgr == nil {
		s.logger.Printf("[严重错误] sessionMgr 为 nil!")
		return fmt.Errorf("sessionMgr is nil")
	}
	if s.logger == nil {
		log.Printf("[警告] logger 为 nil，使用默认日志")
	}

	remoteAddr := conn.RemoteAddr().String()
	addr, port := parseRemoteAddr(remoteAddr)

	s.logger.Printf("开始获取或创建会话: remoteAddr=%s, addr=%s, port=%s", remoteAddr, addr, port)
	sess, err := s.sessionMgr.GetOrCreateSession(s.ctx, addr, port)
	if err != nil {
		s.logger.Printf("获取或创建会话失败: %v", err)
		return err
	}
	if sess == nil {
		s.logger.Printf("会话为 nil，无法继续处理")
		return fmt.Errorf("session is nil")
	}

	// 调试：打印 sess 指针和字段信息
	s.logger.Printf("调试: sess 指针 = %p, &sess.ID = %p, &sess.ThreadID = %p", sess, &sess.ID, &sess.ThreadID)

	s.logger.Printf("新连接来自: %s:%s, SessionID: %s, ThreadID: %d", addr, port, sess.ID, sess.ThreadID)

	// 创建 API Session 并关联到协议 Session
	if s.db != nil && sess.GetAPISession() == nil {
		apiSess := s.db.Session()
		apiSess.SetThreadID(sess.ThreadID) // 设置 threadID 用于 KILL 查询
		sess.SetAPISession(apiSess)
		s.logger.Printf("已为连接创建 API Session, ThreadID=%d", sess.ThreadID)
	}

	if len(sess.User) == 0 {
		err = s.handleHandshake(conn, sess)
		if err != nil {
			return err
		}
	}

	// 命令处理循环
	for {
		packet := &protocol.Packet{}
		if err := packet.Unmarshal(conn); err != nil {
			if err != io.EOF {
				s.logger.Printf("读取包失败: %v", err)
			}
			return err
		}

		commandType := packet.GetCommandType()
		s.logger.Printf("收到命令: 0x%02x", commandType)

		// 解析命令包
		commandPack, err := parseCommandPacket(commandType, packet)
		if err != nil {
			s.logger.Printf("解析命令包失败: %v", err)
			return err
		}

		// 使用注册中心处理命令
		handlerCtx := handler.NewHandlerContext(sess, conn, commandType, s.logger)
		err = s.handlerRegistry.Handle(handlerCtx, commandType, commandPack)
		if err != nil {
			s.logger.Printf("处理命令失败: %v", err)
			return err
		}

		// QUIT 命令不需要发送响应，直接退出循环
		if commandType == protocol.COM_QUIT {
			return nil
		}
	}
}

func (s *Server) handleHandshake(conn net.Conn, sess *pkg_session.Session) error {
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
	s.logger.Printf("已发送握手包, ThreadID: %d", handshakePacket.ThreadID)

	// 计算完整的能力标志 (32位)
	serverCapabilities := (uint32(handshakePacket.CapabilityFlags2) << 16) | uint32(handshakePacket.CapabilityFlags1)

	// 读取握手响应
	handshakeResponse := &protocol.HandshakeResponse{}
	if err := handshakeResponse.Unmarshal(conn, serverCapabilities); err != nil {
		s.logger.Printf("解析认证包失败: %v", err)
		return err
	}

	s.logger.Printf("收到认证包: User=%s, Database=%s, CharacterSet=%d",
		handshakeResponse.User, handshakeResponse.Database, handshakeResponse.CharacterSet)

	// 更新 session 信息
	sess.SetUser(handshakeResponse.User)
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
	s.logger.Printf("已发送认证成功包")

	return nil
}

func parseRemoteAddr(remoteAddr string) (string, string) {
	// 简单解析，格式为 "ip:port"
	parts := make([]byte, 0)
	for i := 0; i < len(remoteAddr); i++ {
		if remoteAddr[i] == ':' {
			return string(parts), remoteAddr[i+1:]
		}
		parts = append(parts, remoteAddr[i])
	}
	return string(parts), ""
}

func parseCommandPacket(commandType uint8, packet *protocol.Packet) (interface{}, error) {
	switch commandType {
	case protocol.COM_PING:
		cmd := &protocol.ComPingPacket{}
		cmd.Packet = *packet
		return cmd, nil
	case protocol.COM_QUIT:
		cmd := &protocol.ComQuitPacket{}
		cmd.Packet = *packet
		return cmd, nil
	case protocol.COM_SET_OPTION:
		cmd := &protocol.ComSetOptionPacket{}
		cmd.Packet = *packet
		return cmd, nil
	case protocol.COM_QUERY:
		cmd := &protocol.ComQueryPacket{}
		cmd.Packet = *packet
		// ComQueryPacket.Unmarshal 会自动从 Payload 中提取 Query 字段
		// 因为 cmd.Packet 已经被赋值,所以不需要再次 Unmarshal
		// Query 字段会在访问时自动提取
		return cmd, nil
	case protocol.COM_INIT_DB:
		cmd := &protocol.ComInitDBPacket{}
		cmd.Packet = *packet
		return cmd, nil
	case protocol.COM_FIELD_LIST:
		cmd := &protocol.ComFieldListPacket{}
		cmd.Packet = *packet
		return cmd, nil
	case protocol.COM_PROCESS_KILL:
		cmd := &protocol.ComProcessKillPacket{}
		cmd.Packet = *packet
		return cmd, nil
	default:
		return nil, fmt.Errorf("unsupported command type: 0x%02x", commandType)
	}
}
