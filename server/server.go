package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/plugin"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	httpds "github.com/kasuganosora/sqlexec/server/datasource/http"
	mysqlds "github.com/kasuganosora/sqlexec/server/datasource/mysql"
	pgds "github.com/kasuganosora/sqlexec/server/datasource/postgresql"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/server/acl"
	"github.com/kasuganosora/sqlexec/server/handler"
	simpleHandlers "github.com/kasuganosora/sqlexec/server/handler/simple"
	queryHandlers "github.com/kasuganosora/sqlexec/server/handler/query"
	processHandlers "github.com/kasuganosora/sqlexec/server/handler/process"
	handshakeHandler "github.com/kasuganosora/sqlexec/server/handler/handshake"
	parsers "github.com/kasuganosora/sqlexec/server/handler/packet_parsers"
	"github.com/kasuganosora/sqlexec/server/protocol"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
	isacl "github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

type Server struct {
	ctx               context.Context
	listener          net.Listener
	sessionMgr        *pkg_session.SessionMgr
	config            *config.Config
	db                *api.DB
	aclManager        *acl.ACLManager
	handlerRegistry   *handler.HandlerRegistry
	parserRegistry    *handler.PacketParserRegistry
	handshakeHandler  handler.HandshakeHandler
	logger            Logger
	auditLogger       handler.AuditLogger
	configDir         string // 配置目录（用于 config 虚拟数据库）
	vdbRegistry       *virtual.VirtualDatabaseRegistry // 虚拟数据库注册表
	debugEnabled      bool // Debug logging switch (from config, default true)
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
		DatabaseDir:  cfg.Database.DatabaseDir,
	})
	if err != nil {
		log.Fatalf("初始化 API DB 失败: %v", err)
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

	// 加载 datasources.json 中配置的数据源
	configDir := dataDir
	dsManager := db.GetDSManager()

	// 注册数据源工厂
	dsManager.GetRegistry().Register(memory.NewMemoryFactory())
	dsManager.GetRegistry().Register(httpds.NewHTTPFactory())
	dsManager.GetRegistry().Register(mysqlds.NewMySQLFactory())
	dsManager.GetRegistry().Register(pgds.NewPostgreSQLFactory())
	dsConfigs, err := config_schema.LoadDatasources(configDir)
	if err != nil {
		log.Printf("加载 datasources.json 失败: %v", err)
	} else if len(dsConfigs) > 0 {
		for _, dsCfg := range dsConfigs {
			dsCfgCopy := dsCfg
			ds, createErr := dsManager.CreateFromConfig(&dsCfgCopy)
			if createErr != nil {
				// Fallback to memory datasource for "memory" type
				if dsCfg.Type == domain.DataSourceTypeMemory {
					ds = memory.NewMVCCDataSource(&dsCfgCopy)
				} else {
					log.Printf("创建数据源 '%s' 失败: %v", dsCfg.Name, createErr)
					continue
				}
			}
			if connectErr := ds.Connect(ctx); connectErr != nil {
				log.Printf("连接数据源 '%s' 失败: %v", dsCfg.Name, connectErr)
				continue
			}
			if regErr := dsManager.Register(dsCfg.Name, ds); regErr != nil {
				log.Printf("注册数据源 '%s' 失败: %v", dsCfg.Name, regErr)
				continue
			}
			log.Printf("已从配置加载数据源: %s (type=%s)", dsCfg.Name, dsCfg.Type)
		}
	}

	// 加载 datasource/ 目录下的插件
	pluginDir := filepath.Join(dataDir, "datasource")
	registry := dsManager.GetRegistry()
	pluginMgr := plugin.NewPluginManager(registry, dsManager, configDir)
	if err := pluginMgr.ScanAndLoad(pluginDir); err != nil {
		log.Printf("加载插件失败: %v", err)
	}

	// 创建虚拟数据库注册表并注册 config 虚拟数据库
	vdbRegistry := virtual.NewVirtualDatabaseRegistry()
	configProvider := config_schema.NewProvider(dsManager, configDir)
	vdbRegistry.Register(&virtual.VirtualDatabaseEntry{
		Name:     "config",
		Provider: configProvider,
		Writable: true,
	})
	log.Printf("已注册虚拟数据库: config")

	// 注册进程列表提供者（用于 SHOW PROCESSLIST）
	optimizer.RegisterProcessListProvider(pkg_session.GetProcessListForOptimizer)

	s := &Server{
		listener:         listener,
		ctx:              ctx,
		sessionMgr:       pkg_session.NewSessionMgr(ctx, pkg_session.NewMemoryDriver()),
		config:           cfg,
		db:               db,
		aclManager:       aclManager,
		handlerRegistry:  handler.NewHandlerRegistry(&serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)}),
		parserRegistry:   handler.NewPacketParserRegistry(&serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)}),
		handshakeHandler: handshakeHandler.NewDefaultHandshakeHandler(db, &serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)}),
		logger:           &serverLogger{logger: log.New(os.Stdout, "[SERVER] ", log.LstdFlags)},
		configDir:        configDir,
		vdbRegistry:      vdbRegistry,
		debugEnabled:     cfg.Server.IsDebugEnabled(),
	}

	// 注册所有处理器
	s.registerHandlers()

	// 注册所有包解析器
	s.registerParsers()

	// 注册全局ACL Manager到information_schema
	if s.aclManager != nil {
		isacl.RegisterACLManager(s.aclManager)
		s.logger.Printf("已注册 ACL Manager 到 information_schema")
	}

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

// registerParsers 注册所有包解析器
func (s *Server) registerParsers() {
	// 注册所有命令包解析器
	s.parserRegistry.Register(parsers.NewPingPacketParser())
	s.parserRegistry.Register(parsers.NewQuitPacketParser())
	s.parserRegistry.Register(parsers.NewSetOptionPacketParser())
	s.parserRegistry.Register(parsers.NewQueryPacketParser())
	s.parserRegistry.Register(parsers.NewInitDBPacketParser())
	s.parserRegistry.Register(parsers.NewFieldListPacketParser())
	s.parserRegistry.Register(parsers.NewProcessKillPacketParser())

	if s.logger != nil {
		s.logger.Printf("已注册 %d 个包解析器", s.parserRegistry.Count())
	}
}

// SetDB 设置服务器的 DB 实例（用于测试）
func (s *Server) SetDB(db *api.DB) {
	s.db = db
}

// SetAuditLogger 设置审计日志记录器
func (s *Server) SetAuditLogger(al handler.AuditLogger) {
	s.auditLogger = al
}

// GetDB 返回服务器的 DB 实例
func (s *Server) GetDB() *api.DB {
	return s.db
}

// GetConfigDir 返回配置目录路径
func (s *Server) GetConfigDir() string {
	return s.configDir
}

// GetVirtualDBRegistry 返回虚拟数据库注册表
func (s *Server) GetVirtualDBRegistry() *virtual.VirtualDatabaseRegistry {
	return s.vdbRegistry
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
	addr, port := utils.ParseRemoteAddr(remoteAddr)

	s.logger.Printf("开始获取或创建会话: remoteAddr=%s, addr=%s, port=%s", remoteAddr, addr, port)
	sess, err := s.sessionMgr.GetOrCreateSession(s.ctx, addr, port)

	// 确保连接断开时清理 session 和 ThreadID
	if sess != nil {
		defer func() {
			s.sessionMgr.CleanupSession(s.ctx, sess)
			s.logger.Printf("已清理会话: SessionID=%s, ThreadID=%d", sess.ID, sess.ThreadID)
		}()
	}
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
		apiSess.SetTraceID(sess.TraceID)   // 传播 trace-id 用于请求追踪
		apiSess.SetVirtualDBRegistry(s.vdbRegistry)  // 设置虚拟数据库注册表
		sess.SetAPISession(apiSess)
		s.logger.Printf("已为连接创建 API Session, ThreadID=%d", sess.ThreadID)
	}

	if len(sess.User) == 0 {
		// 使用注册的握手处理器处理握手
		err = s.handshakeHandler.Handle(conn, sess)
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

		// 使用注册的解析器解析命令包
		commandPack, err := s.parserRegistry.Parse(commandType, packet)
		if err != nil {
			s.logger.Printf("解析命令包失败: %v", err)
			return err
		}

		// 使用注册中心处理命令
		handlerCtx := handler.NewHandlerContext(sess, conn, commandType, s.logger, s.auditLogger)
		handlerCtx.DebugEnabled = s.debugEnabled
		err = s.handlerRegistry.Handle(handlerCtx, commandType, commandPack)
		if err != nil {
			s.logger.Printf("处理命令失败: %v", err)
			// Per MySQL protocol, a single query failure should not terminate
			// the connection. The handler should have already sent an error packet.
			continue
		}

		// QUIT 命令不需要发送响应，直接退出循环
		if commandType == protocol.COM_QUIT {
			return nil
		}
	}
}
