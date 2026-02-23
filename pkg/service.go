package pkg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/monitor"
	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/pool"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/protocol"
)

// 定义 context key
type contextKey string

const (
	// 连接状态相关的 key
	keyHandshakeDone contextKey = "handshake_done"
	keySession       contextKey = "session"
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

// 设置会话到 context
func withSession(ctx context.Context, sess *session.Session) context.Context {
	return context.WithValue(ctx, keySession, sess)
}

// 从 context 获取会话
func getSession(ctx context.Context) *session.Session {
	sess, _ := ctx.Value(keySession).(*session.Session)
	return sess
}

// Server MySQL 服务器
type Server struct {
	sessionMgr        *session.SessionMgr
	dataSourceMgr     *application.DataSourceManager
	defaultDataSource domain.DataSource
	mu                sync.RWMutex
	parser            *parser.Parser
	handler           *parser.HandlerChain
	optimizedExecutor *optimizer.OptimizedExecutor
	useOptimizer      bool

	// 池系统
	goroutinePool *pool.GoroutinePool
	objectPool    *pool.ObjectPool

	// 监控系统
	metricsCollector *monitor.MetricsCollector
	cacheManager     *monitor.CacheManager
}

// NewServer 创建新的服务器实例
func NewServer(cfg *config.Config) *Server {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// 初始化会话配置
	session.InitSessionConfig(&cfg.Session)

	// 创建解析器
	p := parser.NewParser()

	// 创建处理器链
	chain := parser.NewHandlerChain()
	chain.RegisterHandler("SELECT", parser.NewQueryHandler())
	chain.RegisterHandler("INSERT", parser.NewDMLHandler())
	chain.RegisterHandler("UPDATE", parser.NewDMLHandler())
	chain.RegisterHandler("DELETE", parser.NewDMLHandler())
	chain.RegisterHandler("REPLACE", parser.NewDMLHandler())
	chain.RegisterHandler("CREATE_TABLE", parser.NewDDLHandler())
	chain.RegisterHandler("DROP_TABLE", parser.NewDDLHandler())
	chain.RegisterHandler("CREATE_DATABASE", parser.NewDDLHandler())
	chain.RegisterHandler("DROP_DATABASE", parser.NewDDLHandler())
	chain.RegisterHandler("ALTER_TABLE", parser.NewDDLHandler())
	chain.RegisterHandler("TRUNCATE_TABLE", parser.NewDDLHandler())
	chain.RegisterHandler("SET", parser.NewSetHandler())
	chain.RegisterHandler("SHOW", parser.NewShowHandler())
	chain.RegisterHandler("USE", parser.NewUseHandler())
	chain.SetDefaultHandler(parser.NewDefaultHandler())

	// 创建池系统
	goroutinePool := pool.NewGoroutinePool(cfg.Pool.GoroutinePool.MaxWorkers, cfg.Pool.GoroutinePool.QueueSize)

	objectPool := pool.NewObjectPool(
		func() (interface{}, error) {
			return make(map[string]interface{}), nil
		},
		func(obj interface{}) error {
			return nil
		},
		cfg.Pool.ObjectPool.MaxSize,
	)

	// 创建监控系统
	metricsCollector := monitor.NewMetricsCollector()
	cacheManager := monitor.NewCacheManager(
		cfg.Cache.QueryCache.MaxSize,
		cfg.Cache.ResultCache.MaxSize,
		cfg.Cache.SchemaCache.MaxSize,
	)

	server := &Server{
		sessionMgr:       session.NewSessionMgr(context.Background(), session.NewMemoryDriver()),
		dataSourceMgr:    application.GetDefaultManager(),
		parser:           p,
		handler:          chain,
		useOptimizer:     cfg.Optimizer.Enabled,
		goroutinePool:    goroutinePool,
		objectPool:       objectPool,
		metricsCollector: metricsCollector,
		cacheManager:     cacheManager,
	}

	return server
}

// SetDataSource 设置默认数据源
func (s *Server) SetDataSource(ds domain.DataSource) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !ds.IsConnected() {
		if err := ds.Connect(context.Background()); err != nil {
			return fmt.Errorf("failed to connect data source: %w", err)
		}
	}

	s.defaultDataSource = ds

	// 初始化优化执行器
	s.optimizedExecutor = optimizer.NewOptimizedExecutor(ds, s.useOptimizer)

	return nil
}

// GetDataSource 获取默认数据源
func (s *Server) GetDataSource() domain.DataSource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.defaultDataSource
}

// SetUseOptimizer 设置是否使用优化器
func (s *Server) SetUseOptimizer(use bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.useOptimizer = use
	if s.optimizedExecutor != nil {
		s.optimizedExecutor.SetUseOptimizer(use)
	}
}

// GetUseOptimizer 获取是否使用优化器
func (s *Server) GetUseOptimizer() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.useOptimizer
}

// SetDataSourceManager 设置数据源管理器
func (s *Server) SetDataSourceManager(mgr *application.DataSourceManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dataSourceMgr = mgr
}

// GetDataSourceManager 获取数据源管理器
func (s *Server) GetDataSourceManager() *application.DataSourceManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dataSourceMgr
}

// HandleConn 用于处理MYSQL的链接
func (s *Server) HandleConn(ctx context.Context, conn net.Conn) (err error) {
	// 设置连接超时
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(config.DefaultConfig().Server.KeepAlivePeriod)
	}

	// 获取客户端地址
	remoteAddr := conn.RemoteAddr().String()
	addr, port := parseRemoteAddr(remoteAddr)
	log.Printf("新连接来自: %s:%s", addr, port)

	// 获取或创建会话
	sess, err := s.sessionMgr.GetOrCreateSession(ctx, addr, port)
	if err != nil {
		log.Printf("创建会话失败: %v", err)
		return fmt.Errorf("创建会话失败: %w", err)
	}
	sess.ResetSequenceID()
	ctx = withSession(ctx, sess)

	// 检查是否已经完成握手
	if !isHandshakeDone(ctx) {
		// 发送握手包
		handshake := protocol.NewHandshakePacket()
		handshake.ThreadID = sess.ThreadID
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

		// 解析认证响应
		handshakeResponse := &protocol.HandshakeResponse{}
		if err := handshakeResponse.Unmarshal(conn, uint32(handshake.CapabilityFlags1)|uint32(handshake.CapabilityFlags2)<<16); err != nil {
			log.Printf("解析认证响应失败: %v", err)
			return fmt.Errorf("解析认证响应失败: %w", err)
		}
		log.Printf("用户: %s, 数据库: %s, 字符集: %d", handshakeResponse.User, handshakeResponse.Database, handshakeResponse.CharacterSet)

		// 设置用户名
		sess.SetUser(handshakeResponse.User)

		// 发送 OK 包表示认证成功
		log.Printf("发送认证成功包...")
		if err := protocol.SendOK(conn, sess.GetNextSequenceID()); err != nil {
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

		// 更新会话序列号
		sess.SequenceID = packet.SequenceID

		// 解析封包的类型
		packetType := packet.GetCommandType()
		commandName := protocol.GetCommandName(packetType)
		log.Printf("收到命令: %s (0x%02x)", commandName, packetType)

		// 重置会话序列号(新命令开始)
		sess.ResetSequenceID()

		var handleErr error
		switch packetType {
		case protocol.COM_QUIT:
			log.Printf("收到退出命令")
			return nil
		case protocol.COM_QUERY:
			handleErr = s.handleQuery(ctx, conn, packet)
		case protocol.COM_INIT_DB:
			handleErr = s.handleInitDB(ctx, conn, packet)
		case protocol.COM_PING:
			handleErr = s.handlePing(ctx, conn)
		case protocol.COM_STMT_PREPARE:
			handleErr = s.handleStmtPrepare(ctx, conn, packet)
		case protocol.COM_STMT_EXECUTE:
			handleErr = s.handleStmtExecute(ctx, conn, packet)
		case protocol.COM_STMT_CLOSE:
			handleErr = s.handleStmtClose(ctx, conn, packet)
		case protocol.COM_FIELD_LIST:
			handleErr = s.handleFieldList(ctx, conn, packet)
		case protocol.COM_SET_OPTION:
			handleErr = s.handleSetOption(ctx, conn, packet)
		case protocol.COM_REFRESH:
			handleErr = s.handleRefresh(ctx, conn, packet)
		case protocol.COM_STATISTICS:
			handleErr = s.handleStatistics(ctx, conn)
		case protocol.COM_PROCESS_INFO:
			handleErr = s.handleProcessInfo(ctx, conn)
		case protocol.COM_PROCESS_KILL:
			handleErr = s.handleProcessKill(ctx, conn, packet)
		case protocol.COM_DEBUG:
			handleErr = s.handleDebug(ctx, conn)
		case protocol.COM_SHUTDOWN:
			handleErr = s.handleShutdown(ctx, conn)
		default:
			log.Printf("不支持的命令类型: %s (0x%02x)", commandName, packetType)
			protocol.SendError(conn, fmt.Errorf("不支持的命令类型: %d", packetType))
		}

		if handleErr != nil {
			log.Printf("处理命令 %s 失败: %v", commandName, handleErr)
			return handleErr
		}
	}
}

// parseRemoteAddr 解析远程地址
func parseRemoteAddr(addr string) (string, string) {
	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return addr, ""
}

func (s *Server) handleQuery(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	query := string(packet.Payload[1:])
	log.Printf("处理查询: %s", query)

	sess := getSession(ctx)

	// 使用 TiDB parser 解析 SQL 语句
	stmtNode, err := s.parser.ParseOneStmtText(query)
	if err != nil {
		log.Printf("解析 SQL 失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("SQL 解析错误: %w", err))
	}

	// 使用处理器链处理 SQL 语句
	result, err := s.handler.Handle(stmtNode)
	if err != nil {
		log.Printf("处理 SQL 失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("SQL 处理错误: %w", err))
	}

	// 根据结果类型返回不同的响应
	switch r := result.(type) {
	case *parser.QueryResult:
		// SELECT 查询 - 使用QueryBuilder执行
		log.Printf("SELECT 查询结果，涉及表: %v, 列: %v", r.Tables, r.Columns)
		return s.handleSelectQuery(ctx, conn, sess, query)
	case *parser.DMLResult:
		// INSERT/UPDATE/DELETE - 使用QueryBuilder执行
		log.Printf("DML 操作，类型: %s, 涉及表: %v", r.Type, r.Tables)
		return s.handleDMLQuery(ctx, conn, sess, query, r.Type)
	case *parser.DDLResult:
		// CREATE/DROP/ALTER - 使用QueryBuilder执行
		log.Printf("DDL 操作，类型: %s, 涉及表: %v", r.Type, r.Tables)
		return s.handleDDLQuery(ctx, conn, sess, query, r.Type)
	case *parser.SetResult:
		// SET 命令
		log.Printf("设置变量完成: %d 个变量", r.Count)
		// 保存变量到 session
		for name, value := range r.Vars {
			sess.SetVariable(name, value)
		}
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	case *parser.ShowResult:
		// SHOW 命令
		if r.ShowTp == "SHOW_VARIABLES" {
			return s.sendVariablesResultSet(ctx, conn, sess)
		}
		log.Printf("SHOW 命令完成: %s", r.ShowTp)
		return s.sendResultSet(ctx, conn, sess)
	case *parser.UseResult:
		// USE 命令
		sess.Set("current_database", r.Database)
		log.Printf("切换到数据库: %s", r.Database)
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	case *parser.DefaultResult:
		// 默认处理
		log.Printf("默认处理完成: %s", r.Type)
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	default:
		// 未知结果
		log.Printf("未知结果类型: %T", result)
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	}
}

func (s *Server) handleInitDB(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	dbName := string(packet.Payload[1:])
	sess := getSession(ctx)

	log.Printf("切换数据库: %s", dbName)
	sess.Set("current_database", dbName)

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handlePing(ctx context.Context, conn net.Conn) error {
	sess := getSession(ctx)
	log.Printf("处理 PING")
	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleStmtPrepare 处理 COM_STMT_PREPARE 命令
func (s *Server) handleStmtPrepare(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)

	// 解析 COM_STMT_PREPARE 包
	stmtPreparePacket := &protocol.ComStmtPreparePacket{}
	if err := stmtPreparePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_PREPARE 包失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}

	log.Printf("处理 COM_STMT_PREPARE: query='%s'", stmtPreparePacket.Query)

	// 生成语句ID
	stmtID := sess.ThreadID // 简化：使用thread ID

	// 分析SQL语句，提取参数和列信息
	paramCount := countParams(stmtPreparePacket.Query)
	columnCount := analyzeColumns(stmtPreparePacket.Query)

	// 创建 Prepare 响应包
	response := &protocol.StmtPrepareResponsePacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		StatementID:  stmtID,
		ColumnCount:  columnCount,
		ParamCount:   paramCount,
		Reserved:     0,
		WarningCount: 0,
		Params:       make([]protocol.FieldMeta, paramCount),
		Columns:      make([]protocol.FieldMeta, columnCount),
	}

	// 填充参数元数据
	for i := uint16(0); i < paramCount; i++ {
		response.Params[i] = protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "",
			Table:                     "",
			OrgTable:                  "",
			Name:                      "?",
			OrgName:                   "",
			LengthOfFixedLengthFields: 12,
			CharacterSet:              33,
			ColumnLength:              255,
			Type:                      protocol.MYSQL_TYPE_VAR_STRING,
			Flags:                     0,
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		}
	}

	// 填充列元数据
	columnNames := getColumns(stmtPreparePacket.Query)
	for i := uint16(0); i < columnCount && i < uint16(len(columnNames)); i++ {
		response.Columns[i] = protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "test",
			Table:                     "table",
			OrgTable:                  "table",
			Name:                      columnNames[i],
			OrgName:                   columnNames[i],
			LengthOfFixedLengthFields: 12,
			CharacterSet:              33,
			ColumnLength:              255,
			Type:                      protocol.MYSQL_TYPE_VAR_STRING,
			Flags:                     protocol.NOT_NULL_FLAG,
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

	if _, err := conn.Write(data); err != nil {
		log.Printf("发送 COM_STMT_PREPARE 响应失败: %v", err)
		return err
	}

	log.Printf("已发送 COM_STMT_PREPARE 响应: statement_id=%d, params=%d, columns=%d",
		response.StatementID, response.ParamCount, response.ColumnCount)

	// 保存预处理语句到会话
	sess.Set(fmt.Sprintf("stmt_%d", stmtID), stmtPreparePacket.Query)

	return nil
}

// handleStmtExecute 处理 COM_STMT_EXECUTE 命令
func (s *Server) handleStmtExecute(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)

	// 解析 COM_STMT_EXECUTE 包
	stmtExecutePacket := &protocol.ComStmtExecutePacket{}
	if err := stmtExecutePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_EXECUTE 包失败: %v", err)
		protocol.SendError(conn, err)
		return err
	}

	log.Printf("处理 COM_STMT_EXECUTE: statement_id=%d, params=%v",
		stmtExecutePacket.StatementID, stmtExecutePacket.ParamValues)

	// 获取预处理语句的查询
	queryKey := fmt.Sprintf("stmt_%d", stmtExecutePacket.StatementID)
	query, _ := sess.Get(queryKey)
	if query == nil {
		log.Printf("预处理语句不存在: statement_id=%d", stmtExecutePacket.StatementID)
		protocol.SendError(conn, fmt.Errorf("预处理语句不存在"))
		return fmt.Errorf("预处理语句不存在")
	}

	// 分析列
	columnCount := analyzeColumns(query.(string))

	// 发送列数包
	columnCountData := []byte{
		0x01, 0x00, 0x00, // 列数 = 1
		sess.GetNextSequenceID(),
	}
	if _, err := conn.Write(columnCountData); err != nil {
		return err
	}

	// 发送列元数据包
	columnNames := getColumns(query.(string))
	for i := 0; i < int(columnCount) && i < len(columnNames); i++ {
		fieldMeta := protocol.FieldMetaPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			FieldMeta: protocol.FieldMeta{
				Catalog:                   "def",
				Schema:                    "test",
				Table:                     "table",
				OrgTable:                  "table",
				Name:                      columnNames[i],
				OrgName:                   columnNames[i],
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              255,
				Type:                      protocol.MYSQL_TYPE_VAR_STRING,
				Flags:                     protocol.NOT_NULL_FLAG,
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
		if _, err := conn.Write(fieldMetaData); err != nil {
			return err
		}
	}

	// 发送列结束包
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

	// 发送数据行（简化：发送一行示例数据）
	rowData := protocol.RowDataPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		RowData: []string{"1"},
	}
	rowDataBytes, err := rowData.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(rowDataBytes); err != nil {
		return err
	}

	// 发送结束包
	finalEof := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	finalEofData, err := finalEof.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(finalEofData); err != nil {
		return err
	}

	log.Printf("已发送 COM_STMT_EXECUTE 响应: statement_id=%d", stmtExecutePacket.StatementID)
	return nil
}

// handleStmtClose 处理 COM_STMT_CLOSE 命令
func (s *Server) handleStmtClose(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)

	// 解析 COM_STMT_CLOSE 包
	stmtClosePacket := &protocol.ComStmtClosePacket{}
	if err := stmtClosePacket.Unmarshal(bytes.NewReader(packet.RawBytes())); err != nil {
		log.Printf("解析 COM_STMT_CLOSE 包失败: %v", err)
		return err
	}

	log.Printf("处理 COM_STMT_CLOSE: statement_id=%d", stmtClosePacket.StatementID)

	// 释放预处理语句资源
	queryKey := fmt.Sprintf("stmt_%d", stmtClosePacket.StatementID)
	sess.Delete(queryKey)

	// COM_STMT_CLOSE 不需要发送响应
	log.Printf("已关闭预处理语句: statement_id=%d", stmtClosePacket.StatementID)
	return nil
}

// handleFieldList 处理 COM_FIELD_LIST 命令
func (s *Server) handleFieldList(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_FIELD_LIST")

	// 发送结束包
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.Write(eofData)
	return err
}

// handleSetOption 处理 COM_SET_OPTION 命令
func (s *Server) handleSetOption(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_SET_OPTION")

	// 返回OK包
	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleRefresh 处理 COM_REFRESH 命令
func (s *Server) handleRefresh(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_REFRESH")

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleStatistics 处理 COM_STATISTICS 命令
func (s *Server) handleStatistics(ctx context.Context, conn net.Conn) error {
	log.Printf("处理 COM_STATISTICS")

	stats := "Uptime: 3600  Threads: 1  Questions: 10  Slow queries: 0  Opens: 5  Flush tables: 1  Open tables: 4  Queries per second avg: 0.003"

	// 发送统计信息
	if _, err := conn.Write([]byte(stats)); err != nil {
		return err
	}

	return nil
}

// handleProcessInfo 处理 COM_PROCESS_INFO 命令
func (s *Server) handleProcessInfo(ctx context.Context, conn net.Conn) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_PROCESS_INFO")

	// 返回空结果集
	return s.sendResultSet(ctx, conn, sess)
}

// handleProcessKill 处理 COM_PROCESS_KILL 命令
func (s *Server) handleProcessKill(ctx context.Context, conn net.Conn, packet *protocol.Packet) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_PROCESS_KILL")

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleDebug 处理 COM_DEBUG 命令
func (s *Server) handleDebug(ctx context.Context, conn net.Conn) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_DEBUG")

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleShutdown 处理 COM_SHUTDOWN 命令
func (s *Server) handleShutdown(ctx context.Context, conn net.Conn) error {
	sess := getSession(ctx)
	log.Printf("处理 COM_SHUTDOWN")

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// sendResultSet 发送结果集
func (s *Server) sendResultSet(ctx context.Context, conn net.Conn, sess *session.Session) error {
	// 发送列数
	columnCountPacket := &protocol.ColumnCountPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		ColumnCount: 1,
	}
	columnCountData, err := columnCountPacket.MarshalDefault()
	if err != nil {
		return err
	}
	if _, err := conn.Write(columnCountData); err != nil {
		return err
	}

	// 发送列定义
	fieldMeta := protocol.FieldMetaPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		FieldMeta: protocol.FieldMeta{
			Catalog:                   "def",
			Schema:                    "test",
			Table:                     "test_table",
			OrgTable:                  "test_table",
			Name:                      "id",
			OrgName:                   "id",
			LengthOfFixedLengthFields: 12,
			CharacterSet:              33,
			ColumnLength:              11,
			Type:                      protocol.MYSQL_TYPE_LONG,
			Flags:                     protocol.NOT_NULL_FLAG | protocol.PRI_KEY_FLAG,
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		},
	}
	fieldMetaData, err := fieldMeta.MarshalDefault()
	if err != nil {
		return err
	}
	if _, err := conn.Write(fieldMetaData); err != nil {
		return err
	}

	// 发送列结束包
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

	// 发送数据行
	rowData := protocol.RowDataPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		RowData: []string{"1"},
	}
	rowDataBytes, err := rowData.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(rowDataBytes); err != nil {
		return err
	}

	// 发送结果集结束包
	finalEof := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	finalEofData, err := finalEof.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(finalEofData); err != nil {
		return err
	}

	return nil
}

// sendVariablesResultSet 发送 SHOW VARIABLES 结果集
func (s *Server) sendVariablesResultSet(ctx context.Context, conn net.Conn, sess *session.Session) error {
	log.Printf("发送 SHOW VARIABLES 结果集")

	// 获取所有会话变量
	userVariables, err := sess.GetAllVariables()
	if err != nil {
		log.Printf("获取会话变量失败: %v", err)
	}

	// 默认的系统变量
	variables := map[string]string{
		"version":            "8.0.0",
		"version_comment":    "MySQL Proxy",
		"port":               "3306",
		"socket":             "/tmp/mysql.sock",
		"datadir":            "/var/lib/mysql/",
		"basedir":            "/usr/",
		"tmpdir":             "/tmp",
		"slave_skip_errors":  "OFF",
		"autocommit":         "ON",
		"max_allowed_packet": "67108864",
	}

	// 合并用户设置的变量
	for name, value := range userVariables {
		variables[name] = fmt.Sprintf("%v", value)
	}

	// 发送列数（两列：Variable_name, Value）
	columnCountPacket := &protocol.ColumnCountPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		ColumnCount: 2,
	}
	columnCountData, err := columnCountPacket.MarshalDefault()
	if err != nil {
		return err
	}
	if _, err := conn.Write(columnCountData); err != nil {
		return err
	}

	// 发送列定义
	columns := []struct {
		name string
		meta protocol.FieldMeta
	}{
		{
			name: "Variable_name",
			meta: protocol.FieldMeta{
				Catalog:                   "def",
				Schema:                    "information_schema",
				Table:                     "SESSION_VARIABLES",
				OrgTable:                  "SESSION_VARIABLES",
				Name:                      "VARIABLE_NAME",
				OrgName:                   "VARIABLE_NAME",
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              64,
				Type:                      protocol.MYSQL_TYPE_VAR_STRING,
				Flags:                     protocol.NOT_NULL_FLAG,
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			},
		},
		{
			name: "Value",
			meta: protocol.FieldMeta{
				Catalog:                   "def",
				Schema:                    "information_schema",
				Table:                     "SESSION_VARIABLES",
				OrgTable:                  "SESSION_VARIABLES",
				Name:                      "VARIABLE_VALUE",
				OrgName:                   "VARIABLE_VALUE",
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              1024,
				Type:                      protocol.MYSQL_TYPE_VAR_STRING,
				Flags:                     0,
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			},
		},
	}

	for _, col := range columns {
		fieldMeta := protocol.FieldMetaPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			FieldMeta: col.meta,
		}
		fieldMetaData, err := fieldMeta.MarshalDefault()
		if err != nil {
			return err
		}
		if _, err := conn.Write(fieldMetaData); err != nil {
			return err
		}
	}

	// 发送列结束包
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

	// 发送数据行
	for name, value := range variables {
		rowData := protocol.RowDataPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			RowData: []string{name, value},
		}
		rowDataBytes, err := rowData.Marshal()
		if err != nil {
			return err
		}
		if _, err := conn.Write(rowDataBytes); err != nil {
			return err
		}
	}

	// 发送结果集结束包
	finalEof := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	finalEofData, err := finalEof.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(finalEofData); err != nil {
		return err
	}

	return nil
}

// handleSelectQuery 处理SELECT查询 - 使用OptimizedExecutor执行查询
func (s *Server) handleSelectQuery(ctx context.Context, conn net.Conn, sess *session.Session, query string) error {
	// 开始监控查询
	startTime := time.Now()
	s.metricsCollector.StartQuery()
	defer s.metricsCollector.EndQuery()

	// 检查缓存（使用查询缓存）
	queryCache := s.cacheManager.GetQueryCache()
	if cachedResult, found := queryCache.Get(query); found {
		log.Printf("查询命中缓存: %s", query)
		result := cachedResult.(*domain.QueryResult)

		// 记录缓存命中
		s.metricsCollector.RecordQuery(time.Since(startTime), true, "")
		return s.sendQueryResult(ctx, conn, sess, result)
	}

	// 获取数据源
	ds := s.GetDataSource()
	if ds == nil {
		log.Printf("未设置数据源，返回默认结果")
		return s.sendResultSet(ctx, conn, sess)
	}

	// 解析SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(query)
	if err != nil {
		log.Printf("解析SQL失败: %v", err)
		s.metricsCollector.RecordError("SQL_PARSE_ERROR")
		return protocol.SendError(conn, fmt.Errorf("SQL解析错误: %w", err))
	}

	if !parseResult.Success {
		log.Printf("解析SQL失败: %s", parseResult.Error)
		s.metricsCollector.RecordError("SQL_PARSE_ERROR")
		return protocol.SendError(conn, fmt.Errorf("SQL解析失败: %s", parseResult.Error))
	}

	var result *domain.QueryResult

	// 如果启用了优化器，使用 OptimizedExecutor
	if s.useOptimizer {
		s.mu.Lock()
		if s.optimizedExecutor == nil {
			s.optimizedExecutor = optimizer.NewOptimizedExecutor(ds, true)
		}
		executor := s.optimizedExecutor
		s.mu.Unlock()

		// 使用goroutine池执行查询
		errCh := make(chan error, 1)
		resultCh := make(chan *domain.QueryResult, 1)

		err = s.goroutinePool.Submit(func() {
			r, e := executor.ExecuteSelect(ctx, parseResult.Statement.Select)
			resultCh <- r
			errCh <- e
		})

		if err != nil {
			log.Printf("提交查询任务失败: %v", err)
			s.metricsCollector.RecordError("POOL_SUBMIT_ERROR")
			// 降级到传统路径
			builder := parser.NewQueryBuilder(ds)
			result, err = builder.BuildAndExecute(ctx, query)
			if err != nil {
				log.Printf("执行查询失败: %v", err)
				s.metricsCollector.RecordError("QUERY_EXECUTION_ERROR")
				return protocol.SendError(conn, fmt.Errorf("查询执行错误: %w", err))
			}
		} else {
			// 等待查询完成
			result = <-resultCh
			err = <-errCh
			if err != nil {
				log.Printf("优化执行查询失败: %v", err)
				s.metricsCollector.RecordError("OPTIMIZER_ERROR")
				// 降级到传统路径
				builder := parser.NewQueryBuilder(ds)
				result, err = builder.BuildAndExecute(ctx, query)
				if err != nil {
					log.Printf("执行查询失败: %v", err)
					s.metricsCollector.RecordError("QUERY_EXECUTION_ERROR")
					return protocol.SendError(conn, fmt.Errorf("查询执行错误: %w", err))
				}
			}
		}
	} else {
		// 使用传统的 QueryBuilder 路径
		builder := parser.NewQueryBuilder(ds)
		result, err = builder.BuildAndExecute(ctx, query)
		if err != nil {
			log.Printf("执行查询失败: %v", err)
			s.metricsCollector.RecordError("QUERY_EXECUTION_ERROR")
			return protocol.SendError(conn, fmt.Errorf("查询执行错误: %w", err))
		}
	}

	// 缓存查询结果（仅缓存成功的结果）
	if result != nil && len(result.Rows) > 0 {
		queryCache.Set(query, result, 5*time.Minute) // 缓存5分钟
	}

	// 记录成功查询
	s.metricsCollector.RecordQuery(time.Since(startTime), true, "")

	// 发送查询结果
	return s.sendQueryResult(ctx, conn, sess, result)
}

// handleDMLQuery 处理DML查询 - 使用OptimizedExecutor执行
func (s *Server) handleDMLQuery(ctx context.Context, conn net.Conn, sess *session.Session, query string, stmtType string) error {
	// 获取数据源
	ds := s.GetDataSource()
	if ds == nil {
		log.Printf("未设置数据源，返回OK")
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	}

	// 解析SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(query)
	if err != nil {
		log.Printf("解析SQL失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("SQL解析错误: %w", err))
	}

	if !parseResult.Success {
		log.Printf("解析SQL失败: %s", parseResult.Error)
		return protocol.SendError(conn, fmt.Errorf("SQL解析失败: %s", parseResult.Error))
	}

	// 使用 OptimizedExecutor 执行 DML 操作
	s.mu.Lock()
	if s.optimizedExecutor == nil {
		s.optimizedExecutor = optimizer.NewOptimizedExecutor(ds, false)
	}
	executor := s.optimizedExecutor
	s.mu.Unlock()

	var dmlResult *domain.QueryResult
	switch parseResult.Statement.Type {
	case parser.SQLTypeInsert:
		dmlResult, err = executor.ExecuteInsert(ctx, parseResult.Statement.Insert)
	case parser.SQLTypeUpdate:
		dmlResult, err = executor.ExecuteUpdate(ctx, parseResult.Statement.Update)
	case parser.SQLTypeDelete:
		dmlResult, err = executor.ExecuteDelete(ctx, parseResult.Statement.Delete)
	default:
		// 降级到传统路径
		builder := parser.NewQueryBuilder(ds)
		dmlResult, err = builder.BuildAndExecute(ctx, query)
	}

	if err != nil {
		log.Printf("执行DML操作失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("DML执行错误: %w", err))
	}

	log.Printf("%s 操作完成，影响行数: %d", stmtType, dmlResult.Total)
	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// handleDDLQuery 处理DDL查询 - 使用OptimizedExecutor执行
func (s *Server) handleDDLQuery(ctx context.Context, conn net.Conn, sess *session.Session, query string, stmtType string) error {
	// 获取数据源
	ds := s.GetDataSource()
	if ds == nil {
		log.Printf("未设置数据源，返回OK")
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	}

	// 解析SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(query)
	if err != nil {
		log.Printf("解析SQL失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("SQL解析错误: %w", err))
	}

	if !parseResult.Success {
		log.Printf("解析SQL失败: %s", parseResult.Error)
		return protocol.SendError(conn, fmt.Errorf("SQL解析失败: %s", parseResult.Error))
	}

	// 使用 OptimizedExecutor 执行 DDL 操作
	s.mu.Lock()
	if s.optimizedExecutor == nil {
		s.optimizedExecutor = optimizer.NewOptimizedExecutor(ds, false)
	}
	executor := s.optimizedExecutor
	s.mu.Unlock()

	switch parseResult.Statement.Type {
	case parser.SQLTypeCreate:
		_, err = executor.ExecuteCreate(ctx, parseResult.Statement.Create)
	case parser.SQLTypeDrop:
		_, err = executor.ExecuteDrop(ctx, parseResult.Statement.Drop)
	case parser.SQLTypeAlter:
		_, err = executor.ExecuteAlter(ctx, parseResult.Statement.Alter)
	default:
		// 降级到传统路径
		builder := parser.NewQueryBuilder(ds)
		_, err = builder.BuildAndExecute(ctx, query)
	}

	if err != nil {
		log.Printf("执行DDL操作失败: %v", err)
		return protocol.SendError(conn, fmt.Errorf("DDL执行错误: %w", err))
	}

	log.Printf("%s 操作完成", stmtType)
	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// sendQueryResult 发送查询结果集
func (s *Server) sendQueryResult(ctx context.Context, conn net.Conn, sess *session.Session, result *domain.QueryResult) error {
	var err error

	// 发送列数
	columnCountPacket := &protocol.ColumnCountPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		ColumnCount: uint64(len(result.Columns)),
	}
	columnCountData, err := columnCountPacket.MarshalDefault()
	if err != nil {
		return err
	}
	if _, err := conn.Write(columnCountData); err != nil {
		return err
	}

	// 发送列定义
	for _, col := range result.Columns {
		fieldMeta := protocol.FieldMetaPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			FieldMeta: protocol.FieldMeta{
				Catalog:                   "def",
				Schema:                    "",
				Table:                     "",
				OrgTable:                  "",
				Name:                      col.Name,
				OrgName:                   col.Name,
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              255,
				Type:                      s.getMySQLType(col.Type),
				Flags:                     s.getColumnFlags(col),
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			},
		}
		fieldMetaData, err := fieldMeta.MarshalDefault()
		if err != nil {
			return err
		}
		if _, err := conn.Write(fieldMetaData); err != nil {
			return err
		}
	}

	// 发送列结束包
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

	// 发送数据行
	for _, row := range result.Rows {
		rowData := protocol.RowDataPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			RowData: make([]string, len(result.Columns)),
		}

		for i, col := range result.Columns {
			val := row[col.Name]
			rowData.RowData[i] = s.formatValue(val)
		}

		rowDataBytes, err := rowData.Marshal()
		if err != nil {
			return err
		}
		if _, err := conn.Write(rowDataBytes); err != nil {
			return err
		}
	}

	// 发送结果集结束包
	finalEof := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	finalEofData, err := finalEof.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(finalEofData); err != nil {
		return err
	}

	return nil
}

// getMySQLType 获取MySQL类型
func (s *Server) getMySQLType(typeStr string) byte {
	typeStr = strings.ToLower(typeStr)
	switch {
	case typeStr == "int", typeStr == "integer":
		return protocol.MYSQL_TYPE_LONG
	case typeStr == "bigint":
		return protocol.MYSQL_TYPE_LONGLONG
	case typeStr == "float", typeStr == "double":
		return protocol.MYSQL_TYPE_DOUBLE
	case typeStr == "string", typeStr == "varchar", typeStr == "text":
		return protocol.MYSQL_TYPE_VAR_STRING
	case typeStr == "bool", typeStr == "boolean":
		return protocol.MYSQL_TYPE_TINY
	default:
		return protocol.MYSQL_TYPE_VAR_STRING
	}
}

// getColumnFlags 获取列标志
func (s *Server) getColumnFlags(col domain.ColumnInfo) uint16 {
	var flags uint16
	if col.Primary {
		flags |= protocol.PRI_KEY_FLAG
	}
	if !col.Nullable {
		flags |= protocol.NOT_NULL_FLAG
	}
	return flags
}

// formatValue 格式化值
// 对常见类型使用 strconv 避免 fmt.Sprintf 的反射开销。
func (s *Server) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// countParams 统计SQL中的参数数量
// 使用字节遍历而非 rune 遍历，因为 '?' 是 ASCII 字符，无需 UTF-8 解码。
func countParams(query string) uint16 {
	count := uint16(0)
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			count++
		}
	}
	return count
}

// analyzeColumns 分析SQL返回的列数
func analyzeColumns(query string) uint16 {
	queryUpper := strings.ToUpper(query)

	if strings.Contains(queryUpper, "SELECT") {
		// 简化：假设SELECT返回1列
		return 1
	}

	if strings.Contains(queryUpper, "SHOW") {
		return 2
	}

	return 0
}

// getColumns 获取SQL返回的列名
func getColumns(query string) []string {
	queryUpper := strings.ToUpper(query)

	if strings.Contains(queryUpper, "SELECT") {
		return []string{"id"}
	}

	if strings.Contains(queryUpper, "SHOW") {
		return []string{"Variable_name", "Value"}
	}

	return []string{}
}

// handleSetCommand 处理 SET 命令
func (s *Server) handleSetCommand(ctx context.Context, conn net.Conn, sess *session.Session, query string) error {
	log.Printf("处理 SET 命令: %s", query)

	// 去除 SET 关键词和首尾空格
	cmd := strings.TrimSpace(query[3:])

	// 处理 SET NAMES charset
	if strings.HasPrefix(strings.ToUpper(cmd), "NAMES") {
		charset := strings.TrimSpace(cmd[5:])
		collation := ""
		if idx := strings.Index(strings.ToUpper(charset), "COLLATE"); idx > 0 {
			collation = strings.TrimSpace(charset[idx+7:])
			charset = strings.TrimSpace(charset[:idx])
		}
		if err := sess.SetVariable("names", charset); err != nil {
			log.Printf("设置字符集失败: %v", err)
			return err
		}
		if collation != "" {
			sess.SetVariable("COLLATION_CONNECTION", collation)
		}
		log.Printf("设置字符集: %s", charset)
		return protocol.SendOK(conn, sess.GetNextSequenceID())
	}

	// 处理 SET @@variable = value 或 SET @variable = value
	// 支持多个变量设置: SET var1=val1, var2=val2
	assignments := strings.Split(cmd, ",")

	for _, assign := range assignments {
		assign = strings.TrimSpace(assign)

		// 解析变量名和值
		var varName, varValue string

		// 查找等号位置
		eqIdx := strings.Index(assign, "=")
		if eqIdx == -1 {
			// 尝试查找 := 赋值
			eqIdx = strings.Index(assign, ":=")
		}

		if eqIdx == -1 {
			log.Printf("无法解析 SET 命令: %s", assign)
			continue
		}

		varName = strings.TrimSpace(assign[:eqIdx])
		varValue = strings.TrimSpace(assign[eqIdx+1:])

		// 去除值两端的引号
		if (strings.HasPrefix(varValue, "'") && strings.HasSuffix(varValue, "'")) ||
			(strings.HasPrefix(varValue, "\"") && strings.HasSuffix(varValue, "\"")) {
			varValue = varValue[1 : len(varValue)-1]
		}

		// 处理变量名前缀
		varName = strings.TrimSpace(varName)

		// 移除 @@global. 或 @@session. 前缀
		varName = strings.TrimPrefix(varName, "@@global.")
		varName = strings.TrimPrefix(varName, "@@session.")
		varName = strings.TrimPrefix(varName, "@@local.")
		varName = strings.TrimPrefix(varName, "@@")

		// 移除 @ 前缀（用户变量）
		varName = strings.TrimPrefix(varName, "@")

		// 转换为小写（不区分大小写）
		varName = strings.ToLower(varName)

		// 保存到会话
		if err := sess.SetVariable(varName, varValue); err != nil {
			log.Printf("设置变量 %s 失败: %v", varName, err)
			continue
		}

		log.Printf("设置会话变量: %s = %s", varName, varValue)
	}

	return protocol.SendOK(conn, sess.GetNextSequenceID())
}

// Start 启动服务器
func (s *Server) Start(ctx context.Context, listener net.Listener) error {
	log.Println("正在启动服务器...")

	// 监听连接
	log.Printf("开始监听端口: %v", listener.Addr())

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
					return
				}
				log.Printf("接受连接失败: %v", err)
				continue
			}

			// 使用goroutine池处理连接
			s.goroutinePool.Submit(func() {
				defer func() {
					if err := conn.Close(); err != nil {
						log.Printf("关闭连接失败: %v", err)
					}
				}()

				if err := s.HandleConn(ctx, conn); err != nil {
					log.Printf("处理连接失败: %v", err)
				}
			})
		}
	}()

	log.Println("服务器启动成功")
	return nil
}

// Close 关闭服务器并释放资源
func (s *Server) Close() error {
	log.Println("正在关闭服务器并释放资源...")

	var errs []error

	// 关闭goroutine池
	if s.goroutinePool != nil {
		if err := s.goroutinePool.Close(); err != nil {
			log.Printf("关闭goroutine池失败: %v", err)
			errs = append(errs, err)
		} else {
			log.Println("goroutine池已关闭")
		}
	}

	// 关闭对象池
	if s.objectPool != nil {
		if err := s.objectPool.Close(); err != nil {
			log.Printf("关闭对象池失败: %v", err)
			errs = append(errs, err)
		} else {
			log.Println("对象池已关闭")
		}
	}

	// 获取监控指标快照（用于日志）
	if s.metricsCollector != nil {
		snapshot := s.metricsCollector.GetSnapshot()
		log.Printf("查询统计: 总计=%d, 成功=%d, 失败=%d, 成功率=%.2f%%",
			snapshot.QueryCount,
			snapshot.QuerySuccess,
			snapshot.QueryError,
			snapshot.SuccessRate,
		)
		log.Printf("性能统计: 平均耗时=%v",
			snapshot.AvgDuration,
		)

		// 获取缓存统计
		if s.cacheManager != nil {
			allCacheStats := s.cacheManager.GetStats()
			queryCacheStats := allCacheStats["query"]
			if queryCacheStats != nil {
				log.Printf("缓存统计: 命中率=%.2f%%, 缓存大小=%d, 命中=%d, 未命中=%d",
					queryCacheStats.HitRate,
					queryCacheStats.Size,
					queryCacheStats.Hits,
					queryCacheStats.Misses,
				)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("关闭服务器时发生 %d 个错误", len(errs))
	}

	log.Println("服务器已关闭")
	return nil
}

// GetMetricsCollector 获取监控指标收集器
func (s *Server) GetMetricsCollector() *monitor.MetricsCollector {
	return s.metricsCollector
}

// GetCacheManager 获取缓存管理器
func (s *Server) GetCacheManager() *monitor.CacheManager {
	return s.cacheManager
}

// GetGoroutinePoolStats 获取goroutine池统计信息
func (s *Server) GetGoroutinePoolStats() pool.PoolStats {
	if s.goroutinePool == nil {
		return pool.PoolStats{}
	}
	return s.goroutinePool.Stats()
}

// GetObjectPoolStats 获取对象池统计信息
func (s *Server) GetObjectPoolStats() pool.PoolStats {
	if s.objectPool == nil {
		return pool.PoolStats{}
	}
	return s.objectPool.Stats()
}
