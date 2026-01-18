package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/session"
)

type Server struct {
	ctx        context.Context
	listener   net.Listener
	sessionMgr *session.SessionMgr
	config     *config.Config
}

func NewServer(ctx context.Context, listener net.Listener, cfg *config.Config) *Server {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	s := &Server{
		listener:   listener,
		ctx:        ctx,
		sessionMgr: session.NewSessionMgr(ctx, session.NewMemoryDriver()),
		config:     cfg,
	}
	return s
}

func (s *Server) Start() (err error) {
	for {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go func(ctx context.Context, conn net.Conn) {
			err := s.Handle(ctx, conn)
			if err != nil {
				log.Printf("handle error: %+v\n", err)
				conn.Close()
			}
		}(s.ctx, conn)
	}
}

func (s *Server) Handle(ctx context.Context, conn net.Conn) (err error) {
	remoteAddr := conn.RemoteAddr().String()
	addr, port := parseRemoteAddr(remoteAddr)

	sess, err := s.sessionMgr.GetOrCreateSession(ctx, addr, port)
	if err != nil {
		return err
	}

	log.Printf("新连接来自: %s:%s, SessionID: %s, ThreadID: %d", addr, port, sess.ID, sess.ThreadID)
	sess.ResetSequenceID()

	if len(sess.User) == 0 {
		err = s.handleHandshake(ctx, conn, sess)
		if err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		packetContent, err := s.readMySQLPacket(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("客户端正常断开连接")
				return nil
			}
			log.Printf("读取包失败: %v", err)
			s.sendError(conn, err, sess.GetNextSequenceID())
			return err
		}

		commandType := packetContent[4]
		commandName := protocol.GetCommandName(commandType)
		log.Printf("收到命令: %s (0x%02x), SequenceID: %d", commandName, commandType, packetContent[3])

		var commandPack any
		switch commandType {
		case protocol.COM_QUIT:
			commandPack = &protocol.ComQuitPacket{}
		case protocol.COM_PING:
			commandPack = &protocol.ComPingPacket{}
		case protocol.COM_QUERY:
			commandPack = &protocol.ComQueryPacket{}
		case protocol.COM_INIT_DB:
			commandPack = &protocol.ComInitDBPacket{}
		case protocol.COM_SET_OPTION:
			commandPack = &protocol.ComSetOptionPacket{}
		case protocol.COM_STMT_PREPARE:
			commandPack = &protocol.ComStmtPreparePacket{}
		case protocol.COM_STMT_EXECUTE:
			commandPack = &protocol.ComStmtExecutePacket{}
		case protocol.COM_STMT_CLOSE:
			commandPack = &protocol.ComStmtClosePacket{}
		case protocol.COM_STMT_SEND_LONG_DATA:
			commandPack = &protocol.ComStmtSendLongDataPacket{}
		case protocol.COM_STMT_RESET:
			commandPack = &protocol.ComStmtResetPacket{}
		case protocol.COM_FIELD_LIST:
			commandPack = &protocol.ComFieldListPacket{}
		case protocol.COM_REFRESH:
			commandPack = &protocol.ComRefreshPacket{}
		case protocol.COM_STATISTICS:
			commandPack = &protocol.ComStatisticsPacket{}
		case protocol.COM_PROCESS_INFO:
			commandPack = &protocol.ComProcessInfoPacket{}
		case protocol.COM_PROCESS_KILL:
			commandPack = &protocol.ComProcessKillPacket{}
		case protocol.COM_DEBUG:
			commandPack = &protocol.ComDebugPacket{}
		case protocol.COM_SHUTDOWN:
			commandPack = &protocol.ComShutdownPacket{}
		default:
			errMsg := fmt.Sprintf("不支持的命令类型: %s (0x%02x)", commandName, commandType)
			log.Printf(errMsg)
			s.sendError(conn, fmt.Errorf(errMsg), sess.GetNextSequenceID())
			continue
		}

		if err := s.unmarshalPacket(commandPack, packetContent); err != nil {
			log.Printf("解析包失败: %v", err)
			s.sendError(conn, err, sess.GetNextSequenceID())
			return err
		}

		err = s.handleCommand(ctx, sess, conn, commandType, commandPack)
		if err != nil {
			log.Printf("处理命令 %s 失败: %v", commandName, err)
			return err
		}

		if commandType == protocol.COM_QUIT {
			return nil
		}
	}
}

func (s *Server) handleHandshake(ctx context.Context, conn net.Conn, sess *session.Session) error {
	handshakePacket := &protocol.HandshakeV10Packet{}
	handshakePacket.ProtocolVersion = 10
	handshakePacket.ServerVersion = s.config.Server.ServerVersion
	handshakePacket.ThreadID = sess.ThreadID
	handshakePacket.AuthPluginDataPart = []byte(RandomString(8))
	handshakePacket.AuthPluginDataPart2 = []byte(RandomString(12))
	handshakePacket.CapabilityFlags1 = 0xf7fe
	handshakePacket.CharacterSet = 8
	handshakePacket.StatusFlags = 0x0002
	handshakePacket.CapabilityFlags2 = 0x81bf
	handshakePacket.MariaDBCaps = 0x00000007
	handshakePacket.AuthPluginName = "mysql_native_password"

	packetBytes, err := handshakePacket.Marshal()
	if err != nil {
		log.Printf("序列化握手包失败: %v", err)
		return err
	}

	_, err = io.Copy(conn, bytes.NewReader(packetBytes))
	if err != nil {
		log.Printf("发送握手包失败: %v", err)
		return err
	}
	log.Printf("已发送握手包, ThreadID: %d", handshakePacket.ThreadID)

	authRequestPacket := &protocol.HandshakeResponse{}
	if err := authRequestPacket.Unmarshal(conn, uint32(handshakePacket.CapabilityFlags1)|uint32(handshakePacket.CapabilityFlags2)<<16); err != nil {
		log.Printf("解析认证包失败: %v", err)
		return err
	}

	log.Printf("收到认证包: User=%s, Database=%s, CharacterSet=%d",
		authRequestPacket.User, authRequestPacket.Database, authRequestPacket.CharacterSet)

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
	sess.Set("salt", handshakePacket.AuthPluginDataPart)
	sess.Set("salt2", handshakePacket.AuthPluginDataPart2)

	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = sess.GetNextSequenceID()
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 0
	okPacket.OkInPacket.LastInsertId = 0
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	okPacketBytes, err := okPacket.Marshal()
	if err != nil {
		log.Printf("序列化OK包失败: %v", err)
		return err
	}

	_, err = io.Copy(conn, bytes.NewReader(okPacketBytes))
	if err != nil {
		log.Printf("发送OK包失败: %v", err)
		return err
	}
	log.Printf("已发送认证成功包")
	return nil
}

func parseRemoteAddr(addr string) (string, string) {
	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return addr, ""
}

func (s *Server) readMySQLPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	packetLength := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16

	packetBody := make([]byte, packetLength)
	_, err = io.ReadFull(conn, packetBody)
	if err != nil {
		return nil, err
	}

	fullPacket := make([]byte, 4+packetLength)
	copy(fullPacket[:4], header)
	copy(fullPacket[4:], packetBody)

	return fullPacket, nil
}

func (s *Server) unmarshalPacket(packet any, data []byte) error {
	switch p := packet.(type) {
	case *protocol.ComQuitPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComPingPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComQueryPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComInitDBPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComSetOptionPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStmtPreparePacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStmtExecutePacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStmtClosePacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStmtSendLongDataPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStmtResetPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComFieldListPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComRefreshPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComStatisticsPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComProcessInfoPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComProcessKillPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComDebugPacket:
		return p.Unmarshal(bytes.NewReader(data))
	case *protocol.ComShutdownPacket:
		return p.Unmarshal(bytes.NewReader(data))
	default:
		return fmt.Errorf("不支持的包类型")
	}
}

func (s *Server) handleCommand(ctx context.Context, sess *session.Session, conn net.Conn, commandType uint8, command any) error {
	switch commandType {
	case protocol.COM_QUIT:
		return s.handleComQuit(ctx, sess, conn, command.(*protocol.ComQuitPacket))
	case protocol.COM_PING:
		return s.handleComPing(ctx, sess, conn, command.(*protocol.ComPingPacket))
	case protocol.COM_QUERY:
		return s.handleComQuery(ctx, sess, conn, command.(*protocol.ComQueryPacket))
	case protocol.COM_INIT_DB:
		return s.handleComInitDB(ctx, sess, conn, command.(*protocol.ComInitDBPacket))
	case protocol.COM_SET_OPTION:
		return s.handleComSetOption(ctx, sess, conn, command.(*protocol.ComSetOptionPacket))
	case protocol.COM_STMT_PREPARE:
		return s.handleComStmtPrepare(ctx, sess, conn, command.(*protocol.ComStmtPreparePacket))
	case protocol.COM_STMT_EXECUTE:
		return s.handleComStmtExecute(ctx, sess, conn, command.(*protocol.ComStmtExecutePacket))
	case protocol.COM_STMT_CLOSE:
		return s.handleComStmtClose(ctx, sess, conn, command.(*protocol.ComStmtClosePacket))
	case protocol.COM_STMT_SEND_LONG_DATA:
		return s.handleComStmtSendLongData(ctx, sess, conn, command.(*protocol.ComStmtSendLongDataPacket))
	case protocol.COM_STMT_RESET:
		return s.handleComStmtReset(ctx, sess, conn, command.(*protocol.ComStmtResetPacket))
	case protocol.COM_FIELD_LIST:
		return s.handleComFieldList(ctx, sess, conn, command.(*protocol.ComFieldListPacket))
	case protocol.COM_REFRESH:
		return s.handleComRefresh(ctx, sess, conn, command.(*protocol.ComRefreshPacket))
	case protocol.COM_STATISTICS:
		return s.handleComStatistics(ctx, sess, conn, command.(*protocol.ComStatisticsPacket))
	case protocol.COM_PROCESS_INFO:
		return s.handleComProcessInfo(ctx, sess, conn, command.(*protocol.ComProcessInfoPacket))
	case protocol.COM_PROCESS_KILL:
		return s.handleComProcessKill(ctx, sess, conn, command.(*protocol.ComProcessKillPacket))
	case protocol.COM_DEBUG:
		return s.handleComDebug(ctx, sess, conn, command.(*protocol.ComDebugPacket))
	case protocol.COM_SHUTDOWN:
		return s.handleComShutdown(ctx, sess, conn, command.(*protocol.ComShutdownPacket))
	}
	return nil
}

func (s *Server) sendError(conn net.Conn, err error, sequenceID uint8) {
	errPacket := &protocol.ErrorPacket{}
	errPacket.SequenceID = sequenceID
	errPacket.ErrorInPacket.Header = 0xff
	errPacket.ErrorInPacket.ErrorCode = 1064
	errPacket.ErrorInPacket.SqlStateMarker = "#"
	errPacket.ErrorInPacket.SqlState = "42000"
	errPacket.ErrorInPacket.ErrorMessage = err.Error()

	packetBytes, _ := errPacket.Marshal()
	conn.Write(packetBytes)
}

func (s *Server) sendOK(conn net.Conn, sequenceID uint8) error {
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = sequenceID
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 0
	okPacket.OkInPacket.LastInsertId = 0
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	packetBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}
	_, err = conn.Write(packetBytes)
	return err
}

func (s *Server) handleComQuit(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComQuitPacket) error {
	log.Printf("处理 COM_QUIT")
	return nil
}

func (s *Server) handleComPing(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComPingPacket) error {
	log.Printf("处理 COM_PING")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComQuery(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComQueryPacket) error {
	log.Printf("处理 COM_QUERY: %s", commandPack.Query)

	query := strings.TrimSpace(commandPack.Query)
	queryUpper := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(queryUpper, "SELECT") || strings.HasPrefix(queryUpper, "SHOW"):
		return s.handleSelect(sess, conn, query)
	case strings.HasPrefix(queryUpper, "SET"):
		return s.handleSet(sess, conn, query)
	case strings.HasPrefix(queryUpper, "USE"):
		return s.handleUse(sess, conn, query)
	case strings.HasPrefix(queryUpper, "INSERT"), strings.HasPrefix(queryUpper, "UPDATE"),
		strings.HasPrefix(queryUpper, "DELETE"), strings.HasPrefix(queryUpper, "REPLACE"),
		strings.HasPrefix(queryUpper, "CREATE"), strings.HasPrefix(queryUpper, "DROP"),
		strings.HasPrefix(queryUpper, "ALTER"), strings.HasPrefix(queryUpper, "TRUNCATE"):
		return s.handleDML(sess, conn, query)
	default:
		return s.sendOK(conn, sess.GetNextSequenceID())
	}
}

func (s *Server) handleSelect(sess *session.Session, conn net.Conn, query string) error {
	if strings.Contains(query, "@@") {
		return s.handleVariableSelect(sess, conn, query)
	}

	return s.sendResultSet(conn, sess, []protocol.FieldMeta{
		{
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
	}, [][]string{{"1"}})
}

func (s *Server) handleVariableSelect(sess *session.Session, conn net.Conn, query string) error {
	varName := ""
	if idx := strings.Index(query, "@@"); idx > 0 {
		varName = strings.TrimSpace(query[idx+2:])
		if idx := strings.Index(varName, " "); idx > 0 {
			varName = varName[:idx]
		}
		varName = strings.ToLower(strings.TrimSpace(varName))
	}

	log.Printf("查询系统变量: %s", varName)

	varValue := ""
	switch varName {
	case "version_comment":
		varValue = "mariadb.org binary distribution"
	case "version":
		varValue = "10.3.12-MariaDB"
	case "hostname":
		varValue = "localhost"
	default:
		if val, err := sess.GetVariable(varName); err == nil && val != nil {
			varValue = fmt.Sprintf("%v", val)
		} else {
			varValue = ""
		}
	}

	return s.sendResultSet(conn, sess, []protocol.FieldMeta{
		{
			Catalog:                   "def",
			Schema:                    "",
			Table:                     "",
			OrgTable:                  "",
			Name:                      "@@" + varName,
			OrgName:                   "@@" + varName,
			LengthOfFixedLengthFields: 12,
			CharacterSet:              33,
			ColumnLength:              255,
			Type:                      protocol.MYSQL_TYPE_VAR_STRING,
			Flags:                     0,
			Decimals:                  0,
			Reserved:                  "\x00\x00",
		},
	}, [][]string{{varValue}})
}

func (s *Server) handleSet(sess *session.Session, conn net.Conn, query string) error {
	log.Printf("处理 SET 查询: %s", query)

	cmd := strings.TrimSpace(query[3:])

	if strings.HasPrefix(strings.ToUpper(cmd), "NAMES") {
		charset := strings.TrimSpace(cmd[5:])
		if idx := strings.Index(charset, "COLLATE"); idx > 0 {
			charset = strings.TrimSpace(charset[:idx])
		}
		if err := sess.SetVariable("names", charset); err != nil {
			log.Printf("设置字符集失败: %v", err)
			return err
		}
		log.Printf("设置字符集: %s", charset)
		return s.sendOK(conn, sess.GetNextSequenceID())
	}

	assignments := strings.Split(cmd, ",")

	for _, assign := range assignments {
		assign = strings.TrimSpace(assign)

		var varName, varValue string

		eqIdx := strings.Index(assign, "=")
		if eqIdx == -1 {
			eqIdx = strings.Index(assign, ":=")
		}

		if eqIdx == -1 {
			log.Printf("无法解析 SET 命令: %s", assign)
			continue
		}

		varName = strings.TrimSpace(assign[:eqIdx])
		varValue = strings.TrimSpace(assign[eqIdx+1:])

		if (strings.HasPrefix(varValue, "'") && strings.HasSuffix(varValue, "'")) ||
			(strings.HasPrefix(varValue, "\"") && strings.HasSuffix(varValue, "\"")) {
			varValue = varValue[1 : len(varValue)-1]
		}

		varName = strings.TrimSpace(varName)
		varName = strings.TrimPrefix(varName, "@@global.")
		varName = strings.TrimPrefix(varName, "@@session.")
		varName = strings.TrimPrefix(varName, "@@local.")
		varName = strings.TrimPrefix(varName, "@@")
		varName = strings.TrimPrefix(varName, "@")
		varName = strings.ToLower(varName)

		if err := sess.SetVariable(varName, varValue); err != nil {
			log.Printf("设置变量 %s 失败: %v", varName, err)
			continue
		}

		log.Printf("设置会话变量: %s = %s", varName, varValue)
	}

	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleUse(sess *session.Session, conn net.Conn, query string) error {
	dbName := strings.TrimSpace(query[3:])
	log.Printf("切换数据库: %s", dbName)
	sess.Set("current_database", dbName)
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleDML(sess *session.Session, conn net.Conn, query string) error {
	log.Printf("处理 DML 查询: %s", query)
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComInitDB(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComInitDBPacket) error {
	log.Printf("处理 COM_INIT_DB: %s", commandPack.SchemaName)
	sess.Set("current_database", commandPack.SchemaName)
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComSetOption(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComSetOptionPacket) error {
	log.Printf("处理 COM_SET_OPTION: %d", commandPack.OptionOperation)
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComStmtPrepare(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStmtPreparePacket) error {
	log.Printf("处理 COM_STMT_PREPARE: %s", commandPack.Query)

	stmtID := sess.ThreadID

	paramCount := s.countParams(commandPack.Query)
	columnCount := s.analyzeColumns(commandPack.Query)

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

	columnNames := s.getColumns(commandPack.Query)
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

	packetBytes, err := response.Marshal()
	if err != nil {
		log.Printf("序列化 COM_STMT_PREPARE 响应失败: %v", err)
		return err
	}

	_, err = conn.Write(packetBytes)
	if err != nil {
		log.Printf("发送 COM_STMT_PREPARE 响应失败: %v", err)
		return err
	}

	log.Printf("已发送 COM_STMT_PREPARE 响应: statement_id=%d, params=%d, columns=%d",
		response.StatementID, response.ParamCount, response.ColumnCount)

	sess.Set(fmt.Sprintf("stmt_%d", stmtID), commandPack.Query)

	return nil
}

func (s *Server) handleComStmtExecute(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStmtExecutePacket) error {
	log.Printf("处理 COM_STMT_EXECUTE: statement_id=%d", commandPack.StatementID)

	queryKey := fmt.Sprintf("stmt_%d", commandPack.StatementID)
	query, _ := sess.Get(queryKey)
	if query == nil {
		log.Printf("预处理语句不存在: statement_id=%d", commandPack.StatementID)
		s.sendError(conn, fmt.Errorf("预处理语句不存在"), sess.GetNextSequenceID())
		return fmt.Errorf("预处理语句不存在")
	}

	columnCount := s.analyzeColumns(query.(string))

	columnCountData := []byte{0x01, 0x00, 0x00, sess.GetNextSequenceID()}
	_, err := conn.Write(columnCountData)
	if err != nil {
		return err
	}

	columnNames := s.getColumns(query.(string))
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
			return err
		}
		if _, err := conn.Write(fieldMetaData); err != nil {
			return err
		}
	}

	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

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

func (s *Server) handleComStmtClose(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStmtClosePacket) error {
	log.Printf("处理 COM_STMT_CLOSE: statement_id=%d", commandPack.StatementID)

	queryKey := fmt.Sprintf("stmt_%d", commandPack.StatementID)
	sess.Delete(queryKey)

	return nil
}

func (s *Server) handleComStmtSendLongData(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStmtSendLongDataPacket) error {
	log.Printf("处理 COM_STMT_SEND_LONG_DATA")
	return nil
}

func (s *Server) handleComStmtReset(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStmtResetPacket) error {
	log.Printf("处理 COM_STMT_RESET")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComFieldList(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComFieldListPacket) error {
	log.Printf("处理 COM_FIELD_LIST")
	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, _ := eofPacket.Marshal()
	_, err := conn.Write(eofData)
	return err
}

func (s *Server) handleComRefresh(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComRefreshPacket) error {
	log.Printf("处理 COM_REFRESH")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComStatistics(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComStatisticsPacket) error {
	log.Printf("处理 COM_STATISTICS")
	stats := "Uptime: 3600  Threads: 1  Questions: 10  Slow queries: 0  Opens: 5  Flush tables: 1  Open tables: 4  Queries per second avg: 0.003"
	conn.Write([]byte(stats))
	return nil
}

func (s *Server) handleComProcessInfo(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComProcessInfoPacket) error {
	log.Printf("处理 COM_PROCESS_INFO")
	return s.sendResultSet(conn, sess, []protocol.FieldMeta{}, [][]string{})
}

func (s *Server) handleComProcessKill(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComProcessKillPacket) error {
	log.Printf("处理 COM_PROCESS_KILL")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComDebug(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComDebugPacket) error {
	log.Printf("处理 COM_DEBUG")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) handleComShutdown(ctx context.Context, sess *session.Session, conn net.Conn, commandPack *protocol.ComShutdownPacket) error {
	log.Printf("处理 COM_SHUTDOWN")
	return s.sendOK(conn, sess.GetNextSequenceID())
}

func (s *Server) sendResultSet(conn net.Conn, sess *session.Session, columns []protocol.FieldMeta, rows [][]string) error {
	columnCountPacket := &protocol.ColumnCountPacket{
		Packet: protocol.Packet{
			SequenceID: sess.GetNextSequenceID(),
		},
		ColumnCount: uint64(len(columns)),
	}
	columnCountData, err := columnCountPacket.MarshalDefault()
	if err != nil {
		return err
	}
	if _, err := conn.Write(columnCountData); err != nil {
		return err
	}

	for _, col := range columns {
		fieldMeta := protocol.FieldMetaPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			FieldMeta: col,
		}
		fieldMetaData, err := fieldMeta.MarshalDefault()
		if err != nil {
			return err
		}
		if _, err := conn.Write(fieldMetaData); err != nil {
			return err
		}
	}

	eofPacket := protocol.CreateEofPacketWithStatus(sess.GetNextSequenceID(), true, false)
	eofData, err := eofPacket.Marshal()
	if err != nil {
		return err
	}
	if _, err := conn.Write(eofData); err != nil {
		return err
	}

	for _, row := range rows {
		rowData := protocol.RowDataPacket{
			Packet: protocol.Packet{
				SequenceID: sess.GetNextSequenceID(),
			},
			RowData: row,
		}
		rowDataBytes, err := rowData.Marshal()
		if err != nil {
			return err
		}
		if _, err := conn.Write(rowDataBytes); err != nil {
			return err
		}
	}

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

func (s *Server) countParams(query string) uint16 {
	count := uint16(0)
	for _, ch := range query {
		if ch == '?' {
			count++
		}
	}
	return count
}

func (s *Server) analyzeColumns(query string) uint16 {
	queryUpper := strings.ToUpper(query)

	if strings.Contains(queryUpper, "SELECT") {
		return 1
	}

	if strings.Contains(queryUpper, "SHOW") {
		return 2
	}

	return 0
}

func (s *Server) getColumns(query string) []string {
	queryUpper := strings.ToUpper(query)

	if strings.Contains(queryUpper, "SELECT") {
		return []string{"id"}
	}

	if strings.Contains(queryUpper, "SHOW") {
		return []string{"Variable_name", "Value"}
	}

	return []string{}
}

func RandomString(n int) string {
	letters := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-/!@#$%^&*()_~`"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
