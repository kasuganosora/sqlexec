package handler

import (
	"context"
	"errors"
	"net"

	"github.com/kasuganosora/sqlexec/server/protocol"
	pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
)

// Logger 日志接口（支持 Mock）
type Logger interface {
	Printf(format string, v ...interface{})
}

// ResponseWriter 响应写入器接口（支持 Mock）
type ResponseWriter interface {
	Write([]byte) (int, error)
}

// Handler 命令处理器接口
type Handler interface {
	// Handle 处理命令
	Handle(ctx *HandlerContext, packet interface{}) error

	// Command 返回命令类型
	Command() uint8

	// Name 返回处理器名称
	Name() string
}

// HandlerContext 处理器上下文
type HandlerContext struct {
	Session      *pkg_session.Session
	Connection   ResponseWriter
	Command      uint8
	Logger       Logger
	DB           DBAccessor
}

// DBAccessor 数据库访问器接口（避免循环依赖）
type DBAccessor interface {
	GetContext() context.Context
}

// NewHandlerContext 创建处理器上下文
func NewHandlerContext(sess *pkg_session.Session, conn net.Conn, command uint8, logger Logger) *HandlerContext {
	return &HandlerContext{
		Session:      sess,
		Connection:   conn,
		Command:      command,
		Logger:       logger,
	}
}

// SetDB 设置数据库访问器
func (ctx *HandlerContext) SetDB(db DBAccessor) {
	ctx.DB = db
}

// SendOK 发送 OK 包（使用指定的序列号）
func (ctx *HandlerContext) SendOKWithSequenceID(seqID uint8) error {
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = seqID
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 0
	okPacket.OkInPacket.LastInsertId = 0
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	packetBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}

	_, err = ctx.Connection.Write(packetBytes)
	return err
}

// SendOK 发送 OK 包（自动生成序列号）
func (ctx *HandlerContext) SendOK() error {
	if ctx.Logger != nil {
		ctx.Logger.Printf("[DEBUG] SendOK() calling GetNextSequenceID...")
	}
	seqID := ctx.GetNextSequenceID()
	if ctx.Logger != nil {
		ctx.Logger.Printf("[DEBUG] SendOK() got seqID = %d, now calling SendOKWithSequenceID", seqID)
	}
	return ctx.SendOKWithSequenceID(seqID)
}

// SendOKWithRows 发送 OK 包（指定影响行数）
func (ctx *HandlerContext) SendOKWithRows(affectedRows uint64, lastInsertID uint64) error {
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = ctx.GetNextSequenceID()
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = affectedRows
	okPacket.OkInPacket.LastInsertId = lastInsertID
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	packetBytes, err := okPacket.Marshal()
	if err != nil {
		return err
	}

	_, err = ctx.Connection.Write(packetBytes)
	return err
}

// SendError 发送错误包
func (ctx *HandlerContext) SendError(err error) error {
	if ctx.Logger != nil {
		ctx.Logger.Printf("[ERROR] Sending error: %v", err)
	}

	errorCode, sqlState := mapErrorCode(err)

	errPacket := &protocol.ErrorPacket{}
	errPacket.SequenceID = ctx.GetNextSequenceID()
	errPacket.Header = 0xff // MySQL 错误包头
	errPacket.ErrorCode = errorCode
	if sqlState != "" {
		errPacket.SqlStateMarker = "#"
		errPacket.SqlState = sqlState
	}
	errPacket.ErrorMessage = err.Error()

	packetBytes, marshalErr := errPacket.Marshal()
	if marshalErr != nil {
		if ctx.Logger != nil {
			ctx.Logger.Printf("[ERROR] Failed to marshal error packet: %v", marshalErr)
		}
		return marshalErr
	}

	if _, writeErr := ctx.Connection.Write(packetBytes); writeErr != nil {
		if ctx.Logger != nil {
			ctx.Logger.Printf("[ERROR] Failed to write error packet: %v", writeErr)
		}
		return writeErr
	}

	return nil
}

// ResetSequenceID 重置序列号（每个命令开始时调用）
func (ctx *HandlerContext) ResetSequenceID() {
	if ctx.Session != nil {
		ctx.Session.ResetSequenceID()
		if ctx.Logger != nil {
			ctx.Logger.Printf("[DEBUG] ResetSequenceID called, new SequenceID = 0")
		}
	}
}

// GetNextSequenceID 获取下一个序列号
func (ctx *HandlerContext) GetNextSequenceID() uint8 {
	if ctx.Session != nil {
		seqID := ctx.Session.GetNextSequenceID()
		if ctx.Logger != nil {
			ctx.Logger.Printf("[DEBUG] GetNextSequenceID returned: %d", seqID)
		}
		return seqID
	}
	return 0
}

// Log 记录日志
func (ctx *HandlerContext) Log(format string, v ...interface{}) {
	if ctx.Logger != nil {
		ctx.Logger.Printf(format, v...)
	}
}

// mapErrorCode 将错误映射到 MySQL 错误码
func mapErrorCode(err error) (uint16, string) {
	// 检查错误消息内容
	errMsg := err.Error()

	// 表不存在
	if containsSubstring(errMsg, "table") && containsSubstring(errMsg, "not found") {
		return 1146, "42S02" // ER_NO_SUCH_TABLE
	}

	// 列不存在
	if containsSubstring(errMsg, "column") && containsSubstring(errMsg, "not found") {
		return 1054, "42S22" // ER_BAD_FIELD_ERROR
	}

	// 语法错误
	if containsSubstring(errMsg, "syntax") || containsSubstring(errMsg, "SYNTAX_ERROR") || containsSubstring(errMsg, "parse") {
		return 1064, "42000" // ER_PARSE_ERROR
	}

	// 空查询
	if containsSubstring(errMsg, "no statements found") || containsSubstring(errMsg, "empty query") {
		return 1065, "42000" // ER_EMPTY_QUERY
	}

	// 超时或取消
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return 1317, "HY000" // ER_QUERY_INTERRUPTED
	}

	// 默认语法错误
	return 1064, "42000" // ER_PARSE_ERROR
}

// containsSubstring 检查字符串是否包含子串
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && indexOfSubstring(s, substr) >= 0)
}

// indexOfSubstring 查找子串位置
func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// HandlerError 自定义错误类型
type HandlerError struct {
	Message string
}

func NewHandlerError(msg string) *HandlerError {
	return &HandlerError{Message: msg}
}

func (e *HandlerError) Error() string {
	return e.Message
}
