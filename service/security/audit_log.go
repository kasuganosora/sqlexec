package security

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AuditLevel 审计级别
type AuditLevel int

const (
	AuditLevelInfo AuditLevel = iota
	AuditLevelWarning
	AuditLevelError
	AuditLevelCritical
)

// AuditEventType 审计事件类型
type AuditEventType string

const (
	EventTypeLogin      AuditEventType = "login"
	EventTypeLogout     AuditEventType = "logout"
	EventTypeQuery      AuditEventType = "query"
	EventTypeInsert     AuditEventType = "insert"
	EventTypeUpdate     AuditEventType = "update"
	EventTypeDelete     AuditEventType = "delete"
	EventTypeDDL        AuditEventType = "ddl"
	EventTypePermission AuditEventType = "permission"
	EventTypeInjection  AuditEventType = "injection"
	EventTypeError      AuditEventType = "error"
)

// AuditEvent 审计事件
type AuditEvent struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Level     AuditLevel             `json:"level"`
	EventType AuditEventType         `json:"event_type"`
	User      string                 `json:"user"`
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Query     string                 `json:"query"`
	Message   string                 `json:"message"`
	Metadata  map[string]interface{} `json:"metadata"`
	Success   bool                   `json:"success"`
	Duration  int64                  `json:"duration"` // 毫秒
}

// AuditLogEntry 审计日志条目
type AuditLogEntry struct {
	Event *AuditEvent
	Error error
}

// AuditLogger 审计日志记录�?
type AuditLogger struct {
	entries chan *AuditLogEntry
	bufLock sync.RWMutex
	buffer  []*AuditEvent
	size    int
	maxSize int
	index   int
}

// NewAuditLogger 创建审计日志记录�?
func NewAuditLogger(size int) *AuditLogger {
	return &AuditLogger{
		entries: make(chan *AuditLogEntry, 1000),
		buffer:  make([]*AuditEvent, size),
		size:    size,
		maxSize: size,
	}
}

// Log 记录审计事件
func (al *AuditLogger) Log(event *AuditEvent) {
	entry := &AuditLogEntry{
		Event: event,
	}

	// 存储到缓冲区
	al.bufLock.Lock()
	al.buffer[al.index] = event
	al.index = (al.index + 1) % al.size
	al.bufLock.Unlock()

	// 发送到通道（异步处理）
	select {
	case al.entries <- entry:
	default:
		// 通道满，丢弃
	}
}

// LogQuery 记录查询
func (al *AuditLogger) LogQuery(user, database, query string, duration int64, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeQuery,
		User:      user,
		Database:  database,
		Query:     query,
		Success:   success,
		Duration:  duration,
	}

	al.Log(event)
}

// LogInsert 记录插入操作
func (al *AuditLogger) LogInsert(user, database, table string, query string, duration int64, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeInsert,
		User:      user,
		Database:  database,
		Table:     table,
		Query:     query,
		Success:   success,
		Duration:  duration,
	}

	al.Log(event)
}

// LogUpdate 记录更新操作
func (al *AuditLogger) LogUpdate(user, database, table string, query string, duration int64, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeUpdate,
		User:      user,
		Database:  database,
		Table:     table,
		Query:     query,
		Success:   success,
		Duration:  duration,
	}

	al.Log(event)
}

// LogDelete 记录删除操作
func (al *AuditLogger) LogDelete(user, database, table string, query string, duration int64, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelWarning,
		EventType: EventTypeDelete,
		User:      user,
		Database:  database,
		Table:     table,
		Query:     query,
		Success:   success,
		Duration:  duration,
	}

	al.Log(event)
}

// LogDDL 记录DDL操作
func (al *AuditLogger) LogDDL(user, database, query string, duration int64, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelWarning,
		EventType: EventTypeDDL,
		User:      user,
		Database:  database,
		Query:     query,
		Success:   success,
		Duration:  duration,
	}

	al.Log(event)
}

// LogLogin 记录登录
func (al *AuditLogger) LogLogin(user, ip string, success bool) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeLogin,
		User:      user,
		Message:   fmt.Sprintf("Login from %s", ip),
		Success:   success,
		Metadata: map[string]interface{}{
			"ip": ip,
		},
	}

	al.Log(event)
}

// LogLogout 记录登出
func (al *AuditLogger) LogLogout(user string) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeLogout,
		User:      user,
		Message:   "User logged out",
		Success:   true,
	}

	al.Log(event)
}

// LogPermission 记录权限变更
func (al *AuditLogger) LogPermission(user, action string, metadata map[string]interface{}) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelWarning,
		EventType: EventTypePermission,
		User:      user,
		Message:   fmt.Sprintf("Permission %s", action),
		Success:   true,
		Metadata:  metadata,
	}

	al.Log(event)
}

// LogInjection 记录SQL注入尝试
func (al *AuditLogger) LogInjection(user, ip, query string) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelCritical,
		EventType: EventTypeInjection,
		User:      user,
		Query:     query,
		Message:   "SQL injection attempt detected",
		Success:   false,
		Metadata: map[string]interface{}{
			"ip": ip,
		},
	}

	al.Log(event)
}

// LogError 记录错误
func (al *AuditLogger) LogError(user, database, message string, err error) {
	event := &AuditEvent{
		ID:        generateEventID(),
		Timestamp: time.Now(),
		Level:     AuditLevelError,
		EventType: EventTypeError,
		User:      user,
		Database:  database,
		Message:   message,
		Success:   false,
		Metadata: map[string]interface{}{
			"error": err.Error(),
		},
	}

	al.Log(event)
}

// GetEvents 获取事件
func (al *AuditLogger) GetEvents(offset, limit int) []*AuditEvent {
	al.bufLock.Lock()
	defer al.bufLock.Unlock()

	events := make([]*AuditEvent, 0, limit)

	// 计算起始位置
	start := al.index - offset - limit
	if start < 0 {
		start += al.size
	}

	for i := 0; i < limit; i++ {
		pos := (start + i) % al.size
		event := al.buffer[pos]
		if event == nil {
			continue
		}
		events = append(events, event)
	}

	return events
}

// GetEventsByUser 获取用户的事�?
func (al *AuditLogger) GetEventsByUser(user string) []*AuditEvent {
	al.bufLock.RLock()
	defer al.bufLock.RUnlock()

	events := make([]*AuditEvent, 0)
	for _, event := range al.buffer {
		if event != nil && event.User == user {
			events = append(events, event)
		}
	}

	return events
}

// GetEventsByType 获取指定类型的事�?
func (al *AuditLogger) GetEventsByType(eventType AuditEventType) []*AuditEvent {
	al.bufLock.RLock()
	defer al.bufLock.RUnlock()

	events := make([]*AuditEvent, 0)
	for _, event := range al.buffer {
		if event != nil && event.EventType == eventType {
			events = append(events, event)
		}
	}

	return events
}

// GetEventsByLevel 获取指定级别的事�?
func (al *AuditLogger) GetEventsByLevel(level AuditLevel) []*AuditEvent {
	al.bufLock.RLock()
	defer al.bufLock.RUnlock()

	events := make([]*AuditEvent, 0)
	for _, event := range al.buffer {
		if event != nil && event.Level == level {
			events = append(events, event)
		}
	}

	return events
}

// GetEventsByTimeRange 获取时间范围内的事件
func (al *AuditLogger) GetEventsByTimeRange(start, end time.Time) []*AuditEvent {
	al.bufLock.RLock()
	defer al.bufLock.RUnlock()

	events := make([]*AuditEvent, 0)
	for _, event := range al.buffer {
		if event != nil && event.Timestamp.After(start) && event.Timestamp.Before(end) {
			events = append(events, event)
		}
	}

	return events
}

// Export 导出审计日志
func (al *AuditLogger) Export() (string, error) {
	events := al.GetEvents(0, al.size)
	data, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// generateEventID 生成事件ID
func generateEventID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
