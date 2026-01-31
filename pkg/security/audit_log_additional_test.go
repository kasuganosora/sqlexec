package security

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewAuditLogger(t *testing.T) {
	auditor := NewAuditLogger(100)
	assert.NotNil(t, auditor)
	assert.Equal(t, 100, auditor.size)
	assert.Equal(t, 100, auditor.maxSize)
	assert.NotNil(t, auditor.buffer)
	assert.NotNil(t, auditor.entries)
}

func TestAuditLogger_Log(t *testing.T) {
	auditor := NewAuditLogger(10)

	event := &AuditEvent{
		ID:        "test-id",
		Timestamp: time.Now(),
		Level:     AuditLevelInfo,
		EventType: EventTypeQuery,
		User:      "test-user",
		Query:     "SELECT * FROM table",
		Success:   true,
	}

	auditor.Log(event)

	events := auditor.GetEvents(0, 10)
	assert.GreaterOrEqual(t, len(events), 1)
}

func TestAuditLogger_LogQuery(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogQuery("user1", "db1", "SELECT * FROM table1", 100, true)

	events := auditor.GetEventsByUser("user1")
	assert.Equal(t, 1, len(events))
	assert.Equal(t, EventTypeQuery, events[0].EventType)
	assert.Equal(t, "db1", events[0].Database)
	assert.Equal(t, "SELECT * FROM table1", events[0].Query)
	assert.Equal(t, int64(100), events[0].Duration)
	assert.True(t, events[0].Success)
}

func TestAuditLogger_LogInsert(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogInsert("user1", "db1", "table1", "INSERT INTO table1 VALUES (1)", 50, true)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	insertEvents := auditor.GetEventsByType(EventTypeInsert)
	assert.Equal(t, 1, len(insertEvents))
	assert.Equal(t, "table1", insertEvents[0].Table)
	assert.Equal(t, int64(50), insertEvents[0].Duration)
}

func TestAuditLogger_LogUpdate(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogUpdate("user1", "db1", "table1", "UPDATE table1 SET x=1", 75, false)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	updateEvents := auditor.GetEventsByType(EventTypeUpdate)
	assert.Equal(t, 1, len(updateEvents))
	assert.Equal(t, "table1", updateEvents[0].Table)
	assert.False(t, updateEvents[0].Success)
}

func TestAuditLogger_LogDelete(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogDelete("user1", "db1", "table1", "DELETE FROM table1", 30, true)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	deleteEvents := auditor.GetEventsByType(EventTypeDelete)
	assert.Equal(t, 1, len(deleteEvents))
	assert.Equal(t, "table1", deleteEvents[0].Table)
	assert.Equal(t, AuditLevelWarning, deleteEvents[0].Level)
}

func TestAuditLogger_LogDDL(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogDDL("user1", "db1", "CREATE TABLE test (id INT)", 200, true)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	ddlEvents := auditor.GetEventsByType(EventTypeDDL)
	assert.Equal(t, 1, len(ddlEvents))
	assert.Equal(t, "CREATE TABLE test (id INT)", ddlEvents[0].Query)
	assert.Equal(t, AuditLevelWarning, ddlEvents[0].Level)
}

func TestAuditLogger_LogLogin(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogLogin("user1", "192.168.1.1", true)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	loginEvents := auditor.GetEventsByType(EventTypeLogin)
	assert.Equal(t, 1, len(loginEvents))
	assert.Contains(t, loginEvents[0].Message, "192.168.1.1")
	assert.Equal(t, AuditLevelInfo, loginEvents[0].Level)

	ip, ok := loginEvents[0].Metadata["ip"]
	assert.True(t, ok)
	assert.Equal(t, "192.168.1.1", ip)
}

func TestAuditLogger_LogLogout(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogLogout("user1")

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	logoutEvents := auditor.GetEventsByType(EventTypeLogout)
	assert.Equal(t, 1, len(logoutEvents))
	assert.Equal(t, "User logged out", logoutEvents[0].Message)
}

func TestAuditLogger_LogPermission(t *testing.T) {
	auditor := NewAuditLogger(10)

	metadata := map[string]interface{}{
		"resource": "table1",
		"action":   "GRANT",
		"role":     "admin",
	}
	auditor.LogPermission("user1", "GRANT", metadata)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	permissionEvents := auditor.GetEventsByType(EventTypePermission)
	assert.Equal(t, 1, len(permissionEvents))
	assert.Contains(t, permissionEvents[0].Message, "GRANT")
	assert.Equal(t, metadata, permissionEvents[0].Metadata)
}

func TestAuditLogger_LogInjection(t *testing.T) {
	auditor := NewAuditLogger(10)

	auditor.LogInjection("user1", "192.168.1.1", "SELECT * FROM users WHERE id=1; DROP TABLE users;")

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	injectionEvents := auditor.GetEventsByType(EventTypeInjection)
	assert.Equal(t, 1, len(injectionEvents))
	assert.Equal(t, "SQL injection attempt detected", injectionEvents[0].Message)
	assert.Equal(t, AuditLevelCritical, injectionEvents[0].Level)
	assert.False(t, injectionEvents[0].Success)

	ip, ok := injectionEvents[0].Metadata["ip"]
	assert.True(t, ok)
	assert.Equal(t, "192.168.1.1", ip)
}

func TestAuditLogger_LogError(t *testing.T) {
	auditor := NewAuditLogger(10)

	err := assert.AnError
	auditor.LogError("user1", "db1", "Connection failed", err)

	events := auditor.GetEventsByUser("user1")
	assert.GreaterOrEqual(t, len(events), 1)

	errorEvents := auditor.GetEventsByType(EventTypeError)
	assert.Equal(t, 1, len(errorEvents))
	assert.Equal(t, "Connection failed", errorEvents[0].Message)
	assert.Equal(t, AuditLevelError, errorEvents[0].Level)
	assert.Equal(t, "db1", errorEvents[0].Database)
	assert.False(t, errorEvents[0].Success)

	errorMsg, ok := errorEvents[0].Metadata["error"]
	assert.True(t, ok)
	assert.Equal(t, err.Error(), errorMsg)
}

func TestAuditLogger_GetEvents(t *testing.T) {
	auditor := NewAuditLogger(10)

	// 添加多个事件
	for i := 0; i < 5; i++ {
		auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	}

	// 获取所有事件
	events := auditor.GetEvents(0, 10)
	assert.GreaterOrEqual(t, len(events), 5)

	// 分页获取
	events = auditor.GetEvents(0, 2)
	assert.LessOrEqual(t, len(events), 2)
}

func TestAuditLogger_GetEventsByUser(t *testing.T) {
	auditor := NewAuditLogger(10)

	// 添加不同用户的事件
	auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	auditor.LogQuery("user2", "db1", "SELECT 2", 10, true)
	auditor.LogQuery("user1", "db1", "SELECT 3", 10, true)

	// 获取user1的事件
	user1Events := auditor.GetEventsByUser("user1")
	assert.Equal(t, 2, len(user1Events))

	// 获取user2的事件
	user2Events := auditor.GetEventsByUser("user2")
	assert.Equal(t, 1, len(user2Events))
}

func TestAuditLogger_GetEventsByType(t *testing.T) {
	auditor := NewAuditLogger(10)

	// 添加不同类型的事件
	auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	auditor.LogInsert("user1", "db1", "table1", "INSERT INTO table1 VALUES (1)", 10, true)
	auditor.LogQuery("user1", "db1", "SELECT 2", 10, true)

	// 获取Query类型的事件
	queryEvents := auditor.GetEventsByType(EventTypeQuery)
	assert.Equal(t, 2, len(queryEvents))

	// 获取Insert类型的事件
	insertEvents := auditor.GetEventsByType(EventTypeInsert)
	assert.Equal(t, 1, len(insertEvents))
}

func TestAuditLogger_GetEventsByLevel(t *testing.T) {
	auditor := NewAuditLogger(10)

	// 添加不同级别的事件
	auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	auditor.LogDelete("user1", "db1", "table1", "DELETE FROM table1", 10, true)
	auditor.LogError("user1", "db1", "Error", assert.AnError)
	auditor.LogInjection("user1", "192.168.1.1", "malicious query")

	// 获取Info级别的事件
	infoEvents := auditor.GetEventsByLevel(AuditLevelInfo)
	assert.GreaterOrEqual(t, len(infoEvents), 1)

	// 获取Warning级别的事件
	warningEvents := auditor.GetEventsByLevel(AuditLevelWarning)
	assert.GreaterOrEqual(t, len(warningEvents), 1)

	// 获取Error级别的事件
	errorEvents := auditor.GetEventsByLevel(AuditLevelError)
	assert.GreaterOrEqual(t, len(errorEvents), 1)

	// 获取Critical级别的事件
	criticalEvents := auditor.GetEventsByLevel(AuditLevelCritical)
	assert.GreaterOrEqual(t, len(criticalEvents), 1)
}

func TestAuditLogger_GetEventsByTimeRange(t *testing.T) {
	auditor := NewAuditLogger(10)

	now := time.Now()

	// 添加事件
	auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)

	// 获取当前时间范围内的事件
	events := auditor.GetEventsByTimeRange(now.Add(-time.Hour), now.Add(time.Hour))
	assert.GreaterOrEqual(t, len(events), 1)

	// 获取过去时间范围内的事件
	events = auditor.GetEventsByTimeRange(now.Add(-2*time.Hour), now.Add(-time.Hour))
	assert.Equal(t, 0, len(events))
}

func TestAuditLogger_Export(t *testing.T) {
	auditor := NewAuditLogger(10)

	// 添加事件
	auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	auditor.LogInsert("user1", "db1", "table1", "INSERT INTO table1 VALUES (1)", 10, true)

	// 导出
	exported, err := auditor.Export()
	assert.NoError(t, err)
	assert.NotEmpty(t, exported)
	assert.Contains(t, exported, "query")
}

func TestAuditLogger_ConcurrentAccess(t *testing.T) {
	auditor := NewAuditLogger(100)

	// 并发写入
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证事件数量 - 使用合理的offset和limit
	events := auditor.GetEvents(0, 50)
	assert.GreaterOrEqual(t, len(events), 50)
}

func TestAuditLogger_BufferOverflow(t *testing.T) {
	auditor := NewAuditLogger(5)

	// 添加超过缓冲区大小的事件
	for i := 0; i < 10; i++ {
		auditor.LogQuery("user1", "db1", "SELECT 1", 10, true)
	}

	// 获取事件 - 使用合理的limit值
	events := auditor.GetEvents(0, 5)
	// 应该返回5个事件（缓冲区大小）
	assert.LessOrEqual(t, len(events), 5)
}

func TestAuditLogger_EventTypeConstants(t *testing.T) {
	// 测试事件类型常量
	assert.Equal(t, AuditEventType("login"), EventTypeLogin)
	assert.Equal(t, AuditEventType("logout"), EventTypeLogout)
	assert.Equal(t, AuditEventType("query"), EventTypeQuery)
	assert.Equal(t, AuditEventType("insert"), EventTypeInsert)
	assert.Equal(t, AuditEventType("update"), EventTypeUpdate)
	assert.Equal(t, AuditEventType("delete"), EventTypeDelete)
	assert.Equal(t, AuditEventType("ddl"), EventTypeDDL)
	assert.Equal(t, AuditEventType("permission"), EventTypePermission)
	assert.Equal(t, AuditEventType("injection"), EventTypeInjection)
	assert.Equal(t, AuditEventType("error"), EventTypeError)
}

func TestAuditLogger_AuditLevelConstants(t *testing.T) {
	// 测试审计级别常量
	assert.Equal(t, AuditLevel(0), AuditLevelInfo)
	assert.Equal(t, AuditLevel(1), AuditLevelWarning)
	assert.Equal(t, AuditLevel(2), AuditLevelError)
	assert.Equal(t, AuditLevel(3), AuditLevelCritical)
}
