package query

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestInitDBHandler_Command 测试返回正确的命令类型
func TestInitDBHandler_Command(t *testing.T) {
	handler := NewInitDBHandler(nil)
	assert.Equal(t, protocol.COM_INIT_DB, handler.Command())
}

// TestInitDBHandler_Name 测试返回正确的处理器名称
func TestInitDBHandler_Name(t *testing.T) {
	handler := NewInitDBHandler(nil)
	assert.Equal(t, "COM_INIT_DB", handler.Name())
}

// TestQueryHandler_Command 测试返回正确的命令类型
func TestQueryHandler_Command(t *testing.T) {
	handler := NewQueryHandler()
	assert.Equal(t, protocol.COM_QUERY, handler.Command())
}

// TestQueryHandler_Name 测试返回正确的处理器名称
func TestQueryHandler_Name(t *testing.T) {
	handler := NewQueryHandler()
	assert.Equal(t, "COM_QUERY", handler.Name())
}
