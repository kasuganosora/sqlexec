package query

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestQueryHandler_Command 测试返回正确的命令类型
func TestQueryHandler_Command(t *testing.T) {
	h := NewQueryHandler()
	assert.Equal(t, uint8(protocol.COM_QUERY), h.Command())
}

// TestQueryHandler_Name 测试返回正确的处理器名称
func TestQueryHandler_Name(t *testing.T) {
	h := NewQueryHandler()
	assert.Equal(t, "COM_QUERY", h.Name())
}
