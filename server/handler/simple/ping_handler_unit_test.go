package simple

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestPingHandler_Command 测试返回正确的命令类型
func TestPingHandler_Command(t *testing.T) {
	handler := NewPingHandler(nil)
	assert.Equal(t, uint8(protocol.COM_PING), handler.Command())
}

// TestPingHandler_Name 测试返回正确的处理器名称
func TestPingHandler_Name(t *testing.T) {
	handler := NewPingHandler(nil)
	assert.Equal(t, "COM_PING", handler.Name())
}
