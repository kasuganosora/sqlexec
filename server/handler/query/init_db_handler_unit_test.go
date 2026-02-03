package query

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestInitDBHandler_Command 测试返回正确的命令类型
func TestInitDBHandler_Command(t *testing.T) {
	h := NewInitDBHandler(nil)
	assert.Equal(t, uint8(protocol.COM_INIT_DB), h.Command())
}

// TestInitDBHandler_Name 测试返回正确的处理器名称
func TestInitDBHandler_Name(t *testing.T) {
	h := NewInitDBHandler(nil)
	assert.Equal(t, "COM_INIT_DB", h.Name())
}
