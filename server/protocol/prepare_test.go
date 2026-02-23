package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStmtPrepareResponsePacket 测试 COM_STMT_PREPARE 响应包
func TestStmtPrepareResponsePacket(t *testing.T) {
	// 测试序列化
	response := &StmtPrepareResponsePacket{
		Packet: Packet{
			SequenceID: 1,
		},
		StatementID:  1,
		ColumnCount:  1,
		ParamCount:   1,
		Reserved:     0,
		WarningCount: 0,
		Params: []FieldMeta{
			{
				Catalog:                   "def",
				Schema:                    "",
				Table:                     "",
				OrgTable:                  "",
				Name:                      "?",
				OrgName:                   "",
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              255,
				Type:                      0xfd,
				Flags:                     0,
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			},
		},
		Columns: []FieldMeta{
			{
				Catalog:                   "def",
				Schema:                    "test",
				Table:                     "users",
				OrgTable:                  "users",
				Name:                      "id",
				OrgName:                   "id",
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              11,
				Type:                      0x03,
				Flags:                     0x81,
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			},
		},
	}

	data, err := response.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Greater(t, len(data), 0)

	// 反序列化
	response2 := &StmtPrepareResponsePacket{}
	err = response2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, response.StatementID, response2.StatementID)
	assert.Equal(t, response.ColumnCount, response2.ColumnCount)
	assert.Equal(t, response.ParamCount, response2.ParamCount)
	assert.Equal(t, response.Reserved, response2.Reserved)
	assert.Equal(t, response.WarningCount, response2.WarningCount)
}

// TestComStmtPreparePacket 测试 COM_STMT_PREPARE 请求包
func TestComStmtPreparePacketWithQuery(t *testing.T) {
	// 测试带参数的预处理语句
	packet := &ComStmtPreparePacket{
		Packet: Packet{
			SequenceID: 0,
		},
		Command: COM_STMT_PREPARE,
		Query:   "SELECT * FROM users WHERE id = ?",
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	// 反序列化
	packet2 := &ComStmtPreparePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.Query, packet2.Query)
}

// TestComStmtPreparePacketWithoutParams 测试不带参数的预处理语句
func TestComStmtPreparePacketWithoutParams(t *testing.T) {
	packet := &ComStmtPreparePacket{
		Packet: Packet{
			SequenceID: 1,
		},
		Command: COM_STMT_PREPARE,
		Query:   "SELECT * FROM users",
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)

	packet2 := &ComStmtPreparePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.Query, packet2.Query)
}

// TestComStmtExecutePacketWithIntParam 测试带整型参数的执行包
func TestComStmtExecutePacketWithIntParam(t *testing.T) {
	packet := &ComStmtExecutePacket{
		Packet: Packet{
			SequenceID: 2,
		},
		Command:           COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00},
		NewParamsBindFlag: 1,
		ParamTypes: []StmtParamType{
			{Type: 0x01, Flag: 0}, // TINYINT
		},
		ParamValues: []any{int8(123)},
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)

	packet2 := &ComStmtExecutePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.StatementID, packet2.StatementID)
	assert.Equal(t, packet.Flags, packet2.Flags)
	assert.Equal(t, packet.IterationCount, packet2.IterationCount)
	assert.Equal(t, packet.NewParamsBindFlag, packet2.NewParamsBindFlag)
	assert.Equal(t, len(packet.ParamTypes), len(packet2.ParamTypes))
}

// TestComStmtExecutePacketWithStringParam 测试带字符串参数的执行包
func TestComStmtExecutePacketWithStringParam(t *testing.T) {
	packet := &ComStmtExecutePacket{
		Packet: Packet{
			SequenceID: 3,
		},
		Command:           COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00},
		NewParamsBindFlag: 1,
		ParamTypes: []StmtParamType{
			{Type: 0xfd, Flag: 0}, // VAR_STRING
		},
		ParamValues: []any{"test value"},
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)

	packet2 := &ComStmtExecutePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.StatementID, packet2.StatementID)
	assert.Equal(t, packet.NewParamsBindFlag, packet2.NewParamsBindFlag)
	assert.Equal(t, len(packet.ParamTypes), len(packet2.ParamTypes))
}

// TestComStmtExecutePacketWithNullParam 测试带NULL参数的执行包
func TestComStmtExecutePacketWithNullParam(t *testing.T) {
	packet := &ComStmtExecutePacket{
		Packet: Packet{
			SequenceID: 4,
		},
		Command:           COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x04}, // MariaDB协议：第一个参数为NULL，位2=0x04
		NewParamsBindFlag: 1,
		ParamTypes: []StmtParamType{
			{Type: 0xfd, Flag: 0}, // VAR_STRING
		},
		ParamValues: []any{nil},
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)

	packet2 := &ComStmtExecutePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.StatementID, packet2.StatementID)
	assert.Equal(t, len(packet.NullBitmap), len(packet2.NullBitmap))
}

// TestStmtParamType 测试参数类型结构
func TestStmtParamType(t *testing.T) {
	paramType := StmtParamType{
		Type: 0x03, // INT
		Flag: 0,
	}

	assert.Equal(t, uint8(0x03), paramType.Type)
	assert.Equal(t, uint8(0), paramType.Flag)
}

// TestComStmtExecutePacketMultipleParams 测试多个参数的执行包
func TestComStmtExecutePacketMultipleParams(t *testing.T) {
	packet := &ComStmtExecutePacket{
		Packet: Packet{
			SequenceID: 6,
		},
		Command:           COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00},
		NewParamsBindFlag: 1,
		ParamTypes: []StmtParamType{
			{Type: 0x03, Flag: 0}, // INT
			{Type: 0xfd, Flag: 0}, // VAR_STRING
			{Type: 0x01, Flag: 0}, // TINYINT
		},
		ParamValues: []any{
			int32(123),
			"hello",
			int8(1),
		},
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)

	packet2 := &ComStmtExecutePacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.StatementID, packet2.StatementID)
	assert.Equal(t, 3, len(packet2.ParamTypes))
}
