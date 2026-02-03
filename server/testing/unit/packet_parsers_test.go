package unit

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/handler/packet_parsers"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestPingPacketParser 测试 PING 命令包解析器
func TestPingPacketParser(t *testing.T) {
	parser := packet_parsers.NewPingPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_PING), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_PING", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.SequenceID = 5
		packet.Payload = []byte{}

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		pingPacket, ok := result.(*protocol.ComPingPacket)
		assert.True(t, ok)
		assert.Equal(t, uint8(5), pingPacket.Packet.SequenceID)
	})
}

// TestQuitPacketParser 测试 QUIT 命令包解析器
func TestQuitPacketParser(t *testing.T) {
	parser := packet_parsers.NewQuitPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_QUIT), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_QUIT", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.SequenceID = 10

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		quitPacket, ok := result.(*protocol.ComQuitPacket)
		assert.True(t, ok)
		assert.Equal(t, uint8(10), quitPacket.Packet.SequenceID)
	})
}

// TestSetOptionPacketParser 测试 SET_OPTION 命令包解析器
func TestSetOptionPacketParser(t *testing.T) {
	parser := packet_parsers.NewSetOptionPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_SET_OPTION), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_SET_OPTION", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.Payload = []byte{0x00}

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		_, ok := result.(*protocol.ComSetOptionPacket)
		assert.True(t, ok)
	})
}

// TestQueryPacketParser 测试 QUERY 命令包解析器
func TestQueryPacketParser(t *testing.T) {
	parser := packet_parsers.NewQueryPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_QUERY), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_QUERY", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.Payload = []byte("SELECT * FROM test")

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		_, ok := result.(*protocol.ComQueryPacket)
		assert.True(t, ok)
	})
}

// TestInitDBPacketParser 测试 INIT_DB 命令包解析器
func TestInitDBPacketParser(t *testing.T) {
	parser := packet_parsers.NewInitDBPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_INIT_DB), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_INIT_DB", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.Payload = []byte("test_db")

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		_, ok := result.(*protocol.ComInitDBPacket)
		assert.True(t, ok)
	})
}

// TestFieldListPacketParser 测试 FIELD_LIST 命令包解析器
func TestFieldListPacketParser(t *testing.T) {
	parser := packet_parsers.NewFieldListPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_FIELD_LIST), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_FIELD_LIST", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.Payload = []byte("table\x00wildcard")

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		_, ok := result.(*protocol.ComFieldListPacket)
		assert.True(t, ok)
	})
}

// TestProcessKillPacketParser 测试 PROCESS_KILL 命令包解析器
func TestProcessKillPacketParser(t *testing.T) {
	parser := packet_parsers.NewProcessKillPacketParser()

	t.Run("Command returns correct type", func(t *testing.T) {
		assert.Equal(t, uint8(protocol.COM_PROCESS_KILL), parser.Command())
	})

	t.Run("Name returns correct name", func(t *testing.T) {
		assert.Equal(t, "COM_PROCESS_KILL", parser.Name())
	})

	t.Run("Parse creates valid packet", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.Payload = []byte{0x01, 0x00, 0x00, 0x00}

		result, err := parser.Parse(packet)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		_, ok := result.(*protocol.ComProcessKillPacket)
		assert.True(t, ok)
	})
}

// TestAllParsersInterface 测试所有解析器都实现了 PacketParser 接口
func TestAllParsersInterface(t *testing.T) {
	parsers := []struct {
		name   string
		parser interface{}
	}{
		{"PingPacketParser", packet_parsers.NewPingPacketParser()},
		{"QuitPacketParser", packet_parsers.NewQuitPacketParser()},
		{"SetOptionPacketParser", packet_parsers.NewSetOptionPacketParser()},
		{"QueryPacketParser", packet_parsers.NewQueryPacketParser()},
		{"InitDBPacketParser", packet_parsers.NewInitDBPacketParser()},
		{"FieldListPacketParser", packet_parsers.NewFieldListPacketParser()},
		{"ProcessKillPacketParser", packet_parsers.NewProcessKillPacketParser()},
	}

	for _, tc := range parsers {
		t.Run(tc.name+" implements PacketParser", func(t *testing.T) {
			// 通过类型断言验证接口实现
			_, ok := tc.parser.(interface {
				Command() uint8
				Name() string
				Parse(packet *protocol.Packet) (interface{}, error)
			})
			assert.True(t, ok, "%s should implement PacketParser interface", tc.name)
		})
	}
}
