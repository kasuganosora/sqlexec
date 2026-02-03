package unit

import (
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/handler/packet_parsers"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
	"github.com/stretchr/testify/assert"
)

// TestPacketParserRegistry_Register 测试解析器注册功能
func TestPacketParserRegistry_Register(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	t.Run("successful registration", func(t *testing.T) {
		parser := packet_parsers.NewPingPacketParser()
		err := registry.Register(parser)

		assert.NoError(t, err, "Registration should succeed")
		assert.True(t, logger.ContainsLog("Registered parser: COM_PING"))
	})

	t.Run("nil parser registration", func(t *testing.T) {
		err := registry.Register(nil)

		assert.Error(t, err, "Nil parser should return error")
		assert.Equal(t, "cannot register nil parser", err.Error())
	})

	t.Run("duplicate registration", func(t *testing.T) {
		parser1 := packet_parsers.NewPingPacketParser()
		parser2 := packet_parsers.NewPingPacketParser()

		registry.Register(parser1)
		err := registry.Register(parser2)

		assert.Error(t, err, "Duplicate registration should return error")
		assert.Contains(t, err.Error(), "already registered")
	})

	t.Run("register multiple parsers", func(t *testing.T) {
		newRegistry := handler.NewPacketParserRegistry(logger)

		parsers := []handler.PacketParser{
			packet_parsers.NewPingPacketParser(),
			packet_parsers.NewQuitPacketParser(),
			packet_parsers.NewQueryPacketParser(),
			packet_parsers.NewInitDBPacketParser(),
		}

		for _, p := range parsers {
			err := newRegistry.Register(p)
			assert.NoError(t, err, "Should register %s", p.Name())
		}

		assert.Equal(t, 4, newRegistry.Count())
	})
}

// TestPacketParserRegistry_Get 测试获取解析器功能
func TestPacketParserRegistry_Get(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	t.Run("get registered parser", func(t *testing.T) {
		parser := packet_parsers.NewPingPacketParser()
		registry.Register(parser)

		retrieved, exists := registry.Get(protocol.COM_PING)

		assert.True(t, exists, "Parser should exist")
		assert.NotNil(t, retrieved, "Retrieved parser should not be nil")
		assert.Equal(t, "COM_PING", retrieved.Name())
	})

	t.Run("get unregistered parser", func(t *testing.T) {
		retrieved, exists := registry.Get(0xFF)

		assert.False(t, exists, "Parser should not exist")
		assert.Nil(t, retrieved, "Retrieved parser should be nil")
	})
}

// TestPacketParserRegistry_Parse 测试解析功能
func TestPacketParserRegistry_Parse(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	// 注册所有解析器
	registry.Register(packet_parsers.NewPingPacketParser())
	registry.Register(packet_parsers.NewQuitPacketParser())
	registry.Register(packet_parsers.NewQueryPacketParser())

	t.Run("parse COM_PING", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.SequenceID = 1
		packet.Payload = []byte{}

		result, err := registry.Parse(protocol.COM_PING, packet)

		assert.NoError(t, err, "Parse should succeed")
		assert.NotNil(t, result, "Parsed packet should not be nil")

		pingPacket, ok := result.(*protocol.ComPingPacket)
		assert.True(t, ok, "Result should be ComPingPacket")
		assert.Equal(t, uint8(1), pingPacket.Packet.SequenceID)
	})

	t.Run("parse COM_QUIT", func(t *testing.T) {
		packet := &protocol.Packet{}
		packet.SequenceID = 2

		result, err := registry.Parse(protocol.COM_QUIT, packet)

		assert.NoError(t, err, "Parse should succeed")
		_, ok := result.(*protocol.ComQuitPacket)
		assert.True(t, ok, "Result should be ComQuitPacket")
	})

	t.Run("parse unsupported command", func(t *testing.T) {
		packet := &protocol.Packet{}

		result, err := registry.Parse(0xFF, packet)

		assert.Error(t, err, "Parse should fail for unsupported command")
		assert.Nil(t, result, "Result should be nil")
		assert.Contains(t, err.Error(), "no parser registered")
		assert.True(t, logger.ContainsLog("[ERROR] No parser registered for command 0xff"))
	})
}

// TestPacketParserRegistry_Concurrency 测试并发安全性
func TestPacketParserRegistry_Concurrency(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	// 注册多个解析器
	parsers := []handler.PacketParser{
		packet_parsers.NewPingPacketParser(),
		packet_parsers.NewQuitPacketParser(),
		packet_parsers.NewQueryPacketParser(),
		packet_parsers.NewInitDBPacketParser(),
		packet_parsers.NewFieldListPacketParser(),
	}

	for _, p := range parsers {
		registry.Register(p)
	}

	t.Run("concurrent reads", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 100

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					packet := &protocol.Packet{}
					registry.Parse(protocol.COM_PING, packet)
				}
			}()
		}

		wg.Wait()
		assert.Equal(t, len(parsers), registry.Count())
	})

	t.Run("concurrent writes", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 尝试注册新的解析器（会失败但不会panic）
				parser := packet_parsers.NewPingPacketParser()
				registry.Register(parser)
			}()
		}

		wg.Wait()
	})
}

// TestPacketParserRegistry_List 测试列出所有解析器
func TestPacketParserRegistry_List(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	t.Run("empty registry", func(t *testing.T) {
		list := registry.List()

		assert.Empty(t, list, "List should be empty for new registry")
	})

	t.Run("list registered parsers", func(t *testing.T) {
		registry.Register(packet_parsers.NewPingPacketParser())
		registry.Register(packet_parsers.NewQuitPacketParser())
		registry.Register(packet_parsers.NewQueryPacketParser())

		list := registry.List()

		assert.Equal(t, 3, len(list), "Should list all registered parsers")
	})
}

// TestPacketParserRegistry_Count 测试计数功能
func TestPacketParserRegistry_Count(t *testing.T) {
	logger := mock.NewMockLogger()
	registry := handler.NewPacketParserRegistry(logger)

	assert.Equal(t, 0, registry.Count(), "New registry should have count 0")

	registry.Register(packet_parsers.NewPingPacketParser())
	assert.Equal(t, 1, registry.Count(), "Count should be 1 after first registration")

	registry.Register(packet_parsers.NewQuitPacketParser())
	assert.Equal(t, 2, registry.Count(), "Count should be 2 after second registration")
}
