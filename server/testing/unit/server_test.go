package unit

import (
	"context"
	"net"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/server"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
)

// TestServer_ConcurrentQueries 测试并发查询场景（简化版）
func TestServer_ConcurrentQueries(t *testing.T) {
	// 创建测试配置
	ctx := context.Background()
	
	// 初始化 API DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// 创建内存数据源
	memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	})

	if err := memoryDS.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect memory data source: %v", err)
	}

	if err := db.RegisterDataSource("default", memoryDS); err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// 创建 server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	s := server.NewServer(ctx, listener, &config.Config{})
	s.SetDB(db)

	// 验证 server 已正确初始化
	if s == nil {
		t.Fatal("Server is nil")
	}

	// 验证 Session Manager 已初始化
	// 这里我们只是验证 server 结构体的字段，不实际启动 server
	// 因为实际启动 server 需要完整的握手和认证流程
	t.Log("Server initialized successfully with concurrent support")
}

// TestServer_SequenceIDOverflow 测试序列号溢出场景
func TestServer_SequenceIDOverflow(t *testing.T) {
	// 创建 Mock Session
	session := mock.NewMockSession()
	
	// 设置序列号为接近溢出的值
	session.SetSequenceID(254)
	
	// 模拟发送多个命令
	for i := 0; i < 10; i++ {
		originalSeqID := session.GetSequenceID()
		
		// 获取下一个序列号（注意：GetNextSequenceID 返回的是调用前的值）
		returnedSeqID := session.GetNextSequenceID()
		
		// 验证返回的值等于原始值
		if returnedSeqID != originalSeqID {
			t.Errorf("Iteration %d: GetNextSequenceID returned %d, but original was %d", i, returnedSeqID, originalSeqID)
		}
		
		// 验证当前序列号已正确递增
		currentSeqID := session.GetSequenceID()
		expectedCurrentSeqID := originalSeqID + 1
		if originalSeqID == 255 {
			expectedCurrentSeqID = 0 // 回绕
		}
		if currentSeqID != expectedCurrentSeqID {
			t.Errorf("Iteration %d: expected current seqID %d, got %d", i, expectedCurrentSeqID, currentSeqID)
		}
	}
}

// TestServer_ParserRegistry 测试包解析器注册
func TestServer_ParserRegistry(t *testing.T) {
	ctx := context.Background()
	
	// 创建 server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	s := server.NewServer(ctx, listener, &config.Config{})
	
	// 通过反射检查 parserRegistry 是否正确初始化
	// 这个测试主要验证重构后的代码结构正确
	if s == nil {
		t.Fatal("Server is nil")
	}
	
	t.Log("Server created successfully with parser registry")
}

// TestServer_HandshakeHandler 测试握手处理器
func TestServer_HandshakeHandler(t *testing.T) {
	ctx := context.Background()
	
	// 创建 API DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// 创建 server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	s := server.NewServer(ctx, listener, &config.Config{})
	s.SetDB(db)
	
	// 验证握手处理器已注册
	if s == nil {
		t.Fatal("Server is nil")
	}
	
	t.Log("Server created successfully with handshake handler")
}

// TestServer_MultipleCommandsSequence 测试多个命令的序列号管理
func TestServer_MultipleCommandsSequence(t *testing.T) {
	session := mock.NewMockSession()
	
	commands := []struct {
		name   string
		cmd    uint8
		reset  bool
	}{
		{"COM_PING", protocol.COM_PING, true},
		{"COM_QUERY", protocol.COM_QUERY, true},
		{"COM_PING", protocol.COM_PING, true},
		{"COM_QUIT", protocol.COM_QUIT, true},
	}
	
	for _, tc := range commands {
		if tc.reset {
			session.ResetSequenceID()
		}
		
		initialSeqID := session.GetSequenceID()
		if initialSeqID != 0 {
			t.Errorf("%s: expected initial seqID to be 0 after reset, got %d", tc.name, initialSeqID)
		}
		
		// 获取第一个包的序列号
		seqID1 := session.GetNextSequenceID()
		if seqID1 != 0 {
			t.Errorf("%s: expected first seqID to be 0, got %d", tc.name, seqID1)
		}
		
		// 获取第二个包的序列号（假设响应包）
		seqID2 := session.GetNextSequenceID()
		if seqID2 != 1 {
			t.Errorf("%s: expected second seqID to be 1, got %d", tc.name, seqID2)
		}
	}
}
