package mysqltest

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/server"
	_ "github.com/go-sql-driver/mysql"
)

// TestServer 提供一个可以在测试中启动和停止的MySQL协议服务器
type TestServer struct {
	ctx        context.Context
	cancel     context.CancelFunc
	listener   net.Listener
	srv        *server.Server
	port       int
	db         *api.DB
	mu         sync.Mutex
	started    bool
}

// NewTestServer 创建一个新的测试服务器
func NewTestServer() *TestServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &TestServer{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动测试服务器
func (s *TestServer) Start(port int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	// 监听端口
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}
	s.port = port
	s.listener = listener

	// 加载配置
	cfg := config.DefaultConfig()
	cfg.Server.Port = port

	// 初始化 API DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled:  false,
		CacheSize:     1000,
		CacheTTL:      300,
		DebugMode:     false,
	})
	if err != nil {
		listener.Close()
		return fmt.Errorf("failed to initialize API DB: %w", err)
	}
	s.db = db

	// 创建并注册 MVCC 数据源
	memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	})

	if db != nil {
		if err := memoryDS.Connect(s.ctx); err != nil {
			listener.Close()
			return fmt.Errorf("failed to connect memory data source: %w", err)
		}

		if err := db.RegisterDataSource("default", memoryDS); err != nil {
			listener.Close()
			return fmt.Errorf("failed to register data source: %w", err)
		}
	}

	// 创建服务器实例
	srv := server.NewServer(s.ctx, listener, cfg)
	s.srv = srv

	// 让服务器使用我们的 DB 实例（而不是创建新的）
	// 这样 CreateTestTable 创建的表才能在服务器中看到
	if s.db != nil {
		srv.SetDB(s.db)
	}

	// 启动服务器 goroutine
	go func() {
		if err := s.srv.Start(); err != nil {
			// 服务器停止是正常的
		}
	}()

	s.started = true

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	return nil
}

// Stop 停止测试服务器
func (s *TestServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
	s.started = false
}

// GetDB 获取服务器的 API DB 实例
func (s *TestServer) GetDB() *api.DB {
	return s.db
}

// GetPort 获取服务器端口
func (s *TestServer) GetPort() int {
	return s.port
}

// GetDSN 获取 MySQL 连接字符串
func (s *TestServer) GetDSN() string {
	return fmt.Sprintf("root@tcp(127.0.0.1:%d)/", s.port)
}

// GetDSNWithDB 获取指定数据库的 MySQL 连接字符串
func (s *TestServer) GetDSNWithDB(dbName string) string {
	return fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s", s.port, dbName)
}

// CreateTestTable 在测试服务器中创建测试表
func (s *TestServer) CreateTestTable(tableName string, columns []domain.ColumnInfo) error {
	if s.db == nil {
		return fmt.Errorf("server not initialized")
	}

	// 获取默认数据源
	ds, err := s.db.GetDefaultDataSource()
	if err != nil {
		return fmt.Errorf("failed to get default data source: %w", err)
	}
	if ds == nil {
		return fmt.Errorf("no default data source")
	}

	memoryDS, ok := ds.(*memory.MVCCDataSource)
	if !ok {
		return fmt.Errorf("default data source is not MVCCDataSource")
	}

	// 创建表
	tableInfo := &domain.TableInfo{
		Name:    tableName,
		Columns: columns,
	}

	if err := memoryDS.CreateTable(s.ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

// InsertTestData 插入测试数据
func (s *TestServer) InsertTestData(tableName string, rows []domain.Row) error {
	if s.db == nil {
		return fmt.Errorf("server not initialized")
	}

	// 获取默认数据源
	ds, err := s.db.GetDefaultDataSource()
	if err != nil {
		return fmt.Errorf("failed to get default data source: %w", err)
	}
	if ds == nil {
		return fmt.Errorf("no default data source")
	}

	memoryDS, ok := ds.(*memory.MVCCDataSource)
	if !ok {
		return fmt.Errorf("default data source is not MVCCDataSource")
	}

	// 插入数据
	if _, err := memoryDS.Insert(s.ctx, tableName, rows, nil); err != nil {
		return fmt.Errorf("failed to insert data into %s: %w", tableName, err)
	}

	return nil
}

// CreateTestDatabase 创建测试数据库（数据源）
func (s *TestServer) CreateTestDatabase(dbName string) error {
	if s.db == nil {
		return fmt.Errorf("server not initialized")
	}

	// 创建 MVCC 数据源
	memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     dbName,
		Writable: true,
	})

	if err := memoryDS.Connect(s.ctx); err != nil {
		return fmt.Errorf("failed to connect memory data source: %w", err)
	}

	if err := s.db.RegisterDataSource(dbName, memoryDS); err != nil {
		return fmt.Errorf("failed to register data source %s: %w", dbName, err)
	}

	return nil
}

// NewSQLConnection 创建一个新的 SQL 连接
func (s *TestServer) NewSQLConnection() (*sql.DB, error) {
	dsn := s.GetDSN()
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// 设置连接池参数
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	return conn, nil
}

// RunWithClient 运行一个函数，该函数接收一个 MySQL 客户端连接
func (s *TestServer) RunWithClient(fn func(*sql.DB) error) error {
	conn, err := s.NewSQLConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	// 测试连接
	if err := conn.Ping(); err != nil {
		return fmt.Errorf("failed to ping server: %w", err)
	}

	return fn(conn)
}
