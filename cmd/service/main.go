package main

import (
	"context"
	"net"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/security"
	"github.com/kasuganosora/sqlexec/server"
	"github.com/kasuganosora/sqlexec/server/httpapi"
	mcpserver "github.com/kasuganosora/sqlexec/server/mcp"
)

func main() {
	logger := api.NewDefaultLogger(api.LogInfo)

	// 加载配置
	cfg := config.LoadConfigOrDefault()
	logger.Info("加载配置: Host=%s, Port=%d, Address=%s", cfg.Server.Host, cfg.Server.Port, cfg.GetListenAddress())

	// 监听端口
	listener, err := net.Listen("tcp4", cfg.GetListenAddress())
	if err != nil {
		logger.Error("监听端口失败: %v", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// 创建服务器实例
	srv := server.NewServer(ctx, listener, cfg)

	// 创建审计日志并关联到服务器
	auditLogger := security.NewAuditLogger(10000)
	srv.SetAuditLogger(auditLogger)

	// 条件启动 HTTP API
	if cfg.HTTPAPI.Enabled {
		httpServer := httpapi.NewServer(srv.GetDB(), srv.GetConfigDir(), &cfg.HTTPAPI, auditLogger)
		httpServer.SetVirtualDBRegistry(srv.GetVirtualDBRegistry())
		go func() {
			if err := httpServer.Start(); err != nil {
				logger.Error("HTTP API 服务器退出: %v", err)
			}
		}()
	}

	// 条件启动 MCP
	if cfg.MCP.Enabled {
		mcpSrv := mcpserver.NewServer(srv.GetDB(), srv.GetConfigDir(), &cfg.MCP, auditLogger)
		mcpSrv.SetVirtualDBRegistry(srv.GetVirtualDBRegistry())
		go func() {
			if err := mcpSrv.Start(); err != nil {
				logger.Error("MCP 服务器退出: %v", err)
			}
		}()
	}

	// 启动信息
	logger.Info("启动 MySQL 服务器: %s", cfg.GetListenAddress())
	if cfg.HTTPAPI.Enabled {
		logger.Info("HTTP API 服务器: %s:%d", cfg.HTTPAPI.Host, cfg.HTTPAPI.Port)
	}
	if cfg.MCP.Enabled {
		logger.Info("MCP 服务器: %s:%d", cfg.MCP.Host, cfg.MCP.Port)
	}

	// 启动服务器
	if err := srv.Start(); err != nil {
		logger.Error("服务器启动失败: %v", err)
		os.Exit(1)
	}

	// 阻塞主 goroutine
	<-ctx.Done()
	logger.Info("服务器停止")
}
