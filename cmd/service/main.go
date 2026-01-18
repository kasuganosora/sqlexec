package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/kasuganosora/sqlexec/pkg"
	"github.com/kasuganosora/sqlexec/pkg/config"
)

func main() {
	// 加载配置
	cfg := config.LoadConfigOrDefault()

	listener, err := net.Listen("tcp", cfg.GetListenAddress())
	if err != nil {
		log.Fatal("监听端口失败:", err)
		return
	}

	ctx := context.Background()
	
	// 创建服务器实例
	srv := pkg.NewServer(cfg)

	fmt.Println("启动 MySQL 服务器...")
	fmt.Println("支持的命令:")
	fmt.Println("- select * from test")
	fmt.Println("- select @@version_comment limit 1")
	fmt.Printf("使用任何 MySQL 客户端连接 %s\n", cfg.GetListenAddress())

	// 启动服务器
	if err := srv.Start(ctx, listener); err != nil {
		log.Fatal("服务器启动失败:", err)
	}
}

