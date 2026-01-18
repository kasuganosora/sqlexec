package main

import (
	"context"
	"fmt"
	"log"
	"github.com/kasuganosora/sqlexec/server"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":3306")
	if err != nil {
		log.Fatal("监听端口失败:", err)
		return
	}

	ctx := context.Background()
	// 创建服务器实例
	srv := server.NewServer(ctx, listener)

	fmt.Println("启动 MySQL 服务器...")
	fmt.Println("支持的命令:")
	fmt.Println("- select * from test")
	fmt.Println("- select @@version_comment limit 1")
	fmt.Println("使用任何 MySQL 客户端连接 localhost:3306")

	// 启动服务器
	if err := srv.Start(); err != nil {
		log.Fatal("服务器启动失败:", err)
	}
}
