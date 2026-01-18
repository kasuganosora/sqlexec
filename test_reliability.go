package main

import (
	"errors"
	"fmt"
	"log"
	"mysql-proxy/mysql/reliability"
	"time"
)

func main() {
	fmt.Println("=== 阶段7可靠性测试 ===\n")

	testErrorRecovery()
	testCircuitBreaker()
	testFailover()
	testLoadBalancer()
	testBackup()

	fmt.Println("\n=== 所有可靠性测试完成 ===")
}

func testErrorRecovery() {
	fmt.Println("1. 错误恢复测试")
	fmt.Println("-------------------------------")

	manager := reliability.NewErrorRecoveryManager()

	// 注册连接错误的恢复策略
	connectionStrategy := &reliability.RecoveryStrategy{
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
		BackoffFactor: 2.0,
		Action:        reliability.ActionRetry,
		OnError: func(errInfo *reliability.ErrorInfo) {
			fmt.Printf("连接错误: %v (尝试 %d)\n", errInfo.Err, errInfo.Context["attempt"])
		},
		OnSuccess: func() {
			fmt.Println("连接成功")
		},
	}
	manager.RegisterStrategy(reliability.ErrorTypeConnection, connectionStrategy)

	// 测试重试
	attemptCount := 0
	err := manager.ExecuteWithRetry(reliability.ErrorTypeConnection, func() error {
		attemptCount++
		if attemptCount < 3 {
			return errors.New("connection failed")
		}
		return nil
	})

	if err == nil {
		fmt.Printf("重试成功，总尝试次数: %d\n", attemptCount)
	} else {
		fmt.Printf("重试失败: %v\n", err)
	}

	// 测试失败后重试
	attemptCount = 0
	err = manager.ExecuteWithRetry(reliability.ErrorTypeConnection, func() error {
		attemptCount++
		return errors.New("always fail")
	})

	if err != nil {
		fmt.Printf("预期失败，尝试次数: %d\n", attemptCount)
	}

	// 获取错误统计
	stats := manager.GetErrorStats()
	fmt.Printf("错误统计: %+v\n", stats)

	// 获取错误日志
	errorLog := manager.GetErrorLog(0, 5)
	fmt.Printf("错误日志数量: %d\n", len(errorLog))

	fmt.Println()
}

func testCircuitBreaker() {
	fmt.Println("2. 断路器测试")
	fmt.Println("-------------------------------")

	cb := reliability.NewCircuitBreaker(3, 5*time.Second)

	// 测试成功
	for i := 0; i < 2; i++ {
		err := cb.Execute(func() error {
			fmt.Printf("操作 %d: 成功\n", i+1)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// 触发断路器打开
	fmt.Println("触发失败...")
	for i := 0; i < 3; i++ {
		err := cb.Execute(func() error {
			return errors.New("operation failed")
		})
		if err != nil {
			fmt.Printf("操作 %d: %v\n", i+3, err)
		}
	}

	fmt.Printf("断路器状态: %d (0=关闭, 1=打开, 2=半开)\n", cb.GetState())

	// 断路器打开时执行
	err := cb.Execute(func() error {
		fmt.Println("不应该执行到这里")
		return nil
	})
	if err != nil {
		fmt.Printf("断路器打开时执行: %v\n", err)
	}

	// 等待超时
	time.Sleep(6 * time.Second)

	// 半开状态
	fmt.Printf("超时后断路器状态: %d\n", cb.GetState())

	// 成功恢复
	err = cb.Execute(func() error {
		fmt.Println("恢复成功")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("恢复后断路器状态: %d\n", cb.GetState())

	// 重置断路器
	cb.Reset()
	fmt.Printf("重置后断路器状态: %d\n", cb.GetState())

	fmt.Println()
}

func testFailover() {
	fmt.Println("3. 故障转移测试")
	fmt.Println("-------------------------------")

	// 创建健康检查器
	checker := &mockHealthChecker{
		unhealthyNodes: make(map[string]bool),
	}

	// 创建故障转移管理器
	fm := reliability.NewFailoverManager(checker, 2*time.Second, 5*time.Second)

	// 添加节点
	fm.AddNode("node1", "localhost:3301", 10)
	fm.AddNode("node2", "localhost:3302", 8)
	fm.AddNode("node3", "localhost:3303", 6)

	// 启动健康检查
	fm.Start()
	defer fm.Stop()

	// 获取活跃节点
	activeNode := fm.GetActiveNode()
	if activeNode != nil {
		fmt.Printf("活跃节点: %s (权重: %d)\n", activeNode.ID, activeNode.Weight)
	}

	// 获取所有节点
	allNodes := fm.GetAllNodes()
	fmt.Printf("所有节点数量: %d\n", len(allNodes))

	// 标记node1为不健康
	checker.SetUnhealthy("node1", true)
	time.Sleep(3 * time.Second)

	// 获取新的活跃节点
	activeNode = fm.GetActiveNode()
	if activeNode != nil {
		fmt.Printf("故障转移后活跃节点: %s\n", activeNode.ID)
	}

	// 手动故障转移
	err := fm.ManualFailover("node3")
	if err != nil {
		log.Fatal(err)
	}

	activeNode = fm.GetActiveNode()
	if activeNode != nil {
		fmt.Printf("手动故障转移后活跃节点: %s\n", activeNode.ID)
	}

	// 恢复node1
	checker.SetUnhealthy("node1", false)
	time.Sleep(3 * time.Second)

	// 获取节点状态
	nodeStatus, err := fm.GetNodeStatus("node1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("node1状态: %d (0=健康, 1=降级, 2=不健康, 3=离线)\n", nodeStatus.Status)

	fmt.Println()
}

func testLoadBalancer() {
	fmt.Println("4. 负载均衡测试")
	fmt.Println("-------------------------------")

	lb := reliability.NewLoadBalancer()

	// 添加节点
	node1 := &reliability.Node{ID: "node1", Address: "localhost:3301", Weight: 10, Status: reliability.NodeStatusHealthy}
	node2 := &reliability.Node{ID: "node2", Address: "localhost:3302", Weight: 8, Status: reliability.NodeStatusHealthy}
	node3 := &reliability.Node{ID: "node3", Address: "localhost:3303", Weight: 6, Status: reliability.NodeStatusUnhealthy}

	lb.AddNode(node1)
	lb.AddNode(node2)
	lb.AddNode(node3)

	// 轮询
	fmt.Println("轮询测试:")
	for i := 0; i < 6; i++ {
		node := lb.NextNode()
		if node != nil {
			fmt.Printf("  请求 %d: %s\n", i+1, node.ID)
		}
	}

	// 获取负载最低的节点
	leastLoaded := lb.LeastLoadedNode()
	if leastLoaded != nil {
		fmt.Printf("负载最低的节点: %s\n", leastLoaded.ID)
	}

	fmt.Println()
}

func testBackup() {
	fmt.Println("5. 备份恢复测试")
	fmt.Println("-------------------------------")

	// 创建备份管理器
	bm := reliability.NewBackupManager("./backups")

	// 准备测试数据
	testData := map[string][]map[string]interface{}{
		"users": {
			{"id": 1, "name": "Alice", "email": "alice@example.com"},
			{"id": 2, "name": "Bob", "email": "bob@example.com"},
		},
		"products": {
			{"id": 1, "name": "Product A", "price": 100.0},
			{"id": 2, "name": "Product B", "price": 200.0},
		},
	}

	// 执行完整备份
	backupID, err := bm.Backup(reliability.BackupTypeFull, []string{"users", "products"}, testData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("完整备份完成，ID: %s\n", backupID)

	// 获取备份元数据
	metadata, err := bm.GetBackup(backupID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("备份信息: 记录数=%d, 大小=%d, 校验和=%s\n",
		metadata.RecordCount, metadata.Size, metadata.Checksum)

	// 执行恢复
	var restoredData map[string][]map[string]interface{}
	err = bm.Restore(backupID, &restoredData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("恢复完成，表数: %d\n", len(restoredData))

	// 执行增量备份
	testData["logs"] = []map[string]interface{}{
		{"id": 1, "message": "Log entry 1"},
	}
	incrementalBackupID, err := bm.Backup(reliability.BackupTypeIncremental, []string{"logs"}, testData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("增量备份完成，ID: %s\n", incrementalBackupID)

	// 列出所有备份
	backups := bm.ListBackups()
	fmt.Printf("备份列表数量: %d\n", len(backups))

	// 获取备份统计
	totalBackups, totalSize, err := bm.GetBackupStats()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("备份统计: 总数=%d, 总大小=%d bytes\n", totalBackups, totalSize)

	// 清理旧备份
	err = bm.CleanOldBackups(1*time.Hour, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("清理旧备份完成")

	fmt.Println()
}

// mockHealthChecker 模拟健康检查器
type mockHealthChecker struct {
	unhealthyNodes map[string]bool
}

func (m *mockHealthChecker) Check(node *reliability.Node) error {
	if m.unhealthyNodes[node.ID] {
		return errors.New("node is unhealthy")
	}
	return nil
}

func (m *mockHealthChecker) SetUnhealthy(nodeID string, unhealthy bool) {
	if m.unhealthyNodes == nil {
		m.unhealthyNodes = make(map[string]bool)
	}
	m.unhealthyNodes[nodeID] = unhealthy
}
