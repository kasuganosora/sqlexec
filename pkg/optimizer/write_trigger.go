package optimizer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// WriteTriggerManager 写入触发器管理器
// 监控表写入操作，自动触发统计信息刷新
type WriteTriggerManager struct {
	collector         *IncrementalStatisticsCollector
	triggers          map[string]*TableTrigger // 表名 -> 触发器
	triggerThreshold  int64                   // 触发阈值（写入行数）
	refreshInterval   time.Duration            // 最小刷新间隔
	refreshQueue      chan *RefreshTask        // 刷新任务队列
	workerPool        int                      // 工作协程数
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.RWMutex
}

// TableTrigger 表触发器
type TableTrigger struct {
	TableName        string
	LastRefreshTime  time.Time
	TotalWrites      int64 // 累计写入量
	PendingRefresh   bool  // 是否有待处理的刷新
	WriteHistory     []WriteRecord // 写入历史
}

// WriteRecord 写入记录
type WriteRecord struct {
	Operation  string // INSERT, UPDATE, DELETE
	RowCount   int64
	Timestamp  time.Time
}

// RefreshTask 刷新任务
type RefreshTask struct {
	TableName  string
	Reason     string
	Priority   int // 优先级：0=低, 1=中, 2=高
	Timestamp  time.Time
}

// NewWriteTriggerManager 创建写入触发器管理器
func NewWriteTriggerManager(collector *IncrementalStatisticsCollector, threshold int64, workers int) *WriteTriggerManager {
	ctx, cancel := context.WithCancel(context.Background())

	wtm := &WriteTriggerManager{
		collector:        collector,
		triggers:         make(map[string]*TableTrigger),
		triggerThreshold: threshold,
		refreshInterval:  time.Minute * 5, // 默认5分钟
		refreshQueue:     make(chan *RefreshTask, 1000),
		workerPool:       workers,
		ctx:              ctx,
		cancel:           cancel,
	}

	// 启动工作协程
	for i := 0; i < workers; i++ {
		go wtm.refreshWorker(ctx, i)
	}

	return wtm
}

// RegisterTable 注册表到触发器系统
func (wtm *WriteTriggerManager) RegisterTable(tableName string) {
	wtm.mu.Lock()
	defer wtm.mu.Unlock()

	if _, exists := wtm.triggers[tableName]; !exists {
		wtm.triggers[tableName] = &TableTrigger{
			TableName:       tableName,
			LastRefreshTime: time.Now(),
			TotalWrites:     0,
			PendingRefresh:  false,
			WriteHistory:    make([]WriteRecord, 0, 100),
		}
		debugf("  [WRITE TRIGGER] Registered table: %s\n", tableName)
	}
}

// UnregisterTable 注销表
func (wtm *WriteTriggerManager) UnregisterTable(tableName string) {
	wtm.mu.Lock()
	defer wtm.mu.Unlock()

	delete(wtm.triggers, tableName)
	debugf("  [WRITE TRIGGER] Unregistered table: %s\n", tableName)
}

// OnWrite 写入事件回调
func (wtm *WriteTriggerManager) OnWrite(tableName string, operation string, rowCount int64) {
	wtm.mu.Lock()
	trigger, exists := wtm.triggers[tableName]
	wtm.mu.Unlock()

	if !exists {
		// 表未注册，自动注册
		wtm.RegisterTable(tableName)
		wtm.mu.RLock()
		trigger = wtm.triggers[tableName]
		wtm.mu.RUnlock()
	}

	// 记录写入
	wtm.recordWrite(trigger, operation, rowCount)

	// 记录到增量收集器
	switch operation {
	case "INSERT":
		wtm.collector.RecordInsert(tableName, rowCount)
	case "UPDATE":
		wtm.collector.RecordUpdate(tableName, rowCount, []string{}) // 简化：不指定修改的列
	case "DELETE":
		wtm.collector.RecordDelete(tableName, rowCount)
	}

	// 检查是否需要触发刷新
	if wtm.shouldTriggerRefresh(trigger) {
		wtm.triggerRefresh(tableName, operation, rowCount)
	}
}

// recordWrite 记录写入操作
func (wtm *WriteTriggerManager) recordWrite(trigger *TableTrigger, operation string, rowCount int64) {
	wtm.mu.Lock()
	defer wtm.mu.Unlock()

	// 更新总写入量
	trigger.TotalWrites += rowCount

	// 记录写入历史
	record := WriteRecord{
		Operation: operation,
		RowCount:   rowCount,
		Timestamp:  time.Now(),
	}

	// 保持最近100条记录
	trigger.WriteHistory = append(trigger.WriteHistory, record)
	if len(trigger.WriteHistory) > 100 {
		trigger.WriteHistory = trigger.WriteHistory[1:]
	}
}

// shouldTriggerRefresh 判断是否应该触发刷新
func (wtm *WriteTriggerManager) shouldTriggerRefresh(trigger *TableTrigger) bool {
	wtm.mu.RLock()
	defer wtm.mu.RUnlock()

	// 检查是否有待处理的刷新
	if trigger.PendingRefresh {
		return false
	}

	// 检查是否达到阈值
	delta := wtm.collector.GetDeltaBufferSize(trigger.TableName)
	if delta >= wtm.triggerThreshold {
		return true
	}

	// 检查是否超过最小刷新间隔
	if time.Since(trigger.LastRefreshTime) > wtm.refreshInterval && delta > 0 {
		return true
	}

	return false
}

// triggerRefresh 触发刷新任务
func (wtm *WriteTriggerManager) triggerRefresh(tableName string, operation string, rowCount int64) {
	wtm.mu.Lock()
	trigger := wtm.triggers[tableName]
	trigger.PendingRefresh = true
	wtm.mu.Unlock()

	// 创建刷新任务
	task := &RefreshTask{
		TableName: tableName,
		Reason:    fmt.Sprintf("%s operation (%d rows)", operation, rowCount),
		Priority:  1, // 中优先级
		Timestamp: time.Now(),
	}

	// 提交到队列
	select {
	case wtm.refreshQueue <- task:
		debugf("  [WRITE TRIGGER] Queued refresh task for %s: %s\n", tableName, task.Reason)
	default:
		debugf("  [WRITE TRIGGER] Warning: refresh queue full for %s\n", tableName)
	}
}

// refreshWorker 刷新工作协程
func (wtm *WriteTriggerManager) refreshWorker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			debugf("  [WRITE TRIGGER] Worker %d stopped\n", workerID)
			return

		case task, ok := <-wtm.refreshQueue:
			if !ok || task == nil {
				return
			}
			debugf("  [WRITE TRIGGER] Worker %d processing task for %s\n", workerID, task.TableName)
			wtm.processRefreshTask(task)
		}
	}
}

// processRefreshTask 处理刷新任务
func (wtm *WriteTriggerManager) processRefreshTask(task *RefreshTask) {
	// 执行刷新
	startTime := time.Now()

	stats, err := wtm.collector.CollectStatistics(context.Background(), task.TableName)
	if err != nil {
		debugf("  [WRITE TRIGGER] Failed to refresh %s: %v\n", task.TableName, err)
	} else {
		duration := time.Since(startTime)
		debugf("  [WRITE TRIGGER] Refreshed %s: %d rows (took %v)\n",
			task.TableName, stats.RowCount, duration)
	}

	// 清除待处理标志
	wtm.mu.Lock()
	if trigger, exists := wtm.triggers[task.TableName]; exists {
		trigger.PendingRefresh = false
		trigger.LastRefreshTime = time.Now()
	}
	wtm.mu.Unlock()
}

// ForceRefresh 强制刷新指定表的统计信息
func (wtm *WriteTriggerManager) ForceRefresh(tableName string) error {
	task := &RefreshTask{
		TableName: tableName,
		Reason:    "Manual refresh",
		Priority:  2, // 高优先级
		Timestamp: time.Now(),
	}

	// 提交到队列
	select {
	case wtm.refreshQueue <- task:
		debugf("  [WRITE TRIGGER] Force refresh queued for %s\n", tableName)
		return nil
	default:
		return fmt.Errorf("refresh queue full")
	}
}

// RefreshAll 刷新所有注册表的统计信息
func (wtm *WriteTriggerManager) RefreshAll() error {
	wtm.mu.RLock()
	tables := make([]string, 0, len(wtm.triggers))
	for tableName := range wtm.triggers {
		tables = append(tables, tableName)
	}
	wtm.mu.RUnlock()

	var lastError error
	for _, tableName := range tables {
		if err := wtm.ForceRefresh(tableName); err != nil {
			lastError = err
		}
	}

	return lastError
}

// GetWriteStats 获取写入统计信息
func (wtm *WriteTriggerManager) GetWriteStats(tableName string) *TableTriggerStats {
	wtm.mu.RLock()
	defer wtm.mu.RUnlock()

	trigger, exists := wtm.triggers[tableName]
	if !exists {
		return nil
	}

	stats := &TableTriggerStats{
		TableName:        trigger.TableName,
		LastRefreshTime:  trigger.LastRefreshTime,
		TotalWrites:      trigger.TotalWrites,
		PendingRefresh:   trigger.PendingRefresh,
		DeltaBufferSize:  wtm.collector.GetDeltaBufferSize(tableName),
	}

	return stats
}

// GetAllStats 获取所有表的统计信息
func (wtm *WriteTriggerManager) GetAllStats() map[string]*TableTriggerStats {
	wtm.mu.RLock()
	defer wtm.mu.RUnlock()

	stats := make(map[string]*TableTriggerStats)
	for tableName, trigger := range wtm.triggers {
		stats[tableName] = &TableTriggerStats{
			TableName:       trigger.TableName,
			LastRefreshTime: trigger.LastRefreshTime,
			TotalWrites:     trigger.TotalWrites,
			PendingRefresh:  trigger.PendingRefresh,
			DeltaBufferSize: wtm.collector.GetDeltaBufferSize(tableName),
		}
	}

	return stats
}

// TableTriggerStats 表触发器统计信息
type TableTriggerStats struct {
	TableName        string
	LastRefreshTime  time.Time
	TotalWrites      int64
	PendingRefresh   bool
	DeltaBufferSize  int64
}

// SetRefreshInterval 设置刷新间隔
func (wtm *WriteTriggerManager) SetRefreshInterval(interval time.Duration) {
	wtm.mu.Lock()
	defer wtm.mu.Unlock()

	wtm.refreshInterval = interval
}

// SetTriggerThreshold 设置触发阈值
func (wtm *WriteTriggerManager) SetTriggerThreshold(threshold int64) {
	wtm.mu.Lock()
	defer wtm.mu.Unlock()

	wtm.triggerThreshold = threshold
}

// Stop 停止触发器管理器
func (wtm *WriteTriggerManager) Stop() {
	wtm.cancel()
	// Do not close refreshQueue: senders may still be active.
	// Workers will exit via ctx.Done().
	debugln("  [WRITE TRIGGER] Stopped")
}

// WriteTriggerDataSource 带写入触发器的数据源包装器
// 自动监控写入操作并触发统计信息刷新
type WriteTriggerDataSource struct {
	base      domain.DataSource
	trigger   *WriteTriggerManager
}

// NewWriteTriggerDataSource 创建带写入触发器的数据源
func NewWriteTriggerDataSource(base domain.DataSource, trigger *WriteTriggerManager) *WriteTriggerDataSource {
	return &WriteTriggerDataSource{
		base:    base,
		trigger: trigger,
	}
}

// Query 执行查询
func (wtds *WriteTriggerDataSource) Query(ctx context.Context, query string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return wtds.base.Query(ctx, query, options)
}

// Execute 执行SQL语句（监控DML操作）
func (wtds *WriteTriggerDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	// 执行SQL
	result, err := wtds.base.Execute(ctx, sql)
	if err != nil {
		return result, err
	}

	// 解析SQL，判断是否是DML操作
	// 简化：从SQL字符串中检测INSERT/UPDATE/DELETE
	sqlLower := sqlToLower(sql)
	var operation string
	if startsWith(sqlLower, "insert") {
		operation = "INSERT"
	} else if startsWith(sqlLower, "update") {
		operation = "UPDATE"
	} else if startsWith(sqlLower, "delete") {
		operation = "DELETE"
	} else {
		return result, nil // 不是DML操作
	}

	// 获取影响的行数
	rowCount := int64(0)
	if result != nil {
		rowCount = result.Total
	}

	// 触发写入事件
	if rowCount > 0 {
		// 简化：从SQL中提取表名
		tableName := extractTableNameFromSQL(sql)
		if tableName != "" {
			wtds.trigger.OnWrite(tableName, operation, rowCount)
		}
	}

	return result, nil
}

// sqlToLower 将SQL转换为小写
func sqlToLower(sql string) string {
	return strings.ToLower(sql)
}

// startsWith 检查字符串是否以指定前缀开始
func startsWith(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

// extractTableNameFromSQL 从SQL中提取表名
func extractTableNameFromSQL(sql string) string {
	sql = strings.TrimSpace(sql)
	lower := strings.ToLower(sql)

	var afterKeyword string
	switch {
	case strings.HasPrefix(lower, "insert"):
		// INSERT INTO tableName ...
		idx := strings.Index(lower, "into")
		if idx < 0 {
			return ""
		}
		afterKeyword = sql[idx+4:]
	case strings.HasPrefix(lower, "update"):
		// UPDATE tableName SET ...
		afterKeyword = sql[6:]
	case strings.HasPrefix(lower, "delete"):
		// DELETE FROM tableName ...
		idx := strings.Index(lower, "from")
		if idx < 0 {
			return ""
		}
		afterKeyword = sql[idx+4:]
	default:
		return ""
	}

	afterKeyword = strings.TrimSpace(afterKeyword)
	fields := strings.Fields(afterKeyword)
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[0], "`\"")
}

// Explain 解释触发器管理器
func (wtm *WriteTriggerManager) Explain() string {
	wtm.mu.RLock()
	defer wtm.mu.RUnlock()

	pendingRefreshes := 0
	for _, trigger := range wtm.triggers {
		if trigger.PendingRefresh {
			pendingRefreshes++
		}
	}

	return fmt.Sprintf(
		"WriteTriggerManager(tables=%d, threshold=%d, interval=%v, pending=%d, workers=%d)",
		len(wtm.triggers),
		wtm.triggerThreshold,
		wtm.refreshInterval,
		pendingRefreshes,
		wtm.workerPool,
	)
}
