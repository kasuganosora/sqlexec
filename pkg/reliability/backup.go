package reliability

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BackupType 备份类型
type BackupType int

const (
	BackupTypeFull BackupType = iota
	BackupTypeIncremental
	BackupTypeDifferential
)

// BackupStatus 备份状态
type BackupStatus int

const (
	BackupStatusPending BackupStatus = iota
	BackupStatusRunning
	BackupStatusCompleted
	BackupStatusFailed
)

// BackupMetadata 备份元数据
type BackupMetadata struct {
	ID          string
	Type        BackupType
	Status      BackupStatus
	StartTime   time.Time
	EndTime     time.Time
	Tables      []string
	RecordCount int
	Size        int64
	Checksum    string
	FilePath    string
	Error       string
}

// BackupManager 备份管理器
type BackupManager struct {
	metadata     map[string]*BackupMetadata
	backups      []string
	metadataLock sync.RWMutex
	backupDir    string
}

// NewBackupManager 创建备份管理器
func NewBackupManager(backupDir string) *BackupManager {
	// 确保备份目录存在
	os.MkdirAll(backupDir, 0755)

	return &BackupManager{
		metadata:  make(map[string]*BackupMetadata),
		backups:   make([]string, 0),
		backupDir: backupDir,
	}
}

// Backup 备份数据
func (bm *BackupManager) Backup(backupType BackupType, tables []string, data interface{}) (string, error) {
	if data == nil {
		return "", errors.New("backup data cannot be nil")
	}

	metadata := &BackupMetadata{
		ID:        generateBackupID(),
		Type:      backupType,
		Status:    BackupStatusRunning,
		StartTime: time.Now(),
		Tables:    tables,
	}

	bm.metadataLock.Lock()
	bm.metadata[metadata.ID] = metadata
	bm.metadataLock.Unlock()

	// 序列化数据
	jsonData, err := json.Marshal(data)
	if err != nil {
		metadata.Status = BackupStatusFailed
		metadata.Error = err.Error()
		metadata.EndTime = time.Now()
		return metadata.ID, err
	}

	// 压缩数据
	compressed, err := compressData(jsonData)
	if err != nil {
		metadata.Status = BackupStatusFailed
		metadata.Error = err.Error()
		metadata.EndTime = time.Now()
		return metadata.ID, err
	}

	// 写入文件
	filePath := filepath.Join(bm.backupDir, fmt.Sprintf("%s.backup.gz", metadata.ID))
	err = os.WriteFile(filePath, compressed, 0644)
	if err != nil {
		metadata.Status = BackupStatusFailed
		metadata.Error = err.Error()
		metadata.EndTime = time.Now()
		return metadata.ID, err
	}

	// 更新元数据
	metadata.Status = BackupStatusCompleted
	metadata.EndTime = time.Now()
	metadata.Size = int64(len(compressed))
	metadata.FilePath = filePath
	metadata.RecordCount = calculateRecordCount(data)

	// 计算校验和
	metadata.Checksum = calculateChecksum(compressed)

	bm.metadataLock.Lock()
	bm.backups = append(bm.backups, metadata.ID)
	bm.metadataLock.Unlock()

	return metadata.ID, nil
}

// Restore 恢复数据
func (bm *BackupManager) Restore(backupID string, data interface{}) error {
	bm.metadataLock.RLock()
	metadata, ok := bm.metadata[backupID]
	bm.metadataLock.RUnlock()

	if !ok {
		return errors.New("backup not found")
	}

	if metadata.Status != BackupStatusCompleted {
		return fmt.Errorf("backup not completed, status: %d", metadata.Status)
	}

	// 读取文件
	compressed, err := os.ReadFile(metadata.FilePath)
	if err != nil {
		return err
	}

	// 验证校验和
	if calculateChecksum(compressed) != metadata.Checksum {
		return errors.New("checksum verification failed")
	}

	// 解压数据
	jsonData, err := decompressData(compressed)
	if err != nil {
		return err
	}

	// 反序列化
	err = json.Unmarshal(jsonData, data)
	if err != nil {
		return err
	}

	return nil
}

// GetBackup 获取备份元数据
func (bm *BackupManager) GetBackup(backupID string) (*BackupMetadata, error) {
	bm.metadataLock.RLock()
	defer bm.metadataLock.RUnlock()

	metadata, ok := bm.metadata[backupID]
	if !ok {
		return nil, errors.New("backup not found")
	}

	return metadata, nil
}

// ListBackups 列出所有备份
func (bm *BackupManager) ListBackups() []*BackupMetadata {
	bm.metadataLock.RLock()
	defer bm.metadataLock.RUnlock()

	backups := make([]*BackupMetadata, 0, len(bm.backups))
	for _, id := range bm.backups {
		if metadata, ok := bm.metadata[id]; ok {
			backups = append(backups, metadata)
		}
	}

	return backups
}

// DeleteBackup 删除备份
func (bm *BackupManager) DeleteBackup(backupID string) error {
	bm.metadataLock.Lock()
	defer bm.metadataLock.Unlock()

	metadata, ok := bm.metadata[backupID]
	if !ok {
		return errors.New("backup not found")
	}

	// 删除文件
	if metadata.FilePath != "" {
		err := os.Remove(metadata.FilePath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// 删除元数据
	delete(bm.metadata, backupID)

	// 从列表中移除
	for i, id := range bm.backups {
		if id == backupID {
			bm.backups = append(bm.backups[:i], bm.backups[i+1:]...)
			break
		}
	}

	return nil
}

// CleanOldBackups 清理旧备份
func (bm *BackupManager) CleanOldBackups(olderThan time.Duration, keepCount int) error {
	bm.metadataLock.Lock()
	defer bm.metadataLock.Unlock()

	now := time.Now()
	toBeDeleted := make([]string, 0)

	// 标记过期的备份
	for _, metadata := range bm.metadata {
		if now.Sub(metadata.EndTime) > olderThan {
			toBeDeleted = append(toBeDeleted, metadata.ID)
		}
	}

	// 保留最近的N个备份
	if len(toBeDeleted) > 0 && keepCount > 0 {
		// 按时间排序
		allBackups := make([]*BackupMetadata, 0, len(bm.metadata))
		for _, metadata := range bm.metadata {
			if metadata.Status == BackupStatusCompleted {
				allBackups = append(allBackups, metadata)
			}
		}

		// 简单的冒泡排序（按时间降序）
		for i := 0; i < len(allBackups); i++ {
			for j := i + 1; j < len(allBackups); j++ {
				if allBackups[i].EndTime.Before(allBackups[j].EndTime) {
					allBackups[i], allBackups[j] = allBackups[j], allBackups[i]
				}
			}
		}

		// 保留最新的
		keepSet := make(map[string]bool)
		for i := 0; i < keepCount && i < len(allBackups); i++ {
			keepSet[allBackups[i].ID] = true
		}

		// 过滤掉需要保留的
		filtered := make([]string, 0, len(toBeDeleted))
		for _, id := range toBeDeleted {
			if !keepSet[id] {
				filtered = append(filtered, id)
			}
		}
		toBeDeleted = filtered
	}

	// 删除备份
	for _, id := range toBeDeleted {
		if metadata, ok := bm.metadata[id]; ok {
			if metadata.FilePath != "" {
				os.Remove(metadata.FilePath)
			}
			delete(bm.metadata, id)

			// 从列表中移除
			for i, backupID := range bm.backups {
				if backupID == id {
					bm.backups = append(bm.backups[:i], bm.backups[i+1:]...)
					break
				}
			}
		}
	}

	return nil
}

// GetBackupStats 获取备份统计信息
func (bm *BackupManager) GetBackupStats() (totalBackups int, totalSize int64, err error) {
	bm.metadataLock.RLock()
	defer bm.metadataLock.RUnlock()

	for _, metadata := range bm.metadata {
		if metadata.Status == BackupStatusCompleted {
			totalBackups++
			totalSize += metadata.Size
		}
	}

	return totalBackups, totalSize, nil
}

// compressData 压缩数据
func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompressData 解压数据
func decompressData(compressed []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// generateBackupID 生成备份ID
func generateBackupID() string {
	return fmt.Sprintf("backup_%d", time.Now().UnixNano())
}

// calculateRecordCount 计算记录数
func calculateRecordCount(data interface{}) int {
	if data == nil {
		return 0
	}

	// 简化实现，实际应该根据数据类型计算
	switch v := data.(type) {
	case map[string]interface{}:
		return len(v)
	case []interface{}:
		return len(v)
	case map[string][]interface{}:
		count := 0
		for _, rows := range v {
			count += len(rows)
		}
		return count
	default:
		return 1
	}
}

// calculateChecksum 计算校验和
func calculateChecksum(data []byte) string {
	sum := 0
	for _, b := range data {
		sum += int(b)
	}
	return fmt.Sprintf("%x", sum)
}

// ExportMetadata 导出备份元数据
func (bm *BackupManager) ExportMetadata() ([]byte, error) {
	bm.metadataLock.RLock()
	defer bm.metadataLock.RUnlock()

	return json.MarshalIndent(bm.metadata, "", "  ")
}

// ImportMetadata 导入备份元数据
func (bm *BackupManager) ImportMetadata(data []byte) error {
	bm.metadataLock.Lock()
	defer bm.metadataLock.Unlock()

	metadataMap := make(map[string]*BackupMetadata)
	err := json.Unmarshal(data, &metadataMap)
	if err != nil {
		return err
	}

	bm.metadata = metadataMap
	bm.backups = make([]string, 0, len(metadataMap))
	for id := range metadataMap {
		bm.backups = append(bm.backups, id)
	}

	return nil
}
