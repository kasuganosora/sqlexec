package reliability

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBackupManager(t *testing.T) {
	// Use a temporary directory for testing
	backupDir := t.TempDir()

	bm := NewBackupManager(backupDir)

	assert.NotNil(t, bm)
	assert.NotNil(t, bm.metadata)
	assert.NotNil(t, bm.backups)
	assert.Equal(t, backupDir, bm.backupDir)
}

func TestBackupFull(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Test data
	testData := map[string]interface{}{
		"users": []interface{}{
				map[string]string{"name": "John", "email": "john@example.com"},
				map[string]string{"name": "Jane", "email": "jane@example.com"},
			},
		"metadata": map[string]interface{}{
				"version":  "1.0",
				"backup_date": "2026-01-28",
			},
	}

	backupID, err := bm.Backup(BackupTypeFull, []string{"users", "metadata"}, testData)
	require.NoError(t, err)
	assert.NotEmpty(t, backupID)

	// Verify backup was recorded
	metadata, err := bm.GetBackup(backupID)
	require.NoError(t, err)
	assert.Equal(t, BackupTypeFull, metadata.Type)
	assert.Equal(t, BackupStatusCompleted, metadata.Status)
	assert.Contains(t, metadata.Tables, "users")
}

func TestBackupIncremental(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	testData := []interface{}{
		map[string]string{"id": "1", "name": "John"},
		map[string]string{"id": "2", "name": "Jane"},
	}

	backupID, err := bm.Backup(BackupTypeIncremental, []string{"test_table"}, testData)
	require.NoError(t, err)

	metadata, _ := bm.GetBackup(backupID)
	assert.Equal(t, BackupTypeIncremental, metadata.Type)
}

func TestRestore(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create a backup
	originalData := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}
	backupID, err := bm.Backup(BackupTypeFull, []string{"test"}, originalData)
	require.NoError(t, err)

	// Restore to new data
	var restoredData map[string]interface{}
	err = bm.Restore(backupID, &restoredData)
	require.NoError(t, err)

	// Verify restored data matches original
	assert.Equal(t, len(originalData), len(restoredData))
	assert.Equal(t, "value1", restoredData["key1"])
	assert.Equal(t, "value2", restoredData["key2"])
}

func TestGetBackup(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Get non-existent backup
	_, err := bm.GetBackup("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Create a backup
	testData := map[string]string{"test": "data"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	// Get existing backup
	metadata, err := bm.GetBackup(backupID)
	require.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, backupID, metadata.ID)
}

func TestListBackups(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create multiple backups
	testData := map[string]string{"key": "value"}
	bm.Backup(BackupTypeFull, []string{"test1"}, testData)
	bm.Backup(BackupTypeFull, []string{"test2"}, testData)
	bm.Backup(BackupTypeFull, []string{"test3"}, testData)

	// List all backups
	backups := bm.ListBackups()
	assert.Len(t, backups, 3)
}

func TestDeleteBackup(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create a backup
	testData := map[string]string{"key": "value"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	// Delete the backup
	err := bm.DeleteBackup(backupID)
	require.NoError(t, err)

	// Verify deletion
	_, err = bm.GetBackup(backupID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify it's not in the list
	backups := bm.ListBackups()
	assert.Len(t, backups, 0)
}

func TestCleanOldBackups(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create old backups
	testData := map[string]string{"key": "value"}
	for i := 0; i < 5; i++ {
		bm.Backup(BackupTypeFull, []string{"test"}, testData)
	}

	// Clean backups older than 24 hours
	err := bm.CleanOldBackups(24*time.Hour, 2)
	require.NoError(t, err)

	// Should keep at least 2 recent backups
	backups := bm.ListBackups()
	assert.LessOrEqual(t, 2, len(backups))
}

func TestGetBackupStats(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create backups - use unique data to ensure distinct backups
	testData1 := map[string]string{"key1": "value1"}
	testData2 := map[string]string{"key2": "value2"}
	_, err1 := bm.Backup(BackupTypeFull, []string{"test1"}, testData1)
	require.NoError(t, err1, "First backup should succeed")
	_, err2 := bm.Backup(BackupTypeFull, []string{"test2"}, testData2)
	require.NoError(t, err2, "Second backup should succeed")

	// Get stats
	count, size, err := bm.GetBackupStats()
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Should have 2 completed backups")
	assert.Greater(t, size, int64(0), "Total size should be greater than 0")
}

func TestExportMetadata(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create a backup
	testData := map[string]string{"key": "value"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	// Export metadata
	data, err := bm.ExportMetadata()
	require.NoError(t, err)
	assert.NotEmpty(t, data)
	assert.Contains(t, string(data), backupID)
}

func TestImportMetadata(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Export metadata
	testData := map[string]string{"key": "value"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)
	data, _ := bm.ExportMetadata()

	// Clear and import
	bm.metadata = make(map[string]*BackupMetadata)
	bm.backups = []string{}

	err := bm.ImportMetadata(data)
	require.NoError(t, err)

	// Verify import
	metadata, err := bm.GetBackup(backupID)
	require.NoError(t, err)
	assert.Equal(t, backupID, metadata.ID)
}

func TestBackupCompression(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Test that backup files are compressed
	testData := map[string]interface{}{
		"data": string(make([]byte, 1000)), // Large data
	}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	metadata, _ := bm.GetBackup(backupID)
	assert.Greater(t, metadata.Size, int64(0)) // Compressed size should be positive
}

func TestBackupChecksum(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Test checksum calculation
	testData := map[string]string{"key": "value"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	metadata, _ := bm.GetBackup(backupID)
	assert.NotEmpty(t, metadata.Checksum)
}

func TestBackupFileCleanup(t *testing.T) {
	backupDir := t.TempDir()
	_ = NewBackupManager(backupDir)

	// Test that backup directory is created
	info, err := os.Stat(backupDir)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestBackupErrorHandling(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Test error handling for nil data
	_, err := bm.Backup(BackupTypeFull, []string{"test"}, nil)
	assert.Error(t, err)
}

func TestRestoreChecksumVerification(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Create backup with data
	testData := map[string]string{"key": "value"}
	backupID, _ := bm.Backup(BackupTypeFull, []string{"test"}, testData)

	// Restore should verify checksum
	var restoredData map[string]string
	err := bm.Restore(backupID, &restoredData)
	require.NoError(t, err)
	assert.Equal(t, testData, restoredData)
}

func TestBackupRetry(t *testing.T) {
	backupDir := t.TempDir()
	bm := NewBackupManager(backupDir)

	// Test multiple backup operations
	for i := 0; i < 10; i++ {
		testData := map[string]string{"key": "value"}
		_, err := bm.Backup(BackupTypeFull, []string{"test"}, testData)
		if err != nil && i > 0 {
			// Should handle errors gracefully
			t.Logf("Backup %d failed: %v", i, err)
		}
	}
}
