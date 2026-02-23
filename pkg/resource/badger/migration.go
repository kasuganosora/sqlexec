package badger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MigrationManager handles data migration operations
type MigrationManager struct {
	ds *BadgerDataSource
	mu sync.RWMutex
}

// NewMigrationManager creates a new MigrationManager
func NewMigrationManager(ds *BadgerDataSource) *MigrationManager {
	return &MigrationManager{ds: ds}
}

// ExportConfig configuration for export operations
type ExportConfig struct {
	// Tables list of tables to export (empty = all tables)
	Tables []string `json:"tables"`

	// Format export format ("json", "jsonl")
	Format string `json:"format"`

	// IncludeSchema whether to include table schema
	IncludeSchema bool `json:"include_schema"`

	// IncludeData whether to include table data
	IncludeData bool `json:"include_data"`

	// Compression whether to compress output
	Compression bool `json:"compression"`
}

// DefaultExportConfig returns default export configuration
func DefaultExportConfig() *ExportConfig {
	return &ExportConfig{
		Format:        "json",
		IncludeSchema: true,
		IncludeData:   true,
		Compression:   false,
	}
}

// ExportData exports data to a writer
func (m *MigrationManager) ExportData(ctx context.Context, w io.Writer, config *ExportConfig) error {
	if config == nil {
		config = DefaultExportConfig()
	}

	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected {
		return fmt.Errorf("data source not connected")
	}

	// Determine tables to export
	tables := config.Tables
	if len(tables) == 0 {
		for name := range m.ds.tables {
			tables = append(tables, name)
		}
	}

	export := &ExportData{
		Version:    "1.0",
		ExportedAt: time.Now(),
		Tables:     make(map[string]*TableExport),
	}

	// Export each table
	for _, tableName := range tables {
		tableExport, err := m.exportTable(ctx, tableName, config)
		if err != nil {
			return fmt.Errorf("failed to export table %s: %w", tableName, err)
		}
		export.Tables[tableName] = tableExport
	}

	// Write to output
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(export)
}

// exportTable exports a single table
func (m *MigrationManager) exportTable(ctx context.Context, tableName string, config *ExportConfig) (*TableExport, error) {
	export := &TableExport{
		Name: tableName,
	}

	// Export schema
	if config.IncludeSchema {
		tableInfo, err := m.ds.GetTableInfo(ctx, tableName)
		if err != nil {
			return nil, err
		}
		export.Schema = tableInfo
	}

	// Export data
	if config.IncludeData {
		rows := make([]domain.Row, 0)
		err := m.ds.db.View(func(txn *badger.Txn) error {
			prefix := m.ds.keyEncoder.EncodeRowPrefix(tableName)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				var row domain.Row
				if err := item.Value(func(val []byte) error {
					var err error
					row, err = m.ds.rowCodec.Decode(val)
					return err
				}); err != nil {
					return err
				}
				if row != nil {
					rows = append(rows, row)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		export.Rows = rows
		export.RowCount = len(rows)
	}

	return export, nil
}

// ExportData represents exported data structure
type ExportData struct {
	Version    string                  `json:"version"`
	ExportedAt time.Time               `json:"exported_at"`
	Tables     map[string]*TableExport `json:"tables"`
}

// TableExport represents exported table data
type TableExport struct {
	Name     string            `json:"name"`
	Schema   *domain.TableInfo `json:"schema,omitempty"`
	Rows     []domain.Row      `json:"rows,omitempty"`
	RowCount int               `json:"row_count"`
}

// ImportConfig configuration for import operations
type ImportConfig struct {
	// Mode import mode ("create", "replace", "append")
	Mode string `json:"mode"`

	// BatchSize number of rows to import per batch
	BatchSize int `json:"batch_size"`
}

// DefaultImportConfig returns default import configuration
func DefaultImportConfig() *ImportConfig {
	return &ImportConfig{
		Mode:      "create",
		BatchSize: 1000,
	}
}

// ImportData imports data from a reader
func (m *MigrationManager) ImportData(ctx context.Context, r io.Reader, config *ImportConfig) error {
	if config == nil {
		config = DefaultImportConfig()
	}

	m.ds.mu.RLock()
	if !m.ds.connected {
		m.ds.mu.RUnlock()
		return fmt.Errorf("data source not connected")
	}
	m.ds.mu.RUnlock()

	// Parse import data
	var exportData ExportData
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&exportData); err != nil {
		return fmt.Errorf("failed to parse import data: %w", err)
	}

	// Import each table
	for tableName, tableExport := range exportData.Tables {
		if err := m.importTable(ctx, tableName, tableExport, config); err != nil {
			return fmt.Errorf("failed to import table %s: %w", tableName, err)
		}
	}

	return nil
}

// importTable imports a single table
func (m *MigrationManager) importTable(ctx context.Context, tableName string, tableExport *TableExport, config *ImportConfig) error {
	// Handle schema
	if tableExport.Schema != nil {
		switch config.Mode {
		case "replace":
			// Drop existing table if exists
			m.ds.mu.RLock()
			_, exists := m.ds.tables[tableName]
			m.ds.mu.RUnlock()

			if exists {
				if err := m.ds.DropTable(ctx, tableName); err != nil {
					return err
				}
			}
			fallthrough
		case "create":
			// Create table
			if err := m.ds.CreateTable(ctx, tableExport.Schema); err != nil {
				return err
			}
		case "append":
			// Schema must exist for append mode
			m.ds.mu.RLock()
			_, exists := m.ds.tables[tableName]
			m.ds.mu.RUnlock()

			if !exists {
				return fmt.Errorf("table %s does not exist for append mode", tableName)
			}
		}
	}

	// Handle data
	if len(tableExport.Rows) > 0 {
		batchSize := config.BatchSize
		if batchSize <= 0 {
			batchSize = 1000
		}

		for i := 0; i < len(tableExport.Rows); i += batchSize {
			end := i + batchSize
			if end > len(tableExport.Rows) {
				end = len(tableExport.Rows)
			}

			batch := tableExport.Rows[i:end]
			if _, err := m.ds.Insert(ctx, tableName, batch, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// ExportToFile exports data to a file
func (m *MigrationManager) ExportToFile(ctx context.Context, filePath string, config *ExportConfig) error {
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	return m.ExportData(ctx, f, config)
}

// ImportFromFile imports data from a file
func (m *MigrationManager) ImportFromFile(ctx context.Context, filePath string, config *ImportConfig) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	return m.ImportData(ctx, f, config)
}

// Snapshot creates a database snapshot
func (m *MigrationManager) Snapshot(ctx context.Context, dir string) error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected {
		return fmt.Errorf("data source not connected")
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// For in-memory mode, use export
	if m.ds.badgerCfg.InMemory {
		filePath := filepath.Join(dir, "snapshot.json")
		return m.ExportToFile(ctx, filePath, &ExportConfig{
			IncludeSchema: true,
			IncludeData:   true,
			Format:        "json",
		})
	}

	// For persistent mode, use Badger backup
	backupFile := filepath.Join(dir, "badger.bak")
	f, err := os.Create(backupFile)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	_, err = m.ds.db.Backup(f, 0)
	return err
}

// Restore restores database from a snapshot
func (m *MigrationManager) Restore(ctx context.Context, dir string) error {
	m.ds.mu.Lock()
	defer m.ds.mu.Unlock()

	if !m.ds.connected {
		return fmt.Errorf("data source not connected")
	}

	// For in-memory mode, use import
	if m.ds.badgerCfg.InMemory {
		filePath := filepath.Join(dir, "snapshot.json")
		if _, err := os.Stat(filePath); err == nil {
			return m.ImportFromFile(ctx, filePath, &ImportConfig{
				Mode: "replace",
			})
		}
		return fmt.Errorf("snapshot file not found: %s", filePath)
	}

	// For persistent mode, use Badger restore
	backupFile := filepath.Join(dir, "badger.bak")
	if _, err := os.Stat(backupFile); err != nil {
		return fmt.Errorf("backup file not found: %s", backupFile)
	}

	f, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer f.Close()

	return m.ds.db.Load(f, 100)
}
