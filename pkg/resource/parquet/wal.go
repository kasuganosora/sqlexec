package parquet

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// WALEntryType represents the type of a WAL entry.
type WALEntryType uint8

const (
	WALInsert       WALEntryType = iota + 1
	WALUpdate
	WALDelete
	WALCreateTable
	WALDropTable
	WALTruncateTable
	WALCheckpoint // Marks that all previous entries have been flushed
)

// WALEntry represents a single entry in the write-ahead log.
type WALEntry struct {
	Type      WALEntryType
	TableName string
	Rows      []domain.Row     // Insert
	Filters   []domain.Filter  // Update/Delete
	Updates   domain.Row       // Update
	Schema    *domain.TableInfo // CreateTable
}

// WAL manages a write-ahead log for crash recovery.
type WAL struct {
	filePath string
	file     *os.File
	encoder  *gob.Encoder
	mu       sync.Mutex
}

const walFileName = ".wal"

// walPath returns the WAL file path for a given directory.
func walPath(dirPath string) string {
	return filepath.Join(dirPath, walFileName)
}

// newWAL creates or opens a WAL file.
func newWAL(dirPath string) (*WAL, error) {
	path := walPath(dirPath)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %q: %w", path, err)
	}

	return &WAL{
		filePath: path,
		file:     f,
		encoder:  gob.NewEncoder(f),
	}, nil
}

// Append writes an entry to the WAL and fsyncs.
func (w *WAL) Append(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to encode WAL entry: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync WAL: %w", err)
	}

	return nil
}

// ReadAll reads all entries from the WAL file.
// Only entries after the last checkpoint are returned.
func ReadAll(dirPath string) ([]WALEntry, error) {
	path := walPath(dirPath)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open WAL file %q: %w", path, err)
	}
	defer f.Close()

	var entries []WALEntry
	decoder := gob.NewDecoder(f)

	for {
		var entry WALEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			// Partial entry at end of file (crash during write) - stop reading
			break
		}

		if entry.Type == WALCheckpoint {
			// Checkpoint: discard all previous entries
			entries = entries[:0]
			continue
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Truncate truncates the WAL file, discarding all entries.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL for truncation: %w", err)
	}

	// Truncate by recreating
	f, err := os.Create(w.filePath)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	w.file = f
	w.encoder = gob.NewEncoder(f)
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
