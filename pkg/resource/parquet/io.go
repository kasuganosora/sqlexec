package parquet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// readParquetFile reads a native .parquet file and returns schema and rows.
func readParquetFile(filePath string) (*domain.TableInfo, []domain.Row, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open parquet file %q: %w", filePath, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat parquet file %q: %w", filePath, err)
	}

	pf, err := pq.OpenFile(f, stat.Size())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open parquet file %q: %w", filePath, err)
	}

	schema := pf.Schema()
	columns := parquetSchemaToDomain(schema)

	// Derive table name from filename (without extension)
	tableName := strings.TrimSuffix(filepath.Base(filePath), ".parquet")

	tableInfo := &domain.TableInfo{
		Name:    tableName,
		Schema:  "default",
		Columns: columns,
	}

	// Read all rows
	reader := pq.NewReader(f)
	defer reader.Close()

	var rows []domain.Row
	pqRows := make([]pq.Row, 128)

	for {
		n, err := reader.ReadRows(pqRows)
		for i := 0; i < n; i++ {
			row := parquetRowToDomain(columns, pqRows[i])
			rows = append(rows, row)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("failed to read rows from %q: %w", filePath, err)
		}
	}

	return tableInfo, rows, nil
}

// parquetRowToDomain converts a parquet.Row to a domain.Row.
func parquetRowToDomain(columns []domain.ColumnInfo, row pq.Row) domain.Row {
	result := make(domain.Row, len(columns))
	for i, col := range columns {
		if i < len(row) {
			result[col.Name] = parquetValueToGo(col, row[i])
		}
	}
	return result
}

// writeParquetFile writes schema + rows to a native .parquet file atomically.
func writeParquetFile(filePath string, schema *domain.TableInfo, rows []domain.Row, compression string) error {
	dir := filepath.Dir(filePath)

	// Create temp file in the same directory for atomic rename
	tmpFile, err := os.CreateTemp(dir, ".parquet_tmp_*.parquet")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Clean up on failure
	success := false
	defer func() {
		if !success {
			tmpFile.Close()
			os.Remove(tmpPath)
		}
	}()

	// Build parquet schema and writer options
	pqSchema := domainSchemaToParquet(schema.Name, schema.Columns)
	writerOpts := []pq.WriterOption{pqSchema}
	if codec := compressionCodec(compression); codec != nil {
		writerOpts = append(writerOpts, pq.Compression(codec))
	}

	writer := pq.NewGenericWriter[map[string]interface{}](tmpFile, writerOpts...)

	// Write rows in batches
	if len(rows) > 0 {
		batch := make([]map[string]interface{}, 0, min(1024, len(rows)))
		for _, row := range rows {
			batch = append(batch, map[string]interface{}(row))
			if len(batch) >= 1024 {
				if _, err := writer.Write(batch); err != nil {
					return fmt.Errorf("failed to write rows: %w", err)
				}
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			if _, err := writer.Write(batch); err != nil {
				return fmt.Errorf("failed to write rows: %w", err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	success = true
	return nil
}

// compressionCodec returns the parquet compression codec for a given name.
func compressionCodec(name string) compress.Codec {
	switch strings.ToLower(name) {
	case "snappy":
		return &pq.Snappy
	case "gzip":
		return &pq.Gzip
	case "zstd":
		return &pq.Zstd
	case "lz4":
		return &pq.Lz4Raw
	case "none", "uncompressed", "":
		return nil
	default:
		return &pq.Snappy
	}
}
