package filemeta

import (
	"encoding/gob"
	"errors"
	"io/fs"
	"os"
)

// FileMeta holds both schema and index information for a file datasource.
// It is serialized as a gob-encoded sidecar file alongside the data file.
type FileMeta struct {
	Schema  SchemaMeta
	Indexes []IndexMeta
}

// SchemaMeta stores table column definitions.
type SchemaMeta struct {
	TableName string
	Columns   []ColumnMeta
}

// ColumnMeta stores a single column definition.
type ColumnMeta struct {
	Name     string
	Type     string
	Nullable bool
}

// IndexMeta stores a single index definition.
type IndexMeta struct {
	Name    string
	Table   string
	Type    string // "btree", "hash", "fulltext", "spatial_rtree"
	Unique  bool
	Columns []string
}

// MetaPath returns the sidecar metadata path for a data file.
// Convention: <dataFile>.sqlexec_meta
func MetaPath(dataFilePath string) string {
	return dataFilePath + ".sqlexec_meta"
}

// Save serializes FileMeta to disk using gob encoding.
func Save(metaPath string, meta *FileMeta) error {
	f, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(meta)
}

// Load deserializes FileMeta from disk. Returns nil, nil if file doesn't exist.
func Load(metaPath string) (*FileMeta, error) {
	f, err := os.Open(metaPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var meta FileMeta
	if err := gob.NewDecoder(f).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}
