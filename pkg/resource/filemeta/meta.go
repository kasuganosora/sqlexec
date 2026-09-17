package filemeta

import (
	"encoding/gob"
	"errors"
	"io/fs"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
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
	Name       string
	Table      string
	Type       string // "btree", "hash", "fulltext", "spatial_rtree", "vector_hnsw", ...
	Unique     bool
	Columns    []string
	IsVector   bool
	Metric     string
	Dimension  int
	ParamsJSON string
}

func FromDomainIndex(idx domain.IndexMetaInfo) IndexMeta {
	return IndexMeta{
		Name:       idx.Name,
		Table:      idx.Table,
		Type:       idx.Type,
		Unique:     idx.Unique,
		Columns:    idx.Columns,
		IsVector:   idx.IsVector,
		Metric:     idx.Metric,
		Dimension:  idx.Dimension,
		ParamsJSON: idx.ParamsJSON,
	}
}

func (m IndexMeta) ToDomain() domain.IndexMetaInfo {
	return domain.IndexMetaInfo{
		Name:       m.Name,
		Table:      m.Table,
		Type:       m.Type,
		Unique:     m.Unique,
		Columns:    m.Columns,
		IsVector:   m.IsVector,
		Metric:     m.Metric,
		Dimension:  m.Dimension,
		ParamsJSON: m.ParamsJSON,
	}
}

func IndexesFromDomain(indexes []domain.IndexMetaInfo) []IndexMeta {
	out := make([]IndexMeta, len(indexes))
	for i, idx := range indexes {
		out[i] = FromDomainIndex(idx)
	}
	return out
}

func IndexesToDomain(indexes []IndexMeta) []domain.IndexMetaInfo {
	out := make([]domain.IndexMetaInfo, len(indexes))
	for i, idx := range indexes {
		out[i] = idx.ToDomain()
	}
	return out
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
	defer func() { _ = f.Close() }()

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
	defer func() { _ = f.Close() }()

	var meta FileMeta
	if err := gob.NewDecoder(f).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}
