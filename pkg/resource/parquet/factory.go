package parquet

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ParquetFactory creates Parquet datasource instances.
type ParquetFactory struct{}

// NewParquetFactory creates a Parquet datasource factory.
func NewParquetFactory() *ParquetFactory {
	return &ParquetFactory{}
}

// GetType implements DataSourceFactory interface.
func (f *ParquetFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeParquet
}

// GetMetadata implements DataSourceFactory interface.
func (f *ParquetFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "Parquet columnar storage engine with MVCC, WAL persistence, and full index support",
		Transactions: "YES",
		XA:           "NO",
		Savepoints:   "NO",
	}
}

// Create implements DataSourceFactory interface.
func (f *ParquetFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	return NewParquetAdapter(config), nil
}
