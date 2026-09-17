package parquet

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomainTypeToParquetAndBack(t *testing.T) {
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "count", Type: "int", Nullable: true},
		{Name: "score", Type: "float64", Nullable: true},
		{Name: "ratio", Type: "float32", Nullable: true},
		{Name: "active", Type: "bool", Nullable: false},
		{Name: "name", Type: "varchar", Nullable: true},
		{Name: "blob", Type: "bytes", Nullable: true},
		{Name: "unknown", Type: "mystery", Nullable: true},
	}

	schema := domainSchemaToParquet("t", columns)
	require.NotNil(t, schema)

	got := parquetSchemaToDomain(schema)
	require.Len(t, got, len(columns))

	byName := make(map[string]domain.ColumnInfo, len(got))
	for _, col := range got {
		byName[col.Name] = col
	}

	assert.Equal(t, "int64", byName["id"].Type)
	assert.False(t, byName["id"].Nullable)
	assert.Equal(t, "int32", byName["count"].Type)
	assert.True(t, byName["count"].Nullable)
	assert.Equal(t, "float64", byName["score"].Type)
	assert.Equal(t, "float32", byName["ratio"].Type)
	assert.Equal(t, "bool", byName["active"].Type)
	assert.Equal(t, "string", byName["name"].Type)
	assert.Equal(t, "bytes", byName["blob"].Type)
	assert.Equal(t, "string", byName["unknown"].Type)
}
