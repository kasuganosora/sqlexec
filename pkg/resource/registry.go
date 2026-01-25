package resource

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/csv"
	"github.com/kasuganosora/sqlexec/pkg/resource/excel"
	"github.com/kasuganosora/sqlexec/pkg/resource/json"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/resource/mysql"
	"github.com/kasuganosora/sqlexec/pkg/resource/parquet"
	"github.com/kasuganosora/sqlexec/pkg/resource/sqlite"
)

// init 注册所有数据源工厂
func init() {
	registry := application.GetRegistry()

	// 注册基础数据源工厂
	registry.Register(memory.NewMemoryFactory())

	// 注册SQL数据源工厂
	registry.Register(mysql.NewMySQLFactory())
	registry.Register(sqlite.NewSQLiteFactory())

	// 注册文件数据源工厂
	registry.Register(csv.NewCSVFactory())
	registry.Register(json.NewJSONFactory())
	registry.Register(excel.NewExcelFactory())
	registry.Register(parquet.NewParquetFactory())
}
