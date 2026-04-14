package resource

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/csv"
	"github.com/kasuganosora/sqlexec/pkg/resource/excel"
	"github.com/kasuganosora/sqlexec/pkg/resource/json"
	"github.com/kasuganosora/sqlexec/pkg/resource/jsonl"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/resource/parquet"
	"github.com/kasuganosora/sqlexec/pkg/resource/slice"
	xmlds "github.com/kasuganosora/sqlexec/pkg/resource/xml"
)

// init 注册所有数据源工厂
func init() {
	registry := application.GetRegistry()

	// 注册基础数据源工厂
	_ = registry.Register(memory.NewMemoryFactory())

	// 注册文件数据源工厂
	_ = registry.Register(csv.NewCSVFactory())
	_ = registry.Register(json.NewJSONFactory())
	_ = registry.Register(jsonl.NewJSONLFactory())
	_ = registry.Register(excel.NewExcelFactory())
	_ = registry.Register(parquet.NewParquetFactory())

	// 注册目录数据源工厂
	_ = registry.Register(xmlds.NewXMLFactory())

	// 注册内存数据适配器工厂
	_ = registry.Register(slice.NewFactory())
}
