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
	registry.Register(memory.NewMemoryFactory())

	// 注册文件数据源工厂
	registry.Register(csv.NewCSVFactory())
	registry.Register(json.NewJSONFactory())
	registry.Register(jsonl.NewJSONLFactory())
	registry.Register(excel.NewExcelFactory())
	registry.Register(parquet.NewParquetFactory())

	// 注册目录数据源工厂
	registry.Register(xmlds.NewXMLFactory())

	// 注册内存数据适配器工厂
	registry.Register(slice.NewFactory())
}
