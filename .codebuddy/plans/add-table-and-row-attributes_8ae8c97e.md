---
name: add-table-and-row-attributes
overview: 为表和行添加属性字段 ats，表属性体现在 information_schema.tables 的 JSON 字段中，行属性作为隐藏属性供内部使用
todos:
  - id: add-table-atts-field
    content: 在 domain.TableInfo 中添加 Atts 字段
    status: completed
  - id: add-table-attributes-column
    content: 在 information_schema.tables 的 GetSchema 中添加 table_attributes 字段
    status: completed
    dependencies:
      - add-table-atts-field
  - id: update-tables-query
    content: 更新 TablesTable.Query() 序列化表属性到 table_attributes
    status: completed
    dependencies:
      - add-table-attributes-column
  - id: update-mvcc-datasource
    content: 确保 MVCCDataSource 在 CreateTable 和 GetTableInfo 中正确处理 Atts 字段
    status: completed
    dependencies:
      - add-table-atts-field
  - id: add-row-atts-helper
    content: 为 domain.Row 添加行属性辅助函数（内部使用）
    status: completed
---

## 产品概述

为表和行添加属性扩展功能，提供元数据存储能力，支持数据源、查询器、优化器和GC等内部组件使用。

## 核心功能

- 表属性：在 TableInfo 中添加 Atts 字段 (map[string]any)，在 information_schema.tables 中通过 table_attributes JSON 字段对外暴露
- 行属性：为 Row 类型添加内部属性支持，不对外暴露，仅用于数据源、查询器、优化器、GC等内部组件
- JSON序列化：支持表属性的JSON序列化和反序列化

## 技术栈

- 语言：Go
- JSON处理：encoding/json (已在项目中使用)

## 技术架构

### 系统架构

- 数据模型层：扩展 domain.TableInfo 和 domain.Row 类型
- 信息模式层：更新 information_schema.tables 的 schema 和查询逻辑
- 数据存储层：确保 MVCCDataSource 正确处理表属性

### 模块划分

- **Domain Models**: 表和行属性的核心数据结构
- **Information Schema**: 表属性的外部展示接口
- **Storage**: 表属性和行属性的持久化支持

### 数据流

表属性设置 → TableInfo.Atts → MVCCDataSource.CreateTable/GetTableInfo → TablesTable.Query → information_schema.tables (JSON)
行属性设置 → 内部存储 (不对外暴露)

## 实现细节

### 核心目录结构

```
d:/code/db/pkg/
├── resource/domain/
│   └── models.go                      # [MODIFY] 在 TableInfo 中添加 Atts 字段，为 Row 添加属性支持
├── information_schema/
│   └── tables.go                      # [MODIFY] 在 GetSchema() 中添加 table_attributes 字段，在 Query() 中序列化表属性
└── resource/memory/
    └── mvcc_datasource.go            # [MODIFY] 确保 CreateTable 和 GetTableInfo 正确处理 Atts 字段
```

### 关键代码实现

#### 表属性在 TableInfo 中添加

```
// TableInfo 表信息
type TableInfo struct {
    Name       string                 `json:"name"`
    Schema     string                 `json:"schema,omitempty"`
    Columns    []ColumnInfo            `json:"columns"`
    Temporary  bool                   `json:"temporary,omitempty"` // 是否是临时表
    Atts       map[string]interface{}  `json:"atts,omitempty"`       // 表属性
}
```

#### 行属性使用内部键名

```
// 内部保留键名，不对外暴露
const (
    rowAttsKey = "__atts__" // 行属性键名，保留字段，禁止用户访问
)

// SetRowAttributes 设置行属性（内部使用）
func SetRowAttributes(row Row, atts map[string]any) {
    row[rowAttsKey] = atts
}

// GetRowAttributes 获取行属性（内部使用）
func GetRowAttributes(row Row) (map[string]any, bool) {
    atts, ok := row[rowAttsKey].(map[string]any)
    return atts, ok
}
```

#### information_schema.tables 添加字段

```
func (t *TablesTable) GetSchema() []domain.ColumnInfo {
    return []domain.ColumnInfo{
        // ... 现有字段 ...
        {Name: "table_attributes", Type: "json", Nullable: true},
    }
}
```

### 执行说明

- 向后兼容：Atts 字段为可选字段，不影响现有代码
- 性能考虑：table_attributes 使用 lazy evaluation，仅在查询时序列化
- 日志记录：无需额外日志，这是纯数据结构扩展
- 爆炸半径控制：最小化变更，仅扩展数据结构，不修改核心逻辑