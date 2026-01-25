package domain

import (
	"testing"
)

// TestDataSourceType_String 测试DataSourceType的String方法
func TestDataSourceType_String(t *testing.T) {
	tests := []struct {
		name     string
		dataType DataSourceType
		want     string
	}{
		{"memory type", DataSourceTypeMemory, "memory"},
		{"mysql type", DataSourceTypeMySQL, "mysql"},
		{"postgresql type", DataSourceTypePostgreSQL, "postgresql"},
		{"sqlite type", DataSourceTypeSQLite, "sqlite"},
		{"csv type", DataSourceTypeCSV, "csv"},
		{"excel type", DataSourceTypeExcel, "excel"},
		{"json type", DataSourceTypeJSON, "json"},
		{"parquet type", DataSourceTypeParquet, "parquet"},
		{"custom type", DataSourceType("custom"), "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dataType.String(); got != tt.want {
				t.Errorf("DataSourceType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDataSourceConfig_Create 测试DataSourceConfig创建
func TestDataSourceConfig_Create(t *testing.T) {
	config := &DataSourceConfig{
		Type:     DataSourceTypeMemory,
		Name:     "test-db",
		Host:     "localhost",
		Port:     3306,
		Username: "user",
		Password: "pass",
		Database: "testdb",
		Writable:  true,
		Options:  map[string]interface{}{"key": "value"},
	}

	if config.Type != DataSourceTypeMemory {
		t.Errorf("Expected type %v, got %v", DataSourceTypeMemory, config.Type)
	}
	if config.Name != "test-db" {
		t.Errorf("Expected name 'test-db', got %v", config.Name)
	}
	if !config.Writable {
		t.Errorf("Expected writable to be true")
	}
	if config.Options == nil {
		t.Errorf("Expected options to be set")
	}
}

// TestTableInfo_Create 测试TableInfo创建
func TestTableInfo_Create(t *testing.T) {
	tableInfo := &TableInfo{
		Name:   "users",
		Schema: "public",
		Columns: []ColumnInfo{
			{
				Name:         "id",
				Type:         "int64",
				Nullable:     false,
				Primary:      true,
				AutoIncrement: true,
			},
			{
				Name:     "name",
				Type:     "string",
				Nullable: false,
				Unique:   true,
			},
			{
				Name:     "email",
				Type:     "string",
				Nullable: true,
			},
		},
	}

	if tableInfo.Name != "users" {
		t.Errorf("Expected table name 'users', got %v", tableInfo.Name)
	}
	if len(tableInfo.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(tableInfo.Columns))
	}
	if !tableInfo.Columns[0].Primary {
		t.Errorf("Expected first column to be primary key")
	}
	if !tableInfo.Columns[1].Unique {
		t.Errorf("Expected second column to be unique")
	}
}

// TestColumnInfo_CreateWithForeignKey 测试带外键的ColumnInfo
func TestColumnInfo_CreateWithForeignKey(t *testing.T) {
	fkInfo := &ForeignKeyInfo{
		Table:    "departments",
		Column:   "id",
		OnDelete: "CASCADE",
		OnUpdate: "RESTRICT",
	}

	column := ColumnInfo{
		Name:       "department_id",
		Type:       "int64",
		Nullable:   true,
		ForeignKey: fkInfo,
	}

	if column.Name != "department_id" {
		t.Errorf("Expected column name 'department_id', got %v", column.Name)
	}
	if column.ForeignKey == nil {
		t.Errorf("Expected foreign key to be set")
	}
	if column.ForeignKey.Table != "departments" {
		t.Errorf("Expected foreign key table 'departments', got %v", column.ForeignKey.Table)
	}
	if column.ForeignKey.OnDelete != "CASCADE" {
		t.Errorf("Expected OnDelete CASCADE, got %v", column.ForeignKey.OnDelete)
	}
}

// TestRow_Operations 测试Row操作
func TestRow_Operations(t *testing.T) {
	row := Row{
		"id":    1,
		"name":  "Alice",
		"email": "alice@example.com",
	}

	// 测试获取值
	if id, ok := row["id"].(int); !ok || id != 1 {
		t.Errorf("Expected id to be 1, got %v", row["id"])
	}

	if name, ok := row["name"].(string); !ok || name != "Alice" {
		t.Errorf("Expected name to be 'Alice', got %v", row["name"])
	}

	// 测试设置新值
	row["age"] = 30
	if age, ok := row["age"].(int); !ok || age != 30 {
		t.Errorf("Expected age to be 30, got %v", row["age"])
	}

	// 测试长度
	if len(row) != 4 {
		t.Errorf("Expected 4 fields in row, got %d", len(row))
	}
}

// TestQueryResult_Create 测试QueryResult创建
func TestQueryResult_Create(t *testing.T) {
	result := &QueryResult{
		Columns: []ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
		Rows: []Row{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
		},
		Total: 2,
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
	if result.Total != 2 {
		t.Errorf("Expected total 2, got %d", result.Total)
	}
}

// TestFilter_CreateSimple 测试简单Filter创建
func TestFilter_CreateSimple(t *testing.T) {
	filter := Filter{
		Field:    "age",
		Operator: ">",
		Value:    18,
	}

	if filter.Field != "age" {
		t.Errorf("Expected field 'age', got %v", filter.Field)
	}
	if filter.Operator != ">" {
		t.Errorf("Expected operator '>', got %v", filter.Operator)
	}
	if filter.Value != 18 {
		t.Errorf("Expected value 18, got %v", filter.Value)
	}
}

// TestFilter_CreateWithSubFilters 测试带子过滤器的Filter
func TestFilter_CreateWithSubFilters(t *testing.T) {
	filter := Filter{
		Field:   "",
		Operator: "",
		LogicOp: "AND",
		SubFilters: []Filter{
			{Field: "age", Operator: ">", Value: 18},
			{Field: "age", Operator: "<", Value: 65},
		},
	}

	if filter.LogicOp != "AND" {
		t.Errorf("Expected LogicOp 'AND', got %v", filter.LogicOp)
	}
	if len(filter.SubFilters) != 2 {
		t.Errorf("Expected 2 sub filters, got %d", len(filter.SubFilters))
	}
}

// TestQueryOptions_Create 测试QueryOptions创建
func TestQueryOptions_Create(t *testing.T) {
	options := &QueryOptions{
		Filters: []Filter{
			{Field: "status", Operator: "=", Value: "active"},
		},
		OrderBy:   "created_at",
		Order:     "DESC",
		Limit:     10,
		Offset:    0,
		SelectAll: false,
		SelectColumns: []string{"id", "name", "status"},
	}

	if len(options.Filters) != 1 {
		t.Errorf("Expected 1 filter, got %d", len(options.Filters))
	}
	if options.OrderBy != "created_at" {
		t.Errorf("Expected OrderBy 'created_at', got %v", options.OrderBy)
	}
	if options.Limit != 10 {
		t.Errorf("Expected Limit 10, got %d", options.Limit)
	}
	if options.SelectAll {
		t.Errorf("Expected SelectAll to be false")
	}
	if len(options.SelectColumns) != 3 {
		t.Errorf("Expected 3 select columns, got %d", len(options.SelectColumns))
	}
}

// TestInsertOptions_Create 测试InsertOptions创建
func TestInsertOptions_Create(t *testing.T) {
	options := &InsertOptions{
		Replace: true,
	}

	if !options.Replace {
		t.Errorf("Expected Replace to be true")
	}
}

// TestUpdateOptions_Create 测试UpdateOptions创建
func TestUpdateOptions_Create(t *testing.T) {
	options := &UpdateOptions{
		Upsert: true,
	}

	if !options.Upsert {
		t.Errorf("Expected Upsert to be true")
	}
}

// TestDeleteOptions_Create 测试DeleteOptions创建
func TestDeleteOptions_Create(t *testing.T) {
	options := &DeleteOptions{
		Force: true,
	}

	if !options.Force {
		t.Errorf("Expected Force to be true")
	}
}

// TestTransactionOptions_Create 测试TransactionOptions创建
func TestTransactionOptions_Create(t *testing.T) {
	options := &TransactionOptions{
		IsolationLevel: "READ COMMITTED",
		ReadOnly:       true,
	}

	if options.IsolationLevel != "READ COMMITTED" {
		t.Errorf("Expected IsolationLevel 'READ COMMITTED', got %v", options.IsolationLevel)
	}
	if !options.ReadOnly {
		t.Errorf("Expected ReadOnly to be true")
	}
}

// TestConstraintType_Constants 测试ConstraintType常量
func TestConstraintType_Constants(t *testing.T) {
	tests := []struct {
		name  string
		ct    ConstraintType
		value string
	}{
		{"unique constraint", ConstraintTypeUnique, "unique"},
		{"foreign key constraint", ConstraintTypeForeignKey, "foreign_key"},
		{"check constraint", ConstraintTypeCheck, "check"},
		{"primary key constraint", ConstraintTypePrimaryKey, "primary_key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.ct) != tt.value {
				t.Errorf("Expected %v, got %v", tt.value, string(tt.ct))
			}
		})
	}
}

// TestIndexType_Constants 测试IndexType常量
func TestIndexType_Constants(t *testing.T) {
	tests := []struct {
		name  string
		it    IndexType
		value string
	}{
		{"btree index", IndexTypeBTree, "btree"},
		{"hash index", IndexTypeHash, "hash"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.it) != tt.value {
				t.Errorf("Expected %v, got %v", tt.value, string(tt.it))
			}
		})
	}
}

// TestConstraint_Create 测试Constraint创建
func TestConstraint_Create(t *testing.T) {
	constraint := &Constraint{
		Name:     "unique_email",
		Type:     ConstraintTypeUnique,
		Columns:  []string{"email"},
		Table:    "users",
		Enabled:  true,
	}

	if constraint.Name != "unique_email" {
		t.Errorf("Expected name 'unique_email', got %v", constraint.Name)
	}
	if constraint.Type != ConstraintTypeUnique {
		t.Errorf("Expected type ConstraintTypeUnique")
	}
	if !constraint.Enabled {
		t.Errorf("Expected Enabled to be true")
	}
}

// TestConstraint_CreateWithForeignKey 测试带外键的Constraint
func TestConstraint_CreateWithForeignKey(t *testing.T) {
	ref := &ForeignKeyReference{
		Table:   "departments",
		Columns: []string{"id"},
	}

	constraint := &Constraint{
		Name:       "fk_user_department",
		Type:       ConstraintTypeForeignKey,
		Columns:    []string{"department_id"},
		Table:      "users",
		References: ref,
		Enabled:    true,
	}

	if constraint.References == nil {
		t.Errorf("Expected References to be set")
	}
	if constraint.References.Table != "departments" {
		t.Errorf("Expected reference table 'departments', got %v", constraint.References.Table)
	}
}

// TestIndex_Create 测试Index创建
func TestIndex_Create(t *testing.T) {
	index := &Index{
		Name:    "idx_email",
		Table:   "users",
		Columns: []string{"email"},
		Type:    IndexTypeBTree,
		Unique:  true,
		Primary: false,
		Enabled: true,
	}

	if index.Name != "idx_email" {
		t.Errorf("Expected name 'idx_email', got %v", index.Name)
	}
	if index.Type != IndexTypeBTree {
		t.Errorf("Expected type IndexTypeBTree")
	}
	if !index.Unique {
		t.Errorf("Expected Unique to be true")
	}
	if index.Primary {
		t.Errorf("Expected Primary to be false")
	}
}

// TestSchema_Create 测试Schema创建
func TestSchema_Create(t *testing.T) {
	tables := []*TableInfo{
		{
			Name: "users",
			Columns: []ColumnInfo{
				{Name: "id", Type: "int64", Primary: true},
			},
		},
	}

	indexes := []*Index{
		{Name: "idx_email", Table: "users", Type: IndexTypeBTree, Enabled: true},
	}

	constraints := []*Constraint{
		{Name: "unique_email", Type: ConstraintTypeUnique, Table: "users", Enabled: true},
	}

	schema := &Schema{
		Name:        "public",
		Tables:      tables,
		Indexes:     indexes,
		Constraints: constraints,
	}

	if schema.Name != "public" {
		t.Errorf("Expected schema name 'public', got %v", schema.Name)
	}
	if len(schema.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(schema.Tables))
	}
	if len(schema.Indexes) != 1 {
		t.Errorf("Expected 1 index, got %d", len(schema.Indexes))
	}
	if len(schema.Constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(schema.Constraints))
	}
}
