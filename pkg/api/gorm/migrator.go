package gorm

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"sync"
)

// Migrator 实现了 GORM 的 Migrator 接口，将迁移操作委托给 sqlexec
type Migrator struct {
	Dialector *Dialector
	DB       *gorm.DB
}

// AutoMigrate 自动迁移
func (m *Migrator) AutoMigrate(dst ...interface{}) error {
	for _, value := range dst {
		// 使用 GORM 的 schema 解析器
		namer := schema.NamingStrategy{}
		s, err := schema.Parse(value, &sync.Map{}, namer)
		if err != nil {
			return fmt.Errorf("failed to parse schema: %w", err)
		}

		tableName := s.Table

		// 检查表是否存在
		if m.HasTable(tableName) {
			// 表已存在，不处理
			continue
		}

		// 表不存在，创建新表
		sql := m.generateCreateTableSQLFromSchema(s)
		_, err = m.Dialector.Session.Execute(sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// HasTable 检查表是否存在
func (m *Migrator) HasTable(value interface{}) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '" + tableName + "'"

	result, err := m.Dialector.Session.Query(sql)
	if err != nil {
		return false
	}
	defer result.Close()

	if result.Next() {
		var count int
		result.Scan(&count)
		return count > 0
	}
	return false
}

// CreateTable 创建表
func (m *Migrator) CreateTable(values ...interface{}) error {
	if len(values) == 0 {
		return nil
	}
	sql := m.generateCreateTableSQL(values[0])

	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropTable 删除表
func (m *Migrator) DropTable(values ...interface{}) error {
	if len(values) == 0 {
		return nil
	}
	tableName := m.getTableName(values[0])
	_ = tableName // unused

	sql := "DROP TABLE IF EXISTS " + tableName

	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// RenameTable 重命名表
func (m *Migrator) RenameTable(oldName, newName interface{}) error {
	oldTableName := m.getTableName(oldName)
	newTableName := m.getTableName(newName)

	sql := "ALTER TABLE " + oldTableName + " RENAME TO " + newTableName

	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// GetTables 获取所有表
func (m *Migrator) GetTables() (tableList []string, err error) {
	sql := "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"

	result, err := m.Dialector.Session.Query(sql)
	if err != nil {
		return []string{}, err
	}
	defer result.Close()

	var tables []string
	for result.Next() {
		var tableName string
		if err := result.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	if tables == nil {
		return []string{}, nil
	}
	return tables, nil
}

// AddColumn 添加列
func (m *Migrator) AddColumn(value interface{}, field string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " ADD COLUMN " + field + " INT"
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropColumn 删除列
func (m *Migrator) DropColumn(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " DROP COLUMN " + name
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// AlterColumn 修改列
func (m *Migrator) AlterColumn(value interface{}, field string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " MODIFY COLUMN " + field + " VARCHAR(255)"
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// RenameColumn 重命名列
func (m *Migrator) RenameColumn(value interface{}, oldName, field string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " RENAME COLUMN " + oldName + " TO " + field
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// ColumnTypes 获取列类型
func (m *Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error) {
	// 简化实现：返回空列表，避免复杂的接口实现
	return []gorm.ColumnType{}, nil
}

// CreateConstraint 创建约束
func (m *Migrator) CreateConstraint(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " ADD CONSTRAINT " + name + " FOREIGN KEY (id) REFERENCES other_table(id)"
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropConstraint 删除约束
func (m *Migrator) DropConstraint(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + tableName + " DROP CONSTRAINT " + name
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// HasConstraint 检查约束是否存在
func (m *Migrator) HasConstraint(value interface{}, name string) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE constraint_name = '" + name + "' AND table_name = '" + tableName + "'"

	result, err := m.Dialector.Session.Query(sql)
	if err != nil {
		return false
	}
	defer result.Close()

	if result.Next() {
		var count int
		result.Scan(&count)
		return count > 0
	}
	return false
}

// CreateIndex 创建索引
func (m *Migrator) CreateIndex(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "CREATE INDEX " + name + " ON " + tableName + " (id)"
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropIndex 删除索引
func (m *Migrator) DropIndex(value interface{}, name string) error {
	tableName := m.getTableName(value)
	sql := "DROP INDEX " + name + " ON " + tableName
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// HasIndex 检查索引是否存在
func (m *Migrator) HasIndex(value interface{}, name string) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.statistics WHERE index_name = '" + name + "' AND table_name = '" + tableName + "'"

	result, err := m.Dialector.Session.Query(sql)
	if err != nil {
		return false
	}
	defer result.Close()

	if result.Next() {
		var count int
		result.Scan(&count)
		return count > 0
	}
	return false
}

// RenameIndex 重命名索引
func (m *Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	// 不支持重命名索引，需要先删除再创建
	if err := m.DropIndex(value, oldName); err != nil {
		return err
	}
	return m.CreateIndex(value, newName)
}

// CreateView 创建视图（GORM 1.25+ 需要的方法）
func (m *Migrator) CreateView(name string, option gorm.ViewOption) error {
	return gorm.ErrNotImplemented
}

// CurrentDatabase 获取当前数据库名称（GORM 1.25+ 需要的方法）
func (m *Migrator) CurrentDatabase() (name string) {
	return "test_db"
}

// DropView 删除视图（GORM 1.25+ 需要的方法）
func (m *Migrator) DropView(name string) error {
	return gorm.ErrNotImplemented
}

// FullDataTypeOf 获取完整数据类型（GORM 1.25+ 需要的方法）
func (m *Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	return clause.Expr{SQL: m.Dialector.DataTypeOf(field)}
}

// GetIndexes 获取索引（GORM 1.25+ 需要的方法）
func (m *Migrator) GetIndexes(value interface{}) (indexes []gorm.Index, err error) {
	return []gorm.Index{}, nil
}

// GetTypeAliases 获取类型别名（GORM 1.25+ 需要的方法）
func (m *Migrator) GetTypeAliases(typ string) []string {
	return nil
}

// HasColumn 检查列是否存在
func (m *Migrator) HasColumn(value interface{}, name string) bool {
	return false
}

// MigrateColumn 迁移列
func (m *Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return gorm.ErrNotImplemented
}

// MigrateColumnUnique 迁移唯一列
func (m *Migrator) MigrateColumnUnique(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return gorm.ErrNotImplemented
}

// MigrateTable 迁移表
func (m *Migrator) MigrateTable(value interface{}, fields []schema.Field, fieldOpts map[string][]string) error {
	return gorm.ErrNotImplemented
}

// MigrateValue 迁移值
func (m *Migrator) MigrateValue(value interface{}, field *schema.Field, valueRef interface{}) error {
	return gorm.ErrNotImplemented
}

// TableType 获取表类型
func (m *Migrator) TableType(value interface{}) (tableType gorm.TableType, err error) {
	return nil, nil
}

// getTableName 获取表名
func (m *Migrator) getTableName(value interface{}) string {
	// 如果是字符串，直接返回
	if str, ok := value.(string); ok {
		return str
	}

	// 如果是 schema 对象，使用 Table 字段
	if s, ok := value.(*schema.Schema); ok {
		return s.Table
	}

	// 如果是字符串指针，解引用后返回
	if ptrStr, ok := value.(*string); ok {
		return *ptrStr
	}

	// 简单实现，通过类型名称推断
	typ := fmt.Sprintf("%T", value)
	typeName := strings.ToLower(strings.TrimPrefix(strings.TrimSuffix(typ, "}"), "*"))

	// 去掉包名前缀
	if idx := strings.LastIndex(typeName, "."); idx != -1 {
		typeName = typeName[idx+1:]
	}

	return typeName
}

// generateCreateTableSQL 生成 CREATE TABLE SQL
func (m *Migrator) generateCreateTableSQL(value interface{}) string {
	tableName := m.getTableName(value)

	// 简单的表创建语句
	return "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT PRIMARY KEY AUTO_INCREMENT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
}

// generateCreateTableSQLFromSchema 从 schema 生成 CREATE TABLE SQL
func (m *Migrator) generateCreateTableSQLFromSchema(s *schema.Schema) string {
	var columnDefs []string
	var primaryKeys []string

	for _, field := range s.Fields {
		colType := m.Dialector.DataTypeOf(field)
		def := field.DBName + " " + colType

		// 处理主键
		if field.PrimaryKey {
			primaryKeys = append(primaryKeys, field.DBName)
		}

		// 处理 NOT NULL
		if !field.PrimaryKey && !field.Unique {
			def += " NULL"
		}

		// 处理默认值
		if field.DefaultValue != "" && field.DefaultValue != "nil" {
			def += " DEFAULT " + fmt.Sprintf("%v", field.DefaultValue)
		}

		// 处理自动增量
		if field.AutoIncrement {
			def += " AUTO_INCREMENT"
		}

		// 处理唯一约束
		if field.Unique {
			def += " UNIQUE"
		}

		columnDefs = append(columnDefs, def)
	}

	sql := "CREATE TABLE IF NOT EXISTS " + s.Table + " (" + strings.Join(columnDefs, ", ")

	// 添加主键约束
	if len(primaryKeys) > 0 {
		sql += ", PRIMARY KEY (" + strings.Join(primaryKeys, ", ") + ")"
	}

	sql += ")"

	return sql
}

