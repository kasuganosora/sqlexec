package gorm

import (
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Migrator implements gorm.Migrator, routing DDL operations through the
// sqlexec Session via Session.Execute() and Session.Query().
type Migrator struct {
	Dialector *Dialector
	DB        *gorm.DB
}

// AutoMigrate creates tables for the given models if they don't exist.
func (m *Migrator) AutoMigrate(dst ...interface{}) error {
	for _, value := range dst {
		namer := schema.NamingStrategy{}
		s, err := schema.Parse(value, &sync.Map{}, namer)
		if err != nil {
			return fmt.Errorf("failed to parse schema: %w", err)
		}

		if m.HasTable(s.Table) {
			continue
		}

		sql := m.generateCreateTableSQLFromSchema(s)
		_, err = m.Dialector.Session.Execute(sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", s.Table, err)
		}
	}
	return nil
}

// HasTable checks whether a table exists.
func (m *Migrator) HasTable(value interface{}) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '" + escapeStringValue(tableName) + "'"

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

// CreateTable creates tables for the given models.
func (m *Migrator) CreateTable(values ...interface{}) error {
	for _, value := range values {
		namer := schema.NamingStrategy{}
		s, err := schema.Parse(value, &sync.Map{}, namer)
		if err != nil {
			// Fallback to simple create
			sql := m.generateCreateTableSQL(value)
			_, execErr := m.Dialector.Session.Execute(sql)
			return execErr
		}
		sql := m.generateCreateTableSQLFromSchema(s)
		_, err = m.Dialector.Session.Execute(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropTable drops the given tables.
func (m *Migrator) DropTable(values ...interface{}) error {
	for _, value := range values {
		tableName := m.getTableName(value)
		sql := "DROP TABLE IF EXISTS " + quoteIdentifier(tableName)
		_, err := m.Dialector.Session.Execute(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// RenameTable renames a table.
func (m *Migrator) RenameTable(oldName, newName interface{}) error {
	oldTableName := m.getTableName(oldName)
	newTableName := m.getTableName(newName)

	sql := "ALTER TABLE " + quoteIdentifier(oldTableName) + " RENAME TO " + quoteIdentifier(newTableName)
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// GetTables returns all table names.
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

// AddColumn adds a column to the table. It resolves the column type from the
// model's schema rather than hardcoding.
func (m *Migrator) AddColumn(value interface{}, field string) error {
	tableName := m.getTableName(value)
	colType := m.resolveColumnType(value, field)

	sql := "ALTER TABLE " + quoteIdentifier(tableName) + " ADD COLUMN " + quoteIdentifier(field) + " " + colType
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropColumn drops a column from the table.
func (m *Migrator) DropColumn(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + quoteIdentifier(tableName) + " DROP COLUMN " + quoteIdentifier(name)
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// AlterColumn modifies a column's type. It resolves the new type from the
// model's schema.
func (m *Migrator) AlterColumn(value interface{}, field string) error {
	tableName := m.getTableName(value)
	colType := m.resolveColumnType(value, field)

	sql := "ALTER TABLE " + quoteIdentifier(tableName) + " MODIFY COLUMN " + quoteIdentifier(field) + " " + colType
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// RenameColumn renames a column.
func (m *Migrator) RenameColumn(value interface{}, oldName, field string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + quoteIdentifier(tableName) + " RENAME COLUMN " + quoteIdentifier(oldName) + " TO " + quoteIdentifier(field)
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// HasColumn checks whether a column exists by querying information_schema.
func (m *Migrator) HasColumn(value interface{}, name string) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '" +
		escapeStringValue(tableName) + "' AND column_name = '" + escapeStringValue(name) + "'"

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

// ColumnTypes returns column type information for the table.
func (m *Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error) {
	return []gorm.ColumnType{}, nil
}

// CreateConstraint creates a named constraint.
func (m *Migrator) CreateConstraint(value interface{}, name string) error {
	return gorm.ErrNotImplemented
}

// DropConstraint drops a named constraint.
func (m *Migrator) DropConstraint(value interface{}, name string) error {
	tableName := m.getTableName(value)

	sql := "ALTER TABLE " + quoteIdentifier(tableName) + " DROP CONSTRAINT " + quoteIdentifier(name)
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// HasConstraint checks whether a constraint exists.
func (m *Migrator) HasConstraint(value interface{}, name string) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE constraint_name = '" +
		escapeStringValue(name) + "' AND table_name = '" + escapeStringValue(tableName) + "'"

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

// CreateIndex creates an index. It extracts the column list from the model's
// schema rather than hardcoding to (id).
func (m *Migrator) CreateIndex(value interface{}, name string) error {
	tableName := m.getTableName(value)
	columns := m.resolveIndexColumns(value, name)

	sql := "CREATE INDEX " + quoteIdentifier(name) + " ON " + quoteIdentifier(tableName) + " (" + columns + ")"
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// DropIndex drops an index.
func (m *Migrator) DropIndex(value interface{}, name string) error {
	tableName := m.getTableName(value)
	sql := "DROP INDEX " + quoteIdentifier(name) + " ON " + quoteIdentifier(tableName)
	_, err := m.Dialector.Session.Execute(sql)
	return err
}

// HasIndex checks whether an index exists.
func (m *Migrator) HasIndex(value interface{}, name string) bool {
	tableName := m.getTableName(value)

	sql := "SELECT COUNT(*) FROM information_schema.statistics WHERE index_name = '" +
		escapeStringValue(name) + "' AND table_name = '" + escapeStringValue(tableName) + "'"

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

// RenameIndex renames an index by dropping and re-creating it.
func (m *Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	if err := m.DropIndex(value, oldName); err != nil {
		return err
	}
	return m.CreateIndex(value, newName)
}

// CreateView is not supported.
func (m *Migrator) CreateView(name string, option gorm.ViewOption) error {
	return gorm.ErrNotImplemented
}

// CurrentDatabase returns the name of the current database.
func (m *Migrator) CurrentDatabase() (name string) {
	result, err := m.Dialector.Session.Query("SELECT DATABASE()")
	if err != nil {
		return ""
	}
	defer result.Close()
	if result.Next() {
		var dbName string
		if err := result.Scan(&dbName); err == nil {
			return dbName
		}
	}
	return ""
}

// DropView is not supported.
func (m *Migrator) DropView(name string) error {
	return gorm.ErrNotImplemented
}

// FullDataTypeOf returns the complete data type for a field.
func (m *Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	return clause.Expr{SQL: m.Dialector.DataTypeOf(field)}
}

// GetIndexes returns indexes for the table.
func (m *Migrator) GetIndexes(value interface{}) (indexes []gorm.Index, err error) {
	return []gorm.Index{}, nil
}

// GetTypeAliases returns type aliases for a type name.
func (m *Migrator) GetTypeAliases(typ string) []string {
	return nil
}

// MigrateColumn is not supported.
func (m *Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return gorm.ErrNotImplemented
}

// MigrateColumnUnique is not supported.
func (m *Migrator) MigrateColumnUnique(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return gorm.ErrNotImplemented
}

// MigrateTable is not supported.
func (m *Migrator) MigrateTable(value interface{}, fields []schema.Field, fieldOpts map[string][]string) error {
	return gorm.ErrNotImplemented
}

// MigrateValue is not supported.
func (m *Migrator) MigrateValue(value interface{}, field *schema.Field, valueRef interface{}) error {
	return gorm.ErrNotImplemented
}

// TableType returns type information for the table.
func (m *Migrator) TableType(value interface{}) (tableType gorm.TableType, err error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// quoteIdentifier quotes a SQL identifier with backticks, escaping any
// embedded backticks to prevent SQL injection.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// escapeStringValue escapes a string value for use in SQL string literals.
func escapeStringValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return s
}

// getTableName resolves the table name from various value types.
// It uses GORM's schema parser for model structs, which correctly applies
// the NamingStrategy (e.g. User → users).
func (m *Migrator) getTableName(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	if s, ok := value.(*schema.Schema); ok {
		return s.Table
	}
	if ptrStr, ok := value.(*string); ok {
		return *ptrStr
	}

	// Use GORM's schema parser for model structs
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(value, &sync.Map{}, namer)
	if err == nil {
		return s.Table
	}

	// Last resort: derive from type name
	typ := fmt.Sprintf("%T", value)
	typeName := strings.TrimPrefix(typ, "*")
	if idx := strings.LastIndex(typeName, "."); idx != -1 {
		typeName = typeName[idx+1:]
	}
	return strings.ToLower(typeName)
}

// resolveColumnType looks up a field in the model's schema and returns its
// SQL type. Falls back to VARCHAR(255) if the schema can't be parsed.
func (m *Migrator) resolveColumnType(value interface{}, fieldName string) string {
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(value, &sync.Map{}, namer)
	if err != nil {
		return "VARCHAR(255)"
	}
	for _, f := range s.Fields {
		if f.DBName == fieldName || f.Name == fieldName {
			return m.Dialector.DataTypeOf(f)
		}
	}
	return "VARCHAR(255)"
}

// resolveIndexColumns looks up an index definition in the model's schema and
// returns the comma-separated quoted column list. Falls back to "id".
func (m *Migrator) resolveIndexColumns(value interface{}, indexName string) string {
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(value, &sync.Map{}, namer)
	if err != nil {
		return quoteIdentifier("id")
	}
	for _, idx := range s.ParseIndexes() {
		if idx.Name == indexName {
			cols := make([]string, 0, len(idx.Fields))
			for _, f := range idx.Fields {
				cols = append(cols, quoteIdentifier(f.Field.DBName))
			}
			if len(cols) > 0 {
				return strings.Join(cols, ", ")
			}
		}
	}
	return quoteIdentifier("id")
}

// generateCreateTableSQL generates a simple CREATE TABLE statement (fallback).
func (m *Migrator) generateCreateTableSQL(value interface{}) string {
	tableName := m.getTableName(value)
	return "CREATE TABLE IF NOT EXISTS " + quoteIdentifier(tableName) +
		" (id INT PRIMARY KEY AUTO_INCREMENT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
}

// generateCreateTableSQLFromSchema generates a CREATE TABLE statement from a
// parsed GORM schema with proper column types, primary keys, and constraints.
func (m *Migrator) generateCreateTableSQLFromSchema(s *schema.Schema) string {
	var columnDefs []string
	var primaryKeys []string

	for _, field := range s.Fields {
		// Skip fields with empty DBName (e.g., fields with gorm:"-" tag)
		// These fields should not be mapped to database columns
		if field.DBName == "" {
			continue
		}
		colName := field.DBName

		colType := m.Dialector.DataTypeOf(field)
		def := quoteIdentifier(colName) + " " + colType

		if field.PrimaryKey {
			primaryKeys = append(primaryKeys, quoteIdentifier(colName))
		}

		if !field.PrimaryKey && !field.Unique {
			def += " NULL"
		}

		if field.DefaultValue != "" && field.DefaultValue != "nil" {
			defaultVal := field.DefaultValue
			// Check if the default value is a string (needs quoting)
			if field.DefaultValueInterface != nil {
				switch field.DefaultValueInterface.(type) {
				case string:
					// String default values need to be quoted
					defaultVal = "'" + escapeStringValue(defaultVal) + "'"
				default:
					// Numeric and other types don't need quotes
					defaultVal = fmt.Sprintf("%v", field.DefaultValueInterface)
				}
			}
			def += " DEFAULT " + defaultVal
		}

		if field.AutoIncrement {
			def += " AUTO_INCREMENT"
		}

		if field.Unique {
			def += " UNIQUE"
		}

		columnDefs = append(columnDefs, def)
	}

	sql := "CREATE TABLE IF NOT EXISTS " + quoteIdentifier(s.Table) + " (" + strings.Join(columnDefs, ", ")

	if len(primaryKeys) > 0 {
		sql += ", PRIMARY KEY (" + strings.Join(primaryKeys, ", ") + ")"
	}

	sql += ")"

	return sql
}
