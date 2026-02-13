package domain

import (
	"errors"
	"fmt"
)

// TableInfo business methods - following DDD rich domain model pattern

// AddColumn adds a new column to the table with validation
func (t *TableInfo) AddColumn(col ColumnInfo) error {
	if col.Name == "" {
		return errors.New("column name cannot be empty")
	}

	if t.HasColumn(col.Name) {
		return NewErrColumnAlreadyExists(col.Name)
	}

	t.Columns = append(t.Columns, col)
	return nil
}

// RemoveColumn removes a column from the table
func (t *TableInfo) RemoveColumn(columnName string) error {
	for i, col := range t.Columns {
		if col.Name == columnName {
			// Check if column is part of primary key
			if col.Primary {
				return errors.New("cannot remove primary key column")
			}
			t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
			return nil
		}
	}
	return NewErrColumnNotFound(columnName)
}

// HasColumn checks if a column exists
func (t *TableInfo) HasColumn(columnName string) bool {
	for _, col := range t.Columns {
		if col.Name == columnName {
			return true
		}
	}
	return false
}

// GetColumn retrieves a column by name
func (t *TableInfo) GetColumn(columnName string) (ColumnInfo, bool) {
	for _, col := range t.Columns {
		if col.Name == columnName {
			return col, true
		}
	}
	return ColumnInfo{}, false
}

// GetPrimaryKey returns the primary key columns
func (t *TableInfo) GetPrimaryKey() []ColumnInfo {
	var pkCols []ColumnInfo
	for _, col := range t.Columns {
		if col.Primary {
			pkCols = append(pkCols, col)
		}
	}
	return pkCols
}

// HasPrimaryKey checks if the table has a primary key
func (t *TableInfo) HasPrimaryKey() bool {
	for _, col := range t.Columns {
		if col.Primary {
			return true
		}
	}
	return false
}

// SetPrimaryKey sets the primary key for the table
func (t *TableInfo) SetPrimaryKey(columnNames ...string) error {
	// First, clear existing primary keys
	for i := range t.Columns {
		t.Columns[i].Primary = false
	}

	// Then, set new primary keys
	for _, name := range columnNames {
		found := false
		for i, col := range t.Columns {
			if col.Name == name {
				t.Columns[i].Primary = true
				found = true
				break
			}
		}
		if !found {
			return NewErrColumnNotFound(name)
		}
	}
	return nil
}

// GetColumnNames returns all column names
func (t *TableInfo) GetColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}
	return names
}

// Validate validates the table structure
func (t *TableInfo) Validate() error {
	if t.Name == "" {
		return errors.New("table name cannot be empty")
	}

	if len(t.Columns) == 0 {
		return errors.New("table must have at least one column")
	}

	// Check for duplicate column names
	seen := make(map[string]bool)
	for _, col := range t.Columns {
		if seen[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		seen[col.Name] = true

		// Validate column
		if err := col.Validate(); err != nil {
			return fmt.Errorf("invalid column %s: %w", col.Name, err)
		}
	}

	return nil
}

// Clone creates a deep copy of the TableInfo
func (t *TableInfo) Clone() *TableInfo {
	clone := &TableInfo{
		Name:      t.Name,
		Schema:    t.Schema,
		Temporary: t.Temporary,
		Columns:   make([]ColumnInfo, len(t.Columns)),
	}

	copy(clone.Columns, t.Columns)

	if t.Atts != nil {
		clone.Atts = make(map[string]interface{})
		for k, v := range t.Atts {
			clone.Atts[k] = v
		}
	}

	return clone
}

// FullName returns the fully qualified table name (schema.table)
func (t *TableInfo) FullName() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	return t.Name
}

// IsTemporary returns whether the table is temporary
func (t *TableInfo) IsTemporary() bool {
	return t.Temporary
}

// ColumnInfo business methods

// Validate validates the column definition
func (c ColumnInfo) Validate() error {
	if c.Name == "" {
		return errors.New("column name cannot be empty")
	}

	if c.Type == "" {
		return errors.New("column type cannot be empty")
	}

	// Validate generated column dependencies
	if c.IsGenerated && len(c.GeneratedDepends) == 0 {
		return errors.New("generated column must have dependencies")
	}

	// Validate vector column
	if c.VectorDim > 0 && c.VectorType == "" {
		return errors.New("vector column must specify vector_type")
	}

	return nil
}

// IsNullable returns whether the column allows null values
func (c ColumnInfo) IsNullable() bool {
	// Primary key columns are never nullable
	if c.Primary {
		return false
	}
	return c.Nullable
}

// IsGeneratedColumn returns whether this is a generated column
func (c ColumnInfo) IsGeneratedColumn() bool {
	return c.IsGenerated
}

// IsAutoIncrement returns whether the column auto-increments
func (c ColumnInfo) IsAutoIncrement() bool {
	return c.AutoIncrement
}

// HasForeignKey returns whether the column has a foreign key constraint
func (c ColumnInfo) HasForeignKey() bool {
	return c.ForeignKey != nil
}

// GetForeignKeyInfo returns the foreign key information
func (c ColumnInfo) GetForeignKeyInfo() *ForeignKeyInfo {
	return c.ForeignKey
}

// Clone creates a copy of the ColumnInfo
func (c ColumnInfo) Clone() ColumnInfo {
	clone := c
	if c.ForeignKey != nil {
		fk := *c.ForeignKey
		clone.ForeignKey = &fk
	}
	if c.GeneratedDepends != nil {
		clone.GeneratedDepends = make([]string, len(c.GeneratedDepends))
		copy(clone.GeneratedDepends, c.GeneratedDepends)
	}
	return clone
}

// ColumnType returns a formatted string representation of the column type
func (c ColumnInfo) ColumnType() string {
	if c.IsVectorType() {
		return fmt.Sprintf("VECTOR(%d, %s)", c.VectorDim, c.VectorType)
	}
	return c.Type
}
