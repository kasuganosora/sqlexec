package api

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// isInformationSchemaOperation checks if SQL is trying to modify information_schema
func (s *Session) isInformationSchemaOperation(sql string, stmt *parser.SQLStatement) bool {
	// First check if SQL directly mentions information_schema
	if strings.Contains(strings.ToLower(sql), "information_schema") {
		return true
	}

	// Also check parsed statement
	switch stmt.Type {
	case parser.SQLTypeInsert:
		return isInformationSchemaTable(stmt.Insert.Table)
	case parser.SQLTypeUpdate:
		return isInformationSchemaTable(stmt.Update.Table)
	case parser.SQLTypeDelete:
		return isInformationSchemaTable(stmt.Delete.Table)
	}
	return false
}

// isInformationSchemaTable checks if a table name references information_schema
func isInformationSchemaTable(tableName string) bool {
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "information_schema" {
			return true
		}
	}
	return false
}
