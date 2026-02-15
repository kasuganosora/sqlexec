package sql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ScanRows reads all rows from *sql.Rows into domain types.
func ScanRows(rows *sql.Rows, dialect Dialect) ([]domain.Row, []domain.ColumnInfo, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("get column types: %w", err)
	}

	columns := make([]domain.ColumnInfo, len(colTypes))
	colNames := make([]string, len(colTypes))
	for i, ct := range colTypes {
		colNames[i] = ct.Name()
		nullable, _ := ct.Nullable()
		columns[i] = domain.ColumnInfo{
			Name:     ct.Name(),
			Type:     dialect.MapColumnType(ct.DatabaseTypeName(), ct),
			Nullable: nullable,
		}
	}

	var result []domain.Row
	for rows.Next() {
		row, err := scanRow(rows, colNames, colTypes)
		if err != nil {
			return nil, nil, err
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows iteration: %w", err)
	}

	return result, columns, nil
}

func scanRow(rows *sql.Rows, colNames []string, colTypes []*sql.ColumnType) (domain.Row, error) {
	// Create scan targets
	values := make([]interface{}, len(colNames))
	scanTargets := make([]interface{}, len(colNames))
	for i := range values {
		scanTargets[i] = &values[i]
	}

	if err := rows.Scan(scanTargets...); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	row := make(domain.Row, len(colNames))
	for i, name := range colNames {
		row[name] = normalizeValue(values[i])
	}

	return row, nil
}

// normalizeValue converts database/sql scanned values to standard Go types.
func normalizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	case int64:
		return val
	case float64:
		return val
	case bool:
		return val
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
