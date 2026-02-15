package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// BuildSelectSQL builds a SELECT query from QueryOptions.
// paramOffset is the starting parameter number (1-based, for PG $N placeholders).
func BuildSelectSQL(d Dialect, tableName string, options *domain.QueryOptions, paramOffset int) (string, []interface{}) {
	var sb strings.Builder
	var params []interface{}

	// SELECT columns
	sb.WriteString("SELECT ")
	if options != nil && len(options.SelectColumns) > 0 {
		quoted := make([]string, len(options.SelectColumns))
		for i, col := range options.SelectColumns {
			quoted[i] = d.QuoteIdentifier(col)
		}
		sb.WriteString(strings.Join(quoted, ", "))
	} else {
		sb.WriteString("*")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(d.QuoteIdentifier(tableName))

	// WHERE
	if options != nil && len(options.Filters) > 0 {
		whereClause, whereParams := BuildWhereClause(d, options.Filters, paramOffset)
		if whereClause != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(whereClause)
			params = append(params, whereParams...)
			paramOffset += len(whereParams)
		}
	}

	// ORDER BY
	if options != nil && options.OrderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(d.QuoteIdentifier(options.OrderBy))
		if strings.EqualFold(options.Order, "DESC") {
			sb.WriteString(" DESC")
		} else {
			sb.WriteString(" ASC")
		}
	}

	// LIMIT / OFFSET
	if options != nil && options.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", options.Limit))
	}
	if options != nil && options.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", options.Offset))
	}

	return sb.String(), params
}

// BuildWhereClause converts a slice of filters into a WHERE clause string.
func BuildWhereClause(d Dialect, filters []domain.Filter, paramOffset int) (string, []interface{}) {
	if len(filters) == 0 {
		return "", nil
	}

	var parts []string
	var params []interface{}

	for _, f := range filters {
		clause, fParams := buildFilterClause(d, f, paramOffset)
		if clause != "" {
			parts = append(parts, clause)
			params = append(params, fParams...)
			paramOffset += len(fParams)
		}
	}

	if len(parts) == 0 {
		return "", nil
	}

	return strings.Join(parts, " AND "), params
}

func buildFilterClause(d Dialect, f domain.Filter, paramOffset int) (string, []interface{}) {
	// Nested logic via SubFilters (legacy pattern)
	if len(f.SubFilters) > 0 {
		logic := "AND"
		if f.LogicOp != "" {
			logic = strings.ToUpper(f.LogicOp)
		}
		if f.Logic != "" {
			logic = strings.ToUpper(f.Logic)
		}

		var parts []string
		var params []interface{}
		for _, sub := range f.SubFilters {
			clause, subParams := buildFilterClause(d, sub, paramOffset)
			if clause != "" {
				parts = append(parts, clause)
				params = append(params, subParams...)
				paramOffset += len(subParams)
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		return "(" + strings.Join(parts, " "+logic+" ") + ")", params
	}

	// Nested logic via Value as []Filter (newer pattern)
	if f.Logic != "" {
		if subFilters, ok := f.Value.([]domain.Filter); ok {
			logic := strings.ToUpper(f.Logic)
			var parts []string
			var params []interface{}
			for _, sub := range subFilters {
				clause, subParams := buildFilterClause(d, sub, paramOffset)
				if clause != "" {
					parts = append(parts, clause)
					params = append(params, subParams...)
					paramOffset += len(subParams)
				}
			}
			if len(parts) == 0 {
				return "", nil
			}
			return "(" + strings.Join(parts, " "+logic+" ") + ")", params
		}
	}

	// Simple field filter
	if f.Field == "" {
		return "", nil
	}

	op := strings.ToUpper(strings.TrimSpace(f.Operator))
	quotedField := d.QuoteIdentifier(f.Field)

	switch op {
	case "IS NULL":
		return quotedField + " IS NULL", nil
	case "IS NOT NULL":
		return quotedField + " IS NOT NULL", nil
	case "IN":
		values := toSlice(f.Value)
		if len(values) == 0 {
			return "1=0", nil // IN empty set is always false
		}
		placeholders := make([]string, len(values))
		for i := range values {
			placeholders[i] = d.Placeholder(paramOffset + i + 1)
		}
		return quotedField + " IN (" + strings.Join(placeholders, ", ") + ")", values
	case "BETWEEN":
		values := toSlice(f.Value)
		if len(values) < 2 {
			return "", nil
		}
		p1 := d.Placeholder(paramOffset + 1)
		p2 := d.Placeholder(paramOffset + 2)
		return quotedField + " BETWEEN " + p1 + " AND " + p2, values[:2]
	case "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE":
		ph := d.Placeholder(paramOffset + 1)
		if op == "!=" {
			op = "<>"
		}
		return quotedField + " " + op + " " + ph, []interface{}{f.Value}
	default:
		// Default to equality
		ph := d.Placeholder(paramOffset + 1)
		return quotedField + " = " + ph, []interface{}{f.Value}
	}
}

// BuildInsertSQL builds an INSERT statement from rows.
func BuildInsertSQL(d Dialect, tableName string, rows []domain.Row) (string, []interface{}, []string) {
	if len(rows) == 0 {
		return "", nil, nil
	}

	// Collect all column names from first row (sorted for determinism)
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		if col == "__atts__" {
			continue
		}
		columns = append(columns, col)
	}
	sort.Strings(columns)

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = d.QuoteIdentifier(col)
	}

	var params []interface{}
	var valueSets []string
	paramIdx := 0

	for _, row := range rows {
		placeholders := make([]string, len(columns))
		for i, col := range columns {
			paramIdx++
			placeholders[i] = d.Placeholder(paramIdx)
			params = append(params, row[col])
		}
		valueSets = append(valueSets, "("+strings.Join(placeholders, ", ")+")")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		d.QuoteIdentifier(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(valueSets, ", "))

	return sql, params, columns
}

// BuildUpdateSQL builds an UPDATE statement from filters and updates.
func BuildUpdateSQL(d Dialect, tableName string, filters []domain.Filter, updates domain.Row) (string, []interface{}) {
	// Collect update columns (sorted for determinism)
	columns := make([]string, 0, len(updates))
	for col := range updates {
		if col == "__atts__" {
			continue
		}
		columns = append(columns, col)
	}
	sort.Strings(columns)

	var params []interface{}
	paramIdx := 0

	// SET clause
	setClauses := make([]string, len(columns))
	for i, col := range columns {
		paramIdx++
		setClauses[i] = d.QuoteIdentifier(col) + " = " + d.Placeholder(paramIdx)
		params = append(params, updates[col])
	}

	sql := fmt.Sprintf("UPDATE %s SET %s",
		d.QuoteIdentifier(tableName),
		strings.Join(setClauses, ", "))

	// WHERE
	if len(filters) > 0 {
		whereClause, whereParams := BuildWhereClause(d, filters, paramIdx)
		if whereClause != "" {
			sql += " WHERE " + whereClause
			params = append(params, whereParams...)
		}
	}

	return sql, params
}

// BuildDeleteSQL builds a DELETE statement from filters.
func BuildDeleteSQL(d Dialect, tableName string, filters []domain.Filter) (string, []interface{}) {
	sql := fmt.Sprintf("DELETE FROM %s", d.QuoteIdentifier(tableName))

	if len(filters) > 0 {
		whereClause, params := BuildWhereClause(d, filters, 0)
		if whereClause != "" {
			sql += " WHERE " + whereClause
			return sql, params
		}
	}

	return sql, nil
}

// toSlice converts an interface{} to []interface{} for IN/BETWEEN operators.
func toSlice(v interface{}) []interface{} {
	switch val := v.(type) {
	case []interface{}:
		return val
	case []string:
		result := make([]interface{}, len(val))
		for i, s := range val {
			result[i] = s
		}
		return result
	case []int:
		result := make([]interface{}, len(val))
		for i, n := range val {
			result[i] = n
		}
		return result
	case []int64:
		result := make([]interface{}, len(val))
		for i, n := range val {
			result[i] = n
		}
		return result
	case []float64:
		result := make([]interface{}, len(val))
		for i, n := range val {
			result[i] = n
		}
		return result
	default:
		return nil
	}
}
