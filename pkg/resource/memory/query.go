package memory

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== Data Query ====================

// SupportsFiltering implements FilterableDataSource interface capability declaration
func (m *MVCCDataSource) SupportsFiltering(tableName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if table exists
	_, ok := m.tables[tableName]
	return ok
}

// Filter implements FilterableDataSource interface filter and pagination methods
func (m *MVCCDataSource) Filter(
	ctx context.Context,
	tableName string,
	filter domain.Filter,
	offset, limit int,
) ([]domain.Row, int64, error) {
	m.mu.RLock()

	if !m.connected {
		m.mu.RUnlock()
		return nil, 0, domain.NewErrNotConnected("memory")
	}

	// Check if table exists
	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.RUnlock()
		return nil, 0, domain.NewErrTableNotFound(tableName)
	}

	m.mu.RUnlock()

	// Get table data
	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	if tableVer.latest < 0 {
		return nil, 0, domain.NewErrTableNotFound(tableName)
	}

	tableData := tableVer.versions[tableVer.latest]
	if tableData == nil || tableData.schema == nil {
		return nil, 0, domain.NewErrTableNotFound(tableName)
	}

	// Build filter list
	var filters []domain.Filter
	if filter.Field != "" || filter.Operator != "" {
		filters = []domain.Filter{filter}
	} else if subFilters, ok := filter.Value.([]domain.Filter); ok && len(subFilters) > 0 {
		filters = subFilters
	}

	// Use util.ApplyFilters to filter data
	options := &domain.QueryOptions{
		Filters: filters,
	}

	filteredRows := util.ApplyFilters(tableData.Rows(), options)
	total := int64(len(filteredRows))

	// Apply pagination
	if offset < 0 {
		offset = 0
	}
	if offset >= len(filteredRows) {
		return []domain.Row{}, total, nil
	}

	end := len(filteredRows)
	if limit > 0 && (offset+limit) < end {
		end = offset + limit
	}

	return filteredRows[offset:end], total, nil
}

// Query queries data
func (m *MVCCDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.mu.RLock()

	if !m.connected {
		m.mu.RUnlock()
		return nil, domain.NewErrNotConnected("memory")
	}

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.RUnlock()
		return nil, domain.NewErrTableNotFound(tableName)
	}

	txnID, hasTxn := GetTransactionID(ctx)
	var tableData *TableData

	if hasTxn {
		// In transaction, read from COW snapshot
		snapshot, ok := m.snapshots[txnID]
		if ok {
			cowSnapshot, ok := snapshot.tableSnapshots[tableName]
			if ok {
				tableData = cowSnapshot.getTableData(tableVer)
				m.mu.RUnlock()
			} else {
				m.mu.RUnlock()
				tableVer.mu.RLock()
				tableData = tableVer.versions[tableVer.latest]
				tableVer.mu.RUnlock()
			}
		} else {
			m.mu.RUnlock()
			tableVer.mu.RLock()
			tableData = tableVer.versions[tableVer.latest]
			tableVer.mu.RUnlock()
		}
	} else {
		// Non-transaction query, read from latest version
		m.mu.RUnlock()
		tableVer.mu.RLock()
		tableData = tableVer.versions[tableVer.latest]
		tableVer.mu.RUnlock()
	}

	if tableData == nil {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	// Use query optimizer to optimize query
	var queryResult *domain.QueryResult
	var err error

	if options != nil && len(options.Filters) > 0 {
		// Has filter conditions, use query optimizer
		plan, planErr := m.queryPlanner.PlanQuery(tableName, options.Filters, options)
		if planErr != nil {
			// Optimization failed, use full table scan
			pagedRows := util.ApplyQueryOperations(tableData.Rows(), options, &tableData.schema.Columns)
			queryResult = &domain.QueryResult{
				Columns: tableData.schema.Columns,
				Rows:    pagedRows,
				Total:   int64(len(pagedRows)),
			}
		} else {
			// Execute optimized query plan
			queryResult, err = m.queryPlanner.ExecutePlan(plan, tableData)
			if err != nil {
				// Execution failed, use full table scan
				pagedRows := util.ApplyQueryOperations(tableData.Rows(), options, &tableData.schema.Columns)
				queryResult = &domain.QueryResult{
					Columns: tableData.schema.Columns,
					Rows:    pagedRows,
					Total:   int64(len(pagedRows)),
				}
			} else {
				// Phase 2: Process VIRTUAL column dynamic calculation
				virtualCalc := generated.NewVirtualCalculator()
				if virtualCalc.HasVirtualColumns(tableData.schema) {
					// Dynamically calculate all VIRTUAL columns
					calculatedRows, calcErr := virtualCalc.CalculateBatchVirtuals(queryResult.Rows, tableData.schema)
					if calcErr == nil {
						queryResult.Rows = calculatedRows
					}
					// If calculation fails, use original row data (VIRTUAL columns are NULL)
				}

				// Apply sorting and pagination
				if options != nil {
					if options.OrderBy != "" {
						queryResult.Rows = util.ApplyOrder(queryResult.Rows, options)
					}
					// Record total before pagination
					queryResult.Total = int64(len(queryResult.Rows))
					if options.Limit > 0 || options.Offset > 0 {
						queryResult.Rows = util.ApplyPagination(queryResult.Rows, int(options.Offset), int(options.Limit))
					}
				} else {
					queryResult.Total = int64(len(queryResult.Rows))
				}
			}
		}
	} else {
		// No filter conditions, use full table scan
		pagedRows := util.ApplyQueryOperations(tableData.Rows(), options, &tableData.schema.Columns)
		queryResult = &domain.QueryResult{
			Columns: tableData.schema.Columns,
			Rows:    pagedRows,
			Total:   int64(len(pagedRows)),
		}
		// Phase 2: Process VIRTUAL column dynamic calculation
		virtualCalc := generated.NewVirtualCalculator()
		if virtualCalc.HasVirtualColumns(tableData.schema) {
			// Dynamically calculate all VIRTUAL columns
			calculatedRows, calcErr := virtualCalc.CalculateBatchVirtuals(queryResult.Rows, tableData.schema)
			if calcErr == nil {
				queryResult.Rows = calculatedRows
			}
			// If calculation fails, use original row data (VIRTUAL columns are NULL)
		}
	}

	// Convert row types based on schema (e.g., int64(0/1) to bool for BOOL columns)
	schema := tableData.schema
	for _, row := range queryResult.Rows {
		convertRowTypesBasedOnSchema(row, schema)
	}

	return queryResult, nil
}
