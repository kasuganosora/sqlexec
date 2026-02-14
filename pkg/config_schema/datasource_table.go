package config_schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// DatasourceTable is a writable virtual table for managing data source configurations
type DatasourceTable struct {
	dsManager *application.DataSourceManager
	configDir string
}

// NewDatasourceTable creates a new DatasourceTable
func NewDatasourceTable(dsManager *application.DataSourceManager, configDir string) *DatasourceTable {
	return &DatasourceTable{
		dsManager: dsManager,
		configDir: configDir,
	}
}

// GetName returns the table name
func (t *DatasourceTable) GetName() string {
	return "datasource"
}

// GetSchema returns the table schema
func (t *DatasourceTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "name", Type: "varchar(64)", Nullable: false, Primary: true},
		{Name: "type", Type: "varchar(32)", Nullable: false},
		{Name: "host", Type: "varchar(255)", Nullable: true},
		{Name: "port", Type: "int", Nullable: true},
		{Name: "username", Type: "varchar(64)", Nullable: true},
		{Name: "password", Type: "varchar(128)", Nullable: true},
		{Name: "database_name", Type: "varchar(128)", Nullable: true},
		{Name: "writable", Type: "boolean", Nullable: true},
		{Name: "options", Type: "text", Nullable: true},
		{Name: "status", Type: "varchar(16)", Nullable: true},
	}
}

// Query executes a query against the datasource table
func (t *DatasourceTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	rows, err := t.buildRows()
	if err != nil {
		return nil, err
	}

	// Apply filters
	if len(filters) > 0 {
		rows = applyFilters(rows, filters)
	}

	total := int64(len(rows))

	// Apply limit/offset
	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(rows) {
			end = len(rows)
		}
		if start >= len(rows) {
			rows = []domain.Row{}
		} else {
			rows = rows[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    rows,
		Total:   total,
	}, nil
}

// buildRows builds result rows from JSON file + runtime datasources
func (t *DatasourceTable) buildRows() ([]domain.Row, error) {
	// Load from JSON file
	configs, err := loadDatasources(t.configDir)
	if err != nil {
		return nil, err
	}

	// Build a set of names from the JSON file
	configNames := make(map[string]bool)
	rows := make([]domain.Row, 0, len(configs))

	for _, cfg := range configs {
		configNames[cfg.Name] = true
		rows = append(rows, t.configToRow(cfg))
	}

	// Merge runtime datasources not in the JSON file
	for _, name := range t.dsManager.List() {
		if configNames[name] {
			continue
		}
		ds, err := t.dsManager.Get(name)
		if err != nil {
			continue
		}
		dsCfg := ds.GetConfig()
		if dsCfg != nil {
			rows = append(rows, t.configToRow(*dsCfg))
		}
	}

	return rows, nil
}

// configToRow converts a DataSourceConfig to a result Row
func (t *DatasourceTable) configToRow(cfg domain.DataSourceConfig) domain.Row {
	// Determine status from runtime DataSourceManager
	status := "disconnected"
	ds, err := t.dsManager.Get(cfg.Name)
	if err == nil && ds != nil {
		if ds.IsConnected() {
			status = "connected"
		}
	}

	// Mask password
	password := ""
	if cfg.Password != "" {
		password = "****"
	}

	// Serialize options
	optionsStr := ""
	if len(cfg.Options) > 0 {
		if data, err := json.Marshal(cfg.Options); err == nil {
			optionsStr = string(data)
		}
	}

	row := domain.Row{
		"name":          cfg.Name,
		"type":          string(cfg.Type),
		"host":          cfg.Host,
		"port":          cfg.Port,
		"username":      cfg.Username,
		"password":      password,
		"database_name": cfg.Database,
		"writable":      cfg.Writable,
		"options":       optionsStr,
		"status":        status,
	}

	return row
}

// Insert inserts new datasource configurations
func (t *DatasourceTable) Insert(ctx context.Context, rows []domain.Row) (int64, error) {
	// Load existing configs
	fileMu.Lock()
	defer fileMu.Unlock()

	configs, err := loadDatasources(t.configDir)
	if err != nil {
		return 0, err
	}

	// Build existing names set
	existing := make(map[string]bool)
	for _, cfg := range configs {
		existing[cfg.Name] = true
	}

	var inserted int64
	for _, row := range rows {
		cfg, err := t.rowToConfig(row)
		if err != nil {
			return inserted, err
		}

		if cfg.Name == "" {
			return inserted, fmt.Errorf("datasource name is required")
		}
		if cfg.Type == "" {
			return inserted, fmt.Errorf("datasource type is required")
		}

		if existing[cfg.Name] {
			return inserted, fmt.Errorf("datasource '%s' already exists", cfg.Name)
		}

		configs = append(configs, cfg)
		existing[cfg.Name] = true

		// Create and register the datasource at runtime
		if err := t.createAndRegister(ctx, cfg); err != nil {
			return inserted, fmt.Errorf("failed to create datasource '%s': %w", cfg.Name, err)
		}

		inserted++
	}

	// Save to file (we already hold fileMu)
	if err := saveDatasourcesUnlocked(t.configDir, configs); err != nil {
		return inserted, err
	}

	return inserted, nil
}

// Update updates datasource configurations matching the filters
func (t *DatasourceTable) Update(ctx context.Context, filters []domain.Filter, updates domain.Row) (int64, error) {
	fileMu.Lock()
	defer fileMu.Unlock()

	configs, err := loadDatasources(t.configDir)
	if err != nil {
		return 0, err
	}

	var updated int64
	for i := range configs {
		row := domain.Row{"name": configs[i].Name}
		if !matchesFilters(row, filters) {
			continue
		}

		// Apply updates
		if v, ok := updates["type"]; ok {
			configs[i].Type = domain.DataSourceType(fmt.Sprintf("%v", v))
		}
		if v, ok := updates["host"]; ok {
			configs[i].Host = fmt.Sprintf("%v", v)
		}
		if v, ok := updates["port"]; ok {
			configs[i].Port = toInt(v)
		}
		if v, ok := updates["username"]; ok {
			configs[i].Username = fmt.Sprintf("%v", v)
		}
		if v, ok := updates["password"]; ok {
			configs[i].Password = fmt.Sprintf("%v", v)
		}
		if v, ok := updates["database_name"]; ok {
			configs[i].Database = fmt.Sprintf("%v", v)
		}
		if v, ok := updates["writable"]; ok {
			configs[i].Writable = toBool(v)
		}
		if v, ok := updates["options"]; ok {
			optStr := fmt.Sprintf("%v", v)
			if optStr != "" {
				var opts map[string]interface{}
				if json.Unmarshal([]byte(optStr), &opts) == nil {
					configs[i].Options = opts
				}
			}
		}

		// Reconnect the datasource if it exists at runtime
		_ = t.reconnectDatasource(ctx, configs[i])

		updated++
	}

	if updated > 0 {
		if err := saveDatasourcesUnlocked(t.configDir, configs); err != nil {
			return updated, err
		}
	}

	return updated, nil
}

// Delete deletes datasource configurations matching the filters
func (t *DatasourceTable) Delete(ctx context.Context, filters []domain.Filter) (int64, error) {
	fileMu.Lock()
	defer fileMu.Unlock()

	configs, err := loadDatasources(t.configDir)
	if err != nil {
		return 0, err
	}

	var deleted int64
	remaining := make([]domain.DataSourceConfig, 0, len(configs))

	for _, cfg := range configs {
		row := domain.Row{"name": cfg.Name}
		if matchesFilters(row, filters) {
			// Unregister from runtime
			_ = t.dsManager.Unregister(cfg.Name)
			deleted++
		} else {
			remaining = append(remaining, cfg)
		}
	}

	if deleted > 0 {
		if err := saveDatasourcesUnlocked(t.configDir, remaining); err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// createAndRegister creates a datasource from config and registers it
func (t *DatasourceTable) createAndRegister(ctx context.Context, cfg domain.DataSourceConfig) error {
	// Try to create via factory first
	ds, err := t.dsManager.CreateFromConfig(&cfg)
	if err != nil {
		// Fallback to memory datasource for "memory" type
		if cfg.Type == domain.DataSourceTypeMemory {
			ds = memory.NewMVCCDataSource(&cfg)
		} else {
			return err
		}
	}

	if err := ds.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	return t.dsManager.Register(cfg.Name, ds)
}

// reconnectDatasource reconnects a datasource after config update
func (t *DatasourceTable) reconnectDatasource(ctx context.Context, cfg domain.DataSourceConfig) error {
	// Unregister the old datasource if it exists
	_ = t.dsManager.Unregister(cfg.Name)

	// Create and register the new one
	return t.createAndRegister(ctx, cfg)
}

// rowToConfig converts a Row to a DataSourceConfig
func (t *DatasourceTable) rowToConfig(row domain.Row) (domain.DataSourceConfig, error) {
	cfg := domain.DataSourceConfig{
		Writable: true, // default
	}

	if v, ok := row["name"]; ok {
		cfg.Name = fmt.Sprintf("%v", v)
	}
	if v, ok := row["type"]; ok {
		cfg.Type = domain.DataSourceType(fmt.Sprintf("%v", v))
	}
	if v, ok := row["host"]; ok {
		cfg.Host = fmt.Sprintf("%v", v)
	}
	if v, ok := row["port"]; ok {
		cfg.Port = toInt(v)
	}
	if v, ok := row["username"]; ok {
		cfg.Username = fmt.Sprintf("%v", v)
	}
	if v, ok := row["password"]; ok {
		cfg.Password = fmt.Sprintf("%v", v)
	}
	if v, ok := row["database_name"]; ok {
		cfg.Database = fmt.Sprintf("%v", v)
	}
	if v, ok := row["writable"]; ok {
		cfg.Writable = toBool(v)
	}
	if v, ok := row["options"]; ok {
		optStr := fmt.Sprintf("%v", v)
		if optStr != "" {
			var opts map[string]interface{}
			if json.Unmarshal([]byte(optStr), &opts) == nil {
				cfg.Options = opts
			}
		}
	}

	return cfg, nil
}

// saveDatasourcesUnlocked saves without acquiring the file lock (caller must hold it)
func saveDatasourcesUnlocked(configDir string, configs []domain.DataSourceConfig) error {
	filePath := filepath.Join(configDir, datasourcesFileName)

	data, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal datasource configs: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write %s: %w", filePath, err)
	}

	return nil
}

// applyFilters filters rows based on the given filters
func applyFilters(rows []domain.Row, filters []domain.Filter) []domain.Row {
	result := rows
	for _, filter := range filters {
		var filtered []domain.Row
		for _, row := range result {
			if matchesFilter(row, filter) {
				filtered = append(filtered, row)
			}
		}
		result = filtered
	}
	return result
}

// matchesFilters checks if a row matches all filters
func matchesFilters(row domain.Row, filters []domain.Filter) bool {
	for _, f := range filters {
		if !matchesFilter(row, f) {
			return false
		}
	}
	return true
}

// matchesFilter checks if a row matches a single filter
func matchesFilter(row domain.Row, filter domain.Filter) bool {
	value, exists := row[filter.Field]
	if !exists {
		return false
	}

	strValue := fmt.Sprintf("%v", value)
	filterValue := fmt.Sprintf("%v", filter.Value)

	switch strings.ToLower(filter.Operator) {
	case "=", "eq":
		return strValue == filterValue
	case "!=", "ne":
		return strValue != filterValue
	case "like":
		return matchesLike(strValue, filterValue)
	default:
		return false
	}
}

// matchesLike delegates to utils.MatchesLike for full LIKE pattern matching
func matchesLike(value, pattern string) bool {
	return utils.MatchesLike(value, pattern)
}

// toInt converts an interface{} to int
func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		var n int
		fmt.Sscanf(val, "%d", &n)
		return n
	default:
		return 0
	}
}

// toBool converts an interface{} to bool
func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val == "true" || val == "1" || val == "yes"
	case int:
		return val != 0
	case float64:
		return val != 0
	default:
		return false
	}
}
