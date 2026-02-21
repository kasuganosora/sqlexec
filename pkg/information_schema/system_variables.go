package information_schema

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// SystemVariablesTable represents information_schema.system_variables
// It lists all system variables and their values (MariaDB compatibility)
type SystemVariablesTable struct{}

// NewSystemVariablesTable creates a new SystemVariablesTable
func NewSystemVariablesTable() virtual.VirtualTable {
	return &SystemVariablesTable{}
}

// GetName returns table name
func (t *SystemVariablesTable) GetName() string {
	return "system_variables"
}

// GetSchema returns table schema
func (t *SystemVariablesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "VARIABLE_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "VARIABLE_VALUE", Type: "varchar(2048)", Nullable: true},
		{Name: "VARIABLE_TYPE", Type: "varchar(20)", Nullable: false},
		{Name: "VARIABLE_SCOPE", Type: "varchar(10)", Nullable: false},
		{Name: "VARIABLE_SOURCE", Type: "varchar(20)", Nullable: false},
		{Name: "GLOBAL_VALUE", Type: "varchar(2048)", Nullable: true},
		{Name: "GLOBAL_VALUE_ORIGIN", Type: "varchar(20)", Nullable: true},
		{Name: "SESSION_VALUE", Type: "varchar(2048)", Nullable: true},
		{Name: "DEFAULT_VALUE", Type: "varchar(2048)", Nullable: true},
		{Name: "READ_ONLY", Type: "varchar(3)", Nullable: false},
	}
}

// Query executes a query against system_variables table
func (t *SystemVariablesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Get system variables
	variables := t.getSystemVariables()

	// Apply filters if provided
	var err error
	if len(filters) > 0 {
		variables, err = utils.ApplyFilters(variables, filters)
		if err != nil {
			return nil, err
		}
	}

	// Apply limit/offset if specified
	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(variables) {
			end = len(variables)
		}
		if start >= len(variables) {
			variables = []domain.Row{}
		} else {
			variables = variables[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    variables,
		Total:   int64(len(variables)),
	}, nil
}

// SystemVariableDef defines a system variable
type SystemVariableDef struct {
	Name     string
	Value    string
	VarType  string
	Scope    string
	Source   string
	ReadOnly string
}

// GetSystemVariableDefs returns the shared list of system variable definitions.
// Used by both system_variables table and SHOW VARIABLES executor.
func GetSystemVariableDefs() []SystemVariableDef {
	return systemVariableDefs
}

// systemVariableDefs is the canonical list of system variables
var systemVariableDefs = []SystemVariableDef{
		// Version
		{"version", "8.0.0-sqlexec", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"version_comment", "sqlexec MySQL-compatible database", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"version_compile_machine", "x86_64", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"version_compile_os", "Linux", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"version_ssl_library", "", "STRING", "GLOBAL", "COMPILED", "YES"},

		// Network
		{"port", "3307", "INT", "GLOBAL", "CONFIG", "YES"},
		{"hostname", "localhost", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"bind_address", "*", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"socket", "/tmp/mysql.sock", "STRING", "GLOBAL", "CONFIG", "YES"},

		// Paths
		{"datadir", "/var/lib/mysql/", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"tmpdir", "/tmp", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"slow_query_log_file", "/var/lib/mysql/slow.log", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"general_log_file", "/var/lib/mysql/general.log", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"log_error", "/var/lib/mysql/error.log", "STRING", "GLOBAL", "CONFIG", "YES"},

		// Server IDs
		{"server_id", "1", "INT", "GLOBAL", "CONFIG", "NO"},
		{"uuid", "00000000-0000-0000-0000-000000000000", "STRING", "GLOBAL", "COMPILED", "YES"},

		// Character set and collation
		{"character_set_server", "utf8mb4", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"character_set_client", "utf8mb3", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"character_set_connection", "utf8mb4", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"character_set_database", "utf8mb4", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"character_set_results", "utf8mb4", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"character_set_system", "utf8mb3", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"character_set_filesystem", "latin1", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"character_sets_dir", "MYSQL_TEST_DIR/ß/", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"collation_server", "utf8mb4_general_ci", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"collation_database", "utf8mb4_general_ci", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"collation_connection", "utf8mb4_general_ci", "STRING", "SESSION", "DYNAMIC", "NO"},

		// Connections
		{"max_connections", "151", "INT", "GLOBAL", "CONFIG", "NO"},
		{"max_user_connections", "0", "INT", "GLOBAL", "CONFIG", "NO"},
		{"max_connect_errors", "100", "INT", "GLOBAL", "CONFIG", "NO"},
		{"connect_timeout", "10", "INT", "GLOBAL", "CONFIG", "NO"},
		{"wait_timeout", "28800", "INT", "SESSION", "CONFIG", "NO"},
		{"interactive_timeout", "28800", "INT", "SESSION", "CONFIG", "NO"},

		// Transaction and binlog
		{"autocommit", "ON", "BOOL", "SESSION", "DYNAMIC", "NO"},
		{"max_binlog_stmt_cache_size", "18446744073709547520", "BIGINT", "GLOBAL", "CONFIG", "NO"},
		{"max_binlog_cache_size", "18446744073709547520", "BIGINT", "GLOBAL", "CONFIG", "NO"},
		{"binlog_stmt_cache_size", "32768", "INT", "GLOBAL", "CONFIG", "NO"},
		{"binlog_cache_size", "32768", "INT", "GLOBAL", "CONFIG", "NO"},
		{"binlog_format", "STATEMENT", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"sync_binlog", "0", "INT", "GLOBAL", "CONFIG", "NO"},
		{"log_bin", "OFF", "BOOL", "GLOBAL", "COMPILED", "YES"},
		{"gtid_mode", "OFF", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"enforce_gtid_consistency", "OFF", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"transaction_isolation", "REPEATABLE-READ", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"tx_isolation", "REPEATABLE-READ", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"tx_read_only", "OFF", "BOOL", "SESSION", "DYNAMIC", "NO"},

		// Query and SQL
		{"sql_mode", "STRICT_TRANS_TABLES", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"sql_select_limit", "18446744073709547520", "BIGINT", "SESSION", "DYNAMIC", "NO"},
		{"max_execution_time", "0", "INT", "SESSION", "DYNAMIC", "NO"},
		{"max_join_size", "18446744073709547520", "BIGINT", "SESSION", "DYNAMIC", "NO"},
		{"query_cache_type", "OFF", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"query_cache_size", "0", "INT", "GLOBAL", "CONFIG", "NO"},
		{"slow_query_log", "OFF", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"general_log", "OFF", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"log_queries_not_using_indexes", "OFF", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"long_query_time", "10", "INT", "SESSION", "DYNAMIC", "NO"},

		// Memory and buffers
		{"key_buffer_size", "8388608", "INT", "GLOBAL", "CONFIG", "NO"},
		{"table_open_cache", "2000", "INT", "GLOBAL", "CONFIG", "NO"},
		{"table_definition_cache", "1400", "INT", "GLOBAL", "CONFIG", "NO"},
		{"thread_cache_size", "10", "INT", "GLOBAL", "CONFIG", "NO"},
		{"sort_buffer_size", "262144", "INT", "SESSION", "CONFIG", "NO"},
		{"join_buffer_size", "262144", "INT", "SESSION", "CONFIG", "NO"},
		{"read_buffer_size", "131072", "INT", "SESSION", "CONFIG", "NO"},
		{"read_rnd_buffer_size", "262144", "INT", "SESSION", "CONFIG", "NO"},
		{"myisam_sort_buffer_size", "8388608", "INT", "GLOBAL", "CONFIG", "NO"},
		{"bulk_insert_buffer_size", "8388608", "INT", "GLOBAL", "CONFIG", "NO"},
		{"innodb_buffer_pool_size", "134217728", "INT", "GLOBAL", "CONFIG", "YES"},

		// InnoDB
		{"innodb_flush_log_at_trx_commit", "1", "INT", "GLOBAL", "CONFIG", "NO"},
		{"innodb_lock_wait_timeout", "50", "INT", "GLOBAL", "CONFIG", "NO"},
		{"innodb_autoinc_lock_mode", "1", "INT", "GLOBAL", "CONFIG", "YES"},
		{"innodb_file_per_table", "ON", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"innodb_flush_method", "fsync", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"innodb_log_file_size", "50331648", "INT", "GLOBAL", "CONFIG", "YES"},
		{"innodb_log_buffer_size", "16777216", "INT", "GLOBAL", "CONFIG", "NO"},
		{"innodb_thread_concurrency", "0", "INT", "GLOBAL", "CONFIG", "NO"},
		{"innodb_locks_unsafe_for_binlog", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"innodb_rollback_on_timeout", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"innodb_support_xa", "ON", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"innodb_strict_mode", "OFF", "BOOL", "SESSION", "DYNAMIC", "NO"},
		{"innodb_ft_min_token_size", "3", "INT", "GLOBAL", "CONFIG", "YES"},
		{"innodb_ft_max_token_size", "84", "INT", "GLOBAL", "CONFIG", "YES"},

		// MyISAM
		{"myisam_recover_options", "OFF", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"myisam_max_sort_file_size", "9223372036853727232", "BIGINT", "GLOBAL", "CONFIG", "NO"},
		{"myisam_repair_threads", "1", "INT", "GLOBAL", "CONFIG", "NO"},
		{"concurrent_insert", "AUTO", "STRING", "GLOBAL", "CONFIG", "NO"},

		// Security
		{"skip_name_resolve", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"skip_networking", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"skip_show_database", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"local_infile", "ON", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"secure_file_priv", "", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"have_ssl", "DISABLED", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"have_openssl", "YES", "STRING", "GLOBAL", "COMPILED", "YES"},

		// Performance schema
		{"performance_schema", "OFF", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"performance_schema_instrument", "", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"performance_schema_consumers_events_statements_history_long", "ON", "BOOL", "GLOBAL", "CONFIG", "YES"},

		// Other
		{"lower_case_table_names", "0", "INT", "GLOBAL", "CONFIG", "YES"},
		{"explicit_defaults_for_timestamp", "ON", "BOOL", "GLOBAL", "CONFIG", "YES"},
		{"default_storage_engine", "InnoDB", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"default_tmp_storage_engine", "InnoDB", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"ft_min_word_len", "4", "INT", "GLOBAL", "CONFIG", "YES"},
		{"ft_max_word_len", "84", "INT", "GLOBAL", "CONFIG", "YES"},
		{"ft_query_expansion_limit", "20", "INT", "GLOBAL", "CONFIG", "NO"},
		{"lc_messages", "en_US", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"lc_time_names", "en_US", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"open_files_limit", "5000", "INT", "GLOBAL", "CONFIG", "YES"},
		{"pid_file", "/var/run/mysqld/mysqld.pid", "STRING", "GLOBAL", "CONFIG", "YES"},
		{"protocol_version", "10", "INT", "GLOBAL", "COMPILED", "YES"},
		{"read_only", "OFF", "BOOL", "GLOBAL", "DYNAMIC", "NO"},
		{"super_read_only", "OFF", "BOOL", "GLOBAL", "DYNAMIC", "NO"},
		{"storage_engine", "InnoDB", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"system_time_zone", "UTC", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"time_format", "%H:%i:%s", "STRING", "GLOBAL", "COMPILED", "YES"},
		{"time_zone", "SYSTEM", "STRING", "SESSION", "DYNAMIC", "NO"},
		{"updatable_views_with_limit", "YES", "STRING", "GLOBAL", "CONFIG", "NO"},
		{"userstat", "OFF", "BOOL", "GLOBAL", "CONFIG", "NO"},
		{"net_buffer_length", "16384", "INT", "SESSION", "CONFIG", "NO"},
		{"max_allowed_packet", "16777216", "INT", "SESSION", "CONFIG", "NO"},
		{"div_precision_increment", "4", "INT", "SESSION", "DYNAMIC", "NO"},
		{"group_concat_max_len", "1024", "INT", "SESSION", "DYNAMIC", "NO"},
		{"tmp_table_size", "16777216", "INT", "SESSION", "CONFIG", "NO"},
		{"max_heap_table_size", "16777216", "INT", "SESSION", "CONFIG", "NO"},

		// Sanitizer detection (for MariaDB tests)
		{"have_sanitizer", "", "STRING", "GLOBAL", "COMPILED", "YES"},
}

// getSystemVariables returns all system variables
func (t *SystemVariablesTable) getSystemVariables() []domain.Row {
	defs := GetSystemVariableDefs()
	rows := make([]domain.Row, 0, len(defs))
	for _, v := range defs {
		row := domain.Row{
			"VARIABLE_NAME":       v.Name,
			"VARIABLE_VALUE":      v.Value,
			"VARIABLE_TYPE":       v.VarType,
			"VARIABLE_SCOPE":      v.Scope,
			"VARIABLE_SOURCE":     v.Source,
			"GLOBAL_VALUE":        v.Value,
			"GLOBAL_VALUE_ORIGIN": v.Source,
			"SESSION_VALUE":       v.Value,
			"DEFAULT_VALUE":       v.Value,
			"READ_ONLY":           v.ReadOnly,
		}
		rows = append(rows, row)
	}

	return rows
}
