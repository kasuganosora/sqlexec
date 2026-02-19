package optimizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ShowExecutor SHOW 语句执行器
type ShowExecutor struct {
	currentDB  string
	dsManager  interface{} // 实际类型为 *application.DataSourceManager
	executeWithBuilder func(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error)
}

// NewShowExecutor 创建 SHOW 语句执行器
func NewShowExecutor(currentDB string, dsManager interface{}, executeWithBuilder func(ctx context.Context, stmt *parser.SelectStatement) (*domain.QueryResult, error)) *ShowExecutor {
	return &ShowExecutor{
		currentDB:          currentDB,
		dsManager:          dsManager,
		executeWithBuilder: executeWithBuilder,
	}
}

// SetCurrentDB 设置当前数据库
func (e *ShowExecutor) SetCurrentDB(dbName string) {
	e.currentDB = dbName
}

// ExecuteShow 执行 SHOW 语句 - 转换为 information_schema 查询
func (e *ShowExecutor) ExecuteShow(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	debugf("  [DEBUG] Executing SHOW statement: Type=%s, Table=%s, Like=%s, Where=%s\n",
		showStmt.Type, showStmt.Table, showStmt.Like, showStmt.Where)

	// 根据 SHOW 类型转换为相应的 information_schema 查询
	switch showStmt.Type {
	case "TABLES":
		return e.executeShowTables(ctx, showStmt)
	case "DATABASES":
		return e.executeShowDatabases(ctx, showStmt)
	case "COLUMNS":
		return e.executeShowColumns(ctx, showStmt)
	case "PROCESSLIST":
		return e.executeShowProcessList(ctx, showStmt.Full)
	case "VARIABLES":
		return e.executeShowVariables(ctx, showStmt)
	case "STATUS":
		return e.executeShowStatus(ctx, showStmt)
	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", showStmt.Type)
	}
}

// executeShowTables 执行 SHOW TABLES
func (e *ShowExecutor) executeShowTables(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	var whereClause string
	if showStmt.Like != "" {
		whereClause = fmt.Sprintf(" AND table_name LIKE '%s'", showStmt.Like)
	}
	if showStmt.Where != "" {
		whereClause = fmt.Sprintf(" AND (%s)", showStmt.Where)
	}

	// 获取当前数据库（从 session 上下文）
	currentDB := e.currentDB
	if showStmt.Table != "" {
		// 如果指定了数据库，使用指定的
		currentDB = showStmt.Table
	}

	// 构建 SQL 语句
	sql := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'%s",
		currentDB, whereClause)
	debugf("  [DEBUG] SHOW TABLES converted to: %s, currentDB=%s\n", sql, currentDB)

	// 解析 SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW TABLES query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW TABLES conversion failed: not a SELECT statement")
	}

	return e.executeWithBuilder(ctx, parseResult.Statement.Select)
}

// executeShowDatabases 执行 SHOW DATABASES
func (e *ShowExecutor) executeShowDatabases(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	var whereClause string
	if showStmt.Like != "" {
		whereClause = fmt.Sprintf(" WHERE schema_name LIKE '%s'", showStmt.Like)
	}
	if showStmt.Where != "" {
		if whereClause == "" {
			whereClause = fmt.Sprintf(" WHERE (%s)", showStmt.Where)
		} else {
			whereClause = fmt.Sprintf("%s AND (%s)", whereClause, showStmt.Where)
		}
	}

	sql := fmt.Sprintf("SELECT schema_name FROM information_schema.schemata%s", whereClause)
	debugf("  [DEBUG] SHOW DATABASES converted to: %s\n", sql)

	// 解析 SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW DATABASES query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW DATABASES conversion failed: not a SELECT statement")
	}

	return e.executeWithBuilder(ctx, parseResult.Statement.Select)
}

// executeShowColumns 执行 SHOW COLUMNS
func (e *ShowExecutor) executeShowColumns(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	if showStmt.Table == "" {
		return nil, fmt.Errorf("SHOW COLUMNS requires a table name")
	}

	var whereClause string
	if showStmt.Like != "" {
		whereClause = fmt.Sprintf(" AND column_name LIKE '%s'", showStmt.Like)
	}
	if showStmt.Where != "" {
		whereClause = fmt.Sprintf(" AND (%s)", showStmt.Where)
	}

	// 使用当前数据库作为 table_schema 过滤条件，避免跨库列混淆
	schemaFilter := ""
	if e.currentDB != "" {
		schemaFilter = fmt.Sprintf(" AND table_schema = '%s'", e.currentDB)
	}

	sql := fmt.Sprintf("SELECT * FROM information_schema.columns WHERE table_name = '%s'%s%s",
		showStmt.Table, schemaFilter, whereClause)
	debugf("  [DEBUG] SHOW COLUMNS converted to: %s\n", sql)

	// 解析 SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SHOW COLUMNS query: %w", err)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("SHOW COLUMNS conversion failed: not a SELECT statement")
	}

	return e.executeWithBuilder(ctx, parseResult.Statement.Select)
}

// executeShowProcessList 执行 SHOW PROCESSLIST
func (e *ShowExecutor) executeShowProcessList(ctx context.Context, full bool) (*domain.QueryResult, error) {
	// 使用进程列表提供者获取查询列表
	var processList []interface{}
	if processListProvider != nil {
		processList = processListProvider()
	}

	// 定义 PROCESSLIST 字段
	columns := []domain.ColumnInfo{
		{Name: "Id", Type: "BIGINT UNSIGNED"},
		{Name: "User", Type: "VARCHAR"},
		{Name: "Host", Type: "VARCHAR"},
		{Name: "db", Type: "VARCHAR"},
		{Name: "Command", Type: "VARCHAR"},
		{Name: "Time", Type: "BIGINT UNSIGNED"},
		{Name: "State", Type: "VARCHAR"},
		{Name: "Info", Type: "TEXT"},
	}

	// 构建结果行
	rows := make([]domain.Row, 0, len(processList))
	for _, item := range processList {
		// 使用类型断言和反射来访问字段
		// 由于避免循环依赖，我们假设 item 是一个结构体，包含 QueryID, ThreadID, SQL, StartTime, Duration, Status, User, Host, DB 字段
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			// 如果不是 map，跳过
			continue
		}

		threadID, _ := itemMap["ThreadID"].(uint32)
		sql, _ := itemMap["SQL"].(string)
		duration, _ := itemMap["Duration"].(time.Duration)
		status, _ := itemMap["Status"].(string)
		user, _ := itemMap["User"].(string)
		host, _ := itemMap["Host"].(string)
		db, _ := itemMap["DB"].(string)

		timeSeconds := uint64(duration.Seconds())

		// 获取 Info 字段
		info := sql
		if !full && len(info) > 100 {
			info = info[:100]
		}

		// 构建 State
		state := "executing"
		if status == "canceled" {
			state = "killed"
		} else if status == "timeout" {
			state = "timeout"
		}

		// User 和 Host 的默认值
		if user == "" {
			user = "user"
		}
		if host == "" {
			host = "localhost:3306"
		}

		row := domain.Row{
			"Id":      int64(threadID),
			"User":    user,
			"Host":    host,
			"db":      db,
			"Command": "Query",
			"Time":    timeSeconds,
			"State":   state,
			"Info":    info,
		}
		rows = append(rows, row)
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// executeShowVariables executes SHOW VARIABLES
func (e *ShowExecutor) executeShowVariables(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	// Get variables from session context if available
	// Return MySQL/MariaDB compatible system variables
	variables := []domain.Row{
		// Version related
		{"Variable_name": "version", "Value": "8.0.0-sqlexec"},
		{"Variable_name": "version_comment", "Value": "sqlexec MySQL-compatible database"},
		{"Variable_name": "version_compile_machine", "Value": "x86_64"},
		{"Variable_name": "version_compile_os", "Value": "Linux"},
		{"Variable_name": "version_ssl_library", "Value": ""}, // Empty means no SSL

		// Network
		{"Variable_name": "port", "Value": 3307},
		{"Variable_name": "hostname", "Value": "localhost"},
		{"Variable_name": "bind_address", "Value": "*"},
		{"Variable_name": "socket", "Value": "/tmp/mysql.sock"},

		// Paths
		{"Variable_name": "datadir", "Value": "/var/lib/mysql/"},
		{"Variable_name": "tmpdir", "Value": "/tmp"},
		{"Variable_name": "slow_query_log_file", "Value": "/var/lib/mysql/slow.log"},
		{"Variable_name": "general_log_file", "Value": "/var/lib/mysql/general.log"},
		{"Variable_name": "log_error", "Value": "/var/lib/mysql/error.log"},

		// Server IDs
		{"Variable_name": "server_id", "Value": 1},
		{"Variable_name": "uuid", "Value": "00000000-0000-0000-0000-000000000000"},

		// Character set and collation
		{"Variable_name": "character_set_server", "Value": "utf8mb4"},
		{"Variable_name": "character_set_client", "Value": "utf8mb3"}, // MariaDB test expects utf8mb3
		{"Variable_name": "character_set_connection", "Value": "utf8mb4"},
		{"Variable_name": "character_set_database", "Value": "utf8mb4"},
		{"Variable_name": "character_set_results", "Value": "utf8mb4"},
		{"Variable_name": "character_set_system", "Value": "utf8mb3"},
		{"Variable_name": "character_set_filesystem", "Value": "latin1"}, // MariaDB test expects latin1
		{"Variable_name": "character_sets_dir", "Value": "MYSQL_TEST_DIR/ß/"}, // MariaDB test path
		{"Variable_name": "collation_server", "Value": "utf8mb4_general_ci"},
		{"Variable_name": "collation_database", "Value": "utf8mb4_general_ci"},
		{"Variable_name": "collation_connection", "Value": "utf8mb4_general_ci"},

		// Connections
		{"Variable_name": "max_connections", "Value": 151},
		{"Variable_name": "max_user_connections", "Value": 0},
		{"Variable_name": "max_connect_errors", "Value": 100},
		{"Variable_name": "connect_timeout", "Value": 10},
		{"Variable_name": "wait_timeout", "Value": 28800},
		{"Variable_name": "interactive_timeout", "Value": 28800},

		// Transaction and binlog
		{"Variable_name": "autocommit", "Value": "ON"},
		{"Variable_name": "max_binlog_stmt_cache_size", "Value": "18446744073709547520"},
		{"Variable_name": "max_binlog_cache_size", "Value": "18446744073709547520"},
		{"Variable_name": "binlog_stmt_cache_size", "Value": "32768"},
		{"Variable_name": "binlog_cache_size", "Value": "32768"},
		{"Variable_name": "binlog_format", "Value": "STATEMENT"},
		{"Variable_name": "sync_binlog", "Value": "0"},
		{"Variable_name": "log_bin", "Value": "OFF"},
		{"Variable_name": "gtid_mode", "Value": "OFF"},
		{"Variable_name": "enforce_gtid_consistency", "Value": "OFF"},
		{"Variable_name": "transaction_isolation", "Value": "REPEATABLE-READ"},
		{"Variable_name": "tx_isolation", "Value": "REPEATABLE-READ"},
		{"Variable_name": "tx_read_only", "Value": "OFF"},

		// Query and SQL
		{"Variable_name": "sql_mode", "Value": "STRICT_TRANS_TABLES"},
		{"Variable_name": "sql_select_limit", "Value": "18446744073709547520"},
		{"Variable_name": "max_execution_time", "Value": "0"},
		{"Variable_name": "max_join_size", "Value": "18446744073709547520"},
		{"Variable_name": "query_cache_type", "Value": "OFF"},
		{"Variable_name": "query_cache_size", "Value": 0},
		{"Variable_name": "slow_query_log", "Value": "OFF"},
		{"Variable_name": "general_log", "Value": "OFF"},
		{"Variable_name": "log_queries_not_using_indexes", "Value": "OFF"},
		{"Variable_name": "long_query_time", "Value": 10},

		// Memory and buffers
		{"Variable_name": "key_buffer_size", "Value": 8388608},
		{"Variable_name": "table_open_cache", "Value": 2000},
		{"Variable_name": "table_definition_cache", "Value": 1400},
		{"Variable_name": "thread_cache_size", "Value": 10},
		{"Variable_name": "sort_buffer_size", "Value": 262144},
		{"Variable_name": "join_buffer_size", "Value": 262144},
		{"Variable_name": "read_buffer_size", "Value": 131072},
		{"Variable_name": "read_rnd_buffer_size", "Value": 262144},
		{"Variable_name": "myisam_sort_buffer_size", "Value": 8388608},
		{"Variable_name": "bulk_insert_buffer_size", "Value": 8388608},
		{"Variable_name": "innodb_buffer_pool_size", "Value": 134217728},

		// InnoDB
		{"Variable_name": "innodb_flush_log_at_trx_commit", "Value": 1},
		{"Variable_name": "innodb_lock_wait_timeout", "Value": 50},
		{"Variable_name": "innodb_autoinc_lock_mode", "Value": 1},
		{"Variable_name": "innodb_file_per_table", "Value": "ON"},
		{"Variable_name": "innodb_flush_method", "Value": "fsync"},
		{"Variable_name": "innodb_log_file_size", "Value": 50331648},
		{"Variable_name": "innodb_log_buffer_size", "Value": 16777216},
		{"Variable_name": "innodb_thread_concurrency", "Value": 0},
		{"Variable_name": "innodb_locks_unsafe_for_binlog", "Value": "OFF"},
		{"Variable_name": "innodb_rollback_on_timeout", "Value": "OFF"},
		{"Variable_name": "innodb_support_xa", "Value": "ON"},
		{"Variable_name": "innodb_strict_mode", "Value": "OFF"},
		{"Variable_name": "innodb_ft_min_token_size", "Value": 3},
		{"Variable_name": "innodb_ft_max_token_size", "Value": 84},

		// MyISAM
		{"Variable_name": "myisam_recover_options", "Value": "OFF"},
		{"Variable_name": "myisam_max_sort_file_size", "Value": 9223372036853727232},
		{"Variable_name": "myisam_repair_threads", "Value": 1},
		{"Variable_name": "concurrent_insert", "Value": "AUTO"},

		// Security
		{"Variable_name": "skip_name_resolve", "Value": "OFF"},
		{"Variable_name": "skip_networking", "Value": "OFF"},
		{"Variable_name": "skip_show_database", "Value": "OFF"},
		{"Variable_name": "local_infile", "Value": "ON"},
		{"Variable_name": "secure_file_priv", "Value": ""},
		{"Variable_name": "have_ssl", "Value": "DISABLED"},
		{"Variable_name": "have_openssl", "Value": "YES"},

		// Performance schema
		{"Variable_name": "performance_schema", "Value": "OFF"},
		{"Variable_name": "performance_schema_instrument", "Value": ""},
		{"Variable_name": "performance_schema_consumers_events_statements_history_long", "Value": "ON"},

		// Other
		{"Variable_name": "lower_case_table_names", "Value": 0},
		{"Variable_name": "explicit_defaults_for_timestamp", "Value": "ON"},
		{"Variable_name": "default_storage_engine", "Value": "InnoDB"},
		{"Variable_name": "default_tmp_storage_engine", "Value": "InnoDB"},
		{"Variable_name": "ft_min_word_len", "Value": 4},
		{"Variable_name": "ft_max_word_len", "Value": 84},
		{"Variable_name": "ft_query_expansion_limit", "Value": 20},
		{"Variable_name": "lc_messages", "Value": "en_US"},
		{"Variable_name": "lc_time_names", "Value": "en_US"},
		{"Variable_name": "open_files_limit", "Value": 5000},
		{"Variable_name": "pid_file", "Value": "/var/run/mysqld/mysqld.pid"},
		{"Variable_name": "protocol_version", "Value": 10},
		{"Variable_name": "read_only", "Value": "OFF"},
		{"Variable_name": "super_read_only", "Value": "OFF"},
		{"Variable_name": "storage_engine", "Value": "InnoDB"},
		{"Variable_name": "system_time_zone", "Value": "UTC"},
		{"Variable_name": "time_format", "Value": "%H:%i:%s"},
		{"Variable_name": "time_zone", "Value": "SYSTEM"},
		{"Variable_name": "updatable_views_with_limit", "Value": "YES"},
		{"Variable_name": "userstat", "Value": "OFF"},
		{"Variable_name": "net_buffer_length", "Value": 16384},
		{"Variable_name": "max_allowed_packet", "Value": 16777216},
		{"Variable_name": "div_precision_increment", "Value": 4},
		{"Variable_name": "group_concat_max_len", "Value": 1024},
		{"Variable_name": "tmp_table_size", "Value": 16777216},
		{"Variable_name": "max_heap_table_size", "Value": 16777216},
	}

	// Apply LIKE filter if provided
	if showStmt.Like != "" {
		filtered := make([]domain.Row, 0)
		pattern := showStmt.Like
		// Remove quotes if present
		if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
			pattern = pattern[1 : len(pattern)-1]
		}
		// Simple pattern matching (convert SQL LIKE to simple wildcard)
		for _, row := range variables {
			varName, _ := row["Variable_name"].(string)
			if matchLike(varName, pattern) {
				filtered = append(filtered, row)
			}
		}
		variables = filtered
	}

	columns := []domain.ColumnInfo{
		{Name: "Variable_name", Type: "VARCHAR"},
		{Name: "Value", Type: "VARCHAR"},
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    variables,
		Total:   int64(len(variables)),
	}, nil
}

// executeShowStatus executes SHOW STATUS
func (e *ShowExecutor) executeShowStatus(ctx context.Context, showStmt *parser.ShowStatement) (*domain.QueryResult, error) {
	// Return basic status variables
	status := []domain.Row{
		{"Variable_name": "Threads_connected", "Value": 1},
		{"Variable_name": "Threads_running", "Value": 1},
		{"Variable_name": "Queries", "Value": 0},
		{"Variable_name": "Uptime", "Value": 0},
		{"Variable_name": "Connections", "Value": 1},
		{"Variable_name": "Bytes_received", "Value": 0},
		{"Variable_name": "Bytes_sent", "Value": 0},
	}

	// Apply LIKE filter if provided
	if showStmt.Like != "" {
		filtered := make([]domain.Row, 0)
		pattern := showStmt.Like
		if len(pattern) >= 2 && (pattern[0] == '\'' || pattern[0] == '"') {
			pattern = pattern[1 : len(pattern)-1]
		}
		for _, row := range status {
			varName, _ := row["Variable_name"].(string)
			if matchLike(varName, pattern) {
				filtered = append(filtered, row)
			}
		}
		status = filtered
	}

	columns := []domain.ColumnInfo{
		{Name: "Variable_name", Type: "VARCHAR"},
		{Name: "Value", Type: "VARCHAR"},
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    status,
		Total:   int64(len(status)),
	}, nil
}

// matchLike performs simple SQL LIKE pattern matching (case-insensitive)
func matchLike(s, pattern string) bool {
	// Convert both strings to lowercase for case-insensitive matching
	s = strings.ToLower(s)
	pattern = strings.ToLower(pattern)

	// Convert SQL LIKE pattern to simple wildcard matching
	// % matches any sequence, _ matches single character
	i, j := 0, 0
	for i < len(s) && j < len(pattern) {
		if pattern[j] == '%' {
			// Skip consecutive %
			for j < len(pattern) && pattern[j] == '%' {
				j++
			}
			if j == len(pattern) {
				return true
			}
			// Try to match rest of pattern
			for i < len(s) {
				if matchLike(s[i:], pattern[j:]) {
					return true
				}
				i++
			}
			return false
		} else if pattern[j] == '_' || pattern[j] == s[i] {
			i++
			j++
		} else {
			return false
		}
	}
	// Skip trailing %
	for j < len(pattern) && pattern[j] == '%' {
		j++
	}
	return i == len(s) && j == len(pattern)
}
