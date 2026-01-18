package protocol

import "fmt"

const (
	CLIENT_LONG_PASSWORD                  = 1 << iota // Use the improved version of Old Password Authentication.
	CLIENT_FOUND_ROWS                                 // Send found rows instead of affected rows in EOF_Packet.
	CLIENT_LONG_FLAG                                  // Get all column flags.
	CLIENT_CONNECT_WITH_DB                            // Database (schema) name can be specified on connect in Handshake Response Packet.
	CLIENT_NO_SCHEMA                                  // DEPRECATED: Don't allow database.table.column.
	CLIENT_COMPRESS                                   // Compression protocol supported.
	CLIENT_ODBC                                       // Special handling of ODBC behavior.
	CLIENT_LOCAL_FILES                                // Can use LOAD DATA LOCAL.
	CLIENT_IGNORE_SPACE                               // Ignore spaces before '('.
	CLIENT_PROTOCOL_41                                // New 4.1 protocol.
	CLIENT_INTERACTIVE                                // This is an interactive client.
	CLIENT_SSL                                        // Use SSL encryption for the session.
	CLIENT_IGNORE_SIGPIPE                             // Client only flag.
	CLIENT_TRANSACTIONS                               // Client knows about transactions.
	CLIENT_RESERVED                                   // DEPRECATED: Old flag for 4.1 protocol
	CLIENT_SECURE_CONNECTION                          // DEPRECATED: Old flag for 4.1 authentication
	CLIENT_MULTI_STATEMENTS                           // Enable/disable multi-stmt support.
	CLIENT_MULTI_RESULTS                              // Enable/disable multi-results.
	CLIENT_PS_MULTI_RESULTS                           // Multi-results and OUT parameters in PS-protocol.
	CLIENT_PLUGIN_AUTH                                // Client supports plugin authentication.
	CLIENT_CONNECT_ATTRS                              // Client supports connection attributes.
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA             // Enable authentication response packet to be larger than 255 bytes.
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS               // Don't close the connection for a user account with expired password.
	CLIENT_SESSION_TRACK                              // Capable of handling server state change information.
	CLIENT_DEPRECATE_EOF                              // Client no longer needs EOF_Packet and will use OK_Packet instead.
	CLIENT_OPTIONAL_RESULTSET_METADATA                // The client can handle optional metadata information in the resultset.
	CLIENT_ZSTD_COMPRESSION_ALGORITHM                 // Compression protocol extended to support zstd compression method.
	CLIENT_QUERY_ATTRIBUTES                           // Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets.
	_
	MULTI_FACTOR_AUTHENTICATION   // Support Multi factor authentication.
	CLIENT_CAPABILITY_EXTENSION   // This flag will be reserved to extend the 32bit capabilities structure to 64bits.
	CLIENT_SSL_VERIFY_SERVER_CERT // Verify server certificate.
	CLIENT_REMEMBER_OPTIONS       // Don't reset the options after an unsuccessful connect.
)

// MariaDB特定能力标志
const (
	MARIADB_CLIENT_CACHE_METADATA    = 1 << 0 // 客户端可以处理元数据缓存
	MARIADB_CLIENT_EXTENDED_METADATA = 1 << 1 // 客户端可以处理扩展元数据（如'point', 'json'）
)

// MySQL字段类型常量
const (
	MYSQL_TYPE_DECIMAL     = 0x00
	MYSQL_TYPE_TINY        = 0x01
	MYSQL_TYPE_SHORT       = 0x02
	MYSQL_TYPE_LONG        = 0x03
	MYSQL_TYPE_FLOAT       = 0x04
	MYSQL_TYPE_DOUBLE      = 0x05
	MYSQL_TYPE_NULL        = 0x06
	MYSQL_TYPE_TIMESTAMP   = 0x07
	MYSQL_TYPE_LONGLONG    = 0x08
	MYSQL_TYPE_INT24       = 0x09
	MYSQL_TYPE_DATE        = 0x0a
	MYSQL_TYPE_TIME        = 0x0b
	MYSQL_TYPE_DATETIME    = 0x0c
	MYSQL_TYPE_YEAR        = 0x0d
	MYSQL_TYPE_NEWDATE     = 0x0e
	MYSQL_TYPE_VARCHAR     = 0x0f
	MYSQL_TYPE_BIT         = 0x10
	MYSQL_TYPE_NEWDECIMAL  = 0xf6
	MYSQL_TYPE_ENUM        = 0xf7
	MYSQL_TYPE_SET         = 0xf8
	MYSQL_TYPE_TINY_BLOB   = 0xfc
	MYSQL_TYPE_MEDIUM_BLOB = 0xfd
	MYSQL_TYPE_LONG_BLOB   = 0xfe
	MYSQL_TYPE_BLOB        = 0xfc
	MYSQL_TYPE_VAR_STRING  = 0xfd
	MYSQL_TYPE_STRING      = 0xfe
	MYSQL_TYPE_GEOMETRY    = 0xff
)

// 字段标志常量
const (
	NOT_NULL_FLAG        = 1 << 0
	PRI_KEY_FLAG         = 1 << 1
	UNIQUE_KEY_FLAG      = 1 << 2
	MULTIPLE_KEY_FLAG    = 1 << 3
	BLOB_FLAG            = 1 << 4
	UNSIGNED_FLAG        = 1 << 5
	ZEROFILL_FLAG        = 1 << 6
	BINARY_COLLATION_FLAG = 1 << 7
	ENUM_FLAG            = 1 << 8
	AUTO_INCREMENT_FLAG  = 1 << 9
	TIMESTAMP_FLAG       = 1 << 10
	SET_FLAG             = 1 << 11
	NO_DEFAULT_VALUE_FLAG = 1 << 12
	ON_UPDATE_NOW_FLAG   = 1 << 13
	NUM_FLAG             = 1 << 15
)

const (
	SERVER_STATUS_IN_TRANS             = 1 << iota // 1
	SERVER_STATUS_AUTOCOMMIT                       // 2
	_                                              // 跳过 4 (1<<2)
	SERVER_MORE_RESULTS_EXISTS                     // 8 (1<<3)
	SERVER_QUERY_NO_GOOD_INDEX_USED                // 16 (1<<4)
	SERVER_QUERY_NO_INDEX_USED                     // 32 (1<<5)
	SERVER_STATUS_CURSOR_EXISTS                    // 64 (1<<6)
	SERVER_STATUS_LAST_ROW_SENT                    // 128 (1<<7)
	SERVER_STATUS_DB_DROPPED                       // 256 (1<<8)
	SERVER_STATUS_NO_BACKSLASH_ESCAPES             // 512 (1<<9)
	SERVER_STATUS_METADATA_CHANGED                 // 1024 (1<<10)
	SERVER_QUERY_WAS_SLOW                          // 2048 (1<<11)
	SERVER_PS_OUT_PARAMS                           // 4096 (1<<12)
	SERVER_STATUS_IN_TRANS_READONLY                // 8192 (1<<13)
	SERVER_SESSION_STATE_CHANGED       = 1 << 14   // 16384 (1<<14)
)

// MySQL 命令常量表
// 参考: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_packets.html
const (
	COM_SLEEP               = 0x00 // 内部线程状态
	COM_QUIT                = 0x01 // 断开连接
	COM_INIT_DB             = 0x02 // 切换数据库
	COM_QUERY               = 0x03 // 执行 SQL 查询
	COM_FIELD_LIST          = 0x04 // 获取字段列表
	COM_CREATE_DB           = 0x05 // 创建数据库 (已废弃)
	COM_DROP_DB             = 0x06 // 删除数据库 (已废弃)
	COM_REFRESH             = 0x07 // 刷新
	COM_SHUTDOWN            = 0x08 // 关闭服务器
	COM_STATISTICS          = 0x09 // 获取服务器统计信息
	COM_PROCESS_INFO        = 0x0a // 获取进程信息
	COM_CONNECT             = 0x0b // 内部线程状态
	COM_PROCESS_KILL        = 0x0c // 终止进程
	COM_DEBUG               = 0x0d // 调试
	COM_PING                = 0x0e // 心跳包
	COM_TIME                = 0x0f // 获取时间
	COM_DELAYED_INSERT      = 0x10 // 延迟插入
	COM_CHANGE_USER         = 0x11 // 切换用户
	COM_BINLOG_DUMP         = 0x12 // 二进制日志转储
	COM_TABLE_DUMP          = 0x13 // 表转储
	COM_CONNECT_OUT         = 0x14 // 连接输出
	COM_REGISTER_SLAVE      = 0x15 // 注册从服务器
	COM_STMT_PREPARE        = 0x16 // 预处理语句
	COM_STMT_EXECUTE        = 0x17 // 执行预处理语句
	COM_STMT_SEND_LONG_DATA = 0x18 // 发送长数据
	COM_STMT_CLOSE          = 0x19 // 关闭预处理语句
	COM_STMT_RESET          = 0x1a // 重置预处理语句
	COM_SET_OPTION          = 0x1b // 设置选项
	COM_STMT_FETCH          = 0x1c // 获取数据
	COM_DAEMON              = 0x1d // 守护进程
	COM_ERROR               = 0xff // 错误包
)

// EOF 包相关常量
const (
	EOF_MARKER = 0xfe // EOF 包标记
)

// 命令名称映射表，用于调试和日志
var CommandNames = map[uint8]string{
	COM_SLEEP:               "COM_SLEEP",
	COM_QUIT:                "COM_QUIT",
	COM_INIT_DB:             "COM_INIT_DB",
	COM_QUERY:               "COM_QUERY",
	COM_FIELD_LIST:          "COM_FIELD_LIST",
	COM_CREATE_DB:           "COM_CREATE_DB",
	COM_DROP_DB:             "COM_DROP_DB",
	COM_REFRESH:             "COM_REFRESH",
	COM_SHUTDOWN:            "COM_SHUTDOWN",
	COM_STATISTICS:          "COM_STATISTICS",
	COM_PROCESS_INFO:        "COM_PROCESS_INFO",
	COM_CONNECT:             "COM_CONNECT",
	COM_PROCESS_KILL:        "COM_PROCESS_KILL",
	COM_DEBUG:               "COM_DEBUG",
	COM_PING:                "COM_PING",
	COM_TIME:                "COM_TIME",
	COM_DELAYED_INSERT:      "COM_DELAYED_INSERT",
	COM_CHANGE_USER:         "COM_CHANGE_USER",
	COM_BINLOG_DUMP:         "COM_BINLOG_DUMP",
	COM_TABLE_DUMP:          "COM_TABLE_DUMP",
	COM_CONNECT_OUT:         "COM_CONNECT_OUT",
	COM_REGISTER_SLAVE:      "COM_REGISTER_SLAVE",
	COM_STMT_PREPARE:        "COM_STMT_PREPARE",
	COM_STMT_EXECUTE:        "COM_STMT_EXECUTE",
	COM_STMT_SEND_LONG_DATA: "COM_STMT_SEND_LONG_DATA",
	COM_STMT_CLOSE:          "COM_STMT_CLOSE",
	COM_STMT_RESET:          "COM_STMT_RESET",
	COM_SET_OPTION:          "COM_SET_OPTION",
	COM_STMT_FETCH:          "COM_STMT_FETCH",
	COM_DAEMON:              "COM_DAEMON",
	COM_ERROR:               "COM_ERROR",
}

// GetCommandName 根据命令ID获取命令名称
func GetCommandName(commandID uint8) string {
	if name, exists := CommandNames[commandID]; exists {
		return name
	}
	return fmt.Sprintf("UNKNOWN_COMMAND_%d", commandID)
}

// ============================================
// MariaDB Replication Protocol - 二进制日志事件类型
// ============================================

const (
	// 基础事件类型
	BINLOG_UNKNOWN_EVENT          = 0x00
	BINLOG_START_EVENT_V3        = 0x01
	BINLOG_QUERY_EVENT            = 0x02
	BINLOG_STOP_EVENT            = 0x03
	BINLOG_ROTATE_EVENT          = 0x04
	BINLOG_INTVAR_EVENT          = 0x05
	BINLOG_LOAD_EVENT            = 0x06
	BINLOG_SLAVE_EVENT           = 0x07
	BINLOG_CREATE_FILE_EVENT    = 0x08
	BINLOG_APPEND_BLOCK_EVENT   = 0x09
	BINLOG_EXEC_LOAD_EVENT      = 0x0a
	BINLOG_DELETE_FILE_EVENT    = 0x0b
	BINLOG_NEW_LOAD_EVENT       = 0x0c
	BINLOG_RAND_EVENT           = 0x0d
	BINLOG_USER_VAR_EVENT       = 0x0e
	BINLOG_FORMAT_DESCRIPTION_EVENT = 0x0f
	BINLOG_XID_EVENT            = 0x10
	BINLOG_BEGIN_LOAD_QUERY_EVENT = 0x11
	BINLOG_EXECUTE_LOAD_QUERY_EVENT = 0x12
	BINLOG_TABLE_MAP_EVENT       = 0x13
	BINLOG_WRITE_ROWS_EVENT_V1  = 0x14
	BINLOG_UPDATE_ROWS_EVENT_V1 = 0x15
	BINLOG_DELETE_ROWS_EVENT_V1 = 0x16
	BINLOG_WRITE_ROWS_EVENT_V2  = 0x17
	BINLOG_UPDATE_ROWS_EVENT_V2 = 0x18
	BINLOG_DELETE_ROWS_EVENT_V2 = 0x19
	BINLOG_INC_EVENT            = 0x1a
	BINLOG_HEARTBEAT_LOG_EVENT = 0x1b
	BINLOG_IGNORABLE_EVENT     = 0x1c
	BINLOG_ROWS_QUERY_EVENT    = 0x1d
	BINLOG_CREATE_USER_EVENT   = 0x1e
	BINLOG_COMMIT_USER_EVENT   = 0x1f

	// MariaDB 特有事件类型
	BINLOG_BEGIN_LOAD_QUERY_EVENT_V2 = 0x20
	BINLOG_EXECUTE_LOAD_QUERY_EVENT_V2 = 0x21
	BINLOG_DELETE_FILE_EVENT_V2 = 0x22

	// MariaDB 10.0+ 事件类型
	BINLOG_ANNOTATE_ROWS_EVENT   = 0x9a // 154
	BINLOG_BINLOG_CHECKPOINT_EVENT = 0x9b // 155
	BINLOG_GTID_EVENT              = 0xa2 // 162
	BINLOG_GTID_LIST_EVENT        = 0xae // 174
	BINLOG_START_ENCRYPTION_EVENT  = 0xaf // 175
)

// COM_BINLOG_DUMP 标志位
const (
	BINLOG_DUMP_NON_BLOCK                = 0x01 // 非阻塞模式，发送 EOF 包而不是等待新事件
	BINLOG_SEND_ANNOTATE_ROWS_EVENT = 0x02 // 发送 ANNOTATE_ROWS 事件
)

// GTID_EVENT 标志位
const (
	GTID_FL_STANDALONE     = 0x01 // 独立事件（没有终止 COMMIT），通常是 DDL
	GTID_FL_GROUP_COMMIT_ID = 0x02 // 组提交的一部分
	GTID_FL_TRANSACTIONAL   = 0x04 // 可以安全回滚（不包含非事务性引擎操作）
	GTID_FL_ALLOW_PARALLEL = 0x08 // 允许从库并行应用此事务
	GTID_FL_WAITED         = 0x10 // 执行期间检测到行锁等待
	GTID_FL_DDL            = 0x20 // 包含 DDL 语句
	GTID_FL_PREPARED_XA    = 0x40 // 已准备的 XA 事务
	GTID_FL_COMPLETED_XA   = 0x80 // 已完成（提交或回滚）的 XA 事务
)

// QUERY_EVENT 状态变量类型
const (
	Q_FLAGS2_CODE            = 0x00
	Q_SQL_MODE_CODE         = 0x01
	Q_CATALOG_NZ_CODE       = 0x02
	Q_AUTO_INCREMENT        = 0x03
	Q_CHARSET_CODE          = 0x04
	Q_TIMEZONE_CODE         = 0x05
	Q_CATALOG_CODE          = 0x06
	Q_LC_TIME_NAMES_CODE    = 0x07
	Q_CHARSET_DATABASE_CODE   = 0x08
	Q_TABLE_MAP_FOR_UPDATE_CODE = 0x09
	Q_MASTER_DATA_WRITTEN_CODE = 0x0a
	Q_INVOKER              = 0x0b
	Q_UPDATED_DB_NAMES       = 0x0c
	Q_MICROSECONDS        = 0x0d
	Q_HRNOW               = 0x80 // MariaDB 特有
	Q_XID                 = 0x81 // MariaDB 特有
)

// 二进制日志校验和算法
const (
	BINLOG_CHECKSUM_ALG_UNDEF   = 0 // 未定义
	BINLOG_CHECKSUM_ALG_NONE    = 1 // 无校验和
	BINLOG_CHECKSUM_ALG_CRC32   = 2 // CRC32 校验和
)

// 网络流状态字节
const (
	BINLOG_NETWORK_STATUS_OK  = 0x00 // OK（正常）
	BINLOG_NETWORK_STATUS_ERR = 0xff // ERR（错误）
	BINLOG_NETWORK_STATUS_EOF = 0xfe // EOF（文件结束）
)

// 二进制日志事件标志位
const (
	BINLOG_EVENT_LOG_EVENT_IN_PROGRESS = 0x0001 // binlog 文件正在使用中
	BINLOG_EVENT_FORCED_ROTATE          = 0x0002 // 强制轮换
	BINLOG_EVENT_THREAD_SPECIFIC       = 0x0004 // 查询依赖于特定线程
	BINLOG_EVENT_SUPPRESS_USE          = 0x0008 // 抑制 USE 语句生成
	BINLOG_EVENT_UPDATE_TABLE          = 0x0010 // 更新表（已废弃）
	BINLOG_EVENT_ARTIFICIAL            = 0x0020 // 伪事件（如 Fake Rotate）
	BINLOG_EVENT_RELAY_LOG           = 0x0040 // 中继日志事件
	BINLOG_EVENT_IGNORABLE            = 0x0080 // 可忽略的事件
)

// 可选元数据类型（BINLOG_ROW_METADATA）
const (
	METADATA_SIGNEDNESS          = 1 // MIN 模式：有符号性位图
	METADATA_DEFAULT_CHARSET      = 2 // MIN 模式：默认字符集
	METADATA_COLUMN_CHARSET      = 3 // MIN 模式：列字符集
	METADATA_COLUMN_NAME        = 4 // FULL 模式：列名
	METADATA_SET_STR_VALUE     = 5 // FULL 模式：SET 类型的值
	METADATA_ENUM_STR_VALUE    = 6 // FULL 模式：ENUM 类型的值
	METADATA_GEOMETRY_TYPE    = 7 // FULL 模式：GEOMETRY 子类型
	METADATA_SIMPLE_PRIMARY_KEY = 8 // FULL 模式：简单主键
	METADATA_PRIMARY_KEY_WITH_PREFIX = 9 // FULL 模式：带前缀的主键
)

// BinlogEventHeader 事件头长度
const (
	BINLOG_EVENT_HEADER_LENGTH = 19 // 标准事件头长度（字节）
)

// 获取事件类型名称
func GetBinlogEventTypeName(eventType uint8) string {
	names := map[uint8]string{
		BINLOG_UNKNOWN_EVENT:           "UNKNOWN_EVENT",
		BINLOG_START_EVENT_V3:         "START_EVENT_V3",
		BINLOG_QUERY_EVENT:             "QUERY_EVENT",
		BINLOG_STOP_EVENT:             "STOP_EVENT",
		BINLOG_ROTATE_EVENT:           "ROTATE_EVENT",
		BINLOG_INTVAR_EVENT:           "INTVAR_EVENT",
		BINLOG_LOAD_EVENT:             "LOAD_EVENT",
		BINLOG_SLAVE_EVENT:            "SLAVE_EVENT",
		BINLOG_CREATE_FILE_EVENT:      "CREATE_FILE_EVENT",
		BINLOG_APPEND_BLOCK_EVENT:     "APPEND_BLOCK_EVENT",
		BINLOG_EXEC_LOAD_EVENT:       "EXEC_LOAD_EVENT",
		BINLOG_DELETE_FILE_EVENT:     "DELETE_FILE_EVENT",
		BINLOG_NEW_LOAD_EVENT:        "NEW_LOAD_EVENT",
		BINLOG_RAND_EVENT:            "RAND_EVENT",
		BINLOG_USER_VAR_EVENT:        "USER_VAR_EVENT",
		BINLOG_FORMAT_DESCRIPTION_EVENT: "FORMAT_DESCRIPTION_EVENT",
		BINLOG_XID_EVENT:             "XID_EVENT",
		BINLOG_BEGIN_LOAD_QUERY_EVENT: "BEGIN_LOAD_QUERY_EVENT",
		BINLOG_EXECUTE_LOAD_QUERY_EVENT: "EXECUTE_LOAD_QUERY_EVENT",
		BINLOG_TABLE_MAP_EVENT:       "TABLE_MAP_EVENT",
		BINLOG_WRITE_ROWS_EVENT_V1:  "WRITE_ROWS_EVENT_V1",
		BINLOG_UPDATE_ROWS_EVENT_V1: "UPDATE_ROWS_EVENT_V1",
		BINLOG_DELETE_ROWS_EVENT_V1: "DELETE_ROWS_EVENT_V1",
		BINLOG_WRITE_ROWS_EVENT_V2:  "WRITE_ROWS_EVENT_V2",
		BINLOG_UPDATE_ROWS_EVENT_V2: "UPDATE_ROWS_EVENT_V2",
		BINLOG_DELETE_ROWS_EVENT_V2: "DELETE_ROWS_EVENT_V2",
		BINLOG_INC_EVENT:             "INC_EVENT",
		BINLOG_HEARTBEAT_LOG_EVENT:  "HEARTBEAT_LOG_EVENT",
		BINLOG_IGNORABLE_EVENT:      "IGNORABLE_EVENT",
		BINLOG_ROWS_QUERY_EVENT:      "ROWS_QUERY_EVENT",
		BINLOG_CREATE_USER_EVENT:    "CREATE_USER_EVENT",
		BINLOG_COMMIT_USER_EVENT:    "COMMIT_USER_EVENT",
		BINLOG_ANNOTATE_ROWS_EVENT:   "ANNOTATE_ROWS_EVENT",
		BINLOG_BINLOG_CHECKPOINT_EVENT: "BINLOG_CHECKPOINT_EVENT",
		BINLOG_GTID_EVENT:              "GTID_EVENT",
		BINLOG_GTID_LIST_EVENT:        "GTID_LIST_EVENT",
		BINLOG_START_ENCRYPTION_EVENT:  "START_ENCRYPTION_EVENT",
	}
	if name, exists := names[eventType]; exists {
		return name
	}
	return fmt.Sprintf("UNKNOWN_EVENT_%d", eventType)
}
