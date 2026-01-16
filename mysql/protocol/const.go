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
