# MySQL 协议服务器

基于 Golang 实现的 MySQL 协议服务器，集成 TiDB SQL 解析器，提供完整的 MySQL 协议处理能力。

## 功能特性

- ✅ **完整 MySQL 协议支持** - 实现标准的 MySQL 协议交互
- ✅ **TiDB SQL 解析器集成** - 使用 TiDB 的 SQL 解析器进行语句解析
- ✅ **会话管理** - 支持多客户端连接和会话状态管理
- ✅ **权限控制** - 用户认证和权限验证
- ✅ **多种 SQL 语句支持**：
  - SELECT 查询
  - SHOW 命令（DATABASES, TABLES）
  - DESCRIBE/DESC 命令
  - SET 语句（会话变量设置）
  - USE 语句（切换数据库）

## 快速开始

### 1. 安装依赖

```bash
go mod download
```

### 2. 编译项目

```bash
go build -o mysql-server.exe .
```

### 3. 运行服务器

```bash
go run main.go
```

或者直接运行编译后的可执行文件：

```bash
./mysql-server.exe
```

服务器默认监听 `0.0.0.0:3306`。

### 4. 连接服务器

使用 MySQL 客户端连接（需要禁用 SSL）：

```bash
mysql -h 127.0.0.1 -P 3306 -u root -p --ssl-mode=DISABLED
```

### 5. 测试查询

```sql
-- 测试基本查询
SELECT * FROM test;

-- 查看版本
SELECT @@version_comment LIMIT 1;

-- 显示数据库
SHOW DATABASES;

-- 切换数据库
USE testdb;
```

## 项目结构

```
db/
├── main.go                    # 主程序入口
├── go.mod                     # Go 模块依赖
├── go.sum                     # 依赖校验文件
├── server/                    # 服务端模块
│   └── server.go             # 服务器实现
└── mysql/                     # MySQL 协议实现
    ├── service.go            # 服务层
    ├── parser/               # TiDB SQL 解析器
    │   ├── parser.go        # 解析器封装
    │   ├── visitor.go       # AST 访问者
    │   └── handler.go       # 语句处理器
    ├── protocol/             # 协议实现
    │   ├── packet.go        # 数据包定义
    │   ├── const.go         # 协议常量
    │   ├── type.go          # 数据类型
    │   ├── charset.go       # 字符集处理
    │   ├── replication.go    # 复制协议
    │   └── helper.go        # 辅助函数
    └── session/             # 会话管理
        ├── session.go       # 会话定义
        └── memory.go       # 内存存储驱动
```

## 技术栈

- **语言**: Go 1.24.2
- **SQL 解析器**: PingCAP TiDB Parser
- **协议**: MySQL Protocol
- **测试框架**: testify

## 核心组件

### SQL 解析器

集成 TiDB SQL 解析器，提供强大的 SQL 解析能力：

```go
import "mysql-proxy/mysql/parser"

p := parser.NewParser()
stmt, err := p.ParseOneStmtText("SELECT * FROM users WHERE id > 10")
info := parser.ExtractSQLInfo(stmt)
```

### 协议处理

完整的 MySQL 协议包处理：

- 握手协议
- 命令协议（COM_QUERY, COM_STMT_PREPARE 等）
- 结果集协议
- 错误处理

### 会话管理

支持多客户端连接和会话状态管理：

- 会话创建和销毁
- 会话变量存储
- 内存存储驱动

## 运行测试

```bash
# 运行所有测试
go test ./...

# 运行特定包的测试
go test ./mysql/protocol/...
go test ./server/...

# 运行测试并查看覆盖率
go test -cover ./...
```

## 编译检查

```bash
# 代码静态检查
go vet ./...

# 格式化代码
go fmt ./...

# 代码整理
go mod tidy
```

## 当前限制

- 仅支持基本的查询操作
- 不支持事务
- 不支持预编译语句
- 不支持 SSL/TLS 加密连接

## 开发计划

- [ ] 支持更多 SQL 语句类型
- [ ] 实现数据源管理
- [ ] 添加性能监控
- [ ] 支持 SSL/TLS
- [ ] 完善错误处理

## 许可证

MIT License
