# MySQL协议代理服务器

这是一个基于Golang实现的MySQL协议代理服务器，支持从多种数据源读取数据并提供MySQL协议访问。

## 功能特性

- 支持MySQL协议连接和认证
- 支持会话管理
- 支持基本查询操作：
  - SELECT 查询
  - SHOW 命令
  - DESCRIBE/DESC 命令
- 支持用户权限管理
- 配置化管理

## 快速开始

1. 安装依赖：
```bash
go mod download
```

2. 配置环境变量：
程序首次运行时会自动创建 `.env` 文件，包含默认配置。您可以根据需要修改这些配置项。

3. 配置用户权限：
编辑 `config/users.json` 文件

4. 运行服务器：
```bash
go run main.go
```

5. 连接服务器：
使用 MySQL 客户端连接时，需要禁用 SSL：
```bash
# 使用默认配置连接
mysql -h 127.0.0.1 -P 3306 -u admin -p --ssl-mode=DISABLED

# 或者使用更详细的连接参数
mysql --protocol=TCP -h 127.0.0.1 -P 3306 -u admin -p --ssl-mode=DISABLED
```

注意：必须使用 `--ssl-mode=DISABLED` 参数，因为服务器目前不支持 SSL 连接。

## 配置说明

### 环境变量配置 (.env)

程序首次运行时会自动创建 `.env` 文件，包含以下默认配置：

```env
# MySQL服务器配置
MYSQL_HOST=0.0.0.0
MYSQL_PORT=3306

# 日志配置
LOG_LEVEL=info
LOG_FILE=logs/mysql-proxy.log

# 数据源配置
DATA_SOURCE_DIR=./data_sources
```

### 用户权限配置 (config/users.json)

```json
{
    "users": [
        {
            "username": "admin",
            "password": "admin123",
            "permissions": ["SELECT", "SHOW", "DESCRIBE"],
            "allowed_databases": ["*"],
            "allowed_tables": ["*"]
        }
    ]
}
```

## 支持的查询类型

1. SELECT 查询
   - 支持基本的 SELECT 语句
   - 自动进行表权限检查
   - 返回标准的结果集格式

2. SHOW 命令
   - 支持 SHOW DATABASES
   - 支持 SHOW TABLES
   - 返回标准的结果集格式

3. DESCRIBE/DESC 命令
   - 支持表结构查询
   - 返回标准的结果集格式

## 项目结构

```
.
├── auth/           # 用户认证和权限管理
├── config/         # 配置文件
├── datasource/     # 数据源管理
├── mysql/          # MySQL协议实现
├── session/        # 会话管理
├── main.go         # 主程序入口
└── README.md       # 项目文档
```

## 注意事项

- 目前仅支持读取操作，不支持写入操作
- 建议定期清理会话数据
- 确保配置文件中的用户权限设置正确
- 服务器默认使用 UTF-8 字符集