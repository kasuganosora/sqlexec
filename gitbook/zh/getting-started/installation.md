# 安装与编译

## 系统要求

- Go 1.24 或更高版本
- CGO 支持（用于中文分词 Jieba）
- 支持的操作系统：Linux、macOS、Windows

## 作为独立服务器编译

从源码编译 SQLExec 服务器：

```bash
# 克隆项目
git clone https://github.com/kasuganosora/sqlexec.git
cd sqlexec

# 编译
go build -o sqlexec ./cmd/service

# 验证
./sqlexec --version
```

### 交叉编译

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o sqlexec-linux ./cmd/service

# macOS
GOOS=darwin GOARCH=arm64 go build -o sqlexec-darwin ./cmd/service

# Windows
GOOS=windows GOARCH=amd64 go build -o sqlexec.exe ./cmd/service
```

## 作为嵌入式库引入

在你的 Go 项目中引入 SQLExec：

```bash
go get github.com/kasuganosora/sqlexec
```

在代码中导入：

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)
```

## 目录结构

```
sqlexec/
├── cmd/service/          # 独立服务器入口
├── pkg/
│   ├── api/              # 公共 API（DB、Session、Query）
│   │   └── gorm/         # GORM 驱动
│   ├── resource/         # 数据源抽象层
│   │   ├── domain/       # 领域接口与模型
│   │   ├── memory/       # 内存数据源（MVCC）
│   │   ├── csv/          # CSV 数据源
│   │   ├── json/         # JSON 数据源
│   │   ├── jsonl/        # JSONL 数据源
│   │   ├── excel/        # Excel 数据源
│   │   ├── parquet/      # Parquet 数据源
│   │   └── slice/        # Go 结构体适配器
│   ├── builtin/          # 内置 SQL 函数
│   ├── parser/           # SQL 解析器
│   ├── optimizer/        # 查询优化器
│   ├── fulltext/         # 全文搜索引擎
│   └── security/         # 安全模块
├── server/               # 服务器层
│   ├── httpapi/          # HTTP REST API
│   ├── mcp/              # MCP 协议服务器
│   └── datasource/       # MySQL/PostgreSQL/HTTP 数据源
├── docs/                 # 内部技术文档
└── gitbook/              # 用户文档（本文档）
```
