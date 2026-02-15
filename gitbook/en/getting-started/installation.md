# Installation and Building

## System Requirements

- Go 1.24 or later
- CGO support (required for Chinese word segmentation via Jieba)
- Supported operating systems: Linux, macOS, Windows

## Building as a Standalone Server

Build the SQLExec server from source:

```bash
# Clone the project
git clone https://github.com/kasuganosora/sqlexec.git
cd sqlexec

# Build
go build -o sqlexec ./cmd/service

# Verify
./sqlexec --version
```

### Cross-Compilation

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o sqlexec-linux ./cmd/service

# macOS
GOOS=darwin GOARCH=arm64 go build -o sqlexec-darwin ./cmd/service

# Windows
GOOS=windows GOARCH=amd64 go build -o sqlexec.exe ./cmd/service
```

## Using as an Embedded Library

Add SQLExec to your Go project:

```bash
go get github.com/kasuganosora/sqlexec
```

Import in your code:

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)
```

## Directory Structure

```
sqlexec/
├── cmd/service/          # Standalone server entry point
├── pkg/
│   ├── api/              # Public API (DB, Session, Query)
│   │   └── gorm/         # GORM driver
│   ├── resource/         # Data source abstraction layer
│   │   ├── domain/       # Domain interfaces and models
│   │   ├── memory/       # In-memory data source (MVCC)
│   │   ├── csv/          # CSV data source
│   │   ├── json/         # JSON data source
│   │   ├── jsonl/        # JSONL data source
│   │   ├── excel/        # Excel data source
│   │   ├── parquet/      # Parquet data source
│   │   └── slice/        # Go struct adapter
│   ├── builtin/          # Built-in SQL functions
│   ├── parser/           # SQL parser
│   ├── optimizer/        # Query optimizer
│   ├── fulltext/         # Full-text search engine
│   └── security/         # Security module
├── server/               # Server layer
│   ├── httpapi/          # HTTP REST API
│   ├── mcp/              # MCP protocol server
│   └── datasource/       # MySQL/PostgreSQL/HTTP data sources
├── docs/                 # Internal technical documentation
└── gitbook/              # User documentation (this document)
```
