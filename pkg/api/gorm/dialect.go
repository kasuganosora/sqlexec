package gorm

import (
	"database/sql"

	"github.com/kasuganosora/sqlexec/pkg/api"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Dialector 封装了 sqlexec 的 Session 作为 GORM 的数据库驱动
type Dialector struct {
	Session      *api.Session
	SQLParser    func(sql string) (string, []interface{}, error)
	CachedDB     *sql.DB // 用于 GORM 的事务和连接池管理
	Initialized  bool
}

// NewDialector 创建一个新的 GORM 驱动，使用 sqlexec 的 Session
func NewDialector(session *api.Session) gorm.Dialector {
	return &Dialector{
		Session:     session,
		Initialized: false,
	}
}

// Name 返回数据库方言名称
func (d *Dialector) Name() string {
	return "sqlexec"
}

// Initialize 初始化数据库连接
func (d *Dialector) Initialize(db *gorm.DB) error {
	d.Initialized = true
	return nil
}

// Migrator 提供数据库迁移工具
func (d *Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return &Migrator{
		Dialector: d,
		DB:       db,
	}
}

// DataTypeOf 确定架构字段的数据类型
func (d *Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		if field.Size <= 8 {
			return "TINYINT"
		} else if field.Size <= 16 {
			return "SMALLINT"
		} else if field.Size <= 32 {
			return "INT"
		} else if field.Size <= 64 {
			return "BIGINT"
		}
		return "BIGINT"
	case schema.Float:
		if field.Precision > 0 {
			return "FLOAT"
		}
		return "FLOAT"
	// Double 类型已在新版本 GORM 中移除，使用 Float 代替
	case schema.String:
		if field.Size > 0 {
			return "VARCHAR"
		}
		return "TEXT"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		return "BLOB"
	default:
		return "VARCHAR"
	}
}

// DefaultValueOf 提供架构字段的默认值
func (d *Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.DefaultValue != "" {
		return clause.Expr{SQL: "DEFAULT"}
	}
	return nil
}

// BindVarTo 处理 SQL 语句中的变量绑定
func (d *Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// QuoteTo 管理标识符的引号
func (d *Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}

// Explain 格式化带有变量的 SQL 语句
func (d *Dialector) Explain(sql string, vars ...interface{}) string {
	// 简化实现，直接返回 SQL
	return sql
}

// SetSQLParser 设置 SQL 解析器，用于将 GORM 的 SQL 转换为 sqlexec 可识别的格式
func (d *Dialector) SetSQLParser(parser func(sql string) (string, []interface{}, error)) {
	d.SQLParser = parser
}
