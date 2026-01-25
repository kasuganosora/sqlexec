package file

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== 文件解析器接口 ====================

// Parser 文件解析器接口
type Parser interface {
	// Parse 解析文件内容，返回行数据
	Parse(data []byte) ([]domain.Row, error)
	// GetColumns 获取列信息
	GetColumns() []domain.ColumnInfo
}
