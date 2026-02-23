package virtual

import (
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VirtualDatabaseEntry 虚拟数据库注册条目
type VirtualDatabaseEntry struct {
	Name     string               // 数据库名，如 "config"
	Provider VirtualTableProvider // 提供表名和 schema 信息
	Writable bool                 // 是否支持写操作 (INSERT/UPDATE/DELETE)
}

// VirtualDatabaseRegistry 虚拟数据库注册表
// 提供统一的注册机制，让 information_schema 和查询路由自动感知所有虚拟数据库
type VirtualDatabaseRegistry struct {
	mu        sync.RWMutex
	databases map[string]*VirtualDatabaseEntry
}

// NewVirtualDatabaseRegistry 创建虚拟数据库注册表
func NewVirtualDatabaseRegistry() *VirtualDatabaseRegistry {
	return &VirtualDatabaseRegistry{
		databases: make(map[string]*VirtualDatabaseEntry),
	}
}

// Register 注册一个虚拟数据库
func (r *VirtualDatabaseRegistry) Register(entry *VirtualDatabaseEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.databases[strings.ToLower(entry.Name)] = entry
}

// Get 获取虚拟数据库条目
func (r *VirtualDatabaseRegistry) Get(name string) (*VirtualDatabaseEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.databases[strings.ToLower(name)]
	return entry, ok
}

// List 列出所有已注册的虚拟数据库
func (r *VirtualDatabaseRegistry) List() []*VirtualDatabaseEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]*VirtualDatabaseEntry, 0, len(r.databases))
	for _, entry := range r.databases {
		entries = append(entries, entry)
	}
	return entries
}

// IsVirtualDB 判断指定名称是否为已注册的虚拟数据库
func (r *VirtualDatabaseRegistry) IsVirtualDB(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.databases[strings.ToLower(name)]
	return ok
}

// GetDataSource 获取虚拟数据库对应的 DataSource
// 根据 Writable 字段自动选择 WritableVirtualDataSource 或 VirtualDataSource
func (r *VirtualDatabaseRegistry) GetDataSource(name string) domain.DataSource {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.databases[strings.ToLower(name)]
	if !ok {
		return nil
	}
	if entry.Writable {
		return NewWritableVirtualDataSource(entry.Provider, entry.Name)
	}
	return NewVirtualDataSource(entry.Provider)
}

// IsVirtualDBQuery 判断查询是否针对某个虚拟数据库
// 检查表名前缀（如 "config.datasource"）或当前数据库是否为虚拟库
// 返回匹配的虚拟库名（空字符串表示不匹配）
func (r *VirtualDatabaseRegistry) IsVirtualDBQuery(tableName string, currentDB string) string {
	if tableName == "" {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 检查表名前缀 (如 "config.datasource")
	lowerTable := strings.ToLower(tableName)
	for key, entry := range r.databases {
		if strings.HasPrefix(lowerTable, key+".") {
			return entry.Name
		}
	}

	// 检查当前数据库
	if currentDB != "" {
		if entry, ok := r.databases[strings.ToLower(currentDB)]; ok {
			return entry.Name
		}
	}

	return ""
}

// StripDBPrefix 从表名中去除虚拟库前缀
// 如 "config.datasource" → "datasource"
func (r *VirtualDatabaseRegistry) StripDBPrefix(tableName string, dbName string) string {
	prefix := strings.ToLower(dbName) + "."
	if strings.HasPrefix(strings.ToLower(tableName), prefix) {
		return tableName[len(prefix):]
	}
	return tableName
}
