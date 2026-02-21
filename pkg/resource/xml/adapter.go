package xml

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// XMLAdapter XML 目录数据源适配器
// 继承 MVCCDataSource，将目录树映射为多张表
// 每个子目录 = 一张表，每个 XML 文件 = 一行数据
type XMLAdapter struct {
	*memory.MVCCDataSource
	rootPath    string
	writable    bool
	encoding    string          // "utf-16" (默认) 或 "utf-8"
	skipDirs    map[string]bool // 要跳过的目录
	skipSuffix  []string        // 要跳过的文件后缀
	expandLists bool            // 是否展开列表容器
	tableNames  []string        // 已加载的表名列表
}

// NewXMLAdapter 创建 XML 数据源适配器
func NewXMLAdapter(config *domain.DataSourceConfig, rootPath string) *XMLAdapter {
	writable := config.Writable
	encoding := "utf-16"
	expandLists := true
	skipDirs := map[string]bool{"_mail": true}
	skipSuffix := []string{"_cache.xml"}

	if config.Options != nil {
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
		if e, ok := config.Options["encoding"]; ok {
			if s, ok := e.(string); ok && s != "" {
				encoding = s
			}
		}
		if el, ok := config.Options["expand_lists"]; ok {
			if b, ok := el.(bool); ok {
				expandLists = b
			}
		}
		if sd, ok := config.Options["skip_dirs"]; ok {
			if s, ok := sd.(string); ok && s != "" {
				skipDirs = make(map[string]bool)
				for _, dir := range strings.Split(s, ",") {
					skipDirs[strings.TrimSpace(dir)] = true
				}
			}
		}
		if ss, ok := config.Options["skip_suffix"]; ok {
			if s, ok := ss.(string); ok && s != "" {
				skipSuffix = nil
				for _, suffix := range strings.Split(s, ",") {
					skipSuffix = append(skipSuffix, strings.TrimSpace(suffix))
				}
			}
		}
	}

	internalConfig := *config
	internalConfig.Writable = writable

	return &XMLAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		rootPath:       rootPath,
		writable:       writable,
		encoding:       encoding,
		skipDirs:       skipDirs,
		skipSuffix:     skipSuffix,
		expandLists:    expandLists,
	}
}

// Connect 连接数据源 - 扫描目录树并加载 XML 数据到内存
func (a *XMLAdapter) Connect(ctx context.Context) error {
	// 验证根目录存在且是目录
	info, err := os.Stat(a.rootPath)
	if err != nil {
		return fmt.Errorf("failed to access XML root directory %q: %w", a.rootPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("XML root path %q is not a directory", a.rootPath)
	}

	// 读取子目录
	entries, err := os.ReadDir(a.rootPath)
	if err != nil {
		return fmt.Errorf("failed to read XML root directory %q: %w", a.rootPath, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dirName := entry.Name()

		// 跳过隐藏目录和配置的跳过目录
		if strings.HasPrefix(dirName, ".") || a.skipDirs[dirName] {
			continue
		}

		// 加载该子目录为一张表
		if err := a.loadDirectory(dirName); err != nil {
			// 跳过有错误的目录，继续处理其他目录
			continue
		}
	}

	return a.MVCCDataSource.Connect(ctx)
}

// loadDirectory 加载单个子目录为一张表
func (a *XMLAdapter) loadDirectory(dirName string) error {
	dirPath := filepath.Join(a.rootPath, dirName)

	// 列出 XML 文件
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %q: %w", dirPath, err)
	}

	var allRows []domain.Row
	var rootTag string
	var attrCols []string

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()

		// 只处理 .xml 文件
		if !strings.HasSuffix(strings.ToLower(fileName), ".xml") {
			continue
		}

		// 跳过匹配 skipSuffix 的文件
		if a.shouldSkipFile(fileName) {
			continue
		}

		// 读取文件
		filePath := filepath.Join(dirPath, fileName)
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue // 跳过无法读取的文件
		}

		// 解析文件
		filenameWithoutExt := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		rows, tag, cols, err := parseXMLFile(data, filenameWithoutExt, a.expandLists)
		if err != nil {
			continue // 跳过无法解析的文件
		}

		if rootTag == "" {
			rootTag = tag
			attrCols = cols
		}

		allRows = append(allRows, rows...)
	}

	// 空目录（无有效 XML 文件）不创建表
	if len(allRows) == 0 {
		return nil
	}

	// 推断列类型
	columns := inferColumnTypes(allRows)

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name:    dirName,
		Schema:  "",
		Columns: columns,
		Atts: map[string]interface{}{
			"_xml_root_tag":  rootTag,
			"_xml_attr_cols": attrCols,
			"_xml_encoding":  a.encoding,
		},
	}

	// 加载到 MVCC
	if err := a.LoadTable(dirName, tableInfo, allRows); err != nil {
		return fmt.Errorf("failed to load table %q: %w", dirName, err)
	}

	a.tableNames = append(a.tableNames, dirName)
	return nil
}

// shouldSkipFile 检查文件是否应该被跳过
func (a *XMLAdapter) shouldSkipFile(fileName string) bool {
	lower := strings.ToLower(fileName)
	for _, suffix := range a.skipSuffix {
		if strings.HasSuffix(lower, strings.ToLower(suffix)) {
			return true
		}
	}
	return false
}

// Close 关闭连接 - 可选写回 XML 文件
func (a *XMLAdapter) Close(ctx context.Context) error {
	var writeBackErr error
	if a.writable {
		if err := a.writeBack(); err != nil {
			writeBackErr = fmt.Errorf("failed to write back XML files: %w", err)
		}
	}

	closeErr := a.MVCCDataSource.Close(ctx)
	if writeBackErr != nil {
		return writeBackErr
	}
	return closeErr
}

// writeBack 写回修改后的数据到 XML 文件
func (a *XMLAdapter) writeBack() error {
	for _, tableName := range a.tableNames {
		if err := a.writeBackTable(tableName); err != nil {
			return fmt.Errorf("failed to write back table %q: %w", tableName, err)
		}
	}
	return nil
}

// writeBackTable 写回单张表的数据
func (a *XMLAdapter) writeBackTable(tableName string) error {
	schema, rows, err := a.GetLatestTableData(tableName)
	if err != nil {
		return err
	}

	// 获取元数据
	rootTag := "Row"
	var attrCols []string
	if schema.Atts != nil {
		if tag, ok := schema.Atts["_xml_root_tag"].(string); ok && tag != "" {
			rootTag = tag
		}
		if cols, ok := schema.Atts["_xml_attr_cols"].([]string); ok {
			attrCols = cols
		}
	}

	// 构建属性列 set
	attrColSet := make(map[string]bool)
	for _, col := range attrCols {
		attrColSet[col] = true
	}

	dirPath := filepath.Join(a.rootPath, tableName)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dirPath, err)
	}

	// 跟踪写入的文件，以便清理已删除的行
	writtenFiles := make(map[string]bool)

	for _, row := range rows {
		fileName, ok := row["_file"].(string)
		if !ok || fileName == "" {
			continue
		}

		xmlData, err := a.rowToXML(row, rootTag, attrColSet)
		if err != nil {
			return fmt.Errorf("failed to serialize row %q: %w", fileName, err)
		}

		// 编码为 UTF-16（如果需要）
		var outputData []byte
		if strings.ToLower(a.encoding) == "utf-16" {
			xmlData = []byte(strings.Replace(string(xmlData), `encoding="utf-8"`, `encoding="utf-16"`, 1))
			outputData, err = encodeToUTF16(xmlData)
			if err != nil {
				return fmt.Errorf("failed to encode to UTF-16: %w", err)
			}
		} else {
			outputData = xmlData
		}

		// 原子写入
		filePath := filepath.Join(dirPath, fileName+".xml")
		tmpFile, err := os.CreateTemp(dirPath, ".xml_writeback_*.tmp")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		tmpPath := tmpFile.Name()

		if _, err := tmpFile.Write(outputData); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write temp file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("failed to close temp file: %w", err)
		}
		if err := os.Rename(tmpPath, filePath); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("failed to rename temp file: %w", err)
		}

		writtenFiles[fileName+".xml"] = true
	}

	// 删除不再存在的文件（已删除的行）
	existingFiles, _ := os.ReadDir(dirPath)
	for _, f := range existingFiles {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".xml") {
			continue
		}
		if a.shouldSkipFile(f.Name()) {
			continue
		}
		if !writtenFiles[f.Name()] {
			os.Remove(filepath.Join(dirPath, f.Name()))
		}
	}

	return nil
}

// rowToXML 将行数据转换为 XML 字节
func (a *XMLAdapter) rowToXML(row domain.Row, rootTag string, attrCols map[string]bool) ([]byte, error) {
	type xmlAttr struct {
		Name  string
		Value string
	}

	attrs := make([]xmlAttr, 0)

	// 添加属性列
	for col := range attrCols {
		if val, ok := row[col]; ok && val != nil {
			attrs = append(attrs, xmlAttr{Name: col, Value: fmt.Sprintf("%v", val)})
		}
	}

	// 构建 XML
	var buf strings.Builder
	buf.WriteString(`<?xml version="1.0" encoding="utf-8" ?>` + "\n")
	buf.WriteString("<" + rootTag)

	for _, attr := range attrs {
		buf.WriteString(fmt.Sprintf(` %s="%s"`, attr.Name, escapeXMLAttr(attr.Value)))
	}

	buf.WriteString(" />\n")

	return []byte(buf.String()), nil
}

// escapeXMLAttr 转义 XML 属性值
func escapeXMLAttr(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

// GetConfig 获取数据源配置
func (a *XMLAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表
func (a *XMLAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息
func (a *XMLAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据
func (a *XMLAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据
func (a *XMLAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("xml", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据
func (a *XMLAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("xml", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据
func (a *XMLAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("xml", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（XML 不支持）
func (a *XMLAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("xml", "create table")
}

// DropTable 删除表（XML 不支持）
func (a *XMLAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("xml", "drop table")
}

// TruncateTable 清空表（XML 不支持）
func (a *XMLAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("xml", "truncate table")
}

// Execute 执行 SQL（XML 不支持）
func (a *XMLAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("xml", "execute SQL")
}

// IsConnected 检查是否已连接
func (a *XMLAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *XMLAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现 IsWritableSource 接口
func (a *XMLAdapter) SupportsWrite() bool {
	return a.writable
}
