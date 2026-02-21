package xml

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// StorageMode XML storage mode
type StorageMode string

const (
	// StorageModeFilePerRow each row is a separate XML file
	StorageModeFilePerRow StorageMode = "file_per_row"
	// StorageModeSingleFile all rows in one XML file
	StorageModeSingleFile StorageMode = "single_file"
)

// TablePersistConfig holds per-table persistence configuration
type TablePersistConfig struct {
	BasePath    string      // e.g., "./database/mydb"
	TableName   string      // e.g., "users"
	RootTag     string      // XML root element tag, default "Row"
	StorageMode StorageMode // file_per_row or single_file
}

// TableDir returns the full path to the table directory
func (c *TablePersistConfig) TableDir() string {
	return filepath.Join(c.BasePath, c.TableName)
}

// IndexMeta holds serializable index metadata
type IndexMeta struct {
	Name    string   `xml:"name,attr"`
	Table   string   `xml:"table,attr"`
	Type    string   `xml:"type,attr"`
	Unique  bool     `xml:"unique,attr"`
	Columns []string `xml:"Column"`
}

// --- Schema XML types for encoding/xml ---

type xmlSchemaFile struct {
	XMLName     xml.Name       `xml:"TableSchema"`
	Name        string         `xml:"name,attr"`
	Engine      string         `xml:"engine,attr"`
	RootTag     string         `xml:"rootTag,attr"`
	StorageMode string         `xml:"storageMode,attr"`
	Columns     []xmlSchemaCol `xml:"Column"`
}

type xmlSchemaCol struct {
	XMLName       xml.Name `xml:"Column"`
	Name          string   `xml:"name,attr"`
	Type          string   `xml:"type,attr"`
	Nullable      bool     `xml:"nullable,attr"`
	Primary       bool     `xml:"primary,attr,omitempty"`
	AutoIncrement bool     `xml:"autoIncrement,attr,omitempty"`
	Unique        bool     `xml:"unique,attr,omitempty"`
	Default       string   `xml:"default,attr,omitempty"`
}

type xmlIndexFile struct {
	XMLName xml.Name    `xml:"IndexMeta"`
	Indexes []IndexMeta `xml:"Index"`
}

// ParseStorageMode extracts xml_mode from a COMMENT string
// Returns StorageModeFilePerRow as default
func ParseStorageMode(comment string) StorageMode {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return StorageModeFilePerRow
	}
	// Parse key=value pairs from comment
	for _, part := range strings.Split(comment, ";") {
		part = strings.TrimSpace(part)
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			if key == "xml_mode" {
				switch val {
				case "single_file":
					return StorageModeSingleFile
				case "file_per_row":
					return StorageModeFilePerRow
				}
			}
		}
	}
	return StorageModeFilePerRow
}

// PersistTableSchema saves table schema to __schema__.xml
func PersistTableSchema(cfg *TablePersistConfig, tableInfo *domain.TableInfo) error {
	tableDir := cfg.TableDir()
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory %s: %w", tableDir, err)
	}

	schema := xmlSchemaFile{
		Name:        cfg.TableName,
		Engine:      "xml",
		RootTag:     cfg.RootTag,
		StorageMode: string(cfg.StorageMode),
	}

	for _, col := range tableInfo.Columns {
		schema.Columns = append(schema.Columns, xmlSchemaCol{
			Name:          col.Name,
			Type:          col.Type,
			Nullable:      col.Nullable,
			Primary:       col.Primary,
			AutoIncrement: col.AutoIncrement,
			Unique:        col.Unique,
			Default:       col.Default,
		})
	}

	data, err := xml.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	content := []byte(xml.Header)
	content = append(content, data...)
	content = append(content, '\n')

	schemaPath := filepath.Join(tableDir, "__schema__.xml")
	return os.WriteFile(schemaPath, content, 0644)
}

// PersistTableData saves all rows to XML files
func PersistTableData(cfg *TablePersistConfig, tableInfo *domain.TableInfo, rows []domain.Row) error {
	tableDir := cfg.TableDir()
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory %s: %w", tableDir, err)
	}

	if len(rows) == 0 {
		// Still need to clean old data files
		deleteDataFiles(tableDir)
		return nil
	}

	// Find primary key column for file naming
	pkCol := findPrimaryKey(tableInfo)
	rootTag := cfg.RootTag
	if rootTag == "" {
		rootTag = "Row"
	}

	if cfg.StorageMode == StorageModeSingleFile {
		// For single file, clean old data files first (removes any stale per-row files)
		deleteDataFiles(tableDir)
		return persistSingleFile(tableDir, rootTag, tableInfo, rows, pkCol)
	}
	return persistFilePerRow(tableDir, rootTag, tableInfo, rows, pkCol)
}

// persistFilePerRow writes each row as a separate XML file using concurrent I/O.
// Orphaned files from previous writes are cleaned up after writing.
func persistFilePerRow(tableDir, rootTag string, tableInfo *domain.TableInfo, rows []domain.Row, pkCol string) error {
	numWorkers := runtime.NumCPU()
	if numWorkers > len(rows) {
		numWorkers = len(rows)
	}

	// Pre-build all row data sequentially (CPU-bound, avoids contention)
	type rowFile struct {
		path string
		data []byte
		name string // filename for orphan tracking
	}
	files := make([]rowFile, len(rows))
	var buf bytes.Buffer
	for i, row := range rows {
		filename := getRowFilename(row, pkCol, i)
		xmlName := filename + ".xml"
		buf.Reset()
		buildRowXMLInto(&buf, rootTag, tableInfo, row)
		// Copy buf content since we reuse it
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		files[i] = rowFile{
			path: filepath.Join(tableDir, xmlName),
			data: data,
			name: xmlName,
		}
	}

	// Write files concurrently
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, numWorkers)

	for i := range files {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[idx] = os.WriteFile(files[idx].path, files[idx].data, 0644)
		}(i)
	}
	wg.Wait()

	// Check for write errors
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("failed to write row file: %w", err)
		}
	}

	// Clean up orphaned files (files from previous writes that are no longer needed)
	writtenFiles := make(map[string]bool, len(files))
	for _, f := range files {
		writtenFiles[f.name] = true
	}
	entries, _ := os.ReadDir(tableDir)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "__schema__.xml" || name == "__meta__.xml" {
			continue
		}
		if strings.HasSuffix(name, ".xml") && !writtenFiles[name] {
			os.Remove(filepath.Join(tableDir, name))
		}
	}

	return nil
}

// persistSingleFile writes all rows to a single data.xml file
func persistSingleFile(tableDir, rootTag string, tableInfo *domain.TableInfo, rows []domain.Row, pkCol string) error {
	containerTag := rootTag + "s"
	if strings.HasSuffix(rootTag, "s") {
		containerTag = rootTag + "List"
	}

	// Estimate buffer size: header(~38) + open/close tags(~20) + ~80 bytes per row
	var buf bytes.Buffer
	buf.Grow(len(xml.Header) + 40 + len(rows)*80)

	buf.WriteString(xml.Header)
	buf.WriteByte('<')
	buf.WriteString(containerTag)
	buf.WriteString(">\n")
	for _, row := range rows {
		buf.WriteString("  ")
		buildRowXMLInto(&buf, rootTag, tableInfo, row)
		buf.WriteByte('\n')
	}
	buf.WriteString("</")
	buf.WriteString(containerTag)
	buf.WriteString(">\n")

	filePath := filepath.Join(tableDir, "data.xml")
	return os.WriteFile(filePath, buf.Bytes(), 0644)
}

// buildRowXMLInto appends a single row as an XML element with attributes into buf.
// The caller is responsible for calling buf.Reset() if reusing the buffer between rows.
func buildRowXMLInto(buf *bytes.Buffer, rootTag string, tableInfo *domain.TableInfo, row domain.Row) {
	buf.WriteByte('<')
	buf.WriteString(rootTag)
	for _, col := range tableInfo.Columns {
		val, ok := row[col.Name]
		if !ok || val == nil {
			continue
		}
		buf.WriteByte(' ')
		buf.WriteString(col.Name)
		buf.WriteString(`="`)
		escapeXMLAttrInto(buf, formatValue(val))
		buf.WriteByte('"')
	}
	buf.WriteString(" />")
}

// escapeXMLAttrInto writes s into buf with XML attribute escaping in a single pass.
func escapeXMLAttrInto(buf *bytes.Buffer, s string) {
	// Fast path: no special chars
	if !strings.ContainsAny(s, `&<>"`) {
		buf.WriteString(s)
		return
	}
	last := 0
	for i := 0; i < len(s); i++ {
		var esc string
		switch s[i] {
		case '&':
			esc = "&amp;"
		case '<':
			esc = "&lt;"
		case '>':
			esc = "&gt;"
		case '"':
			esc = "&quot;"
		default:
			continue
		}
		buf.WriteString(s[last:i])
		buf.WriteString(esc)
		last = i + 1
	}
	buf.WriteString(s[last:])
}

// PersistIndexMeta saves index metadata to __meta__.xml
func PersistIndexMeta(cfg *TablePersistConfig, indexes []*IndexMeta) error {
	tableDir := cfg.TableDir()
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory %s: %w", tableDir, err)
	}

	meta := xmlIndexFile{
		Indexes: make([]IndexMeta, len(indexes)),
	}
	for i, idx := range indexes {
		meta.Indexes[i] = *idx
	}

	data, err := xml.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index metadata: %w", err)
	}

	content := []byte(xml.Header)
	content = append(content, data...)
	content = append(content, '\n')

	metaPath := filepath.Join(tableDir, "__meta__.xml")
	return os.WriteFile(metaPath, content, 0644)
}

// LoadPersistedTables scans basePath for table directories with __schema__.xml
func LoadPersistedTables(basePath string) ([]*TablePersistConfig, error) {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read database directory %s: %w", basePath, err)
	}

	var configs []*TablePersistConfig
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		schemaPath := filepath.Join(basePath, entry.Name(), "__schema__.xml")
		if _, err := os.Stat(schemaPath); err != nil {
			continue // Not a persisted table directory
		}

		cfg, err := loadSchemaConfig(basePath, entry.Name(), schemaPath)
		if err != nil {
			continue // Skip invalid schemas
		}
		configs = append(configs, cfg)
	}

	return configs, nil
}

// LoadTableFromDisk loads schema, rows, and index metadata from disk
func LoadTableFromDisk(cfg *TablePersistConfig) (*domain.TableInfo, []domain.Row, []*IndexMeta, error) {
	tableDir := cfg.TableDir()

	// Load schema
	schemaPath := filepath.Join(tableDir, "__schema__.xml")
	tableInfo, err := loadSchema(schemaPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to load schema: %w", err)
	}

	// Load data
	var rows []domain.Row
	if cfg.StorageMode == StorageModeSingleFile {
		rows, err = loadSingleFileData(tableDir, tableInfo)
	} else {
		rows, err = loadFilePerRowData(tableDir, tableInfo)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to load data: %w", err)
	}

	// Load index metadata
	indexes, _ := loadIndexMeta(filepath.Join(tableDir, "__meta__.xml"))

	return tableInfo, rows, indexes, nil
}

// LoadTableFromDiskBatched loads schema and index metadata, then streams row data
// in batches of pageSize rows via batchFn callback. Each batch is a fresh []domain.Row
// slice whose ownership is transferred to the caller. This avoids loading all rows
// into memory at once, enabling buffer pool integration for datasets larger than RAM.
func LoadTableFromDiskBatched(cfg *TablePersistConfig, pageSize int, batchFn func(batch []domain.Row)) (*domain.TableInfo, []*IndexMeta, error) {
	tableDir := cfg.TableDir()

	// Load schema
	schemaPath := filepath.Join(tableDir, "__schema__.xml")
	tableInfo, err := loadSchema(schemaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load schema: %w", err)
	}

	// Load data in batches
	if cfg.StorageMode == StorageModeSingleFile {
		err = loadSingleFileBatched(tableDir, tableInfo, pageSize, batchFn)
	} else {
		err = loadFilePerRowBatched(tableDir, tableInfo, pageSize, batchFn)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load data: %w", err)
	}

	// Load index metadata
	indexes, _ := loadIndexMeta(filepath.Join(tableDir, "__meta__.xml"))

	return tableInfo, indexes, nil
}

// loadFilePerRowBatched reads row XML files in batches of pageSize, parsing each batch
// concurrently and calling batchFn with the results.
func loadFilePerRowBatched(tableDir string, tableInfo *domain.TableInfo, pageSize int, batchFn func([]domain.Row)) error {
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		return err
	}

	// Collect XML data file names
	var xmlFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "__schema__.xml" || name == "__meta__.xml" || name == "data.xml" {
			continue
		}
		if strings.HasSuffix(name, ".xml") {
			xmlFiles = append(xmlFiles, name)
		}
	}

	if len(xmlFiles) == 0 {
		return nil
	}

	colTypes := buildColTypes(tableInfo)
	numWorkers := runtime.NumCPU()

	// Process files in chunks of pageSize
	for start := 0; start < len(xmlFiles); start += pageSize {
		end := start + pageSize
		if end > len(xmlFiles) {
			end = len(xmlFiles)
		}
		chunk := xmlFiles[start:end]

		// Parse this chunk concurrently
		workers := numWorkers
		if workers > len(chunk) {
			workers = len(chunk)
		}
		rows := make([]domain.Row, len(chunk))
		errs := make([]error, len(chunk))
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)

		for i, name := range chunk {
			wg.Add(1)
			sem <- struct{}{}
			go func(idx int, fileName string) {
				defer wg.Done()
				defer func() { <-sem }()

				data, err := os.ReadFile(filepath.Join(tableDir, fileName))
				if err != nil {
					errs[idx] = err
					return
				}
				row, err := parseRowXMLFast(data, colTypes)
				if err != nil {
					errs[idx] = err
					return
				}
				rows[idx] = row
			}(i, name)
		}
		wg.Wait()

		// Collect successful results into a batch
		batch := make([]domain.Row, 0, len(chunk))
		for i, row := range rows {
			if errs[i] == nil && row != nil {
				batch = append(batch, row)
			}
		}

		if len(batch) > 0 {
			batchFn(batch)
		}
	}

	return nil
}

// loadSingleFileBatched reads data.xml, scans for row elements using the fast parser,
// and calls batchFn for every pageSize rows.
func loadSingleFileBatched(tableDir string, tableInfo *domain.TableInfo, pageSize int, batchFn func([]domain.Row)) error {
	dataPath := filepath.Join(tableDir, "data.xml")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	colTypes := buildColTypes(tableInfo)
	s := string(data)
	batch := make([]domain.Row, 0, pageSize)

	// Scan for self-closing elements: <Tag ... />
	// We look for '<' followed by a tag name, then attributes, ending with '/>'
	pos := 0
	for pos < len(s) {
		// Find next '<' that is not the container tag (skip container open/close tags)
		idx := strings.IndexByte(s[pos:], '<')
		if idx < 0 {
			break
		}
		idx += pos

		// Skip XML declaration and container tags (those ending with '>' not '/>')
		// Find the end of this element
		closeIdx := strings.Index(s[idx:], "/>")
		if closeIdx < 0 {
			// Not a self-closing element, skip past '>'
			gt := strings.IndexByte(s[idx:], '>')
			if gt < 0 {
				break
			}
			pos = idx + gt + 1
			continue
		}
		closeIdx += idx

		// Check if there's a '>' before '/>' (meaning this is not self-closing)
		gt := strings.IndexByte(s[idx:closeIdx], '>')
		if gt >= 0 {
			// Regular opening/closing tag, skip
			pos = idx + gt + 1
			continue
		}

		// Extract the element: from '<' to '/>'
		element := s[idx : closeIdx+2]
		row, err := parseRowXMLFast([]byte(element), colTypes)
		if err == nil && row != nil {
			batch = append(batch, row)
			if len(batch) >= pageSize {
				batchFn(batch)
				batch = make([]domain.Row, 0, pageSize)
			}
		}

		pos = closeIdx + 2
	}

	// Flush remaining rows
	if len(batch) > 0 {
		batchFn(batch)
	}

	return nil
}

// DeleteTableDir removes the entire table directory
func DeleteTableDir(cfg *TablePersistConfig) error {
	return os.RemoveAll(cfg.TableDir())
}

// --- Internal helpers ---

func loadSchemaConfig(basePath, tableName, schemaPath string) (*TablePersistConfig, error) {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	var schema xmlSchemaFile
	if err := xml.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	mode := StorageModeFilePerRow
	if schema.StorageMode == string(StorageModeSingleFile) {
		mode = StorageModeSingleFile
	}

	rootTag := schema.RootTag
	if rootTag == "" {
		rootTag = "Row"
	}

	return &TablePersistConfig{
		BasePath:    basePath,
		TableName:   tableName,
		RootTag:     rootTag,
		StorageMode: mode,
	}, nil
}

func loadSchema(schemaPath string) (*domain.TableInfo, error) {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	var schema xmlSchemaFile
	if err := xml.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	tableInfo := &domain.TableInfo{
		Name:    schema.Name,
		Columns: make([]domain.ColumnInfo, 0, len(schema.Columns)),
	}

	for _, col := range schema.Columns {
		tableInfo.Columns = append(tableInfo.Columns, domain.ColumnInfo{
			Name:          col.Name,
			Type:          col.Type,
			Nullable:      col.Nullable,
			Primary:       col.Primary,
			AutoIncrement: col.AutoIncrement,
			Unique:        col.Unique,
			Default:       col.Default,
		})
	}

	return tableInfo, nil
}

// buildColTypes builds a column-name-to-uppercase-type map for type conversion.
// Built once and shared across all row parses.
func buildColTypes(tableInfo *domain.TableInfo) map[string]string {
	colTypes := make(map[string]string, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		colTypes[col.Name] = strings.ToUpper(col.Type)
	}
	return colTypes
}

func loadFilePerRowData(tableDir string, tableInfo *domain.TableInfo) ([]domain.Row, error) {
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		return nil, err
	}

	// Collect XML data file entries
	var xmlFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "__schema__.xml" || name == "__meta__.xml" || name == "data.xml" {
			continue
		}
		if strings.HasSuffix(name, ".xml") {
			xmlFiles = append(xmlFiles, name)
		}
	}

	if len(xmlFiles) == 0 {
		return nil, nil
	}

	// Build colTypes map once for all rows
	colTypes := buildColTypes(tableInfo)

	// Concurrent file reads
	numWorkers := runtime.NumCPU()
	if numWorkers > len(xmlFiles) {
		numWorkers = len(xmlFiles)
	}

	rows := make([]domain.Row, len(xmlFiles))
	errs := make([]error, len(xmlFiles))
	var wg sync.WaitGroup
	sem := make(chan struct{}, numWorkers)

	for i, name := range xmlFiles {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, fileName string) {
			defer wg.Done()
			defer func() { <-sem }()

			data, err := os.ReadFile(filepath.Join(tableDir, fileName))
			if err != nil {
				errs[idx] = err
				return
			}

			row, err := parseRowXMLFast(data, colTypes)
			if err != nil {
				errs[idx] = err
				return
			}
			rows[idx] = row
		}(i, name)
	}
	wg.Wait()

	// Collect successful results (skip errors like before)
	var result []domain.Row
	for i, row := range rows {
		if errs[i] == nil && row != nil {
			result = append(result, row)
		}
	}

	return result, nil
}

func loadSingleFileData(tableDir string, tableInfo *domain.TableInfo) ([]domain.Row, error) {
	dataPath := filepath.Join(tableDir, "data.xml")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No data yet
		}
		return nil, err
	}

	return parseMultiRowXML(data, tableInfo)
}

func loadIndexMeta(metaPath string) ([]*IndexMeta, error) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, nil // No index metadata
	}

	var meta xmlIndexFile
	if err := xml.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	result := make([]*IndexMeta, len(meta.Indexes))
	for i := range meta.Indexes {
		result[i] = &meta.Indexes[i]
	}
	return result, nil
}

// parseRowXML parses a single-row XML element into a domain.Row (legacy, used by tests)
func parseRowXML(data []byte, tableInfo *domain.TableInfo) (domain.Row, error) {
	colTypes := buildColTypes(tableInfo)
	return parseRowXMLFast(data, colTypes)
}

// parseRowXMLFast parses a self-closing XML element by scanning for name="value" attribute pairs.
// This avoids the overhead of encoding/xml.Unmarshal and reflection.
// Expected format: <Tag attr1="val1" attr2="val2" ... />
func parseRowXMLFast(data []byte, colTypes map[string]string) (domain.Row, error) {
	s := string(data)
	row := make(domain.Row, len(colTypes))

	// Find the opening '<' and skip past the tag name
	start := strings.IndexByte(s, '<')
	if start < 0 {
		return nil, fmt.Errorf("no opening tag found")
	}
	// Skip '<' and tag name (until first space, '/' or '>')
	i := start + 1
	for i < len(s) && s[i] != ' ' && s[i] != '/' && s[i] != '>' {
		i++
	}

	// Parse attributes
	for i < len(s) {
		// Skip whitespace
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
			i++
		}
		if i >= len(s) || s[i] == '/' || s[i] == '>' {
			break
		}

		// Parse attribute name
		nameStart := i
		for i < len(s) && s[i] != '=' && s[i] != ' ' {
			i++
		}
		if i >= len(s) || s[i] != '=' {
			break
		}
		attrName := s[nameStart:i]
		i++ // skip '='

		// Parse attribute value (quoted)
		if i >= len(s) || s[i] != '"' {
			break
		}
		i++ // skip opening '"'
		valueStart := i
		for i < len(s) && s[i] != '"' {
			i++
		}
		if i >= len(s) {
			break
		}
		attrValue := s[valueStart:i]
		i++ // skip closing '"'

		// Unescape XML entities if needed
		if strings.ContainsAny(attrValue, "&") {
			attrValue = unescapeXMLAttr(attrValue)
		}

		// Convert to typed value
		row[attrName] = convertXMLValue(attrValue, colTypes[attrName])
	}

	return row, nil
}

// unescapeXMLAttr reverses XML attribute escaping
func unescapeXMLAttr(s string) string {
	s = strings.ReplaceAll(s, "&quot;", `"`)
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&apos;", "'")
	s = strings.ReplaceAll(s, "&amp;", "&") // must be last
	return s
}

// parseMultiRowXML parses a container XML with multiple row elements
func parseMultiRowXML(data []byte, tableInfo *domain.TableInfo) ([]domain.Row, error) {
	var container xmlNode
	if err := xml.Unmarshal(data, &container); err != nil {
		return nil, err
	}

	colTypes := buildColTypes(tableInfo)

	rows := make([]domain.Row, 0, len(container.Children))
	for _, child := range container.Children {
		row := make(domain.Row, len(colTypes))
		for _, attr := range child.Attrs {
			colName := attr.Name.Local
			row[colName] = convertXMLValue(attr.Value, colTypes[colName])
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// convertXMLValue converts a string value to the appropriate Go type based on column type.
// Expects colType to already be uppercased.
func convertXMLValue(value string, colType string) interface{} {
	switch {
	case strings.Contains(colType, "INT"):
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			return v
		}
	case strings.Contains(colType, "FLOAT") || strings.Contains(colType, "DOUBLE") || strings.Contains(colType, "DECIMAL") || strings.Contains(colType, "NUMERIC"):
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			return v
		}
	case strings.Contains(colType, "BOOL"):
		switch strings.ToLower(value) {
		case "true", "1":
			return true
		case "false", "0":
			return false
		}
	}
	return value
}

// formatValue converts a Go value to string for XML attribute
func formatValue(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// findPrimaryKey returns the name of the primary key column, or "" if none
func findPrimaryKey(tableInfo *domain.TableInfo) string {
	for _, col := range tableInfo.Columns {
		if col.Primary {
			return col.Name
		}
	}
	return ""
}

// getRowFilename returns the filename for a row (without extension)
func getRowFilename(row domain.Row, pkCol string, index int) string {
	if pkCol != "" {
		if val, ok := row[pkCol]; ok && val != nil {
			return formatValue(val)
		}
	}
	return strconv.Itoa(index + 1)
}

// deleteDataFiles removes all data XML files from a table directory,
// preserving __schema__.xml and __meta__.xml
func deleteDataFiles(tableDir string) error {
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "__schema__.xml" || name == "__meta__.xml" {
			continue
		}
		if strings.HasSuffix(name, ".xml") {
			os.Remove(filepath.Join(tableDir, name))
		}
	}
	return nil
}
