package xml

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// helper: 创建临时 XML 目录结构
func createTestXMLDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// account 表：纯属性模式
	accountDir := filepath.Join(dir, "account")
	os.MkdirAll(accountDir, 0755)
	writeFile(t, filepath.Join(accountDir, "luna.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Account id="luna" password="BCFE94820B4EA7FC" name="Luna" flag="-1" authority="0" />`)
	writeFile(t, filepath.Join(accountDir, "admin.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Account id="admin" password="ABCD1234" name="Admin" flag="0" authority="1" />`)

	// idpool 表：简单属性
	idpoolDir := filepath.Join(dir, "idpool")
	os.MkdirAll(idpoolDir, 0755)
	writeFile(t, filepath.Join(idpoolDir, "charidpool.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<CharIDPool count="8000" />`)

	// mixed 表：属性 + 简单文本子元素
	mixedDir := filepath.Join(dir, "mixed")
	os.MkdirAll(mixedDir, 0755)
	writeFile(t, filepath.Join(mixedDir, "item1.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Item id="100" name="Sword">`+"\n"+
			`  <quality>85</quality>`+"\n"+
			`  <weight>3.5</weight>`+"\n"+
			`</Item>`)

	// bestcook 表：列表容器模式
	bestcookDir := filepath.Join(dir, "bestcook")
	os.MkdirAll(bestcookDir, 0755)
	writeFile(t, filepath.Join(bestcookDir, "bestcook.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<BestCook>`+"\n"+
			`  <BestCookData>`+"\n"+
			`    <BestCookData>`+"\n"+
			`      <classId>50507</classId>`+"\n"+
			`      <characterName>Alice</characterName>`+"\n"+
			`      <quality>100</quality>`+"\n"+
			`    </BestCookData>`+"\n"+
			`    <BestCookData>`+"\n"+
			`      <classId>50115</classId>`+"\n"+
			`      <characterName>Bob</characterName>`+"\n"+
			`      <quality>85</quality>`+"\n"+
			`    </BestCookData>`+"\n"+
			`  </BestCookData>`+"\n"+
			`</BestCook>`)

	// _mail 目录（应该被跳过）
	mailDir := filepath.Join(dir, "_mail")
	os.MkdirAll(mailDir, 0755)
	writeFile(t, filepath.Join(mailDir, "log.xml"),
		`<?xml version="1.0" encoding="utf-8" ?><Log msg="test" />`)

	// empty 目录（应该被跳过，无 XML 文件）
	emptyDir := filepath.Join(dir, "empty")
	os.MkdirAll(emptyDir, 0755)

	// cache 目录（包含 _cache.xml 文件，应该被过滤）
	cacheDir := filepath.Join(dir, "cachetest")
	os.MkdirAll(cacheDir, 0755)
	writeFile(t, filepath.Join(cacheDir, "data.xml"),
		`<?xml version="1.0" encoding="utf-8" ?><Data id="1" value="real" />`)
	writeFile(t, filepath.Join(cacheDir, "data_cache.xml"),
		`<?xml version="1.0" encoding="utf-8" ?><Data id="1" value="cached" />`)

	return dir
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file %s: %v", path, err)
	}
}

func TestXMLAdapter_Connect_MultiTable(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 验证已加载的表
	tables, err := adapter.GetTables(ctx)
	if err != nil {
		t.Fatalf("GetTables failed: %v", err)
	}

	tableSet := make(map[string]bool)
	for _, table := range tables {
		tableSet[table] = true
	}

	// 应该有 account, idpool, mixed, bestcook, cachetest
	expectedTables := []string{"account", "idpool", "mixed", "bestcook", "cachetest"}
	for _, expected := range expectedTables {
		if !tableSet[expected] {
			t.Errorf("expected table %q not found, got tables: %v", expected, tables)
		}
	}

	// _mail 和 empty 不应该出现
	if tableSet["_mail"] {
		t.Error("_mail directory should be skipped")
	}
	if tableSet["empty"] {
		t.Error("empty directory should be skipped")
	}
}

func TestXMLAdapter_QueryFlatAttributes(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 查询 account 表
	result, err := adapter.Query(ctx, "account", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "_file", Operator: "=", Value: "luna"},
		},
	})
	if err != nil {
		t.Fatalf("Query account failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row["id"] != "luna" {
		t.Errorf("expected id=luna, got %v", row["id"])
	}
	if row["name"] != "Luna" {
		t.Errorf("expected name=Luna, got %v", row["name"])
	}
	if row["password"] != "BCFE94820B4EA7FC" {
		t.Errorf("expected password=BCFE94820B4EA7FC, got %v", row["password"])
	}
}

func TestXMLAdapter_QueryAllRows(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 查询所有 account 行
	result, err := adapter.Query(ctx, "account", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query all account failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows in account, got %d", len(result.Rows))
	}
}

func TestXMLAdapter_MixedContent(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 查询 mixed 表（属性 + 子元素）
	result, err := adapter.Query(ctx, "mixed", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query mixed failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row in mixed, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	// 属性列
	if row["id"] != "100" {
		t.Errorf("expected id=100, got %v", row["id"])
	}
	if row["name"] != "Sword" {
		t.Errorf("expected name=Sword, got %v", row["name"])
	}
	// 简单文本子元素
	if row["quality"] != "85" {
		t.Errorf("expected quality=85, got %v", row["quality"])
	}
	if row["weight"] != "3.5" {
		t.Errorf("expected weight=3.5, got %v", row["weight"])
	}
}

func TestXMLAdapter_ListExpansion(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 查询 bestcook 表（列表展开后应有 2 行）
	result, err := adapter.Query(ctx, "bestcook", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query bestcook failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows in bestcook (expanded), got %d", len(result.Rows))
	}

	// 验证展开的行数据
	foundAlice := false
	foundBob := false
	for _, row := range result.Rows {
		if row["characterName"] == "Alice" {
			foundAlice = true
			if row["classId"] != "50507" {
				t.Errorf("Alice's classId expected 50507, got %v", row["classId"])
			}
			if row["quality"] != "100" {
				t.Errorf("Alice's quality expected 100, got %v", row["quality"])
			}
		}
		if row["characterName"] == "Bob" {
			foundBob = true
			if row["classId"] != "50115" {
				t.Errorf("Bob's classId expected 50115, got %v", row["classId"])
			}
		}
	}

	if !foundAlice {
		t.Error("Alice row not found in expanded bestcook")
	}
	if !foundBob {
		t.Error("Bob row not found in expanded bestcook")
	}
}

func TestXMLAdapter_CacheFileSkipped(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// cachetest 表应该只有 1 行（data.xml），不包含 data_cache.xml
	result, err := adapter.Query(ctx, "cachetest", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query cachetest failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row in cachetest (cache file skipped), got %d", len(result.Rows))
	}

	if result.Rows[0]["value"] != "real" {
		t.Errorf("expected value=real, got %v", result.Rows[0]["value"])
	}
}

func TestXMLAdapter_SchemaInference(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// 获取 account 表信息
	tableInfo, err := adapter.GetTableInfo(ctx, "account")
	if err != nil {
		t.Fatalf("GetTableInfo failed: %v", err)
	}

	// _file 列应该是主键
	foundFile := false
	for _, col := range tableInfo.Columns {
		if col.Name == "_file" {
			foundFile = true
			if !col.Primary {
				t.Error("_file column should be primary key")
			}
			if col.Type != "string" {
				t.Errorf("_file column type expected string, got %s", col.Type)
			}
		}
	}
	if !foundFile {
		t.Error("_file column not found in table schema")
	}

	// flag 列应该被推断为 int64（值为 "-1" 和 "0"）
	for _, col := range tableInfo.Columns {
		if col.Name == "flag" {
			if col.Type != "int64" {
				t.Errorf("flag column type expected int64, got %s", col.Type)
			}
		}
	}
}

func TestXMLAdapter_ReadOnly(t *testing.T) {
	dir := createTestXMLDir(t)
	ctx := context.Background()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-xml",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	// Insert 应该失败
	_, err := adapter.Insert(ctx, "account", []domain.Row{{"_file": "test"}}, nil)
	if err == nil {
		t.Error("Insert should fail on read-only adapter")
	}

	// Update 应该失败
	_, err = adapter.Update(ctx, "account", nil, domain.Row{"name": "x"}, nil)
	if err == nil {
		t.Error("Update should fail on read-only adapter")
	}

	// Delete 应该失败
	_, err = adapter.Delete(ctx, "account", nil, nil)
	if err == nil {
		t.Error("Delete should fail on read-only adapter")
	}

	// CreateTable 应该失败
	err = adapter.CreateTable(ctx, &domain.TableInfo{Name: "new"})
	if err == nil {
		t.Error("CreateTable should fail on read-only adapter")
	}
}

func TestXMLAdapter_UTF16(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 创建 UTF-16 LE 编码的 XML 文件
	dataDir := filepath.Join(dir, "data")
	os.MkdirAll(dataDir, 0755)

	utf8Content := `<?xml version="1.0" encoding="utf-16" ?>` + "\n" +
		`<Record id="1" name="测试" value="42" />`
	utf16Data, err := encodeToUTF16([]byte(utf8Content))
	if err != nil {
		t.Fatalf("failed to encode to UTF-16: %v", err)
	}
	writeFileBytes(t, filepath.Join(dataDir, "record1.xml"), utf16Data)

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-utf16",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	result, err := adapter.Query(ctx, "data", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query data failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row["name"] != "测试" {
		t.Errorf("expected name=测试, got %v", row["name"])
	}
	if row["value"] != "42" {
		t.Errorf("expected value=42, got %v", row["value"])
	}
}

func writeFileBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write test file %s: %v", path, err)
	}
}

func TestXMLAdapter_WriteBack(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 创建初始数据
	dataDir := filepath.Join(dir, "items")
	os.MkdirAll(dataDir, 0755)
	writeFile(t, filepath.Join(dataDir, "sword.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Item id="1" name="Sword" price="100" />`)
	writeFile(t, filepath.Join(dataDir, "shield.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Item id="2" name="Shield" price="80" />`)

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-writeback",
		Database: dir,
		Writable: true,
		Options: map[string]interface{}{
			"encoding": "utf-8",
		},
	}

	// 读取并修改数据
	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// 删除 shield 行
	deleted, err := adapter.Delete(ctx, "items", []domain.Filter{
		{Field: "_file", Operator: "=", Value: "shield"},
	}, nil)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	// 关闭（触发写回）
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证 shield.xml 已被删除
	if _, err := os.Stat(filepath.Join(dataDir, "shield.xml")); !os.IsNotExist(err) {
		t.Error("shield.xml should have been deleted")
	}

	// 验证 sword.xml 仍然存在
	if _, err := os.Stat(filepath.Join(dataDir, "sword.xml")); err != nil {
		t.Error("sword.xml should still exist")
	}
}

func TestXMLFactory_Create(t *testing.T) {
	dir := t.TempDir()

	factory := NewXMLFactory()

	// 验证类型
	if factory.GetType() != domain.DataSourceTypeXML {
		t.Errorf("expected type xml, got %s", factory.GetType())
	}

	// 使用 Database 路径
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Database: dir,
	}
	ds, err := factory.Create(config)
	if err != nil {
		t.Fatalf("Factory.Create failed: %v", err)
	}
	if ds == nil {
		t.Fatal("Factory.Create returned nil")
	}

	// 使用 Options path
	config2 := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeXML,
		Options: map[string]interface{}{
			"path": dir,
		},
	}
	ds2, err := factory.Create(config2)
	if err != nil {
		t.Fatalf("Factory.Create with options path failed: %v", err)
	}
	if ds2 == nil {
		t.Fatal("Factory.Create with options path returned nil")
	}

	// 空路径应该失败
	config3 := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeXML,
	}
	_, err = factory.Create(config3)
	if err == nil {
		t.Error("Factory.Create should fail with empty path")
	}

	// nil config 应该失败
	_, err = factory.Create(nil)
	if err == nil {
		t.Error("Factory.Create should fail with nil config")
	}
}

func TestXMLAdapter_ComplexChildren(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 创建带复杂子元素的 XML
	dataDir := filepath.Join(dir, "players")
	os.MkdirAll(dataDir, 0755)
	writeFile(t, filepath.Join(dataDir, "player1.xml"),
		`<?xml version="1.0" encoding="utf-8" ?>`+"\n"+
			`<Player id="1" name="Hero">`+"\n"+
			`  <character id="100" name="Warrior" level="50" />`+"\n"+
			`  <character id="101" name="Mage" level="30" />`+"\n"+
			`</Player>`)

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeXML,
		Name:     "test-complex",
		Database: dir,
		Writable: false,
	}

	adapter := NewXMLAdapter(config, dir)
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close(ctx)

	result, err := adapter.Query(ctx, "players", &domain.QueryOptions{SelectAll: true})
	if err != nil {
		t.Fatalf("Query players failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row["id"] != "1" {
		t.Errorf("expected id=1, got %v", row["id"])
	}
	if row["name"] != "Hero" {
		t.Errorf("expected name=Hero, got %v", row["name"])
	}

	// character 子元素应该被序列化为 JSON 字符串
	charData, ok := row["character"].(string)
	if !ok {
		t.Fatalf("character column should be a JSON string, got %T", row["character"])
	}
	if charData == "" {
		t.Error("character column should not be empty")
	}
	// 验证是 JSON 数组
	if charData[0] != '[' {
		t.Errorf("character column should be a JSON array, got: %s", charData)
	}
}

func TestDetectStringType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"42", "int64"},
		{"-1", "int64"},
		{"0", "int64"},
		{"3.14", "float64"},
		{"-2.5", "float64"},
		{"true", "bool"},
		{"false", "bool"},
		{"hello", "string"},
		{"2024-12-01T16:37:28", "string"},
		{"", "string"},
		{"BCFE94820B4EA7FC", "string"},
		{"100000000000", "int64"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := detectStringType(tt.input)
			if result != tt.expected {
				t.Errorf("detectStringType(%q) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeXMLBytes_UTF8(t *testing.T) {
	// UTF-8 输入应该原样返回
	input := []byte(`<?xml version="1.0" encoding="utf-8" ?><Root />`)
	output, err := decodeXMLBytes(input)
	if err != nil {
		t.Fatalf("decodeXMLBytes failed: %v", err)
	}
	if string(output) != string(input) {
		t.Errorf("UTF-8 input should be returned as-is")
	}
}

func TestDecodeXMLBytes_UTF16(t *testing.T) {
	utf8Content := `<?xml version="1.0" encoding="utf-16" ?><Root id="test" />`
	utf16Data, err := encodeToUTF16([]byte(utf8Content))
	if err != nil {
		t.Fatalf("encodeToUTF16 failed: %v", err)
	}

	output, err := decodeXMLBytes(utf16Data)
	if err != nil {
		t.Fatalf("decodeXMLBytes failed: %v", err)
	}

	// 输出应该是 UTF-8，且 encoding 声明已被替换
	if len(output) == 0 {
		t.Fatal("output should not be empty")
	}
	if string(output) == string(utf16Data) {
		t.Error("output should differ from UTF-16 input")
	}
}

func TestDecodeXMLBytes_Empty(t *testing.T) {
	output, err := decodeXMLBytes([]byte{})
	if err != nil {
		t.Fatalf("decodeXMLBytes should handle empty input: %v", err)
	}
	if len(output) != 0 {
		t.Error("empty input should return empty output")
	}
}
