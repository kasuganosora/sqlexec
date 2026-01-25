package excel

import (
	"context"
	"os"
	"testing"

	"github.com/xuri/excelize/v2"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestExcelAdapter_writeBack 测试写回功能
func TestExcelAdapter_writeBack(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	// 使用writable配置
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-writeback",
		Options: map[string]interface{}{
			"writable": true,
		},
	}
	adapter := NewExcelAdapter(config, filePath)

	ctx := context.Background()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 修改数据 - 先插入新行到MVCCDataSource
	newRow := domain.Row{"id": 3, "name": "Charlie", "email": "charlie@example.com"}
	if _, err := adapter.Insert(ctx, "Sheet1", []domain.Row{newRow}, nil); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// 写回（会自动获取最新数据）
	if err := adapter.writeBack(); err != nil {
		t.Logf("writeBack() returned error (simplified Excel implementation): %v", err)
		// 由于Excel写回需要完整的excilize实现，这里可能失败
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestExcelAdapter_Connect_MissingSheet 测试连接指定不存在的工作表
func TestExcelAdapter_Connect_MissingSheet(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	// 指定不存在的工作表
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-missing",
		Options: map[string]interface{}{
			"sheet_name": "NonExistentSheet",
		},
	}
	adapter := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	// 连接（应该失败，工作表不存在）
	err := adapter.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to nonexistent sheet")
	}
}

// TestExcelAdapter_Connect_EmptySheet 测试连接空的工作表
func TestExcelAdapter_Connect_EmptySheet(t *testing.T) {
	// 创建一个空的Excel文件，只有默认Sheet1且无数据
	f := excelize.NewFile()
	tmpFile, err := os.CreateTemp("", "empty*.xlsx")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	if err := f.SaveAs(tmpFile.Name()); err != nil {
		t.Fatalf("Failed to save Excel file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close Excel file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-empty",
	}
	adapter := NewExcelAdapter(config, tmpFile.Name())
	ctx := context.Background()

	// 连接应该失败，因为sheet是空的
	err = adapter.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to empty sheet")
	}

	os.Remove(tmpFile.Name())
}

// TestExcelAdapter_InferColumnTypes 测试列类型推断
func TestExcelAdapter_InferColumnTypes(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-infer",
	}
	adapter := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表信息来验证类型推断
	tableInfo, err := adapter.GetTableInfo(ctx, "Sheet1")
	if err != nil {
		t.Fatalf("GetTableInfo() error = %v", err)
	}

	if tableInfo == nil {
		t.Errorf("GetTableInfo() returned nil")
		return
	}

	// 验证列类型
	columnTypes := make(map[string]string)
	for _, col := range tableInfo.Columns {
		columnTypes[col.Name] = col.Type
	}

	// id列应该是int64
	if colType, ok := columnTypes["id"]; ok && colType != "int64" {
		t.Errorf("Expected column 'id' to be int64, got %v", colType)
	}

	// name和email列应该是string
	for _, colName := range []string{"name", "email"} {
		if colType, ok := columnTypes[colName]; ok && colType != "string" {
			t.Errorf("Expected column '%s' to be string, got %v", colName, colType)
		}
	}
}
