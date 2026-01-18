package resource

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 测试CSV数据源
func TestCSVSource(t *testing.T) {
	// 创建临时文件
	tmpDir, err := os.MkdirTemp("", "csv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	csvFile := filepath.Join(tmpDir, "test.csv")
	if err := generateCSV(csvFile, 1000); err != nil {
		t.Fatal(err)
	}
	
	// 创建CSV数据源
	config := &DataSourceConfig{
		Type: DataSourceTypeCSV,
		Name: csvFile,
		Options: map[string]interface{}{
			"delimiter":  ',',
			"header":     true,
			"chunk_size": int64(1 << 20),
			"workers":    2,
		},
	}
	
	source, err := CreateDataSource(config)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}
	defer source.Close(context.Background())
	
	// 连接
	ctx := context.Background()
	if err := source.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	
	// 测试表信息
	tableInfo, err := source.GetTableInfo(ctx, "csv_data")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	
	if len(tableInfo.Columns) == 0 {
		t.Error("Expected at least 1 column")
	}
	
	// 测试查询
	result, err := source.Query(ctx, "csv_data", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	
	if result.Total != 1000 {
		t.Errorf("Expected 1000 rows, got %d", result.Total)
	}
	
	// 测试过滤查询
	filteredResult, err := source.Query(ctx, "csv_data", &QueryOptions{
		Filters: []Filter{
			{Field: "age", Operator: ">", Value: 30},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query with filter: %v", err)
	}
	
	if filteredResult.Total == 0 {
		t.Error("Expected some rows with age > 30")
	}
	
	// 测试分页
	pagedResult, err := source.Query(ctx, "csv_data", &QueryOptions{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Failed to query with limit: %v", err)
	}
	
	if len(pagedResult.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(pagedResult.Rows))
	}
	
	fmt.Println("✓ CSV数据源测试通过")
}

// 测试JSON数据源
func TestJSONSource(t *testing.T) {
	// 创建临时文件
	tmpDir, err := os.MkdirTemp("", "json_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	jsonFile := filepath.Join(tmpDir, "test.json")
	if err := generateJSON(jsonFile, 1000); err != nil {
		t.Fatal(err)
	}
	
	// 创建JSON数据源
	config := &DataSourceConfig{
		Type: DataSourceTypeJSON,
		Name: jsonFile,
		Options: map[string]interface{}{
			"array_mode": true,
			"workers":    2,
		},
	}
	
	source, err := CreateDataSource(config)
	if err != nil {
		t.Fatalf("Failed to create JSON source: %v", err)
	}
	defer source.Close(context.Background())
	
	// 连接
	ctx := context.Background()
	if err := source.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	
	// 测试表信息
	tableInfo, err := source.GetTableInfo(ctx, "json_data")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	
	if len(tableInfo.Columns) == 0 {
		t.Error("Expected at least 1 column")
	}
	
	// 测试查询
	result, err := source.Query(ctx, "json_data", nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	
	if result.Total != 1000 {
		t.Errorf("Expected 1000 rows, got %d", result.Total)
	}
	
	// 测试过滤查询
	filteredResult, err := source.Query(ctx, "json_data", &QueryOptions{
		Filters: []Filter{
			{Field: "age", Operator: ">", Value: 30},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query with filter: %v", err)
	}
	
	if filteredResult.Total == 0 {
		t.Error("Expected some rows with age > 30")
	}
	
	// 测试分页
	pagedResult, err := source.Query(ctx, "json_data", &QueryOptions{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Failed to query with limit: %v", err)
	}
	
	if len(pagedResult.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(pagedResult.Rows))
	}
	
	fmt.Println("✓ JSON数据源测试通过")
}

// 性能测试
func BenchmarkCSVSource(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "csv_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	csvFile := filepath.Join(tmpDir, "bench.csv")
	if err := generateCSV(csvFile, 10000); err != nil {
		b.Fatal(err)
	}
	
	config := &DataSourceConfig{
		Type: DataSourceTypeCSV,
		Name: csvFile,
		Options: map[string]interface{}{
			"delimiter": ',',
			"header":    true,
			"workers":   4,
		},
	}
	
	source, err := CreateDataSource(config)
	if err != nil {
		b.Fatal(err)
	}
	defer source.Close(context.Background())
	
	ctx := context.Background()
	if err := source.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := source.Query(ctx, "csv_data", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONSource(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "json_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	jsonFile := filepath.Join(tmpDir, "bench.json")
	if err := generateJSON(jsonFile, 10000); err != nil {
		b.Fatal(err)
	}
	
	config := &DataSourceConfig{
		Type: DataSourceTypeJSON,
		Name: jsonFile,
		Options: map[string]interface{}{
			"array_mode": true,
			"workers":    4,
		},
	}
	
	source, err := CreateDataSource(config)
	if err != nil {
		b.Fatal(err)
	}
	defer source.Close(context.Background())
	
	ctx := context.Background()
	if err := source.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := source.Query(ctx, "json_data", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// generateCSV 生成测试CSV文件
func generateCSV(filePath string, numRows int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// 写入头部
	headers := []string{"id", "name", "age", "salary", "active"}
	if err := writer.Write(headers); err != nil {
		return err
	}
	
	// 写入数据
	for i := 0; i < numRows; i++ {
		record := []string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("user_%d", i+1),
			fmt.Sprintf("%d", 20+(i%50)),
			fmt.Sprintf("%.2f", float64(50000+i*100)),
			fmt.Sprintf("%t", i%3 == 0),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	
	return nil
}

// generateJSON 生成测试JSON文件
func generateJSON(filePath string, numRows int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// 生成JSON数组
	data := make([]map[string]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = map[string]interface{}{
			"id":     i + 1,
			"name":   fmt.Sprintf("user_%d", i+1),
			"age":    20 + (i % 50),
			"salary": 50000.0 + float64(i)*100,
			"active": i%3 == 0,
		}
	}
	
	encoder := json.NewEncoder(file)
	return encoder.Encode(data)
}

// 性能对比测试
func TestFileSourcePerformance(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "perf_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	
	// 生成测试数据
	numRows := 10000
	csvFile := filepath.Join(tmpDir, "perf.csv")
	jsonFile := filepath.Join(tmpDir, "perf.json")
	
	if err := generateCSV(csvFile, numRows); err != nil {
		t.Fatal(err)
	}
	if err := generateJSON(jsonFile, numRows); err != nil {
		t.Fatal(err)
	}
	
	ctx := context.Background()
	
	// 测试CSV性能
	fmt.Println("\n=== CSV性能测试 ===")
	csvTimes := make([]time.Duration, 3)
	for i := 0; i < 3; i++ {
		config := &DataSourceConfig{
			Type: DataSourceTypeCSV,
			Name: csvFile,
			Options: map[string]interface{}{
				"delimiter": ',',
				"header":    true,
				"workers":   4,
			},
		}
		
		source, err := CreateDataSource(config)
		if err != nil {
			t.Fatal(err)
		}
		
		if err := source.Connect(ctx); err != nil {
			source.Close(ctx)
			t.Fatal(err)
		}
		
		start := time.Now()
		result, err := source.Query(ctx, "csv_data", nil)
		if err != nil {
			source.Close(ctx)
			t.Fatal(err)
		}
		csvTimes[i] = time.Since(start)
		source.Close(ctx)
		
		if result.Total != int64(numRows) {
			t.Errorf("Expected %d rows, got %d", numRows, result.Total)
		}
	}
	
	// 测试JSON性能
	fmt.Println("\n=== JSON性能测试 ===")
	jsonTimes := make([]time.Duration, 3)
	for i := 0; i < 3; i++ {
		config := &DataSourceConfig{
			Type: DataSourceTypeJSON,
			Name: jsonFile,
			Options: map[string]interface{}{
				"array_mode": true,
				"workers":    4,
			},
		}
		
		source, err := CreateDataSource(config)
		if err != nil {
			t.Fatal(err)
		}
		
		if err := source.Connect(ctx); err != nil {
			source.Close(ctx)
			t.Fatal(err)
		}
		
		start := time.Now()
		result, err := source.Query(ctx, "json_data", nil)
		if err != nil {
			source.Close(ctx)
			t.Fatal(err)
		}
		jsonTimes[i] = time.Since(start)
		source.Close(ctx)
		
		if result.Total != int64(numRows) {
			t.Errorf("Expected %d rows, got %d", numRows, result.Total)
		}
	}
	
	// 输出结果
	avgCSV := avgDuration(csvTimes)
	avgJSON := avgDuration(jsonTimes)
	
	fmt.Printf("\nCSV平均耗时: %v\n", avgCSV)
	fmt.Printf("JSON平均耗时: %v\n", avgJSON)
	fmt.Printf("CSV吞吐量: %.0f 行/秒\n", float64(numRows)/avgCSV.Seconds())
	fmt.Printf("JSON吞吐量: %.0f 行/秒\n", float64(numRows)/avgJSON.Seconds())
	
	ratio := float64(avgJSON) / float64(avgCSV)
	if ratio > 1 {
		fmt.Printf("CSV比JSON快%.2fx\n", ratio)
	} else {
		fmt.Printf("JSON比CSV快%.2fx\n", 1/ratio)
	}
}

func avgDuration(durations []time.Duration) time.Duration {
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}
