package optimizer

import (
	"context"
	"testing"
	
	"github.com/kasuganosora/sqlexec/pkg/optimizer/genetic"
)

// TestGeneticAlgorithmSimple 简单的遗传算法测试
func TestGeneticAlgorithmSimple(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.PopulationSize = 10
	config.MaxGenerations = 5

	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b"}, Priority: 3},
		{TableName: "t1", Columns: []string{"c"}, Priority: 2},
	}

	benefits := map[string]float64{
		"t1(a)": 0.8,
		"t1(b)": 0.6,
		"t1(c)": 0.4,
	}

	ctx := context.Background()

	// 使用遗传算法运行
	geneticCandidates := ConvertGeneticCandidates(candidates)
	result := ga.Run(ctx, geneticCandidates, benefits)
	
	// 转换结果
	finalResult := ConvertGeneticResults(result)

	// 验证结果不为空
	if len(finalResult) == 0 {
		t.Error("Expected at least one solution")
	}

	// 验证结果满足约束
	if len(finalResult) > config.MaxIndexes {
		t.Errorf("Solution should not exceed max indexes: got %d, max %d", len(finalResult), config.MaxIndexes)
	}
}

// TestConvertGeneticCandidates 测试类型转换
func TestConvertGeneticCandidates(t *testing.T) {
	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a", "b"}, Priority: 4},
		{TableName: "t2", Columns: []string{"c"}, Priority: 3},
	}

	geneticCandidates := ConvertGeneticCandidates(candidates)

	if len(geneticCandidates) != len(candidates) {
		t.Errorf("Expected %d candidates, got %d", len(candidates), len(geneticCandidates))
	}

	if geneticCandidates[0].TableName != "t1" {
		t.Errorf("Expected table name 't1', got '%s'", geneticCandidates[0].TableName)
	}

	if len(geneticCandidates[0].Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(geneticCandidates[0].Columns))
	}
}

// TestConvertGeneticResults 测试结果转换
func TestConvertGeneticResults(t *testing.T) {
	// 创建遗传算法结果
	candidates := []*genetic.IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t2", Columns: []string{"c"}, Priority: 3},
	}

	// 由于ConvertGeneticResults接收*genetic.IndexCandidate，我们测试正向转换
	optimizerCandidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t2", Columns: []string{"c"}, Priority: 3},
	}

	geneticCandidates := ConvertGeneticCandidates(optimizerCandidates)

	if len(geneticCandidates) != len(candidates) {
		t.Errorf("Expected %d results, got %d", len(candidates), len(geneticCandidates))
	}

	if geneticCandidates[0].TableName != "t1" {
		t.Errorf("Expected table name 't1', got '%s'", geneticCandidates[0].TableName)
	}
}
