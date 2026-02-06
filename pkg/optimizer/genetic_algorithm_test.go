package optimizer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewGeneticAlgorithm 测试创建遗传算法实例
func TestNewGeneticAlgorithm(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	assert.NotNil(t, ga)
	assert.Equal(t, config.PopulationSize, ga.PopulationSize)
	assert.Equal(t, config.MaxGenerations, ga.MaxGenerations)
	assert.Equal(t, config.MutationRate, ga.MutationRate)
	assert.Equal(t, config.CrossoverRate, ga.CrossoverRate)
}

// TestInitializePopulation 测试种群初始化
func TestInitializePopulation(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

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

	pop := ga.InitializePopulation(candidates, benefits)

	assert.NotNil(t, pop)
	assert.Equal(t, ga.PopulationSize, len(pop.Individuals))
	assert.NotNil(t, ga.candidates)
	assert.Equal(t, 3, len(ga.candidates))
}

// TestCalculateFitness 测试适应度计算
func TestCalculateFitness(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b"}, Priority: 3},
	}

	benefits := map[string]float64{
		"t1(a)": 0.8,
		"t1(b)": 0.6,
	}

	ga.candidates = candidates
	ga.benefits = benefits

	// 测试选中所有索引
	ind1 := &Individual{Genes: []bool{true, true}}
	fitness1 := ga.calculateFitness(ind1)
	assert.Greater(t, fitness1, 0.0)

	// 测试选中部分索引
	ind2 := &Individual{Genes: []bool{true, false}}
	fitness2 := ga.calculateFitness(ind2)
	assert.Greater(t, fitness2, 0.0)

	// 测试不选中任何索引
	ind3 := &Individual{Genes: []bool{false, false}}
	fitness3 := ga.calculateFitness(ind3)
	assert.Equal(t, 0.0, fitness3)
}

// TestCheckConstraints 测试约束检查
func TestCheckConstraints(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b", "c"}, Priority: 3},
		{TableName: "t1", Columns: []string{"d", "e", "f"}, Priority: 2},
	}

	ga.candidates = candidates
	ga.maxIndexes = 3
	ga.maxColumns = 3

	// 测试满足约束
	ind1 := &Individual{Genes: []bool{true, false, false}}
	assert.True(t, ga.checkConstraints(ind1))

	// 测试索引数量超限
	ind2 := &Individual{Genes: []bool{true, true, true, true}}
	ga.candidates = append(candidates, &IndexCandidate{Columns: []string{"g"}})
	assert.False(t, ga.checkConstraints(ind2))

	// 测试列数超限
	ga.maxColumns = 2
	ind3 := &Individual{Genes: []bool{false, false, true}}
	assert.False(t, ga.checkConstraints(ind3))
}

// TestSelect 测试选择操作
func TestSelect(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b"}, Priority: 3},
	}

	benefits := map[string]float64{
		"t1(a)": 0.8,
		"t1(b)": 0.6,
	}

	pop := ga.InitializePopulation(candidates, benefits)

	// 选择操作
	newPop := ga.Select(pop, 2)

	assert.NotNil(t, newPop)
	assert.Equal(t, ga.PopulationSize, len(newPop.Individuals))
}

// TestCrossover 测试交叉操作
func TestCrossover(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	parent1 := &Individual{Genes: []bool{true, true, false, false}}
	parent2 := &Individual{Genes: []bool{false, false, true, true}}

	child1, child2 := ga.Crossover(parent1, parent2)

	assert.NotNil(t, child1)
	assert.NotNil(t, child2)
	assert.Equal(t, len(parent1.Genes), len(child1.Genes))
	assert.Equal(t, len(parent2.Genes), len(child2.Genes))
}

// TestMutate 测试变异操作
func TestMutate(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	original := &Individual{Genes: []bool{true, false, true, false}}

	mutated := ga.Mutate(original)

	assert.NotNil(t, mutated)
	assert.Equal(t, len(original.Genes), len(mutated.Genes))
	// 由于变异率较低，可能不变异，所以这里不强制检查是否变化
}

// TestIsConverged 测试收敛判断
func TestIsConverged(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	// 未收敛的种群
	pop1 := &Population{
		Individuals: []*Individual{
			{Fitness: 1.0},
			{Fitness: 0.5},
			{Fitness: 0.2},
		},
	}

	assert.False(t, ga.IsConverged(pop1, 0.01))

	// 收敛的种群
	pop2 := &Population{
		Individuals: []*Individual{
			{Fitness: 1.0},
			{Fitness: 0.99},
			{Fitness: 0.98},
		},
	}

	assert.True(t, ga.IsConverged(pop2, 0.015)) // 使用稍大的阈值考虑浮点精度
}

// TestGetBestIndividual 测试获取最优个体
func TestGetBestIndividual(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 0.5},
			{Fitness: 1.0},
			{Fitness: 0.8},
		},
	}

	best := ga.GetBestIndividual(pop)

	assert.NotNil(t, best)
	assert.Equal(t, 1.0, best.Fitness)
}

// TestExtractSolution 测试提取解
func TestExtractSolution(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b"}, Priority: 3},
		{TableName: "t1", Columns: []string{"c"}, Priority: 2},
	}

	ga.candidates = candidates

	ind := &Individual{Genes: []bool{true, false, true}}

	solution := ga.ExtractSolution(ind)

	assert.NotNil(t, solution)
	assert.Equal(t, 2, len(solution))
	assert.Equal(t, "a", solution[0].Columns[0])
	assert.Equal(t, "c", solution[1].Columns[0])
}

// TestRun 测试完整运行
func TestRun(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)

	// 使用较小的种群和代数以加快测试
	ga.PopulationSize = 10
	ga.MaxGenerations = 5

	candidates := []*IndexCandidate{
		{TableName: "t1", Columns: []string{"a"}, Priority: 4},
		{TableName: "t1", Columns: []string{"b"}, Priority: 3},
		{TableName: "t1", Columns: []string{"c"}, Priority: 2},
		{TableName: "t1", Columns: []string{"d"}, Priority: 1},
	}

	benefits := map[string]float64{
		"t1(a)": 0.8,
		"t1(b)": 0.6,
		"t1(c)": 0.4,
		"t1(d)": 0.2,
	}

	ctx := context.Background()
	solution := ga.Run(ctx, candidates, benefits)

	assert.NotNil(t, solution)
	// 解应该不为空且满足约束
	assert.LessOrEqual(t, len(solution), ga.maxIndexes)
}

// BenchmarkGeneticAlgorithm 基准测试
func BenchmarkGeneticAlgorithm(b *testing.B) {
	ga := NewGeneticAlgorithm(nil)
	ga.PopulationSize = 50
	ga.MaxGenerations = 20

	candidates := make([]*IndexCandidate, 20)
	for i := 0; i < 20; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4 - i/5,
		}
	}

	benefits := make(map[string]float64)
	for i := range candidates {
		key := "t1(" + string(rune('a'+i)) + ")"
		benefits[key] = 1.0 - float64(i)*0.05
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ga.Run(ctx, candidates, benefits)
	}
}

// TestTournamentSelect 测试锦标赛选择
func TestTournamentSelect(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)
	ga.TournamentSize = 3

	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 0.2},
			{Fitness: 0.8},
			{Fitness: 0.5},
			{Fitness: 0.1},
			{Fitness: 0.9},
		},
	}

	// 多次运行验证统计特性
	highFitnessCount := 0
	iterations := 100

	for i := 0; i < iterations; i++ {
		selected := ga.tournamentSelect(pop.Individuals)
		if selected.Fitness >= 0.5 {
			highFitnessCount++
		}
	}

	// 锦标赛选择应该倾向于选择高适应度个体
	ratio := float64(highFitnessCount) / float64(iterations)
	if ratio < 0.6 {
		t.Errorf("Tournament selection should favor high fitness individuals, ratio: %.2f", ratio)
	}

	fmt.Printf("High fitness selected %d/%d times (%.2f%%)\n",
		highFitnessCount, iterations, ratio*100)
}

// TestCalculateConvergenceMetrics 测试收敛指标计算
func TestCalculateConvergenceMetrics(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)
	ga.windowSize = 3

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

	ga.candidates = candidates
	ga.benefits = benefits

	pop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true, true, false}},
			{Genes: []bool{true, false, true}},
			{Genes: []bool{false, true, true}},
		},
	}

	// 计算适应度
	for _, ind := range pop.Individuals {
		ind.Fitness = ga.calculateFitness(ind)
	}

	// 计算收敛指标
	convergenceMetric, changeRate, maxFitness := ga.CalculateConvergenceMetrics(pop)

	// 验证返回值
	if convergenceMetric < 0 {
		t.Errorf("Convergence metric should be non-negative, got %.4f", convergenceMetric)
	}
	if maxFitness <= 0 {
		t.Errorf("Max fitness should be positive, got %.4f", maxFitness)
	}

	fmt.Printf("Convergence metric: %.4f\n", convergenceMetric)
	fmt.Printf("Change rate: %.6f\n", changeRate)
	fmt.Printf("Max fitness: %.4f\n", maxFitness)
}

// TestAdaptParameters 测试自适应参数调整
func TestAdaptParameters(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)
	ga.AdaptiveEnabled = true
	ga.windowSize = 5

	// 模拟已收敛状态（历史记录变化很小）
	for i := 0; i < 5; i++ {
		ga.avgFitnessHistory = append(ga.avgFitnessHistory, 0.95)
		ga.bestFitnessHistory = append(ga.bestFitnessHistory, 1.0)
	}

	// 设置初始参数
	ga.MutationRate = 0.1
	ga.CrossoverRate = 0.8

	// 在收敛状态下调整参数（generation >= 5）
	ga.AdaptParameters(10, 0.005, 0.0001)

	// 应该提高变异率以引入多样性
	if ga.MutationRate <= 0.1 {
		t.Errorf("Expected higher mutation rate when converged, got %.3f", ga.MutationRate)
	}

	// 应该降低交叉率
	if ga.CrossoverRate >= 0.8 {
		t.Errorf("Expected lower crossover rate when converged, got %.3f", ga.CrossoverRate)
	}

	fmt.Printf("After convergence adjustment - MutationRate: %.3f, CrossoverRate: %.3f\n",
		ga.MutationRate, ga.CrossoverRate)
}

// TestSelectionStrategies 测试不同选择策略
func TestSelectionStrategies(t *testing.T) {
	candidates := make([]*IndexCandidate, 5)
	for i := 0; i < 5; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4,
		}
	}

	benefits := map[string]float64{
		"t1(a)": 0.9,
		"t1(b)": 0.8,
		"t1(c)": 0.7,
		"t1(d)": 0.6,
		"t1(e)": 0.5,
	}

	ctx := context.Background()

	// 测试轮盘赌选择
	config1 := DefaultGeneticAlgorithmConfig()
	config1.SelectionStrategy = "roulette"
	config1.MaxGenerations = 10
	ga1 := NewGeneticAlgorithm(config1)
	result1 := ga1.Run(ctx, candidates, benefits)

	// 测试锦标赛选择
	config2 := DefaultGeneticAlgorithmConfig()
	config2.SelectionStrategy = "tournament"
	config2.TournamentSize = 3
	config2.MaxGenerations = 10
	ga2 := NewGeneticAlgorithm(config2)
	result2 := ga2.Run(ctx, candidates, benefits)

	// 验证两种策略都能产生有效的解
	if len(result1) == 0 {
		t.Error("Roulette selection should produce at least one solution")
	}
	if len(result2) == 0 {
		t.Error("Tournament selection should produce at least one solution")
	}

	fmt.Printf("Roulette selection: %d indexes\n", len(result1))
	fmt.Printf("Tournament selection: %d indexes\n", len(result2))
}

// TestGeneticAlgorithmConfig 测试配置选项
func TestGeneticAlgorithmConfig(t *testing.T) {
	config := &GeneticAlgorithmConfig{
		PopulationSize:    100,
		MaxGenerations:    200,
		MutationRate:      0.15,
		CrossoverRate:     0.75,
		MaxIndexes:        10,
		MaxTotalSize:      500 * 1024 * 1024,
		MaxColumns:        4,
		AdaptiveEnabled:   true,
		SelectionStrategy: "tournament",
		TournamentSize:    7,
	}

	ga := NewGeneticAlgorithm(config)

	assert.Equal(t, 100, ga.PopulationSize)
	assert.Equal(t, 200, ga.MaxGenerations)
	assert.Equal(t, 0.15, ga.MutationRate)
	assert.Equal(t, 0.75, ga.CrossoverRate)
	assert.Equal(t, 10, ga.maxIndexes)
	assert.Equal(t, int64(500*1024*1024), ga.maxTotalSize)
	assert.Equal(t, 4, ga.maxColumns)
	assert.True(t, ga.AdaptiveEnabled)
	assert.Equal(t, "tournament", ga.SelectionStrategy)
	assert.Equal(t, 7, ga.TournamentSize)
}

