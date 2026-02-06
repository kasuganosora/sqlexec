package optimizer

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkGeneticAlgorithmWithAdaptive 基准测试：自适应参数 vs 固定参数
func BenchmarkGeneticAlgorithmWithAdaptive(b *testing.B) {
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
		config := DefaultGeneticAlgorithmConfig()
		config.AdaptiveEnabled = true
		ga := NewGeneticAlgorithm(config)
		ga.Run(ctx, candidates, benefits)
	}
}

// BenchmarkGeneticAlgorithmWithoutAdaptive 基准测试：固定参数
func BenchmarkGeneticAlgorithmWithoutAdaptive(b *testing.B) {
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
		config := DefaultGeneticAlgorithmConfig()
		config.AdaptiveEnabled = false
		ga := NewGeneticAlgorithm(config)
		ga.Run(ctx, candidates, benefits)
	}
}

// BenchmarkTournamentSelection 基准测试：锦标赛选择 vs 轮盘赌
func BenchmarkTournamentSelection(b *testing.B) {
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
		config := DefaultGeneticAlgorithmConfig()
		config.SelectionStrategy = "tournament"
		config.TournamentSize = 5
		ga := NewGeneticAlgorithm(config)
		ga.Run(ctx, candidates, benefits)
	}
}

// BenchmarkRouletteSelection 基准测试：轮盘赌选择
func BenchmarkRouletteSelection(b *testing.B) {
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
		config := DefaultGeneticAlgorithmConfig()
		config.SelectionStrategy = "roulette"
		ga := NewGeneticAlgorithm(config)
		ga.Run(ctx, candidates, benefits)
	}
}

// BenchmarkConvergenceSpeedLargeProblem 基准测试：大问题规模的收敛速度
func BenchmarkConvergenceSpeedLargeProblem(b *testing.B) {
	candidates := make([]*IndexCandidate, 50)
	for i := 0; i < 50; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4 - i/10,
		}
	}

	benefits := make(map[string]float64)
	for i := range candidates {
		key := "t1(" + string(rune('a'+i)) + ")"
		benefits[key] = 1.0 - float64(i)*0.02
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config := DefaultGeneticAlgorithmConfig()
		config.AdaptiveEnabled = true
		config.MaxGenerations = 200
		ga := NewGeneticAlgorithm(config)
		ga.Run(ctx, candidates, benefits)
	}
}

// TestAdaptiveParameters 测试自适应参数调整
func TestAdaptiveParameters(t *testing.T) {
	candidates := make([]*IndexCandidate, 10)
	for i := 0; i < 10; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4 - i/3,
		}
	}

	benefits := make(map[string]float64)
	for i := range candidates {
		key := "t1(" + string(rune('a'+i)) + ")"
		benefits[key] = 1.0 - float64(i)*0.1
	}

	ctx := context.Background()

	// 测试启用自适应
	config1 := DefaultGeneticAlgorithmConfig()
	config1.AdaptiveEnabled = true
	config1.MaxGenerations = 30
	ga1 := NewGeneticAlgorithm(config1)
	result1 := ga1.Run(ctx, candidates, benefits)

	// 测试禁用自适应
	config2 := DefaultGeneticAlgorithmConfig()
	config2.AdaptiveEnabled = false
	config2.MaxGenerations = 30
	ga2 := NewGeneticAlgorithm(config2)
	result2 := ga2.Run(ctx, candidates, benefits)

	// 验证两个版本都能产生有效的解
	if len(result1) == 0 {
		t.Error("Adaptive version should produce at least one solution")
	}
	if len(result2) == 0 {
		t.Error("Non-adaptive version should produce at least one solution")
	}

	fmt.Printf("Adaptive: found %d indexes\n", len(result1))
	fmt.Printf("Non-adaptive: found %d indexes\n", len(result2))
}

// TestTournamentSelection 测试锦标赛选择
func TestTournamentSelection(t *testing.T) {
	candidates := make([]*IndexCandidate, 10)
	for i := 0; i < 10; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4 - i/3,
		}
	}

	benefits := make(map[string]float64)
	for i := range candidates {
		key := "t1(" + string(rune('a'+i)) + ")"
		benefits[key] = 1.0 - float64(i)*0.1
	}

	ctx := context.Background()

	// 测试锦标赛选择
	config1 := DefaultGeneticAlgorithmConfig()
	config1.SelectionStrategy = "tournament"
	config1.TournamentSize = 5
	config1.MaxGenerations = 20
	ga1 := NewGeneticAlgorithm(config1)
	result1 := ga1.Run(ctx, candidates, benefits)

	// 测试轮盘赌选择
	config2 := DefaultGeneticAlgorithmConfig()
	config2.SelectionStrategy = "roulette"
	config2.MaxGenerations = 20
	ga2 := NewGeneticAlgorithm(config2)
	result2 := ga2.Run(ctx, candidates, benefits)

	// 验证两个策略都能产生有效的解
	if len(result1) == 0 {
		t.Error("Tournament selection should produce at least one solution")
	}
	if len(result2) == 0 {
		t.Error("Roulette selection should produce at least one solution")
	}

	fmt.Printf("Tournament: found %d indexes\n", len(result1))
	fmt.Printf("Roulette: found %d indexes\n", len(result2))
}

// TestImprovedConvergenceDetection 测试改进的收敛检测
func TestImprovedConvergenceDetection(t *testing.T) {
	candidates := make([]*IndexCandidate, 10)
	for i := 0; i < 10; i++ {
		candidates[i] = &IndexCandidate{
			TableName: "t1",
			Columns:   []string{string(rune('a' + i))},
			Priority:  4 - i/3,
		}
	}

	benefits := make(map[string]float64)
	for i := range candidates {
		key := "t1(" + string(rune('a'+i)) + ")"
		benefits[key] = 1.0 - float64(i)*0.1
	}

	ctx := context.Background()

	config := DefaultGeneticAlgorithmConfig()
	config.AdaptiveEnabled = true
	config.MaxGenerations = 50
	ga := NewGeneticAlgorithm(config)

	result := ga.Run(ctx, candidates, benefits)

	// 验证收敛历史记录
	if len(ga.convergenceHistory) == 0 {
		t.Error("Convergence history should not be empty")
	}
	if len(ga.bestFitnessHistory) == 0 {
		t.Error("Best fitness history should not be empty")
	}
	if len(ga.avgFitnessHistory) == 0 {
		t.Error("Average fitness history should not be empty")
	}

	// 验证解的有效性
	if len(result) == 0 {
		t.Error("Should produce at least one solution")
	}

	fmt.Printf("Converged in %d generations\n", len(ga.convergenceHistory))
	fmt.Printf("Final convergence metric: %.6f\n", ga.convergenceHistory[len(ga.convergenceHistory)-1])
	fmt.Printf("Found %d indexes\n", len(result))
}

// TestConvergenceMetricsCalculation 测试收敛指标计算
func TestConvergenceMetricsCalculation(t *testing.T) {
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

	ga := NewGeneticAlgorithm(nil)
	pop := ga.InitializePopulation(candidates, benefits)

	// 计算收敛指标
	convergenceMetric, changeRate, maxFitness := ga.CalculateConvergenceMetrics(pop)

	// 验证返回值
	if convergenceMetric < 0 {
		t.Error("Convergence metric should be non-negative")
	}
	if maxFitness <= 0 {
		t.Error("Max fitness should be positive")
	}

	// 再次计算，验证历史记录
	convergenceMetric2, _, _ := ga.CalculateConvergenceMetrics(pop)
	if len(ga.convergenceHistory) != 2 {
		t.Errorf("Expected 2 convergence history entries, got %d", len(ga.convergenceHistory))
	}

	fmt.Printf("Convergence metric 1: %.4f\n", convergenceMetric)
	fmt.Printf("Convergence metric 2: %.4f\n", convergenceMetric2)
	fmt.Printf("Change rate: %.6f\n", changeRate)
	fmt.Printf("Max fitness: %.4f\n", maxFitness)
}

// TestAdaptiveParameterAdjustment 测试自适应参数调整逻辑
func TestAdaptiveParameterAdjustment(t *testing.T) {
	ga := NewGeneticAlgorithm(nil)
	ga.AdaptiveEnabled = true
	ga.windowSize = 5

	// 模拟历史记录，使种群处于已收敛状态（变化率很小）
	for i := 0; i < 10; i++ {
		ga.avgFitnessHistory = append(ga.avgFitnessHistory, 0.995)
		ga.bestFitnessHistory = append(ga.bestFitnessHistory, 1.0)
	}

	// 计算收敛指标
	convergenceMetric, changeRate, _ := ga.CalculateConvergenceMetrics(&Population{
		Individuals: []*Individual{{Fitness: 1.0}, {Fitness: 0.995}, {Fitness: 0.995}},
	})

	// 应用自适应调整 - 传递收敛指标和变化率
	ga.AdaptParameters(20, convergenceMetric, changeRate)

	// 验证参数调整：当收敛且变化率小时，应该提高变异率以引入多样性
	if ga.MutationRate < 0.15 {
		t.Errorf("Expected higher mutation rate when converged, got %.3f", ga.MutationRate)
	}
	// 当收敛时，应该降低交叉率以增加探索
	if ga.CrossoverRate > 0.75 {
		t.Errorf("Expected lower crossover rate when converged, got %.3f", ga.CrossoverRate)
	}

	fmt.Printf("Adjusted MutationRate: %.3f\n", ga.MutationRate)
	fmt.Printf("Adjusted CrossoverRate: %.3f\n", ga.CrossoverRate)
}
