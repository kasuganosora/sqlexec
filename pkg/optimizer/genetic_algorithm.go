package optimizer

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// GeneticAlgorithm 遗传算法，用于搜索最优索引组合
type GeneticAlgorithm struct {
	PopulationSize int
	MaxGenerations int
	MutationRate    float64
	CrossoverRate   float64

	// 候选索引
	candidates []*IndexCandidate
	// 索引收益映射
	benefits map[string]float64
	// 约束条件
	maxIndexes   int
	maxTotalSize int64
	maxColumns   int

	// 随机数生成器
	rng *rand.Rand
	mu  sync.Mutex

	// 自适应参数控制
	AdaptiveEnabled    bool
	SelectionStrategy  string // "roulette" or "tournament"
	TournamentSize     int

	// 收敛状态跟踪
	convergenceHistory []float64  // 历史收敛指标
	bestFitnessHistory []float64  // 历史最优适应度
	avgFitnessHistory  []float64  // 历史平均适应度
	windowSize         int         // 滑动窗口大小
}

// NewGeneticAlgorithm 创建遗传算法实例
func NewGeneticAlgorithm(config *GeneticAlgorithmConfig) *GeneticAlgorithm {
	if config == nil {
		config = DefaultGeneticAlgorithmConfig()
	}

	return &GeneticAlgorithm{
		PopulationSize: config.PopulationSize,
		MaxGenerations: config.MaxGenerations,
		MutationRate:    config.MutationRate,
		CrossoverRate:   config.CrossoverRate,
		maxIndexes:      config.MaxIndexes,
		maxTotalSize:    config.MaxTotalSize,
		maxColumns:      config.MaxColumns,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		AdaptiveEnabled: config.AdaptiveEnabled,
		SelectionStrategy: config.SelectionStrategy,
		TournamentSize:  config.TournamentSize,
		windowSize:      5, // 默认滑动窗口大小为 5
		convergenceHistory: make([]float64, 0),
		bestFitnessHistory: make([]float64, 0),
		avgFitnessHistory:  make([]float64, 0),
	}
}

// GeneticAlgorithmConfig 遗传算法配置
type GeneticAlgorithmConfig struct {
	PopulationSize int
	MaxGenerations int
	MutationRate   float64
	CrossoverRate  float64
	MaxIndexes     int
	MaxTotalSize   int64
	MaxColumns     int
	AdaptiveEnabled   bool   // 是否启用自适应参数
	SelectionStrategy string // 选择策略: "roulette" or "tournament"
	TournamentSize    int    // 锦标赛选择的大小
}

// DefaultGeneticAlgorithmConfig 返回默认配置
func DefaultGeneticAlgorithmConfig() *GeneticAlgorithmConfig {
	return &GeneticAlgorithmConfig{
		PopulationSize: 50,
		MaxGenerations: 100,
		MutationRate:   0.1,
		CrossoverRate:  0.8,
		MaxIndexes:     5,
		MaxTotalSize:   100 * 1024 * 1024, // 100MB
		MaxColumns:     3,
		AdaptiveEnabled:   true,
		SelectionStrategy: "roulette",
		TournamentSize:    5,
	}
}

// Individual 个体（候选解）
type Individual struct {
	Genes   []bool  // 每个基因对应一个候选索引，true=选中
	Fitness float64 // 适应度
}

// Population 种群
type Population struct {
	Individuals []*Individual
}

// InitializePopulation 初始化种群
func (ga *GeneticAlgorithm) InitializePopulation(candidates []*IndexCandidate, benefits map[string]float64) *Population {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	ga.candidates = candidates
	ga.benefits = benefits

	pop := &Population{
		Individuals: make([]*Individual, 0, ga.PopulationSize),
	}

	// 生成初始个体
	for i := 0; i < ga.PopulationSize; i++ {
		individual := ga.createIndividual()
		pop.Individuals = append(pop.Individuals, individual)
	}

	// 计算适应度
	for _, ind := range pop.Individuals {
		ind.Fitness = ga.calculateFitness(ind)
	}

	return pop
}

// createIndividual 创建一个随机个体
func (ga *GeneticAlgorithm) createIndividual() *Individual {
	numCandidates := len(ga.candidates)
	individual := &Individual{
		Genes: make([]bool, numCandidates),
	}

	// 随机选择约 30% 的索引
	for i := 0; i < numCandidates; i++ {
		if ga.rng.Float64() < 0.3 {
			individual.Genes[i] = true
		}
	}

	return individual
}

// calculateFitness 计算个体的适应度
// 适应度 = 总收益 - 惩罚项
func (ga *GeneticAlgorithm) calculateFitness(individual *Individual) float64 {
	totalBenefit := 0.0

	// 计算总收益
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			candidate := ga.candidates[i]
			key := fmt.Sprintf("%s(%s)", candidate.TableName, strings.Join(candidate.Columns, ","))

			// 查找对应的收益
			benefit := 0.0
			if val, ok := ga.benefits[key]; ok {
				benefit = val
			} else {
				// 如果没有明确的收益映射，基于优先级估算
				benefit = float64(candidate.Priority) * 0.01
			}

			totalBenefit += benefit
		}
	}

	// 惩罚项
	penalty := 0.0

	// 检查约束
	if !ga.checkConstraints(individual) {
		penalty += 1000.0 // 大惩罚
	}

	return totalBenefit - penalty
}

// checkConstraints 检查约束条件
func (ga *GeneticAlgorithm) checkConstraints(individual *Individual) bool {
	selectedCount := 0

	// 检查索引数量
	for _, gene := range individual.Genes {
		if gene {
			selectedCount++
		}
	}

	if selectedCount > ga.maxIndexes {
		return false
	}

	// 检查列数
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			if len(ga.candidates[i].Columns) > ga.maxColumns {
				return false
			}
		}
	}

	return true
}

// Select 选择操作（支持多种策略 + 精英保留）
func (ga *GeneticAlgorithm) Select(pop *Population, eliteCount int) *Population {
	// 按适应度排序
	sorted := make([]*Individual, len(pop.Individuals))
	copy(sorted, pop.Individuals)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Fitness > sorted[j].Fitness
	})

	// 保留精英
	newPop := &Population{
		Individuals: make([]*Individual, 0, ga.PopulationSize),
	}

	for i := 0; i < eliteCount && i < len(sorted); i++ {
		newPop.Individuals = append(newPop.Individuals, sorted[i])
	}

	// 根据配置的选择策略选择剩余个体
	for i := eliteCount; i < ga.PopulationSize; i++ {
		var selected *Individual
		if ga.SelectionStrategy == "tournament" {
			selected = ga.tournamentSelect(sorted)
		} else {
			// 默认使用轮盘赌
			selected = ga.rouletteSelect(sorted)
		}
		newPop.Individuals = append(newPop.Individuals, ga.cloneIndividual(selected))
	}

	return newPop
}

// rouletteSelect 轮盘赌选择
func (ga *GeneticAlgorithm) rouletteSelect(sorted []*Individual) *Individual {
	// 计算总适应度
	totalFitness := 0.0
	for _, ind := range sorted {
		totalFitness += ind.Fitness
	}

	if totalFitness <= 0 {
		// 如果所有适应度为 0，随机选择
		return sorted[ga.rng.Intn(len(sorted))]
	}

	// 轮盘赌
	r := ga.rng.Float64() * totalFitness
	accumulate := 0.0

	for _, ind := range sorted {
		accumulate += ind.Fitness
		if accumulate >= r {
			return ind
		}
	}

	return sorted[len(sorted)-1]
}

// cloneIndividual 克隆个体
func (ga *GeneticAlgorithm) cloneIndividual(ind *Individual) *Individual {
	cloned := &Individual{
		Genes:   make([]bool, len(ind.Genes)),
		Fitness: ind.Fitness,
	}
	copy(cloned.Genes, ind.Genes)
	return cloned
}

// Crossover 交叉操作（单点交叉）
func (ga *GeneticAlgorithm) Crossover(parent1, parent2 *Individual) (*Individual, *Individual) {
	if ga.rng.Float64() > ga.CrossoverRate || len(parent1.Genes) == 0 {
		// 不交叉，直接返回克隆
		return ga.cloneIndividual(parent1), ga.cloneIndividual(parent2)
	}

	// 单点交叉
	crossoverPoint := ga.rng.Intn(len(parent1.Genes))

	child1 := &Individual{
		Genes: make([]bool, len(parent1.Genes)),
	}
	child2 := &Individual{
		Genes: make([]bool, len(parent2.Genes)),
	}

	// 交叉
	copy(child1.Genes[:crossoverPoint], parent1.Genes[:crossoverPoint])
	copy(child1.Genes[crossoverPoint:], parent2.Genes[crossoverPoint:])

	copy(child2.Genes[:crossoverPoint], parent2.Genes[:crossoverPoint])
	copy(child2.Genes[crossoverPoint:], parent1.Genes[crossoverPoint:])

	return child1, child2
}

// Mutate 变异操作（随机翻转）
func (ga *GeneticAlgorithm) Mutate(individual *Individual) *Individual {
	mutated := ga.cloneIndividual(individual)

	for i := range mutated.Genes {
		if ga.rng.Float64() < ga.MutationRate {
			mutated.Genes[i] = !mutated.Genes[i]
		}
	}

	return mutated
}

// IsConverged 检查种群是否收敛
func (ga *GeneticAlgorithm) IsConverged(pop *Population, threshold float64) bool {
	if len(pop.Individuals) < 2 {
		return true
	}

	// 计算平均适应度
	totalFitness := 0.0
	for _, ind := range pop.Individuals {
		totalFitness += ind.Fitness
	}
	avgFitness := totalFitness / float64(len(pop.Individuals))

	// 计算最大适应度
	maxFitness := pop.Individuals[0].Fitness
	for _, ind := range pop.Individuals {
		if ind.Fitness > maxFitness {
			maxFitness = ind.Fitness
		}
	}

	// 如果最大和平均的差异小于或等于阈值，认为收敛
	if maxFitness > 0 {
		diff := (maxFitness - avgFitness) / maxFitness
		return diff <= threshold
	}

	return true
}

// CalculateConvergenceMetrics 计算收敛指标（使用滑动窗口）
func (ga *GeneticAlgorithm) CalculateConvergenceMetrics(pop *Population) (float64, float64, float64) {
	if len(pop.Individuals) == 0 {
		return 0, 0, 0
	}

	// 计算当前代的平均适应度和最大适应度
	totalFitness := 0.0
	maxFitness := 0.0
	for _, ind := range pop.Individuals {
		totalFitness += ind.Fitness
		if ind.Fitness > maxFitness {
			maxFitness = ind.Fitness
		}
	}
	avgFitness := totalFitness / float64(len(pop.Individuals))

	// 更新历史记录
	ga.bestFitnessHistory = append(ga.bestFitnessHistory, maxFitness)
	ga.avgFitnessHistory = append(ga.avgFitnessHistory, avgFitness)

	// 计算滑动窗口内的平均适应度变化率
	windowSize := ga.windowSize
	if len(ga.avgFitnessHistory) < windowSize {
		windowSize = len(ga.avgFitnessHistory)
	}

	var avgChangeRate float64
	if windowSize >= 2 {
		changes := 0.0
		for i := windowSize - 1; i > 0; i-- {
			curr := ga.avgFitnessHistory[len(ga.avgFitnessHistory)-i]
			prev := ga.avgFitnessHistory[len(ga.avgFitnessHistory)-i-1]
			if prev > 0 {
				changes += (curr - prev) / prev
			}
		}
		avgChangeRate = changes / float64(windowSize-1)
	}

	// 计算当前收敛指标：最大适应度与平均适应度的差异
	var convergenceMetric float64
	if maxFitness > 0 {
		convergenceMetric = (maxFitness - avgFitness) / maxFitness
	}

	ga.convergenceHistory = append(ga.convergenceHistory, convergenceMetric)

	return convergenceMetric, avgChangeRate, maxFitness
}

// AdaptParameters 根据收敛状态自适应调整参数
func (ga *GeneticAlgorithm) AdaptParameters(generation int, convergenceMetric, changeRate float64) {
	if !ga.AdaptiveEnabled {
		return
	}

	// 如果适应度变化率很小，说明种群可能陷入局部最优
	if ga.windowSize < 5 {
		return
	}

	// 根据收敛指标和变化率调整参数
	// 策略1: 当收敛指标很低（种群已经收敛）且变化率也很小时，增加变异率以引入新的多样性
	if convergenceMetric < 0.01 && changeRate < 0.001 {
		ga.MutationRate = 0.2 // 提高变异率
		ga.CrossoverRate = 0.7 // 降低交叉率，增加探索
	} else if convergenceMetric > 0.1 {
		// 策略2: 当种群多样性很大时，提高交叉率，降低变异率以加速收敛
		ga.MutationRate = 0.05
		ga.CrossoverRate = 0.9
	} else {
		// 策略3: 中等状态，使用默认参数
		ga.MutationRate = 0.1
		ga.CrossoverRate = 0.8
	}
}

// tournamentSelect 锦标赛选择
func (ga *GeneticAlgorithm) tournamentSelect(pop []*Individual) *Individual {
	if len(pop) == 0 {
		return nil
	}

	// 确保锦标赛大小不超过种群大小
	tournamentSize := ga.TournamentSize
	if tournamentSize > len(pop) {
		tournamentSize = len(pop)
	}

	// 随机选择 tournamentSize 个个体
	best := pop[ga.rng.Intn(len(pop))]
	for i := 1; i < tournamentSize; i++ {
		competitor := pop[ga.rng.Intn(len(pop))]
		if competitor.Fitness > best.Fitness {
			best = competitor
		}
	}

	return best
}

// GetBestIndividual 获取最优个体
func (ga *GeneticAlgorithm) GetBestIndividual(pop *Population) *Individual {
	if len(pop.Individuals) == 0 {
		return nil
	}

	best := pop.Individuals[0]
	for _, ind := range pop.Individuals {
		if ind.Fitness > best.Fitness {
			best = ind
		}
	}

	return best
}

// ExtractSolution 从最优个体提取索引集合
func (ga *GeneticAlgorithm) ExtractSolution(individual *Individual) []*IndexCandidate {
	if individual == nil {
		return nil
	}

	var selected []*IndexCandidate
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			selected = append(selected, ga.candidates[i])
		}
	}

	return selected
}

// GetConvergenceHistory 返回收敛历史记录
func (ga *GeneticAlgorithm) GetConvergenceHistory() []float64 {
	return ga.convergenceHistory
}

// GetBestFitnessHistory 返回最优适应度历史记录
func (ga *GeneticAlgorithm) GetBestFitnessHistory() []float64 {
	return ga.bestFitnessHistory
}

// GetAvgFitnessHistory 返回平均适应度历史记录
func (ga *GeneticAlgorithm) GetAvgFitnessHistory() []float64 {
	return ga.avgFitnessHistory
}

// Run 运行遗传算法
func (ga *GeneticAlgorithm) Run(ctx context.Context, candidates []*IndexCandidate, benefits map[string]float64) []*IndexCandidate {
	// 初始化种群
	pop := ga.InitializePopulation(candidates, benefits)

	// 进化循环
	eliteCount := 2 // 保留前2个精英
	convergenceThreshold := 0.01 // 1% 差异认为收敛

	for generation := 0; generation < ga.MaxGenerations; generation++ {
		// 检查上下文取消
		select {
		case <-ctx.Done():
			fmt.Printf("[GeneticAlgorithm] Context cancelled at generation %d\n", generation)
			best := ga.GetBestIndividual(pop)
			return ga.ExtractSolution(best)
		default:
		}

		// 计算收敛指标和变化率
		convergenceMetric, changeRate, _ := ga.CalculateConvergenceMetrics(pop)

		// 自适应参数调整
		if generation >= 5 {
			ga.AdaptParameters(generation, convergenceMetric, changeRate)
		}

		// 选择
		pop = ga.Select(pop, eliteCount)

		// 交叉和变异
		newIndividuals := make([]*Individual, 0, ga.PopulationSize)

		// 保留精英
		for i := 0; i < eliteCount && i < len(pop.Individuals); i++ {
			newIndividuals = append(newIndividuals, pop.Individuals[i])
		}

		// 生成后代
		for i := eliteCount; i < ga.PopulationSize; i += 2 {
			// 随机选择父母
			parent1 := pop.Individuals[ga.rng.Intn(len(pop.Individuals))]
			parent2 := pop.Individuals[ga.rng.Intn(len(pop.Individuals))]

			// 交叉
			child1, child2 := ga.Crossover(parent1, parent2)

			// 变异
			child1 = ga.Mutate(child1)
			child2 = ga.Mutate(child2)

			// 计算适应度
			child1.Fitness = ga.calculateFitness(child1)
			child2.Fitness = ga.calculateFitness(child2)

			newIndividuals = append(newIndividuals, child1, child2)
		}

		// 更新种群
		if len(newIndividuals) < ga.PopulationSize {
			pop.Individuals = newIndividuals
		} else {
			pop.Individuals = newIndividuals[:ga.PopulationSize]
		}

		// 检查收敛（使用改进的收敛判断）
		if generation > 10 && ga.IsConverged(pop, convergenceThreshold) {
			fmt.Printf("[GeneticAlgorithm] Converged at generation %d (metric: %.4f, change rate: %.6f)\n",
				generation, convergenceMetric, changeRate)
			break
		}

		if generation%10 == 0 {
			best := ga.GetBestIndividual(pop)
			fmt.Printf("[GeneticAlgorithm] Generation %d, Best fitness: %.4f, Mutation: %.3f, Crossover: %.3f\n",
				generation, best.Fitness, ga.MutationRate, ga.CrossoverRate)
		}
	}

	// 返回最优解
	best := ga.GetBestIndividual(pop)
	return ga.ExtractSolution(best)
}
