package genetic

import (
	"math/rand"
)

// SelectionOperator 选择算子接口
type SelectionOperator interface {
	Select(population *Population, eliteCount int) *Population
}

// CrossoverOperator 交叉算子接口
type CrossoverOperator interface {
	Crossover(parent1, parent2 *Individual) (*Individual, *Individual)
}

// MutationOperator 变异算子接口
type MutationOperator interface {
	Mutate(individual *Individual) *Individual
}

// DefaultSelectionOperator 默认选择算子实现
type DefaultSelectionOperator struct {
	populationSize    int
	selectionStrategy string
	tournamentSize    int
	rng               *rand.Rand
}

// NewDefaultSelectionOperator 创建默认选择算子
func NewDefaultSelectionOperator(populationSize int, selectionStrategy string, tournamentSize int, rng *rand.Rand) *DefaultSelectionOperator {
	return &DefaultSelectionOperator{
		populationSize:    populationSize,
		selectionStrategy: selectionStrategy,
		tournamentSize:    tournamentSize,
		rng:               rng,
	}
}

// Select 执行选择操作
func (s *DefaultSelectionOperator) Select(pop *Population, eliteCount int) *Population {
	// 按适应度排序
	sorted := make([]*Individual, len(pop.Individuals))
	copy(sorted, pop.Individuals)

	// 简单排序实现（在实际使用中应该有更好的排序算法）
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i].Fitness < sorted[j].Fitness {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// 保留精英
	newPop := &Population{
		Individuals: make([]*Individual, 0, s.populationSize),
	}

	for i := 0; i < eliteCount && i < len(sorted); i++ {
		newPop.Individuals = append(newPop.Individuals, s.cloneIndividual(sorted[i]))
	}

	// 根据配置的选择策略选择剩余个体
	for i := eliteCount; i < s.populationSize; i++ {
		var selected *Individual
		if s.selectionStrategy == "tournament" {
			selected = s.tournamentSelect(sorted)
		} else {
			// 默认使用轮盘赌
			selected = s.rouletteSelect(sorted)
		}
		newPop.Individuals = append(newPop.Individuals, s.cloneIndividual(selected))
	}

	return newPop
}

// rouletteSelect 轮盘赌选择
func (s *DefaultSelectionOperator) rouletteSelect(sorted []*Individual) *Individual {
	// 计算总适应度
	totalFitness := 0.0
	for _, ind := range sorted {
		totalFitness += ind.Fitness
	}

	if totalFitness <= 0 {
		// 如果所有适应度为 0，随机选择
		return sorted[s.rng.Intn(len(sorted))]
	}

	// 轮盘赌
	r := s.rng.Float64() * totalFitness
	accumulate := 0.0

	for _, ind := range sorted {
		accumulate += ind.Fitness
		if accumulate >= r {
			return ind
		}
	}

	return sorted[len(sorted)-1]
}

// tournamentSelect 锦标赛选择
func (s *DefaultSelectionOperator) tournamentSelect(pop []*Individual) *Individual {
	if len(pop) == 0 {
		return nil
	}

	// 确保锦标赛大小不超过种群大小
	tournamentSize := s.tournamentSize
	if tournamentSize > len(pop) {
		tournamentSize = len(pop)
	}

	// 随机选择 tournamentSize 个个体
	best := pop[s.rng.Intn(len(pop))]
	for i := 1; i < tournamentSize; i++ {
		competitor := pop[s.rng.Intn(len(pop))]
		if competitor.Fitness > best.Fitness {
			best = competitor
		}
	}

	return best
}

// cloneIndividual 克隆个体
func (s *DefaultSelectionOperator) cloneIndividual(ind *Individual) *Individual {
	cloned := &Individual{
		Genes:   make([]bool, len(ind.Genes)),
		Fitness: ind.Fitness,
	}
	copy(cloned.Genes, ind.Genes)
	return cloned
}

// DefaultCrossoverOperator 默认交叉算子实现
type DefaultCrossoverOperator struct {
	crossoverRate float64
	rng           *rand.Rand
}

// NewDefaultCrossoverOperator 创建默认交叉算子
func NewDefaultCrossoverOperator(crossoverRate float64, rng *rand.Rand) *DefaultCrossoverOperator {
	return &DefaultCrossoverOperator{
		crossoverRate: crossoverRate,
		rng:           rng,
	}
}

// Crossover 执行交叉操作（单点交叉）
func (c *DefaultCrossoverOperator) Crossover(parent1, parent2 *Individual) (*Individual, *Individual) {
	if c.rng.Float64() > c.crossoverRate || len(parent1.Genes) == 0 {
		// 不交叉，直接返回克隆
		return c.cloneIndividual(parent1), c.cloneIndividual(parent2)
	}

	// 单点交叉
	crossoverPoint := c.rng.Intn(len(parent1.Genes))

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

// cloneIndividual 克隆个体
func (c *DefaultCrossoverOperator) cloneIndividual(ind *Individual) *Individual {
	cloned := &Individual{
		Genes:   make([]bool, len(ind.Genes)),
		Fitness: ind.Fitness,
	}
	copy(cloned.Genes, ind.Genes)
	return cloned
}

// DefaultMutationOperator 默认变异算子实现
type DefaultMutationOperator struct {
	mutationRate float64
	rng          *rand.Rand
}

// NewDefaultMutationOperator 创建默认变异算子
func NewDefaultMutationOperator(mutationRate float64, rng *rand.Rand) *DefaultMutationOperator {
	return &DefaultMutationOperator{
		mutationRate: mutationRate,
		rng:          rng,
	}
}

// Mutate 执行变异操作（随机翻转）
func (m *DefaultMutationOperator) Mutate(individual *Individual) *Individual {
	mutated := m.cloneIndividual(individual)

	for i := range mutated.Genes {
		if m.rng.Float64() < m.mutationRate {
			mutated.Genes[i] = !mutated.Genes[i]
		}
	}

	return mutated
}

// cloneIndividual 克隆个体
func (m *DefaultMutationOperator) cloneIndividual(ind *Individual) *Individual {
	cloned := &Individual{
		Genes:   make([]bool, len(ind.Genes)),
		Fitness: ind.Fitness,
	}
	copy(cloned.Genes, ind.Genes)
	return cloned
}
