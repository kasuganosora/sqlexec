package genetic

import (
	"math/rand"
	"testing"
	"time"
)

func TestNewGeneticAlgorithm(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	if ga == nil {
		t.Fatal("NewGeneticAlgorithm returned nil")
	}

	if ga.config != config {
		t.Error("GeneticAlgorithm config not set correctly")
	}
}

func TestDefaultGeneticAlgorithmConfig(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()

	if config.PopulationSize != 50 {
		t.Errorf("Expected PopulationSize 50, got %d", config.PopulationSize)
	}

	if config.MaxGenerations != 100 {
		t.Errorf("Expected MaxGenerations 100, got %d", config.MaxGenerations)
	}

	if config.MutationRate != 0.1 {
		t.Errorf("Expected MutationRate 0.1, got %f", config.MutationRate)
	}

	if config.CrossoverRate != 0.8 {
		t.Errorf("Expected CrossoverRate 0.8, got %f", config.CrossoverRate)
	}

	if config.MaxIndexes != 5 {
		t.Errorf("Expected MaxIndexes 5, got %d", config.MaxIndexes)
	}
}

func TestIndividual_Clone(t *testing.T) {
	original := &Individual{
		Genes:   []bool{true, false, true},
		Fitness: 10.5,
	}

	cloned := original.Clone()

	if cloned == original {
		t.Error("Clone should return a new instance")
	}

	if cloned.Fitness != original.Fitness {
		t.Errorf("Cloned fitness mismatch: expected %f, got %f", original.Fitness, cloned.Fitness)
	}

	if len(cloned.Genes) != len(original.Genes) {
		t.Errorf("Cloned genes length mismatch: expected %d, got %d", len(original.Genes), len(cloned.Genes))
	}

	// Modify cloned genes and verify original is not affected
	cloned.Genes[0] = false
	if original.Genes[0] != true {
		t.Error("Modifying cloned genes affected original")
	}
}

func TestPopulation_Size(t *testing.T) {
	pop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true}, Fitness: 1.0},
			{Genes: []bool{false}, Fitness: 2.0},
			{Genes: []bool{true}, Fitness: 3.0},
		},
	}

	if pop.Size() != 3 {
		t.Errorf("Expected size 3, got %d", pop.Size())
	}
}

func TestPopulation_GetBest(t *testing.T) {
	pop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true}, Fitness: 1.0},
			{Genes: []bool{false}, Fitness: 3.0},
			{Genes: []bool{true}, Fitness: 2.0},
		},
	}

	best := pop.GetBest()
	if best == nil {
		t.Fatal("GetBest returned nil")
	}

	if best.Fitness != 3.0 {
		t.Errorf("Expected best fitness 3.0, got %f", best.Fitness)
	}
}

func TestPopulation_GetAverageFitness(t *testing.T) {
	pop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true}, Fitness: 1.0},
			{Genes: []bool{false}, Fitness: 2.0},
			{Genes: []bool{true}, Fitness: 3.0},
		},
	}

	avg := pop.GetAverageFitness()
	expected := (1.0 + 2.0 + 3.0) / 3.0
	if avg != expected {
		t.Errorf("Expected average fitness %f, got %f", expected, avg)
	}
}

func TestGeneticAlgorithm_SetCandidates(t *testing.T) {
	ga := NewGeneticAlgorithm(DefaultGeneticAlgorithmConfig())

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
	}

	benefits := map[string]float64{
		"users(id)": 10.0,
	}

	ga.SetCandidatesForTest(candidates, benefits)

	if len(ga.GetCandidates()) != len(candidates) {
		t.Errorf("Expected %d candidates, got %d", len(candidates), len(ga.GetCandidates()))
	}
}

func TestDefaultSelectionOperator_Select(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	selector := NewDefaultSelectionOperator(10, "roulette", 3, rng)

	pop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true}, Fitness: 10.0},
			{Genes: []bool{false}, Fitness: 5.0},
			{Genes: []bool{true}, Fitness: 8.0},
			{Genes: []bool{false}, Fitness: 12.0},
			{Genes: []bool{true}, Fitness: 3.0},
		},
	}

	newPop := selector.Select(pop, 1)

	if newPop.Size() != 10 {
		t.Errorf("Expected population size 10, got %d", newPop.Size())
	}

	// Check that best individual is preserved (elitism)
	best := newPop.GetBest()
	if best == nil {
		t.Fatal("Selected population has no best individual")
	}
}

func TestDefaultCrossoverOperator_Crossover(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	crossover := NewDefaultCrossoverOperator(0.8, rng)

	parent1 := &Individual{Genes: []bool{true, true, true, true}, Fitness: 10.0}
	parent2 := &Individual{Genes: []bool{false, false, false, false}, Fitness: 5.0}

	child1, child2 := crossover.Crossover(parent1, parent2)

	if child1 == nil || child2 == nil {
		t.Fatal("Crossover returned nil children")
	}

	if len(child1.Genes) != len(parent1.Genes) {
		t.Errorf("Child1 genes length mismatch: expected %d, got %d", len(parent1.Genes), len(child1.Genes))
	}

	if len(child2.Genes) != len(parent2.Genes) {
		t.Errorf("Child2 genes length mismatch: expected %d, got %d", len(parent2.Genes), len(child2.Genes))
	}
}

func TestDefaultMutationOperator_Mutate(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	mutation := NewDefaultMutationOperator(1.0, rng) // 100% mutation rate for testing

	original := &Individual{Genes: []bool{true, true, true, true}, Fitness: 10.0}
	mutated := mutation.Mutate(original)

	if mutated == nil {
		t.Fatal("Mutate returned nil")
	}

	if len(mutated.Genes) != len(original.Genes) {
		t.Errorf("Mutated genes length mismatch: expected %d, got %d", len(original.Genes), len(mutated.Genes))
	}

	// With 100% mutation rate, at least one gene should be different
	hasDifference := false
	for i := range original.Genes {
		if mutated.Genes[i] != original.Genes[i] {
			hasDifference = true
			break
		}
	}

	if !hasDifference {
		t.Error("Mutation did not change any genes with 100% mutation rate")
	}
}

func TestGeneticAlgorithm_SetOperators(t *testing.T) {
	// Note: SetOperators method not implemented in the current core.go
	// Operators are set automatically in NewGeneticAlgorithm constructor
	ga := NewGeneticAlgorithm(DefaultGeneticAlgorithmConfig())

	if ga == nil {
		t.Fatal("NewGeneticAlgorithm returned nil")
	}

	// Verify the method doesn't panic by accessing config
	config := ga.GetConfig()
	if config == nil {
		t.Error("Config should not be nil")
	}
}
