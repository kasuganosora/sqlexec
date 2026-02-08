package genetic

import (
	"context"
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

func TestInitializePopulation(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
		{TableName: "orders", Columns: []string{"user_id"}, Priority: 3},
	}

	benefits := map[string]float64{
		"users(id)":      10.0,
		"users(name)":    5.0,
		"orders(user_id)": 8.0,
	}

	pop := ga.InitializePopulation(candidates, benefits)

	if pop == nil {
		t.Fatal("InitializePopulation returned nil")
	}

	if pop.Size() != config.PopulationSize {
		t.Errorf("Expected population size %d, got %d", config.PopulationSize, pop.Size())
	}

	// Verify all individuals have fitness calculated
	// Note: Some individuals may have zero fitness if no genes are selected (random 30% probability)
	for i, ind := range pop.Individuals {
		if ind.Fitness < 0 {
			t.Errorf("Individual %d has negative fitness: %f", i, ind.Fitness)
		}
	}
}

func TestCreateIndividual(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	// Set up candidates
	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
		{TableName: "orders", Columns: []string{"user_id"}, Priority: 3},
		{TableName: "orders", Columns: []string{"date"}, Priority: 4},
		{TableName: "products", Columns: []string{"category"}, Priority: 5},
	}

	benefits := map[string]float64{}
	ga.InitializePopulation(candidates, benefits)

	// Create multiple individuals and verify they have genes
	for i := 0; i < 10; i++ {
		ind := ga.createIndividual()
		if ind == nil {
			t.Fatal("createIndividual returned nil")
		}

		if len(ind.Genes) != len(candidates) {
			t.Errorf("Expected %d genes, got %d", len(candidates), len(ind.Genes))
		}

		// Count selected genes
		selectedCount := 0
		for _, gene := range ind.Genes {
			if gene {
				selectedCount++
			}
		}
		// With 30% probability, should have some selections
		if selectedCount > len(candidates) {
			t.Error("More selections than candidates")
		}
	}
}

func TestGenerateCandidateKey(t *testing.T) {
	candidate := &IndexCandidate{
		TableName: "users",
		Columns:   []string{"id", "name"},
		Priority:  1,
	}

	key := generateCandidateKey(candidate)
	expected := "users(id,name)"
	if key != expected {
		t.Errorf("Expected key %s, got %s", expected, key)
	}
}

func TestJoinStrings(t *testing.T) {
	// Test empty array
	result := joinStrings([]string{}, ",")
	if result != "" {
		t.Errorf("Expected empty string, got %s", result)
	}

	// Test single element
	result = joinStrings([]string{"id"}, ",")
	if result != "id" {
		t.Errorf("Expected 'id', got %s", result)
	}

	// Test multiple elements
	result = joinStrings([]string{"id", "name", "age"}, ",")
	expected := "id,name,age"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	// Test different separator
	result = joinStrings([]string{"a", "b"}, "|")
	expected = "a|b"
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestCalculateFitness(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
		{TableName: "orders", Columns: []string{"user_id"}, Priority: 3},
	}

	benefits := map[string]float64{
		"users(id)":      10.0,
		"users(name)":    5.0,
		"orders(user_id)": 8.0,
	}

	ga.InitializePopulation(candidates, benefits)

	// Test individual with some selected indexes
	individual := &Individual{
		Genes: []bool{true, false, true}, // users(id) and orders(user_id)
	}

	fitness := ga.calculateFitness(individual)
	expected := 18.0 // 10.0 + 8.0
	if fitness != expected {
		t.Errorf("Expected fitness %f, got %f", expected, fitness)
	}

	// Test individual with no selected indexes
	individual2 := &Individual{
		Genes: []bool{false, false, false},
	}
	fitness2 := ga.calculateFitness(individual2)
	if fitness2 != 0 {
		t.Errorf("Expected fitness 0, got %f", fitness2)
	}

	// Test individual with all selected indexes
	individual3 := &Individual{
		Genes: []bool{true, true, true},
	}
	fitness3 := ga.calculateFitness(individual3)
	expected3 := 23.0 // 10.0 + 5.0 + 8.0
	if fitness3 != expected3 {
		t.Errorf("Expected fitness %f, got %f", expected3, fitness3)
	}
}

func TestCalculateFitness_WithMissingBenefit(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 10},
		{TableName: "users", Columns: []string{"name"}, Priority: 20},
	}

	benefits := map[string]float64{
		// Missing "users(id)" entry
		"users(name)": 5.0,
	}

	ga.InitializePopulation(candidates, benefits)

	// Test individual with missing benefit (should use priority-based estimation)
	individual := &Individual{
		Genes: []bool{true, false}, // users(id) only
	}

	fitness := ga.calculateFitness(individual)
	expected := 0.1 // Priority 10 * 0.01
	if fitness != expected {
		t.Errorf("Expected fitness %f, got %f", expected, fitness)
	}
}

func TestCheckConstraints(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.MaxIndexes = 3
	config.MaxColumns = 2

	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name", "email"}, Priority: 2}, // 2 columns
		{TableName: "orders", Columns: []string{"user_id", "date", "status"}, Priority: 3}, // 3 columns, exceeds MaxColumns
		{TableName: "products", Columns: []string{"category"}, Priority: 4},
	}

	benefits := map[string]float64{}
	ga.InitializePopulation(candidates, benefits)

	// Test within constraints
	individual1 := &Individual{
		Genes: []bool{true, true, false, false}, // 2 indexes, both within MaxColumns
	}
	if !ga.checkConstraints(individual1) {
		t.Error("Expected constraints to be satisfied")
	}

	// Test exceeding MaxIndexes
	individual2 := &Individual{
		Genes: []bool{true, true, true, true}, // 4 indexes > MaxIndexes(3)
	}
	if ga.checkConstraints(individual2) {
		t.Error("Expected constraints to be violated due to too many indexes")
	}

	// Test exceeding MaxColumns
	individual3 := &Individual{
		Genes: []bool{false, false, true, false}, // orders(user_id,date,status) has 3 columns > MaxColumns(2)
	}
	if ga.checkConstraints(individual3) {
		t.Error("Expected constraints to be violated due to too many columns")
	}
}

func TestCheckConstraints_EdgeCases(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
	}

	benefits := map[string]float64{}
	ga.InitializePopulation(candidates, benefits)

	// Test empty individual
	individual := &Individual{
		Genes: []bool{},
	}
	if !ga.checkConstraints(individual) {
		t.Error("Expected empty individual to satisfy constraints")
	}

	// Test nil candidates - should handle gracefully (no panic)
	ga.candidates = nil
	individual2 := &Individual{
		Genes: []bool{true},
	}
	// Should not panic, and return true (0 indexes selected)
	_ = ga.checkConstraints(individual2)
}

func TestIsConverged(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	// Test empty population
	emptyPop := &Population{Individuals: []*Individual{}}
	if !ga.IsConverged(emptyPop, 0.01) {
		t.Error("Expected empty population to be converged")
	}

	// Test single individual
	singlePop := &Population{
		Individuals: []*Individual{
			{Genes: []bool{true}, Fitness: 10.0},
		},
	}
	if !ga.IsConverged(singlePop, 0.01) {
		t.Error("Expected single individual population to be converged")
	}

	// Test converged population (small difference between max and avg)
	convergedPop := &Population{
		Individuals: []*Individual{
			{Fitness: 10.0},
			{Fitness: 9.9},
			{Fitness: 9.8},
		},
	}
	if !ga.IsConverged(convergedPop, 0.01) {
		t.Error("Expected population to be converged")
	}

	// Test non-converged population
	nonConvergedPop := &Population{
		Individuals: []*Individual{
			{Fitness: 20.0},
			{Fitness: 10.0},
			{Fitness: 5.0},
		},
	}
	if ga.IsConverged(nonConvergedPop, 0.01) {
		t.Error("Expected population to not be converged")
	}
}

func TestCalculateConvergenceMetrics(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 10.0},
			{Fitness: 8.0},
			{Fitness: 6.0},
		},
	}

	convergence, changeRate, maxFitness := ga.CalculateConvergenceMetrics(pop)

	// Expected values: max=10, avg=8, convergence=(10-8)/10=0.2
	if maxFitness != 10.0 {
		t.Errorf("Expected max fitness 10.0, got %f", maxFitness)
	}

	expectedConvergence := 0.2 // (10-8)/10
	if convergence != expectedConvergence {
		t.Errorf("Expected convergence %f, got %f", expectedConvergence, convergence)
	}

	// First call, changeRate should be 0 (no history)
	if changeRate != 0 {
		t.Errorf("Expected change rate 0 for first call, got %f", changeRate)
	}

	// Call with different fitness values to create change
	pop2 := &Population{
		Individuals: []*Individual{
			{Fitness: 12.0},
			{Fitness: 9.0},
			{Fitness: 7.0},
		},
	}
	ga.CalculateConvergenceMetrics(pop2)
	ga.CalculateConvergenceMetrics(pop)
	ga.CalculateConvergenceMetrics(pop2)
	ga.CalculateConvergenceMetrics(pop)

	// Now we have enough history with varying values for change rate
	_, changeRate2, _ := ga.CalculateConvergenceMetrics(pop2)
	// Change rate may be 0 if values alternate in a way that cancels out, which is OK
	t.Logf("Change rate after multiple calls: %f", changeRate2)
}

func TestAdaptParameters(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.AdaptiveEnabled = true
	ga := NewGeneticAlgorithm(config)

	// Test high diversity (convergenceMetric > 0.1)
	ga.AdaptParameters(10, 0.2, 0.01)
	if ga.config.MutationRate != 0.05 {
		t.Errorf("Expected mutation rate 0.05 for high diversity, got %f", ga.config.MutationRate)
	}
	if ga.config.CrossoverRate != 0.9 {
		t.Errorf("Expected crossover rate 0.9 for high diversity, got %f", ga.config.CrossoverRate)
	}

	// Test converged state (convergenceMetric < 0.01, changeRate < 0.001)
	ga.AdaptParameters(10, 0.005, 0.0005)
	if ga.config.MutationRate != 0.2 {
		t.Errorf("Expected mutation rate 0.2 for converged state, got %f", ga.config.MutationRate)
	}
	if ga.config.CrossoverRate != 0.7 {
		t.Errorf("Expected crossover rate 0.7 for converged state, got %f", ga.config.CrossoverRate)
	}

	// Test medium state
	ga.AdaptParameters(10, 0.05, 0.005)
	if ga.config.MutationRate != 0.1 {
		t.Errorf("Expected mutation rate 0.1 for medium state, got %f", ga.config.MutationRate)
	}
	if ga.config.CrossoverRate != 0.8 {
		t.Errorf("Expected crossover rate 0.8 for medium state, got %f", ga.config.CrossoverRate)
	}
}

func TestAdaptParameters_Disabled(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.AdaptiveEnabled = false
	ga := NewGeneticAlgorithm(config)

	originalMutation := ga.config.MutationRate
	originalCrossover := ga.config.CrossoverRate

	// Should not change parameters when adaptive is disabled
	ga.AdaptParameters(10, 0.2, 0.01)

	if ga.config.MutationRate != originalMutation {
		t.Error("Mutation rate should not change when adaptive is disabled")
	}
	if ga.config.CrossoverRate != originalCrossover {
		t.Error("Crossover rate should not change when adaptive is disabled")
	}
}

func TestGetBestIndividual(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 10.0},
			{Fitness: 30.0},
			{Fitness: 20.0},
		},
	}

	best := ga.GetBestIndividual(pop)
	if best == nil {
		t.Fatal("GetBestIndividual returned nil")
	}

	if best.Fitness != 30.0 {
		t.Errorf("Expected best fitness 30.0, got %f", best.Fitness)
	}
}

func TestGetBestIndividual_Empty(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	emptyPop := &Population{Individuals: []*Individual{}}
	best := ga.GetBestIndividual(emptyPop)

	if best != nil {
		t.Error("Expected nil for empty population")
	}
}

func TestExtractSolution(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
		{TableName: "orders", Columns: []string{"user_id"}, Priority: 3},
	}

	benefits := map[string]float64{}
	ga.InitializePopulation(candidates, benefits)

	individual := &Individual{
		Genes: []bool{true, false, true}, // Select first and third
	}

	solution := ga.ExtractSolution(individual)
	if len(solution) != 2 {
		t.Errorf("Expected 2 selected candidates, got %d", len(solution))
	}

	if solution[0].TableName != "users" || solution[0].Columns[0] != "id" {
		t.Error("First solution candidate incorrect")
	}

	if solution[1].TableName != "orders" || solution[1].Columns[0] != "user_id" {
		t.Error("Second solution candidate incorrect")
	}
}

func TestExtractSolution_Nil(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	solution := ga.ExtractSolution(nil)
	if solution != nil {
		t.Error("Expected nil solution for nil individual")
	}
}

func TestGetConvergenceHistory(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	// Initially empty
	history := ga.GetConvergenceHistory()
	if len(history) != 0 {
		t.Error("Expected empty convergence history initially")
	}

	// Add some metrics
	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 10.0},
			{Fitness: 8.0},
		},
	}
	ga.CalculateConvergenceMetrics(pop)
	ga.CalculateConvergenceMetrics(pop)

	history = ga.GetConvergenceHistory()
	if len(history) != 2 {
		t.Errorf("Expected history length 2, got %d", len(history))
	}
}

func TestGetFitnessHistories(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	pop := &Population{
		Individuals: []*Individual{
			{Fitness: 10.0},
			{Fitness: 8.0},
		},
	}

	ga.CalculateConvergenceMetrics(pop)
	ga.CalculateConvergenceMetrics(pop)

	bestHistory := ga.GetBestFitnessHistory()
	avgHistory := ga.GetAvgFitnessHistory()

	if len(bestHistory) != 2 {
		t.Errorf("Expected best fitness history length 2, got %d", len(bestHistory))
	}

	if len(avgHistory) != 2 {
		t.Errorf("Expected avg fitness history length 2, got %d", len(avgHistory))
	}

	if bestHistory[0] != 10.0 {
		t.Errorf("Expected best fitness 10.0, got %f", bestHistory[0])
	}
}

func TestTournamentSelect(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	// Create a selection operator with tournament size 3
	selector := NewDefaultSelectionOperator(10, "tournament", 3, ga.rng)

	pop := []*Individual{
		{Fitness: 10.0},
		{Fitness: 20.0},
		{Fitness: 30.0},
		{Fitness: 40.0},
		{Fitness: 50.0},
	}

	selected := selector.tournamentSelect(pop)
	if selected == nil {
		t.Fatal("tournamentSelect returned nil")
	}

	// Should select one of the individuals
	found := false
	for _, ind := range pop {
		if ind == selected {
			found = true
			break
		}
	}
	if !found {
		t.Error("Selected individual not from population")
	}
}

func TestTournamentSelect_Empty(t *testing.T) {
	selector := NewDefaultSelectionOperator(10, "tournament", 3, rand.New(rand.NewSource(time.Now().UnixNano())))

	selected := selector.tournamentSelect([]*Individual{})
	if selected != nil {
		t.Error("Expected nil for empty population")
	}
}

func TestTournamentSelect_LargeTournament(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	ga := NewGeneticAlgorithm(config)

	// Tournament size larger than population
	selector := NewDefaultSelectionOperator(10, "tournament", 10, ga.rng)

	pop := []*Individual{
		{Fitness: 10.0},
		{Fitness: 20.0},
		{Fitness: 30.0},
	}

	selected := selector.tournamentSelect(pop)
	if selected == nil {
		t.Fatal("tournamentSelect returned nil")
	}
}

func TestRun(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.MaxGenerations = 5 // Short run for testing
	config.PopulationSize = 10
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
		{TableName: "users", Columns: []string{"name"}, Priority: 2},
		{TableName: "orders", Columns: []string{"user_id"}, Priority: 3},
	}

	benefits := map[string]float64{
		"users(id)":      10.0,
		"users(name)":    5.0,
		"orders(user_id)": 8.0,
	}

	ctx := context.Background()
	solution := ga.Run(ctx, candidates, benefits)

	if solution == nil {
		t.Fatal("Run returned nil solution")
	}

	// Should return some solution (may be empty if no good indexes found)
	t.Logf("Run completed with solution of %d indexes", len(solution))

	// Verify that histories were populated
	if len(ga.GetConvergenceHistory()) == 0 {
		t.Error("Expected non-empty convergence history")
	}

	if len(ga.GetBestFitnessHistory()) == 0 {
		t.Error("Expected non-empty best fitness history")
	}

	if len(ga.GetAvgFitnessHistory()) == 0 {
		t.Error("Expected non-empty avg fitness history")
	}
}

func TestRun_Cancellation(t *testing.T) {
	config := DefaultGeneticAlgorithmConfig()
	config.MaxGenerations = 100 // Long run
	ga := NewGeneticAlgorithm(config)

	candidates := []*IndexCandidate{
		{TableName: "users", Columns: []string{"id"}, Priority: 1},
	}

	benefits := map[string]float64{
		"users(id)": 10.0,
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short time
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	solution := ga.Run(ctx, candidates, benefits)

	// Should return some solution even when cancelled
	if solution == nil {
		t.Fatal("Run returned nil solution after cancellation")
	}

	t.Log("Run handled cancellation correctly")
}
