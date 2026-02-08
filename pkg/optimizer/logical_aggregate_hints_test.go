package optimizer

import (
	"testing"
)

func TestLogicalAggregateAlgorithm(t *testing.T) {
	tests := []struct {
		name               string
		algorithm          AggregationAlgorithm
		expectedAlgorithm  AggregationAlgorithm
	}{
		{"default algorithm", 0, HashAggAlgorithm},
		{"hash aggregation", HashAggAlgorithm, HashAggAlgorithm},
		{"stream aggregation", StreamAggAlgorithm, StreamAggAlgorithm},
		{"MPP 1 phase", MPP1PhaseAggAlgorithm, MPP1PhaseAggAlgorithm},
		{"MPP 2 phase", MPP2PhaseAggAlgorithm, MPP2PhaseAggAlgorithm},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &LogicalAggregate{
				algorithm: tt.algorithm,
			}
			result := agg.Algorithm()
			if result != tt.expectedAlgorithm {
				t.Errorf("Algorithm() = %v, expected %v", result, tt.expectedAlgorithm)
			}
		})
	}
}

func TestLogicalAggregateSetAlgorithm(t *testing.T) {
	agg := &LogicalAggregate{}

	algos := []AggregationAlgorithm{
		HashAggAlgorithm,
		StreamAggAlgorithm,
		MPP1PhaseAggAlgorithm,
		MPP2PhaseAggAlgorithm,
	}

	for _, algo := range algos {
		agg.SetAlgorithm(algo)
		if agg.Algorithm() != algo {
			t.Errorf("SetAlgorithm(%v) failed", algo)
		}
	}
}

func TestLogicalAggregateSetHintApplied(t *testing.T) {
	agg := &LogicalAggregate{}

	hints := []string{"HASH_AGG", "STREAM_AGG", "MPP_1PHASE_AGG"}

	for _, hint := range hints {
		agg.SetHintApplied(hint)
	}

	appliedHints := agg.GetAppliedHints()
	if len(appliedHints) != len(hints) {
		t.Errorf("Expected %d hints, got %d", len(hints), len(appliedHints))
	}

	for i, hint := range hints {
		if appliedHints[i] != hint {
			t.Errorf("Expected hint %s at index %d, got %s", hint, i, appliedHints[i])
		}
	}
}

func TestLogicalAggregateGetAppliedHints(t *testing.T) {
	t.Run("no hints applied", func(t *testing.T) {
		agg := &LogicalAggregate{}
		hints := agg.GetAppliedHints()
		if hints != nil {
			t.Errorf("Expected nil for uninitialised hints, got %v", hints)
		}
	})

	t.Run("with hints", func(t *testing.T) {
		agg := &LogicalAggregate{}
		agg.SetHintApplied("HASH_AGG")
		agg.SetHintApplied("STREAM_AGG")

		hints := agg.GetAppliedHints()
		if len(hints) != 2 {
			t.Errorf("Expected 2 hints, got %d", len(hints))
		}
	})
}
