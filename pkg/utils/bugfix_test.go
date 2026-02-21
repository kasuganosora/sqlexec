package utils

import (
	"errors"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// ==========================================================================
// Bug 11 (P1): MatchesAnySubFilter silently swallows errors
// The comment says "Returns the first error encountered" but errors are
// silently discarded with `continue`, and the function returns (false, nil).
// This is inconsistent with MatchesAllSubFilters which properly returns
// errors. Callers cannot distinguish "no match" from "evaluation error".
// ==========================================================================

func TestBug11_MatchesAnySubFilter_PropagatesError(t *testing.T) {
	row := domain.Row{"name": "alice"}

	// Create a filter with an unsupported operator that will cause an error
	subFilters := []domain.Filter{
		{Field: "name", Operator: "UNSUPPORTED_OP", Value: "alice"},
	}

	_, err := MatchesAnySubFilter(row, subFilters)
	assert.Error(t, err,
		"MatchesAnySubFilter should propagate errors, not swallow them")
}

func TestBug11_MatchesAnySubFilter_ErrorWithMatch(t *testing.T) {
	row := domain.Row{"name": "alice", "age": 30}

	// First filter will error, second will match
	subFilters := []domain.Filter{
		{Field: "name", Operator: "UNSUPPORTED_OP", Value: "alice"},
		{Field: "age", Operator: "=", Value: 30},
	}

	// Even with an error on one filter, a match on another should return true
	matched, _ := MatchesAnySubFilter(row, subFilters)
	assert.True(t, matched,
		"should return true when at least one filter matches (OR logic)")
}

// ==========================================================================
// Bug 12 (P1): MapErrorCode returns error code for nil error
// When err is nil, MapErrorCode returns (ErrParseError, SqlStateSyntaxError)
// instead of a success indicator. A nil error should not be mapped to an
// error code — it should return (0, "00000") for success.
// ==========================================================================

func TestBug12_MapErrorCode_NilError(t *testing.T) {
	code, state := MapErrorCode(nil)

	// nil error should not return an error code
	assert.NotEqual(t, uint16(ErrParseError), code,
		"nil error should not return ErrParseError")
	assert.Equal(t, uint16(0), code,
		"nil error should return code 0 (success)")
	assert.Equal(t, "00000", state,
		"nil error should return SQL state 00000 (success)")
}

func TestBug12_MapErrorCode_RealError(t *testing.T) {
	// Verify that real errors still map correctly
	code, state := MapErrorCode(errors.New("table not found"))
	assert.Equal(t, uint16(ErrNoSuchTable), code)
	assert.Equal(t, SqlStateNoSuchTable, state)
}
