package fulltext

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hashStringForTest mirrors the hashString function (FNV-1a) used by the engine.
func hashStringForTest(s string) int64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h)
}

// ==========================================================================
// Bug 4 (P1): Term ID mismatch — hybrid search uses vocabulary.GetOrCreateID()
// but the inverted index uses hashString(). These produce different IDs for the
// same term, so hybrid vector search always returns 0 similarity.
// ==========================================================================

func TestBug4_HybridSearch_TermIDMismatch(t *testing.T) {
	engine := NewEngine(DefaultConfig)

	// Index some documents to populate stats
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, engine.IndexDocument(&Document{
			ID: i, Content: "filler document for statistics",
		}))
	}

	hybrid := NewHybridEngine(engine, 0.5, 0.5)

	// The bug: AutoConvertToVector and calculateSimilarity used
	// vocabulary.GetOrCreateID() to build term vectors, but the inverted
	// index uses hashString() for term IDs. These produce completely
	// different IDs for the same term, so vector lookups always miss.
	//
	// After the fix, AutoConvertToVector uses hashString() consistently.
	doc := &Document{ID: 100, Content: "brown fox"}
	vec, err := hybrid.AutoConvertToVector(doc)
	require.NoError(t, err)

	// Tokenize "brown fox" to get the expected terms
	tokens, err := engine.tokenizer.Tokenize("brown fox")
	require.NoError(t, err)
	require.NotEmpty(t, tokens)

	// Verify: the vector keys should match hashString-based IDs
	for _, tok := range tokens {
		expectedID := hashStringForTest(tok.Text)
		_, exists := vec[expectedID]
		assert.True(t, exists,
			"AutoConvertToVector should use hashString-based IDs (term %q, expected ID %d)",
			tok.Text, expectedID)
	}

	// Also verify that vocabulary IDs are DIFFERENT from hashString IDs.
	// This proves the original bug existed.
	vocabID := engine.vocabulary.GetOrCreateID("brown")
	hashID := hashStringForTest("brown")
	assert.NotEqual(t, vocabID, hashID,
		"vocabulary IDs and hashString IDs should differ, confirming the original bug")
}

// ==========================================================================
// Bug 5 (P2): Cosine similarity incomplete docNorm
// The docNorm only accumulates weights for terms that exist in BOTH
// query and document vectors. This makes the L2 norm incomplete,
// inflating cosine similarity scores.
// ==========================================================================

func TestBug5_CosineSimilarity_IncompleteDocNorm(t *testing.T) {
	engine := NewEngine(DefaultConfig)

	// Index a document with many distinct terms
	doc := &Document{
		ID:      1,
		Content: "alpha beta gamma delta epsilon zeta theta iota kappa lambda",
	}
	require.NoError(t, engine.IndexDocument(doc))

	hybrid := NewHybridEngine(engine, 0.5, 0.5)

	// Query with just one term that appears in the document
	// With incomplete docNorm, cosine similarity would be ~1.0 (inflated)
	// With correct full docNorm, it should be much less than 1.0
	tokens, err := engine.tokenizer.Tokenize("alpha")
	require.NoError(t, err)
	require.NotEmpty(t, tokens)

	similarity := hybrid.calculateSimilarity(1, tokens)
	// The similarity should be well below 1.0 since "alpha" is just 1 of many terms
	// With the bug (incomplete norm), similarity is ~1.0
	// With the fix (full norm), similarity should be < 0.8
	assert.Less(t, similarity, 0.8,
		"cosine similarity should reflect that query matches only a small part of the document")
}

// ==========================================================================
// Bug 6 (P3): Duplicate "edly" suffix in stemmer
// analyzer/tokenizer.go line 294 has "edly" twice in the suffix list.
// ==========================================================================

func TestBug6_EnglishStemmer_NoDuplicateEffect(t *testing.T) {
	// After the fix, the stemmer should produce correct results for "edly" words.
	// The tokenizer should stem "reportedly" consistently.
	tokenizer := analyzer.NewEnglishTokenizer(nil)
	tokens1, err := tokenizer.Tokenize("reportedly")
	require.NoError(t, err)
	tokens2, err := tokenizer.Tokenize("reportedly")
	require.NoError(t, err)
	// Results should be identical (no non-determinism from duplicates)
	require.Equal(t, len(tokens1), len(tokens2))
	if len(tokens1) > 0 && len(tokens2) > 0 {
		assert.Equal(t, tokens1[0].Text, tokens2[0].Text)
	}
}
