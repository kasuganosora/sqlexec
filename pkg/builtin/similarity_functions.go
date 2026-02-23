package builtin

import (
	"fmt"
	"math"
)

func init() {
	similarityFunctions := []*FunctionInfo{
		{
			Name: "levenshtein",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "levenshtein", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityLevenshtein,
			Description: "Compute Levenshtein edit distance between two strings",
			Example:     "LEVENSHTEIN('kitten', 'sitting') -> 3",
			Category:    "similarity",
		},
		{
			Name: "damerau_levenshtein",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "damerau_levenshtein", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityDamerauLevenshtein,
			Description: "Compute Damerau-Levenshtein distance (with transpositions)",
			Example:     "DAMERAU_LEVENSHTEIN('ca', 'abc') -> 2",
			Category:    "similarity",
		},
		{
			Name: "hamming",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "hamming", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityHamming,
			Description: "Compute Hamming distance (strings must be equal length)",
			Example:     "HAMMING('karolin', 'kathrin') -> 3",
			Category:    "similarity",
		},
		{
			Name: "jaccard",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "jaccard", ReturnType: "float", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityJaccard,
			Description: "Compute bigram Jaccard similarity coefficient",
			Example:     "JACCARD('night', 'nacht') -> 0.25",
			Category:    "similarity",
		},
		{
			Name: "jaro_similarity",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "jaro_similarity", ReturnType: "float", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityJaro,
			Description: "Compute Jaro similarity between two strings",
			Example:     "JARO_SIMILARITY('martha', 'marhta') -> 0.944",
			Category:    "similarity",
		},
		{
			Name: "jaro_winkler_similarity",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "jaro_winkler_similarity", ReturnType: "float", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     similarityJaroWinkler,
			Description: "Compute Jaro-Winkler similarity with prefix bonus",
			Example:     "JARO_WINKLER_SIMILARITY('martha', 'marhta') -> 0.961",
			Category:    "similarity",
		},
	}

	for _, fn := range similarityFunctions {
		RegisterGlobal(fn)
	}
}

// levenshtein: Wagner-Fischer dynamic programming algorithm
func similarityLevenshtein(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("levenshtein() requires exactly 2 arguments")
	}
	a := []rune(toString(args[0]))
	b := []rune(toString(args[1]))

	la, lb := len(a), len(b)
	if la == 0 {
		return int64(lb), nil
	}
	if lb == 0 {
		return int64(la), nil
	}

	// Use two rows to save memory
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = minInt(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}

	return int64(prev[lb]), nil
}

// damerau_levenshtein: DP with transposition
func similarityDamerauLevenshtein(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("damerau_levenshtein() requires exactly 2 arguments")
	}
	a := []rune(toString(args[0]))
	b := []rune(toString(args[1]))

	la, lb := len(a), len(b)
	if la == 0 {
		return int64(lb), nil
	}
	if lb == 0 {
		return int64(la), nil
	}

	// Full matrix needed for transposition look-back
	d := make([][]int, la+1)
	for i := range d {
		d[i] = make([]int, lb+1)
	}

	for i := 0; i <= la; i++ {
		d[i][0] = i
	}
	for j := 0; j <= lb; j++ {
		d[0][j] = j
	}

	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			d[i][j] = minInt(
				d[i-1][j]+1,      // deletion
				d[i][j-1]+1,      // insertion
				d[i-1][j-1]+cost, // substitution
			)
			// transposition
			if i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				d[i][j] = minInt(d[i][j], d[i-2][j-2]+cost)
			}
		}
	}

	return int64(d[la][lb]), nil
}

// hamming: count differing positions (error if different lengths)
func similarityHamming(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("hamming() requires exactly 2 arguments")
	}
	a := []rune(toString(args[0]))
	b := []rune(toString(args[1]))

	if len(a) != len(b) {
		return nil, fmt.Errorf("hamming() requires strings of equal length, got %d and %d", len(a), len(b))
	}

	var dist int64
	for i := range a {
		if a[i] != b[i] {
			dist++
		}
	}
	return dist, nil
}

// jaccard: bigram set Jaccard coefficient |A∩B|/|A∪B|
func similarityJaccard(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("jaccard() requires exactly 2 arguments")
	}
	a := []rune(toString(args[0]))
	b := []rune(toString(args[1]))

	bigramsA := bigrams(a)
	bigramsB := bigrams(b)

	if len(bigramsA) == 0 && len(bigramsB) == 0 {
		return float64(1), nil
	}

	// Intersection and union
	intersection := 0
	for bg, countA := range bigramsA {
		if countB, ok := bigramsB[bg]; ok {
			if countA < countB {
				intersection += countA
			} else {
				intersection += countB
			}
		}
	}

	// |A∪B| = |A| + |B| - |A∩B|
	totalA := 0
	for _, c := range bigramsA {
		totalA += c
	}
	totalB := 0
	for _, c := range bigramsB {
		totalB += c
	}
	union := totalA + totalB - intersection

	if union == 0 {
		return float64(1), nil
	}

	return float64(intersection) / float64(union), nil
}

// bigrams returns a multiset of character bigrams for a rune slice
func bigrams(runes []rune) map[string]int {
	m := make(map[string]int)
	for i := 0; i < len(runes)-1; i++ {
		bg := string(runes[i : i+2])
		m[bg]++
	}
	return m
}

// jaro_similarity: Jaro algorithm
func similarityJaro(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("jaro_similarity() requires exactly 2 arguments")
	}
	s1 := []rune(toString(args[0]))
	s2 := []rune(toString(args[1]))

	return jaroDistance(s1, s2), nil
}

func jaroDistance(s1, s2 []rune) float64 {
	l1, l2 := len(s1), len(s2)
	if l1 == 0 && l2 == 0 {
		return 1.0
	}
	if l1 == 0 || l2 == 0 {
		return 0.0
	}

	// Match window
	maxDist := 0
	if l1 > l2 {
		maxDist = l1/2 - 1
	} else {
		maxDist = l2/2 - 1
	}
	if maxDist < 0 {
		maxDist = 0
	}

	s1Matches := make([]bool, l1)
	s2Matches := make([]bool, l2)

	matches := 0
	transpositions := 0

	// Find matches
	for i := 0; i < l1; i++ {
		start := i - maxDist
		if start < 0 {
			start = 0
		}
		end := i + maxDist + 1
		if end > l2 {
			end = l2
		}
		for j := start; j < end; j++ {
			if s2Matches[j] || s1[i] != s2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return 0.0
	}

	// Count transpositions
	k := 0
	for i := 0; i < l1; i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}

	m := float64(matches)
	return (m/float64(l1) + m/float64(l2) + (m-float64(transpositions)/2.0)/m) / 3.0
}

// jaro_winkler_similarity: Jaro-Winkler with prefix bonus (p=0.1, max 4 chars)
func similarityJaroWinkler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("jaro_winkler_similarity() requires exactly 2 arguments")
	}
	s1 := []rune(toString(args[0]))
	s2 := []rune(toString(args[1]))

	jaro := jaroDistance(s1, s2)

	// Common prefix length (max 4)
	prefixLen := 0
	maxPrefix := 4
	if len(s1) < maxPrefix {
		maxPrefix = len(s1)
	}
	if len(s2) < maxPrefix {
		maxPrefix = len(s2)
	}
	for i := 0; i < maxPrefix; i++ {
		if s1[i] == s2[i] {
			prefixLen++
		} else {
			break
		}
	}

	p := 0.1
	return jaro + float64(prefixLen)*p*(1.0-jaro), nil
}

// minInt returns the minimum of the given integers
func minInt(vals ...int) int {
	m := math.MaxInt64
	for _, v := range vals {
		if v < m {
			m = v
		}
	}
	return m
}
