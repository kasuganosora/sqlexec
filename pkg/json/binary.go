package json

import (
	"fmt"
)

// Extract extracts a value from JSON using a path expression
func (bj BinaryJSON) Extract(pathStr string) (BinaryJSON, error) {
	if pathStr == "" {
		return bj, nil
	}

	// Parse path
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Evaluate path
	return path.Extract(bj)
}

// Get extracts a value from JSON using a path
func (bj BinaryJSON) Get(pathStr string) (interface{}, error) {
	result, err := bj.Extract(pathStr)
	if err != nil {
		return nil, err
	}

	return result.GetInterface(), nil
}

// Set sets a value at the specified path
func (bj BinaryJSON) Set(pathStr string, value interface{}) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Convert value to BinaryJSON
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Apply path recursively
	return applyPath(bj, path, parsedValue, 0)
}

// applyPath applies a path recursively
func applyPath(bj BinaryJSON, path *Path, value BinaryJSON, depth int) (BinaryJSON, error) {
	if depth >= len(path.Legs) {
		return value, nil
	}

	leg := path.Legs[depth]
	results, err := leg.Apply(bj)
	if err != nil {
		return BinaryJSON{}, err
	}

	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError(path.String())
	}

	// Multiple matches - need to apply to all
	newResults := make([]interface{}, 0, len(results))
	for i, result := range results {
		remainingPath := &Path{Legs: path.Legs[depth+1:]}
		newValue, err := applyPath(result, remainingPath, value, depth+1)
		if err != nil {
			return BinaryJSON{}, err
		}
		newResults[i] = newValue.GetInterface()
	}

	// Reconstruct based on type
	var parent interface{}
	if bj.IsObject() {
		obj, _ := bj.GetObject()
		parent = reconstructObject(obj, results, leg)
	} else if bj.IsArray() {
		arr, _ := bj.GetArray()
		parent = reconstructArray(arr, results, leg)
	} else {
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "can only set path in objects or arrays"}
	}

	result, _ := NewBinaryJSON(parent)
	return result, nil
}

// reconstructObject reconstructs an object after applying a path leg
func reconstructObject(obj map[string]interface{}, results []BinaryJSON, leg PathLeg) map[string]interface{} {
	newObj := make(map[string]interface{})

	// Copy all non-modified keys
	for k, v := range obj {
		newObj[k] = v
	}

	// Apply modifications
	switch l := leg.(type) {
	case *KeyLeg:
		keyLeg := l
		if keyLeg.Wildcard {
			// Wildcard - apply to all keys
			for k := range obj {
				if len(results) > 0 {
					newObj[k] = results[0].GetInterface()
				}
			}
		} else {
			// Set specific key
			if len(results) > 0 {
				newObj[keyLeg.Key] = results[0].GetInterface()
			} else {
				delete(newObj, keyLeg.Key)
			}
		}
	default:
		// For other leg types, just return original
		return obj
	}

	return newObj
}

// reconstructArray reconstructs an array after applying a path leg
func reconstructArray(arr []interface{}, results []BinaryJSON, leg PathLeg) []interface{} {
	newArr := make([]interface{}, len(arr))
	copy(newArr, arr)

	// Apply modifications
	switch l := leg.(type) {
	case *ArrayLeg:
		arrayLeg := l
		if arrayLeg.Wildcard {
			// Wildcard - apply to all elements
			for i := range arr {
				if len(results) > 0 {
					newArr[i] = results[0].GetInterface()
				}
			}
		} else {
			// Set specific index
			idx := arrayLeg.Index
			if arrayLeg.Last {
				idx = len(arr) - 1
			}

			if idx >= 0 && idx < len(arr) {
				if len(results) > 0 {
					newArr[idx] = results[0].GetInterface()
				}
			}
		}
	case *RangeLeg:
		rangeLeg := l
		start, end := getRangeIndices(rangeLeg, len(arr))
		for i := start; i <= end; i++ {
			if i >= 0 && i < len(arr) && len(results) > 0 {
				newArr[i] = results[0].GetInterface()
			}
		}
	default:
		// For other leg types, just return original
		return arr
	}

	return newArr
}

// getRangeIndices calculates actual indices from a RangeLeg
func getRangeIndices(r *RangeLeg, arrLen int) (int, int) {
	start := r.Start
	if r.StartLast {
		start = arrLen - 1
	}

	end := r.End
	if r.EndLast {
		end = arrLen - 1
	}

	return start, end
}
