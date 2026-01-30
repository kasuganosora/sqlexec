package json

// This file contains the reconstruct helper functions used by mutate operations.
// These functions are kept separate from the main operation files to avoid circular
// dependencies and to keep the codebase organized by functionality.

// reconstructObject reconstructs an object after applying a path leg
func reconstructObject(obj map[string]interface{}, value interface{}, leg PathLeg) map[string]interface{} {
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
			if value != nil {
				for k := range obj {
					newObj[k] = value
				}
			}
		} else {
			// Set specific key
			if value != nil {
				newObj[keyLeg.Key] = value
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
func reconstructArray(arr []interface{}, value interface{}, leg PathLeg) []interface{} {
	newArr := make([]interface{}, len(arr))
	copy(newArr, arr)

	// Apply modifications
	switch l := leg.(type) {
	case *ArrayLeg:
		arrayLeg := l
		if arrayLeg.Wildcard {
			// Wildcard - apply to all elements
			if value != nil {
				for i := range arr {
					newArr[i] = value
				}
			}
		} else {
			// Set specific index
			idx := arrayLeg.Index
			if arrayLeg.Last {
				idx = len(arr) - 1
			}

			if idx >= 0 && idx < len(arr) {
				if value != nil {
					newArr[idx] = value
				}
			}
		}
	case *RangeLeg:
		rangeLeg := l
		start, end := getRangeIndices(rangeLeg, len(arr))
		for i := start; i <= end; i++ {
			if i >= 0 && i < len(arr) && value != nil {
				newArr[i] = value
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
