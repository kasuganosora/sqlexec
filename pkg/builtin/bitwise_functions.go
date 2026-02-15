package builtin

import (
	"fmt"
	"math/bits"
)

func init() {
	bitwiseFunctions := []*FunctionInfo{
		{
			Name: "bit_count",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bit_count", ReturnType: "integer", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     bitwiseBitCount,
			Description: "Count the number of set bits (1s) in an integer",
			Example:     "BIT_COUNT(7) -> 3",
			Category:    "bitwise",
		},
		{
			Name: "get_bit",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "get_bit", ReturnType: "integer", ParamTypes: []string{"integer", "integer"}, Variadic: false},
			},
			Handler:     bitwiseGetBit,
			Description: "Get the bit value at a given position",
			Example:     "GET_BIT(5, 0) -> 1",
			Category:    "bitwise",
		},
		{
			Name: "set_bit",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "set_bit", ReturnType: "integer", ParamTypes: []string{"integer", "integer", "integer"}, Variadic: false},
			},
			Handler:     bitwiseSetBit,
			Description: "Set or clear the bit at a given position",
			Example:     "SET_BIT(5, 1, 1) -> 7",
			Category:    "bitwise",
		},
		{
			Name: "bit_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bit_length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     bitwiseBitLength,
			Description: "Return the bit length of a string",
			Example:     "BIT_LENGTH('hello') -> 40",
			Category:    "bitwise",
		},
	}

	for _, fn := range bitwiseFunctions {
		RegisterGlobal(fn)
	}
}

// bitwiseBitCount counts the number of set bits (1s) in an integer.
func bitwiseBitCount(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("bit_count() requires exactly 1 argument")
	}

	n, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("bit_count(): %v", err)
	}
	return int64(bits.OnesCount64(uint64(n))), nil
}

// bitwiseGetBit gets the bit value at a given position (0-indexed from LSB).
func bitwiseGetBit(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("get_bit() requires exactly 2 arguments")
	}

	n, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("get_bit(): %v", err)
	}
	pos, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("get_bit(): %v", err)
	}
	if pos < 0 || pos > 63 {
		return nil, fmt.Errorf("get_bit(): position must be between 0 and 63")
	}
	return int64((n >> uint(pos)) & 1), nil
}

// bitwiseSetBit sets or clears the bit at a given position (0-indexed from LSB).
// val=1 sets the bit, val=0 clears it.
func bitwiseSetBit(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("set_bit() requires exactly 3 arguments")
	}

	n, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("set_bit(): %v", err)
	}
	pos, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("set_bit(): %v", err)
	}
	val, err := toInt64(args[2])
	if err != nil {
		return nil, fmt.Errorf("set_bit(): %v", err)
	}

	if pos < 0 || pos > 63 {
		return nil, fmt.Errorf("set_bit(): position must be between 0 and 63")
	}
	if val != 0 && val != 1 {
		return nil, fmt.Errorf("set_bit(): value must be 0 or 1")
	}

	if val == 1 {
		n = n | (1 << uint(pos))
	} else {
		n = n &^ (1 << uint(pos))
	}
	return n, nil
}

// bitwiseBitLength returns the bit length of a string (len in bytes * 8).
func bitwiseBitLength(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("bit_length() requires exactly 1 argument")
	}

	s := toString(args[0])
	return int64(len(s) * 8), nil
}
