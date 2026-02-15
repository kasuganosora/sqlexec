package builtin

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strconv"
)

func init() {
	encodingFunctions := []*FunctionInfo{
		{
			Name: "hex",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "hex", ReturnType: "string", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     encodingHex,
			Description: "Convert value to hexadecimal string",
			Example:     "HEX('hello') -> '68656C6C6F'",
			Category:    "encoding",
		},
		{
			Name: "unhex",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "unhex", ReturnType: "bytes", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     encodingUnhex,
			Description: "Decode hexadecimal string to bytes",
			Example:     "UNHEX('68656C6C6F') -> 'hello'",
			Category:    "encoding",
		},
		{
			Name: "to_base64",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "to_base64", ReturnType: "string", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     encodingToBase64,
			Description: "Encode value to base64 string",
			Example:     "TO_BASE64('hello') -> 'aGVsbG8='",
			Category:    "encoding",
		},
		{
			Name: "base64",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "base64", ReturnType: "string", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     encodingToBase64,
			Description: "Encode value to base64 string (alias for to_base64)",
			Example:     "BASE64('hello') -> 'aGVsbG8='",
			Category:    "encoding",
		},
		{
			Name: "from_base64",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "from_base64", ReturnType: "bytes", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     encodingFromBase64,
			Description: "Decode base64 string to bytes",
			Example:     "FROM_BASE64('aGVsbG8=') -> 'hello'",
			Category:    "encoding",
		},
		{
			Name: "bin",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bin", ReturnType: "string", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     encodingBin,
			Description: "Convert integer to binary string",
			Example:     "BIN(10) -> '1010'",
			Category:    "encoding",
		},
		{
			Name: "md5",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "md5", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     encodingMd5,
			Description: "Compute MD5 hash of string",
			Example:     "MD5('hello') -> '5d41402abc4b2a76b9719d911017c592'",
			Category:    "encoding",
		},
		{
			Name: "sha1",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sha1", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     encodingSha1,
			Description: "Compute SHA1 hash of string",
			Example:     "SHA1('hello') -> 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'",
			Category:    "encoding",
		},
		{
			Name: "sha2",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sha2", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
			},
			Handler:     encodingSha2,
			Description: "Compute SHA-2 hash of string with given bit length (256 or 512)",
			Example:     "SHA2('hello', 256) -> '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'",
			Category:    "encoding",
		},
		{
			Name: "hash",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "hash", ReturnType: "integer", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     encodingHash,
			Description: "Compute FNV-64a hash of value, return int64",
			Example:     "HASH('hello') -> 11831194018420276491",
			Category:    "encoding",
		},
		{
			Name: "encode",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "encode", ReturnType: "string", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     encodingEncode,
			Description: "Encode string using specified charset (hex or base64)",
			Example:     "ENCODE('hello', 'hex') -> '68656c6c6f'",
			Category:    "encoding",
		},
		{
			Name: "decode",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "decode", ReturnType: "bytes", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     encodingDecode,
			Description: "Decode string using specified charset (hex or base64)",
			Example:     "DECODE('68656c6c6f', 'hex') -> 'hello'",
			Category:    "encoding",
		},
	}

	for _, fn := range encodingFunctions {
		RegisterGlobal(fn)
	}
}

// encodingHex converts a value to its hexadecimal representation.
// For strings/bytes, it hex-encodes the bytes. For integers, it formats as uppercase hex.
func encodingHex(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("hex() requires exactly 1 argument")
	}

	switch v := args[0].(type) {
	case int:
		return fmt.Sprintf("%X", v), nil
	case int8:
		return fmt.Sprintf("%X", v), nil
	case int16:
		return fmt.Sprintf("%X", v), nil
	case int32:
		return fmt.Sprintf("%X", v), nil
	case int64:
		return fmt.Sprintf("%X", v), nil
	case uint:
		return fmt.Sprintf("%X", v), nil
	case uint8:
		return fmt.Sprintf("%X", v), nil
	case uint16:
		return fmt.Sprintf("%X", v), nil
	case uint32:
		return fmt.Sprintf("%X", v), nil
	case uint64:
		return fmt.Sprintf("%X", v), nil
	case []byte:
		return hex.EncodeToString(v), nil
	default:
		s := toString(args[0])
		return hex.EncodeToString([]byte(s)), nil
	}
}

// encodingUnhex decodes a hexadecimal string to bytes.
func encodingUnhex(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("unhex() requires exactly 1 argument")
	}

	s := toString(args[0])
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("unhex(): invalid hex string: %v", err)
	}
	return b, nil
}

// encodingToBase64 encodes a value to a base64 string.
func encodingToBase64(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("to_base64() requires exactly 1 argument")
	}

	var data []byte
	switch v := args[0].(type) {
	case []byte:
		data = v
	default:
		data = []byte(toString(args[0]))
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// encodingFromBase64 decodes a base64 string to bytes.
func encodingFromBase64(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("from_base64() requires exactly 1 argument")
	}

	s := toString(args[0])
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("from_base64(): invalid base64 string: %v", err)
	}
	return b, nil
}

// encodingBin converts an integer to its binary string representation.
func encodingBin(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("bin() requires exactly 1 argument")
	}

	n, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("bin(): %v", err)
	}
	return strconv.FormatInt(n, 2), nil
}

// encodingMd5 computes the MD5 hash of a string, returning a hex string.
func encodingMd5(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("md5() requires exactly 1 argument")
	}

	s := toString(args[0])
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:]), nil
}

// encodingSha1 computes the SHA1 hash of a string, returning a hex string.
func encodingSha1(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sha1() requires exactly 1 argument")
	}

	s := toString(args[0])
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:]), nil
}

// encodingSha2 computes a SHA-2 hash of a string with the given bit length (256 or 512).
func encodingSha2(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("sha2() requires exactly 2 arguments")
	}

	s := toString(args[0])
	bits, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("sha2(): %v", err)
	}

	switch bits {
	case 256:
		h := sha256.Sum256([]byte(s))
		return hex.EncodeToString(h[:]), nil
	case 512:
		h := sha512.Sum512([]byte(s))
		return hex.EncodeToString(h[:]), nil
	default:
		return nil, fmt.Errorf("sha2(): unsupported bit length %d, use 256 or 512", bits)
	}
}

// encodingHash computes an FNV-64a hash of a value, returning int64.
func encodingHash(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("hash() requires exactly 1 argument")
	}

	s := toString(args[0])
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64()), nil
}

// encodingEncode encodes a string using the specified charset ("hex" or "base64").
func encodingEncode(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("encode() requires exactly 2 arguments")
	}

	s := toString(args[0])
	charset := toString(args[1])

	switch charset {
	case "hex":
		return hex.EncodeToString([]byte(s)), nil
	case "base64":
		return base64.StdEncoding.EncodeToString([]byte(s)), nil
	default:
		return nil, fmt.Errorf("encode(): unsupported charset %q, use 'hex' or 'base64'", charset)
	}
}

// encodingDecode decodes a string using the specified charset ("hex" or "base64").
func encodingDecode(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("decode() requires exactly 2 arguments")
	}

	s := toString(args[0])
	charset := toString(args[1])

	switch charset {
	case "hex":
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("decode(): invalid hex string: %v", err)
		}
		return b, nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("decode(): invalid base64 string: %v", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("decode(): unsupported charset %q, use 'hex' or 'base64'", charset)
	}
}
