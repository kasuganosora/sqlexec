package builtin

import (
	"crypto/rand"
	"fmt"
	"math/big"
	mathrand "math/rand"
	"time"
)

func init() {
	systemFunctions := []*FunctionInfo{
		{
			Name: "typeof",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "typeof", ReturnType: "string", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     sysTypeOf,
			Description: "返回值的SQL类型名称",
			Example:     "TYPEOF(42) -> 'INTEGER'",
			Category:    "system",
		},
		{
			Name: "version",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "version", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     sysVersion,
			Description: "返回数据库版本",
			Example:     "VERSION() -> 'SQLExec 1.0.0'",
			Category:    "system",
		},
		{
			Name: "current_database",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "current_database", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     sysCurrentDatabase,
			Description: "返回当前数据库名称",
			Example:     "CURRENT_DATABASE() -> 'default'",
			Category:    "system",
		},
		{
			Name: "current_schema",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "current_schema", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     sysCurrentDatabase,
			Description: "返回当前schema名称",
			Example:     "CURRENT_SCHEMA() -> 'default'",
			Category:    "system",
		},
		{
			Name: "uuid",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "uuid", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     sysUUID,
			Description: "生成随机UUID v4",
			Example:     "UUID() -> '550e8400-e29b-41d4-a716-446655440000'",
			Category:    "system",
		},
		{
			Name: "gen_random_uuid",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "gen_random_uuid", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     sysUUID,
			Description: "生成随机UUID v4（uuid的别名）",
			Example:     "GEN_RANDOM_UUID() -> '550e8400-e29b-41d4-a716-446655440000'",
			Category:    "system",
		},
		{
			Name: "setseed",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "setseed", ReturnType: "null", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     sysSetseed,
			Description: "设置随机数种子",
			Example:     "SETSEED(0.5) -> NULL",
			Category:    "system",
		},
		{
			Name: "sleep",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sleep", ReturnType: "integer", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     sysSleep,
			Description: "暂停执行指定秒数",
			Example:     "SLEEP(1) -> 0",
			Category:    "system",
		},
	}

	for _, fn := range systemFunctions {
		RegisterGlobal(fn)
	}
}

// sysTypeOf returns the SQL type name of the value.
func sysTypeOf(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("typeof() requires exactly 1 argument")
	}
	if args[0] == nil {
		return "NULL", nil
	}
	switch args[0].(type) {
	case bool:
		return "BOOLEAN", nil
	case int, int8, int16, int32, int64:
		return "INTEGER", nil
	case uint, uint8, uint16, uint32, uint64:
		return "INTEGER", nil
	case float32, float64:
		return "DOUBLE", nil
	case string:
		return "VARCHAR", nil
	case []byte:
		return "BLOB", nil
	case time.Time:
		return "TIMESTAMP", nil
	case []interface{}:
		return "LIST", nil
	case map[string]interface{}:
		return "STRUCT", nil
	default:
		return fmt.Sprintf("%T", args[0]), nil
	}
}

// sysVersion returns the database version string.
func sysVersion(args []interface{}) (interface{}, error) {
	return "SQLExec 1.0.0", nil
}

// sysCurrentDatabase returns the current database name.
func sysCurrentDatabase(args []interface{}) (interface{}, error) {
	return "default", nil
}

// sysUUID generates a UUID v4 string.
func sysUUID(args []interface{}) (interface{}, error) {
	var uuid [16]byte
	_, err := rand.Read(uuid[:])
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %w", err)
	}
	// Set version 4 and variant bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

// sysSetseed sets the global random seed.
func sysSetseed(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("setseed() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	// Convert seed to int64 (seed range: -1.0 to 1.0 maps to int64)
	switch v := args[0].(type) {
	case float64:
		mathrand.Seed(int64(v * float64(1<<31)))
	case float32:
		mathrand.Seed(int64(float64(v) * float64(1<<31)))
	case int:
		mathrand.Seed(int64(v))
	case int64:
		mathrand.Seed(v)
	default:
		n, ok := new(big.Float).SetString(fmt.Sprintf("%v", v))
		if ok {
			f, _ := n.Float64()
			mathrand.Seed(int64(f * float64(1<<31)))
		}
	}
	return nil, nil
}

// sysSleep pauses execution for the specified number of seconds.
func sysSleep(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sleep() requires exactly 1 argument")
	}
	if args[0] == nil {
		return int64(0), nil
	}
	var seconds float64
	switch v := args[0].(type) {
	case float64:
		seconds = v
	case float32:
		seconds = float64(v)
	case int:
		seconds = float64(v)
	case int64:
		seconds = float64(v)
	default:
		return nil, fmt.Errorf("sleep() requires a numeric argument")
	}
	if seconds > 0 && seconds <= 300 { // cap at 5 minutes
		time.Sleep(time.Duration(seconds * float64(time.Second)))
	}
	return int64(0), nil
}
