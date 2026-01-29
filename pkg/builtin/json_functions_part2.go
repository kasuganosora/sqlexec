package builtin

import "github.com/kasuganosora/sqlexec/pkg/json"

func jsonSet(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args)%2 != 0 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_SET requires 3 or more arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.Set(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonInsert(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args)%2 != 0 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_INSERT requires 3 or more arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.Insert(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonReplace(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args)%2 != 0 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_REPLACE requires 3 or more arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.Replace(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonRemove(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_REMOVE requires at least 2 arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.Remove(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonMergePreserve(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_MERGE_PRESERVE requires at least 2 arguments"}
	}
	result, err := json.MergePreserve(args...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonMergePatch(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_MERGE_PATCH requires at least 2 arguments"}
	}
	result, err := json.MergePatch(args...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonLength(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_LENGTH requires 1-2 arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	path := "$"
	if len(args) > 1 {
		path = json.ConvertToString(args[1])
	}
	length, err := json.Length(bj, path)
	if err != nil {
		return nil, err
	}
	return int64(length), nil
}

func jsonDepth(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_DEPTH requires 1 argument"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	depth, err := json.Depth(bj)
	if err != nil {
		return nil, err
	}
	return int64(depth), nil
}

func jsonPretty(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_PRETTY requires 1 argument"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	pretty, err := json.Pretty(bj)
	if err != nil {
		return nil, err
	}
	return pretty, nil
}

func jsonStorageSize(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_STORAGE_SIZE requires 1 argument"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	size, err := json.StorageSize(bj)
	if err != nil {
		return nil, err
	}
	return int64(size), nil
}

func jsonArrayAppend(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_ARRAY_APPEND requires at least 2 arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.ArrayAppend(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonArrayInsert(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 3 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_ARRAY_INSERT requires 3 arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	result, err := json.ArrayInsert(bj, args[1:]...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonArrayGet(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_ARRAY_GET requires 3 arguments"}
	}
	bj, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	pathStr := json.ConvertToString(args[1])
	index, err := json.ConvertToInt64(args[2])
	if err != nil {
		return nil, err
	}
	result, err := json.ArrayGet(bj, pathStr, int(index))
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}

func jsonMemberOf(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_MEMBER_OF requires 2 arguments"}
	}
	target, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	array, err := json.FromBuiltinArg(args[1])
	if err != nil {
		return nil, err
	}
	member, err := json.MemberOf(target, array)
	if err != nil {
		return nil, err
	}
	return int64(0), nil
}

func jsonOverlaps(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, &json.JSONError{Code: json.ErrInvalidParam, Message: "JSON_OVERLAPS requires 2 arguments"}
	}
	array1, err := json.FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	array2, err := json.FromBuiltinArg(args[1])
	if err != nil {
		return nil, err
	}
	overlaps, err := json.Overlaps(array1, array2)
	if err != nil {
		return nil, err
	}
	return int64(0), nil
}
