package security

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// EscapeSQL 将参数安全地转义并插入到 SQL 字符串中
// 使用格式说明符：
//   %? - 自动类型转换的参数（类似数据库的参数绑定）
//   %% - 输出 %
//   %n - 标识符（表名、列名等），自动用反引号包裹
//
// 注意：此工具不能阻止所有类型的 SQL 注入。
// 编写安全 SQL 仍然是开发者的责任。
// 建议使用参数化查询（预编译语句）来获得最佳安全性。
//
// 示例：
//
//	query, err := sqlescape.EscapeSQL("SELECT * FROM %n WHERE id = %?", "users", 123)
func EscapeSQL(sql string, args ...interface{}) (string, error) {
	result, err := escapeSQL(sql, args...)
	return string(result), err
}

// MustEscapeSQL 是 EscapeSQL 的便捷版本，遇到错误会 panic
// 适用于参数类型在编译时已知安全的场景
func MustEscapeSQL(sql string, args ...interface{}) string {
	result, err := EscapeSQL(sql, args...)
	if err != nil {
		panic(err)
	}
	return result
}

// FormatSQL 是 EscapeSQL 的 Writer 版本，适用于构建复杂 SQL
func FormatSQL(w io.Writer, sql string, args ...interface{}) error {
	result, err := escapeSQL(sql, args...)
	if err != nil {
		return err
	}
	_, err = w.Write(result)
	return err
}

// MustFormatSQL 是 FormatSQL 的便捷版本，适用于 strings.Builder
func MustFormatSQL(w *strings.Builder, sql string, args ...interface{}) {
	if err := FormatSQL(w, sql, args...); err != nil {
		panic(err)
	}
}

// escapeSQL 内部实现
func escapeSQL(sql string, args ...interface{}) ([]byte, error) {
	buf := make([]byte, 0, len(sql)+100)
	argPos := 0

	for i := 0; i < len(sql); i++ {
		// 查找 %
		pctIdx := strings.IndexByte(sql[i:], '%')
		if pctIdx == -1 {
			buf = append(buf, sql[i:]...)
			break
		}

		// 复制 % 之前的部分
		buf = append(buf, sql[i:i+pctIdx]...)
		i += pctIdx

		// 检查 % 后的字符
		if i+1 >= len(sql) {
			// % 在末尾，直接添加 %
			buf = append(buf, '%')
			break
		}

		specifier := sql[i+1]
		switch specifier {
		case 'n':
			// 标识符 %n - 用于表名、列名
			if argPos >= len(args) {
				return nil, newArgError(argPos+1, len(args))
			}
			arg := args[argPos]
			argPos++

			identifier, ok := arg.(string)
			if !ok {
				return nil, &formatError{
					arg:    argPos + 1,
					want:    "string identifier",
					got:     arg,
					message: "identifier must be a string",
				}
			}

			buf = append(buf, '`')
			buf = append(buf, strings.ReplaceAll(identifier, "`", "``")...)
			buf = append(buf, '`')
			i++ // 跳过 specifier

		case '?':
			// 参数 %? - 自动类型转换
			if argPos >= len(args) {
				return nil, newArgError(argPos+1, len(args))
			}
			arg := args[argPos]
			argPos++

			if arg == nil {
				buf = append(buf, "NULL"...)
			} else {
				var err error
				buf, err = appendSQLArg(buf, arg)
				if err != nil {
					return nil, err
				}
			}
			i++ // 跳过 specifier

		case '%':
			// 转义的 %%
			buf = append(buf, '%')
			i++ // 跳过 specifier

		default:
			// 未知的格式说明符，保留 %
			buf = append(buf, '%')
			// 不跳过 specifier，让外层循环继续处理
		}
	}

	// 检查是否有未使用的参数
	if argPos < len(args) {
		return nil, &formatError{
			arg:    argPos + 1,
			want:    "no more placeholders",
			got:     len(args) - argPos,
			message: "too many arguments provided",
		}
	}

	return buf, nil
}

// appendSQLArg 将参数转换为 SQL 值
func appendSQLArg(buf []byte, arg interface{}) ([]byte, error) {
	switch v := arg.(type) {
	case int:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int8:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int16:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int32:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint8:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint16:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint32:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case uint64:
		buf = strconv.AppendUint(buf, v, 10)
	case float32:
		buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		if v {
			buf = append(buf, '1')
		} else {
			buf = append(buf, '0')
		}
	case time.Time:
		buf = appendTime(buf, v)
	case json.RawMessage:
		buf = append(buf, '\'')
		buf = escapeBytes(buf, v)
		buf = append(buf, '\'')
	case []byte:
		if v == nil {
			buf = append(buf, "NULL"...)
		} else {
			buf = append(buf, "_binary'"...)
			buf = escapeBytes(buf, v)
			buf = append(buf, '\'')
		}
	case string:
		buf = append(buf, '\'')
		buf = escapeString(buf, v)
		buf = append(buf, '\'')
	case []string:
		for i, s := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '\'')
			buf = escapeString(buf, s)
			buf = append(buf, '\'')
		}
	case []int:
		for i, n := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendInt(buf, int64(n), 10)
		}
	case []int64:
		for i, n := range v {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendInt(buf, n, 10)
		}
	default:
		// 使用反射处理未知类型
		rv := reflect.ValueOf(arg)
		kind := rv.Kind()

		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buf = strconv.AppendInt(buf, rv.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			buf = strconv.AppendUint(buf, rv.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			buf = strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64)
		case reflect.Bool:
			if rv.Bool() {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case reflect.String:
			buf = append(buf, '\'')
			buf = escapeString(buf, rv.String())
			buf = append(buf, '\'')
		case reflect.Interface, reflect.Ptr:
			if rv.IsNil() {
				buf = append(buf, "NULL"...)
			} else {
				var err error
				buf, err = appendSQLArg(buf, rv.Elem().Interface())
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, &formatError{
				arg:    0,
				want:    "supported SQL type",
				got:     kind.String(),
				message: "unsupported argument type",
			}
		}
	}

	return buf, nil
}

// escapeString 转义字符串（使用反斜杠转义）
func escapeString(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case 0:
			buf = append(buf, '\\', '0')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\'':
			buf = append(buf, '\\', '\'')
		case '"':
			buf = append(buf, '\\', '"')
		case 0x1a:
			buf = append(buf, '\\', 'Z')
		default:
			buf = append(buf, c)
		}
	}
	return buf
}

// escapeBytes 转义字节数组
func escapeBytes(buf []byte, b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch c {
		case 0:
			buf = append(buf, '\\', '0')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\'':
			buf = append(buf, '\\', '\'')
		case '"':
			buf = append(buf, '\\', '"')
		case 0x1a:
			buf = append(buf, '\\', 'Z')
		default:
			buf = append(buf, c)
		}
	}
	return buf
}

// appendTime 格式化时间
func appendTime(buf []byte, t time.Time) []byte {
	if t.IsZero() {
		buf = append(buf, "'0000-00-00'"...)
	} else {
		buf = append(buf, '\'')
		buf = t.AppendFormat(buf, "2006-01-02 15:04:05.999999")
		buf = append(buf, '\'')
	}
	return buf
}

// newArgError 创建参数数量错误
func newArgError(arg, total int) *formatError {
	return &formatError{
		arg:    arg,
		want:    "argument",
		got:     total,
		message: "not enough arguments",
	}
}

// formatError 格式化错误
type formatError struct {
	arg    int
	want   string
	got    interface{}
	message string
}

func (e *formatError) Error() string {
	return "sqlescape: argument " + strconv.Itoa(e.arg) + ": " + e.message +
		" (want " + e.want + ", got " + formatValue(e.got) + ")"
}

func formatValue(v interface{}) string {
	switch v := v.(type) {
	case string:
		return "\"" + v + "\""
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%T", v)
	}
}
