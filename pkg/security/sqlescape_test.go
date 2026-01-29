package security

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestEscapeSQL_Basic(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
		err  bool
	}{
		{
			name: "simple string",
			sql:  "SELECT * FROM %n WHERE name = %?",
			args: []interface{}{"users", "test"},
			want: "SELECT * FROM `users` WHERE name = 'test'",
		},
		{
			name: "integer",
			sql:  "SELECT * FROM %n WHERE id = %?",
			args: []interface{}{"users", 123},
			want: "SELECT * FROM `users` WHERE id = 123",
		},
		{
			name: "multiple args",
			sql:  "INSERT INTO %n (name, age) VALUES (%?, %?)",
			args: []interface{}{"users", "Alice", 30},
			want: "INSERT INTO `users` (name, age) VALUES ('Alice', 30)",
		},
		{
			name: "nil value",
			sql:  "SELECT * FROM %n WHERE status = %?",
			args: []interface{}{"users", nil},
			want: "SELECT * FROM `users` WHERE status = NULL",
		},
		{
			name: "boolean",
			sql:  "SELECT * FROM %n WHERE active = %?",
			args: []interface{}{"users", true},
			want: "SELECT * FROM `users` WHERE active = 1",
		},
		{
			name: "boolean false",
			sql:  "SELECT * FROM %n WHERE active = %?",
			args: []interface{}{"users", false},
			want: "SELECT * FROM `users` WHERE active = 0",
		},
		{
			name: "float",
			sql:  "SELECT * FROM %n WHERE price = %?",
			args: []interface{}{"products", 19.99},
			want: "SELECT * FROM `products` WHERE price = 19.99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if (err != nil) != tt.err {
				t.Errorf("EscapeSQL() error = %v, wantErr %v", err, tt.err)
				return
			}
			if !tt.err && got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeSQL_Escaping(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "single quote",
			sql:  "SELECT * FROM %n WHERE name = %?",
			args: []interface{}{"users", "O'Reilly"},
			want: "SELECT * FROM `users` WHERE name = 'O\\'Reilly'",
		},
		{
			name: "backslash",
			sql:  "SELECT * FROM %n WHERE path = %?",
			args: []interface{}{"files", "C:\\Users\\test"},
			want: "SELECT * FROM `files` WHERE path = 'C:\\\\Users\\\\test'",
		},
		{
			name: "newline",
			sql:  "SELECT * FROM %n WHERE content = %?",
			args: []interface{}{"logs", "line1\nline2"},
			want: "SELECT * FROM `logs` WHERE content = 'line1\\nline2'",
		},
		{
			name: "identifier with backtick",
			sql:  "SELECT * FROM %n",
			args: []interface{}{"my`table"},
			want: "SELECT * FROM `my``table`",
		},
		{
			name: "null byte",
			sql:  "SELECT * FROM %n WHERE data = %?",
			args: []interface{}{"users", string([]byte{0, 'a', 0, 'b'})},
			want: "SELECT * FROM `users` WHERE data = '\\0a\\0b'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if err != nil {
				t.Errorf("EscapeSQL() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeSQL_ArrayTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "string array",
			sql:  "SELECT * FROM %n WHERE name IN (%?)",
			args: []interface{}{"users", []string{"Alice", "Bob", "Charlie"}},
			want: "SELECT * FROM `users` WHERE name IN ('Alice','Bob','Charlie')",
		},
		{
			name: "int array",
			sql:  "SELECT * FROM %n WHERE id IN (%?)",
			args: []interface{}{"users", []int{1, 2, 3, 4, 5}},
			want: "SELECT * FROM `users` WHERE id IN (1,2,3,4,5)",
		},
		{
			name: "int64 array",
			sql:  "SELECT * FROM %n WHERE id IN (%?)",
			args: []interface{}{"users", []int64{100, 200, 300}},
			want: "SELECT * FROM `users` WHERE id IN (100,200,300)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if err != nil {
				t.Errorf("EscapeSQL() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeSQL_Time(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "time",
			sql:  "SELECT * FROM %n WHERE created_at = %?",
			args: []interface{}{"logs", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
			want: "SELECT * FROM `logs` WHERE created_at = '2024-01-15 10:30:00'",
		},
		{
			name: "zero time",
			sql:  "SELECT * FROM %n WHERE deleted_at = %?",
			args: []interface{}{"users", time.Time{}},
			want: "SELECT * FROM `users` WHERE deleted_at = '0000-00-00'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if err != nil {
				t.Errorf("EscapeSQL() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeSQL_Bytes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "bytes",
			sql:  "INSERT INTO %n (data) VALUES (%?)",
			args: []interface{}{"files", []byte{0x01, 0x02, 0x03, '\n'}},
			want: "INSERT INTO `files` (data) VALUES (_binary'\x01\x02\x03\\n')",
		},
		{
			name: "nil bytes",
			sql:  "INSERT INTO %n (data) VALUES (%?)",
			args: []interface{}{"files", []byte(nil)},
			want: "INSERT INTO `files` (data) VALUES (NULL)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if err != nil {
				t.Errorf("EscapeSQL() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEscapeSQL_Errors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		args    []interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name:    "missing argument",
			sql:     "SELECT * FROM %n WHERE id = %?",
			args:    []interface{}{"users"},
			wantErr: true,
		},
		{
			name:    "too many arguments",
			sql:     "SELECT * FROM %n",
			args:    []interface{}{"users", 123},
			wantErr: true,
		},
		{
			name:    "invalid identifier type",
			sql:     "SELECT * FROM %n",
			args:    []interface{}{123},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EscapeSQL(tt.sql, tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("EscapeSQL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEscapeSQL_Percent(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "double percent",
			sql:  "SELECT %% as value",
			args: []interface{}{},
			want: "SELECT % as value",
		},
		{
			name: "triple percent",
			sql:  "SELECT %%% as value",
			args: []interface{}{},
			want: "SELECT %% as value",
		},
		{
			name: "percent in string literal",
			sql:  "SELECT %? as percent",
			args: []interface{}{"50%"},
			want: "SELECT '50%' as percent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeSQL(tt.sql, tt.args...)
			if err != nil {
				t.Errorf("EscapeSQL() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("EscapeSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMustEscapeSQL(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustEscapeSQL() did not panic on error")
		}
	}()

	MustEscapeSQL("SELECT * FROM %n") // 缺少参数
}

func TestFormatSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []interface{}
		want string
	}{
		{
			name: "basic",
			sql:  "SELECT * FROM %n WHERE id = %?",
			args: []interface{}{"users", 123},
			want: "SELECT * FROM `users` WHERE id = 123",
		},
		{
			name: "multiple placeholders",
			sql:  "INSERT INTO %n (a, b, c) VALUES (%?, %?, %?)",
			args: []interface{}{"test", 1, "two", 3.0},
			want: "INSERT INTO `test` (a, b, c) VALUES (1, 'two', 3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			err := FormatSQL(&buf, tt.sql, tt.args...)
			if err != nil {
				t.Errorf("FormatSQL() unexpected error: %v", err)
				return
			}
			if buf.String() != tt.want {
				t.Errorf("FormatSQL() = %q, want %q", buf.String(), tt.want)
			}
		})
	}
}

func TestMustFormatSQL(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustFormatSQL() did not panic on error")
		}
	}()

	var buf strings.Builder
	MustFormatSQL(&buf, "SELECT * FROM %n") // 缺少参数
}

func TestFormatSQL_WithWriter(t *testing.T) {
	t.Run("bytes.Buffer", func(t *testing.T) {
		var buf bytes.Buffer
		err := FormatSQL(&buf, "SELECT * FROM %n WHERE id = %?", "users", 123)
		if err != nil {
			t.Errorf("FormatSQL() unexpected error: %v", err)
			return
		}
		got := buf.String()
		want := "SELECT * FROM `users` WHERE id = 123"
		if got != want {
			t.Errorf("FormatSQL() = %q, want %q", got, want)
		}
	})
}

func TestComplexQuery(t *testing.T) {
	// 测试复杂查询构建
	var buf strings.Builder

	// 构建 SELECT 语句
	FormatSQL(&buf, "SELECT %n, %n, %n FROM %n WHERE %n = %? AND %n > %?",
		"id", "name", "email", "users", "status", "active", "age", 18)

	// 构建子查询
	FormatSQL(&buf, " AND id IN (SELECT user_id FROM %n WHERE %n = %?)",
		"orders", "status", "completed")

	want := "SELECT `id`, `name`, `email` FROM `users` WHERE `status` = 'active' AND `age` > 18" +
		" AND id IN (SELECT user_id FROM `orders` WHERE `status` = 'completed')"

	if buf.String() != want {
		t.Errorf("Complex query = %q, want %q", buf.String(), want)
	}
}

// TestUnsupportedType 测试不支持的类型
func TestUnsupportedType(t *testing.T) {
	type CustomType struct{}

	_, err := EscapeSQL("SELECT %?", CustomType{})
	if err == nil {
		t.Errorf("EscapeSQL() expected error for unsupported type")
	}
}
