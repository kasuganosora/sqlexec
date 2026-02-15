package builtin

import (
	"testing"
)

func TestCoalesce(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{nil, nil, "hello"}, "hello"},
		{[]interface{}{nil, 42, "hello"}, 42},
		{[]interface{}{"first", 42}, "first"},
		{[]interface{}{nil, nil, nil}, nil},
		{[]interface{}{}, nil},
		{[]interface{}{0, 1, 2}, 0},
		{[]interface{}{nil, false}, false},
	}
	for _, tt := range tests {
		result, err := controlCoalesce(tt.args)
		if err != nil {
			t.Errorf("controlCoalesce(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlCoalesce(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestNullIf(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{1, 1}, nil},
		{[]interface{}{1, 2}, 1},
		{[]interface{}{"abc", "abc"}, nil},
		{[]interface{}{"abc", "xyz"}, "abc"},
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{nil, 1}, nil},
		{[]interface{}{1, nil}, 1},
	}
	for _, tt := range tests {
		result, err := controlNullIf(tt.args)
		if err != nil {
			t.Errorf("controlNullIf(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlNullIf(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestIfNull(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{nil, "default"}, "default"},
		{[]interface{}{"value", "default"}, "value"},
		{[]interface{}{0, "default"}, 0},
		{[]interface{}{nil, nil}, nil},
	}
	for _, tt := range tests {
		result, err := controlIfNull(tt.args)
		if err != nil {
			t.Errorf("controlIfNull(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlIfNull(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestControlIf(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{true, "yes", "no"}, "yes"},
		{[]interface{}{false, "yes", "no"}, "no"},
		{[]interface{}{1, "yes", "no"}, "yes"},
		{[]interface{}{0, "yes", "no"}, "no"},
		{[]interface{}{nil, "yes", "no"}, "no"},
		{[]interface{}{"true", "yes", "no"}, "yes"},
		{[]interface{}{"", "yes", "no"}, "no"},
	}
	for _, tt := range tests {
		result, err := controlIf(tt.args)
		if err != nil {
			t.Errorf("controlIf(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlIf(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestGreatest(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{1, 5, 3}, 5},
		{[]interface{}{1.5, 2.5, 0.5}, 2.5},
		{[]interface{}{"apple", "cherry", "banana"}, "cherry"},
		{[]interface{}{nil, 5, nil}, 5},
		{[]interface{}{nil, nil, nil}, nil},
		{[]interface{}{42}, 42},
	}
	for _, tt := range tests {
		result, err := controlGreatest(tt.args)
		if err != nil {
			t.Errorf("controlGreatest(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlGreatest(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestLeast(t *testing.T) {
	tests := []struct {
		args []interface{}
		want interface{}
	}{
		{[]interface{}{1, 5, 3}, 1},
		{[]interface{}{1.5, 2.5, 0.5}, 0.5},
		{[]interface{}{"apple", "cherry", "banana"}, "apple"},
		{[]interface{}{nil, 5, nil}, 5},
		{[]interface{}{nil, nil, nil}, nil},
		{[]interface{}{42}, 42},
	}
	for _, tt := range tests {
		result, err := controlLeast(tt.args)
		if err != nil {
			t.Errorf("controlLeast(%v) error = %v", tt.args, err)
			continue
		}
		if result != tt.want {
			t.Errorf("controlLeast(%v) = %v, want %v", tt.args, result, tt.want)
		}
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		input interface{}
		want  bool
	}{
		{true, true},
		{false, false},
		{1, true},
		{0, false},
		{int64(1), true},
		{int64(0), false},
		{1.0, true},
		{0.0, false},
		{"hello", true},
		{"", false},
		{"0", false},
		{"false", false},
		{nil, false},
	}
	for _, tt := range tests {
		result := toBool(tt.input)
		if result != tt.want {
			t.Errorf("toBool(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

func TestControlFunctions_Registration(t *testing.T) {
	funcs := []string{"coalesce", "nullif", "ifnull", "nvl", "if", "iif", "greatest", "least"}
	for _, name := range funcs {
		fn, ok := GetGlobal(name)
		if !ok {
			t.Errorf("function %s not registered", name)
			continue
		}
		if fn.Category != "control" {
			t.Errorf("function %s category = %s, want control", name, fn.Category)
		}
	}
}

func TestControlFunctions_ErrorCases(t *testing.T) {
	_, err := controlNullIf([]interface{}{1})
	if err == nil {
		t.Error("expected error for nullif with 1 arg")
	}

	_, err = controlIfNull([]interface{}{1})
	if err == nil {
		t.Error("expected error for ifnull with 1 arg")
	}

	_, err = controlIf([]interface{}{true, "yes"})
	if err == nil {
		t.Error("expected error for if with 2 args")
	}

	_, err = controlGreatest([]interface{}{})
	if err == nil {
		t.Error("expected error for greatest with 0 args")
	}

	_, err = controlLeast([]interface{}{})
	if err == nil {
		t.Error("expected error for least with 0 args")
	}
}
