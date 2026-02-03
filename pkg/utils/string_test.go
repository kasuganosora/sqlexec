package utils

import (
	"testing"
)

func TestStartsWith(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		prefix   string
		expected bool
	}{
		{"正常匹配", "hello world", "hello", true},
		{"空前缀匹配", "hello", "", true},
		{"不匹配", "hello", "world", false},
		{"空字符串s", "", "", true},
		{"空字符串prefix", "", "a", false},
		{"前缀长于字符串", "hi", "hello", false},
		{"精确匹配", "hello", "hello", true},
		{"区分大小写", "Hello", "hello", false},
		{"单字符匹配", "abc", "a", true},
		{"Unicode字符", "你好世界", "你好", true},
		{"数字字符串", "12345", "123", true},
		{"特殊字符", "!@#$%", "!@", true},
		{"包含空格", "hello world", "hello ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWith(tt.s, tt.prefix); got != tt.expected {
				t.Errorf("StartsWith(%q, %q) = %v, want %v", tt.s, tt.prefix, got, tt.expected)
			}
		})
	}
}

func TestEndsWith(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		suffix   string
		expected bool
	}{
		{"正常匹配", "hello world", "world", true},
		{"空后缀匹配", "hello", "", true},
		{"不匹配", "hello", "world", false},
		{"空字符串s", "", "", true},
		{"空字符串suffix", "", "a", false},
		{"后缀长于字符串", "hi", "hello", false},
		{"精确匹配", "hello", "hello", true},
		{"区分大小写", "Hello", "hello", false},
		{"单字符匹配", "abc", "c", true},
		{"Unicode字符", "你好世界", "世界", true},
		{"数字字符串", "12345", "345", true},
		{"特殊字符", "!@#$%", "$%", true},
		{"包含空格", "hello world", " world", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EndsWith(tt.s, tt.suffix); got != tt.expected {
				t.Errorf("EndsWith(%q, %q) = %v, want %v", tt.s, tt.suffix, got, tt.expected)
			}
		})
	}
}

func TestContainsSimple(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{"正常包含", "hello world", "lo wo", true},
		{"不包含", "hello", "world", false},
		{"空字符串s", "", "", true},
		{"空字符串substr", "hello", "", true},
		{"整个字符串", "hello", "hello", true},
		{"单字符", "abc", "b", true},
		{"开头", "hello", "he", true},
		{"结尾", "hello", "lo", true},
		{"区分大小写", "Hello", "he", false},
		{"Unicode字符", "你好世界", "好", true},
		{"数字字符串", "12345", "234", true},
		{"特殊字符", "!@#$%", "@#", true},
		{"重复字符", "aaa", "aa", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsSimple(tt.s, tt.substr); got != tt.expected {
				t.Errorf("ContainsSimple(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.expected)
			}
		})
	}
}

func TestFindSubstring(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected int
	}{
		{"正常查找", "hello world", "world", 6},
		{"不包含", "hello", "world", -1},
		{"空字符串s", "", "", 0},
		{"空字符串substr", "hello", "", 0},
		{"整个字符串", "hello", "hello", 0},
		{"单字符", "abc", "b", 1},
		{"开头", "hello", "he", 0},
		{"重复", "hello hello", "hello", 0},
		{"第二次出现", "hello hello", "lo", 3},
		{"Unicode字符", "你好世界", "世界", 2},
		{"数字字符串", "12345", "234", 1},
		{"特殊字符", "!@#$%", "@#", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindSubstring(tt.s, tt.substr); got != tt.expected {
				t.Errorf("FindSubstring(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.expected)
			}
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{"正常包含", "hello world", "world", true},
		{"不包含", "hello", "world", false},
		{"空字符串s", "", "", true},
		{"空字符串substr", "hello", "", false},
		{"整个字符串", "hello", "hello", true},
		{"通配符*匹配全部", "hello", "*", true},
		{"前缀通配符", "hello world", "*world", true},
		{"后缀通配符", "hello world", "hello*", true},
		{"前后通配符", "hello world", "*ll*", true},
		{"百分号通配符", "hello world", "%world", true},
		{"精确匹配", "hello", "hello", true},
		{"单字符", "abc", "b", false},
		{"Unicode字符", "你好世界", "*世界", true},
		{"数字字符串", "12345", "*345", true},
		{"特殊字符", "!@#$%", "*#$%", true},
		{"不匹配通配符", "hello", "x*", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Contains(tt.s, tt.substr); got != tt.expected {
				t.Errorf("Contains(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.expected)
			}
		})
	}
}

func TestReplaceAll(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		old      string
		new      string
		expected string
	}{
		{"正常替换", "hello world", "world", "universe", "hello universe"},
		{"多次替换", "hello hello", "hello", "hi", "hi hi"},
		{"不存在的子串", "hello", "world", "universe", "hello"},
		{"空字符串s", "", "a", "b", ""},
		{"空字符串old", "hello", "", "world", "hello"},
		{"空字符串new", "hello hello", "hello", "", " "},
		{"替换为空", "hello world", "world", "", "hello "},
		{"替换为相同", "hello", "hello", "hello", "hello"},
		{"单字符替换", "aaa", "a", "b", "bbb"},
		{"Unicode字符", "你好世界", "世界", "中国", "你好中国"},
		{"特殊字符", "!@#!@#", "@#", "$$", "!$$!$$"},
		{"重叠替换", "aaa", "aa", "b", "ba"},
		{"数字字符串", "123123", "123", "456", "456456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReplaceAll(tt.s, tt.old, tt.new); got != tt.expected {
				t.Errorf("ReplaceAll(%q, %q, %q) = %q, want %q", tt.s, tt.old, tt.new, got, tt.expected)
			}
		})
	}
}

func TestContainsWord(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		word     string
		expected bool
	}{
		{"单词在中间", "hello world", "world", true},
		{"单词在开头", "hello world", "hello", true},
		{"单词在结尾", "hello world", "world", true},
		{"单词不存在", "hello world", "universe", false},
		{"空字符串str", "", "hello", false},
		{"空字符串word", "hello world", "", false},
		{"部分匹配", "hello", "hel", false},
		{"区分大小写", "Hello World", "hello", true},
		{"带逗号", "hello,world", "world", true},
		{"带分号", "hello;world", "world", true},
		{"带括号", "(hello world)", "hello", true},
		{"多个单词", "hello world universe", "world", true},
		{"带换行", "hello\nworld", "world", true},
		{"特殊分隔符", "hello(world)", "hello", true},
		{"重复单词", "hello hello", "hello", true},
		{"包含部分", "helloworld", "world", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsWord(tt.str, tt.word); got != tt.expected {
				t.Errorf("ContainsWord(%q, %q) = %v, want %v", tt.str, tt.word, got, tt.expected)
			}
		})
	}
}

func TestJoinWith(t *testing.T) {
	tests := []struct {
		name     string
		strs     []string
		sep      string
		expected string
	}{
		{"正常连接", []string{"hello", "world"}, " ", "hello world"},
		{"空数组", []string{}, ",", ""},
		{"单元素", []string{"hello"}, ",", "hello"},
		{"多个元素", []string{"a", "b", "c"}, ",", "a,b,c"},
		{"空分隔符", []string{"hello", "world"}, "", "helloworld"},
		{"包含空字符串", []string{"a", "", "c"}, ",", "a,,c"},
		{"特殊分隔符", []string{"hello", "world"}, "@@", "hello@@world"},
		{"Unicode字符", []string{"你好", "世界"}, " ", "你好 世界"},
		{"数字字符串", []string{"1", "2", "3"}, "-", "1-2-3"},
		{"大量元素", []string{"a", "b", "c", "d", "e"}, "-", "a-b-c-d-e"},
		{"元素含分隔符", []string{"a,b", "c"}, ",", "a,b,c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinWith(tt.strs, tt.sep); got != tt.expected {
				t.Errorf("JoinWith(%v, %q) = %q, want %q", tt.strs, tt.sep, got, tt.expected)
			}
		})
	}
}

func TestToLowerCase(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected string
	}{
		{"正常转小写", "HELLO", "hello"},
		{"混合大小写", "HeLLo", "hello"},
		{"全小写", "hello", "hello"},
		{"空字符串", "", ""},
		{"带空格", "HELLO WORLD", "hello world"},
		{"带数字", "HELLO123", "hello123"},
		{"带特殊字符", "HELLO@WORLD", "hello@world"},
		{"Unicode字符", "你好世界", "你好世界"},
		{"单字符", "H", "h"},
		{"已经小写", "hello", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToLowerCase(tt.s); got != tt.expected {
				t.Errorf("ToLowerCase(%q) = %q, want %q", tt.s, got, tt.expected)
			}
		})
	}
}

func TestContainsSubstring(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{"正常包含", "hello world", "world", true},
		{"不包含", "hello", "world", false},
		{"空字符串s", "", "", true},
		{"空字符串substr", "hello", "", true},
		{"整个字符串", "hello", "hello", true},
		{"单字符", "abc", "b", true},
		{"开头", "hello", "he", true},
		{"结尾", "hello", "lo", true},
		{"区分大小写", "Hello", "he", false},
		{"Unicode字符", "你好世界", "好", true},
		{"数字字符串", "12345", "234", true},
		{"特殊字符", "!@#$%", "@#", true},
		{"重复字符", "aaa", "aa", true},
		{"substr比s长", "hi", "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsSubstring(tt.s, tt.substr); got != tt.expected {
				t.Errorf("ContainsSubstring(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.expected)
			}
		})
	}
}

func TestIndexOfSubstring(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected int
	}{
		{"正常查找", "hello world", "world", 6},
		{"不包含", "hello", "world", -1},
		{"空字符串s", "", "", 0},
		{"空字符串substr", "hello", "", 0},
		{"整个字符串", "hello", "hello", 0},
		{"单字符", "abc", "b", 1},
		{"开头", "hello", "he", 0},
		{"重复", "hello hello", "hello", 0},
		{"第二次出现", "hello hello", "lo", 3},
		{"Unicode字符", "你好世界", "世界", 2},
		{"数字字符串", "12345", "234", 1},
		{"特殊字符", "!@#$%", "@#", 1},
		{"substr比s长", "hi", "hello", -1},
		{"最后一次出现", "ababa", "ba", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IndexOfSubstring(tt.s, tt.substr); got != tt.expected {
				t.Errorf("IndexOfSubstring(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.expected)
			}
		})
	}
}
