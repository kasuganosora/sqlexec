package http

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderTemplate_SimpleVariables(t *testing.T) {
	ctx := &TemplateContext{
		Method:    "POST",
		Path:      "/api/query",
		Body:      `{"table":"users"}`,
		AuthToken: "secret123",
	}

	tests := []struct {
		name     string
		template string
		check    func(t *testing.T, result string)
	}{
		{
			name:     "method variable",
			template: "{{method}}",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "POST", result)
			},
		},
		{
			name:     "path variable",
			template: "{{path}}",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "/api/query", result)
			},
		},
		{
			name:     "body variable",
			template: "{{body}}",
			check: func(t *testing.T, result string) {
				assert.Equal(t, `{"table":"users"}`, result)
			},
		},
		{
			name:     "auth_token variable",
			template: "{{auth_token}}",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "secret123", result)
			},
		},
		{
			name:     "timestamp is numeric",
			template: "{{timestamp}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^\d+$`, result)
			},
		},
		{
			name:     "timestamp_ms is numeric",
			template: "{{timestamp_ms}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^\d+$`, result)
			},
		},
		{
			name:     "uuid format",
			template: "{{uuid}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, result)
			},
		},
		{
			name:     "nonce is hex",
			template: "{{nonce}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^[0-9a-f]+$`, result)
			},
		},
		{
			name:     "date format",
			template: "{{date}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, result)
			},
		},
		{
			name:     "datetime format",
			template: "{{datetime}}",
			check: func(t *testing.T, result string) {
				assert.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`, result)
			},
		},
		{
			name:     "no template",
			template: "plain text",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "plain text", result)
			},
		},
		{
			name:     "mixed text and variables",
			template: "Bearer {{auth_token}}",
			check: func(t *testing.T, result string) {
				assert.Equal(t, "Bearer secret123", result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RenderTemplate(tt.template, ctx)
			tt.check(t, result)
		})
	}
}

func TestRenderTemplate_Functions(t *testing.T) {
	ctx := &TemplateContext{
		Method:    "POST",
		Path:      "/api/query",
		Body:      "hello",
		AuthToken: "secret",
	}

	t.Run("md5", func(t *testing.T) {
		result := RenderTemplate("{{md5(body)}}", ctx)
		assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", result) // md5("hello")
	})

	t.Run("sha256", func(t *testing.T) {
		result := RenderTemplate("{{sha256(body)}}", ctx)
		assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", result)
	})

	t.Run("base64", func(t *testing.T) {
		result := RenderTemplate("{{base64(body)}}", ctx)
		assert.Equal(t, "aGVsbG8=", result) // base64("hello")
	})

	t.Run("upper", func(t *testing.T) {
		result := RenderTemplate("{{upper(method)}}", ctx)
		assert.Equal(t, "POST", result)
	})

	t.Run("lower", func(t *testing.T) {
		result := RenderTemplate("{{lower(method)}}", ctx)
		assert.Equal(t, "post", result)
	})

	t.Run("hmac_sha256", func(t *testing.T) {
		result := RenderTemplate("{{hmac_sha256(auth_token, body)}}", ctx)
		// hmac_sha256("secret", "hello")
		require.NotEmpty(t, result)
		assert.Len(t, result, 64) // SHA256 hex = 64 chars
	})

	t.Run("hmac_md5", func(t *testing.T) {
		result := RenderTemplate("{{hmac_md5(auth_token, body)}}", ctx)
		require.NotEmpty(t, result)
		assert.Len(t, result, 32) // MD5 hex = 32 chars
	})
}

func TestRenderTemplate_StringConcatenation(t *testing.T) {
	ctx := &TemplateContext{
		Method:    "POST",
		Path:      "/api/query",
		Body:      "bodydata",
		AuthToken: "mykey",
	}

	t.Run("concat two variables", func(t *testing.T) {
		result := RenderTemplate("{{md5(path+body)}}", ctx)
		expected := hashMD5("/api/query" + "bodydata")
		assert.Equal(t, expected, result)
	})

	t.Run("concat with string literal", func(t *testing.T) {
		result := RenderTemplate(`{{md5(body+"suffix")}}`, ctx)
		expected := hashMD5("bodydata" + "suffix")
		assert.Equal(t, expected, result)
	})

	t.Run("hmac with concat data", func(t *testing.T) {
		result := RenderTemplate("{{hmac_sha256(auth_token, path+body)}}", ctx)
		expected := hmacSHA256("mykey", "/api/query"+"bodydata")
		assert.Equal(t, expected, result)
	})
}

func TestRenderTemplate_ComplexSignature(t *testing.T) {
	ctx := &TemplateContext{
		Method:    "POST",
		Path:      "/api/query",
		Body:      `{"table":"users"}`,
		AuthToken: "secret_key_123",
	}

	// 模拟真实签名场景
	tmpl := "{{hmac_sha256(auth_token, path+body)}}"
	result := RenderTemplate(tmpl, ctx)
	expected := hmacSHA256("secret_key_123", "/api/query"+`{"table":"users"}`)
	assert.Equal(t, expected, result)
}

func TestRenderTemplate_UnknownVariable(t *testing.T) {
	ctx := &TemplateContext{}
	result := RenderTemplate("{{unknown_var}}", ctx)
	assert.Equal(t, "", result)
}

func TestRenderTemplate_UnknownFunction(t *testing.T) {
	ctx := &TemplateContext{Body: "test"}
	result := RenderTemplate("{{unknown_func(body)}}", ctx)
	assert.Equal(t, "", result)
}

func TestRenderTemplate_MultipleReplacements(t *testing.T) {
	ctx := &TemplateContext{
		Method:    "GET",
		Path:      "/test",
		AuthToken: "token",
	}

	result := RenderTemplate("{{method}} {{path}} {{auth_token}}", ctx)
	assert.Equal(t, "GET /test token", result)
}

func TestSplitArgs(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"a, b", []string{"a", " b"}},
		{"a, b, c", []string{"a", " b", " c"}},
		{"func(a,b), c", []string{"func(a,b)", " c"}},
	}

	for _, tt := range tests {
		result := splitArgs(tt.input)
		assert.Equal(t, tt.expected, result, "splitArgs(%q)", tt.input)
	}
}

func TestGenerateNonce(t *testing.T) {
	n1 := generateNonce()
	n2 := generateNonce()
	assert.NotEqual(t, n1, n2, "nonces should be unique")
	assert.True(t, len(n1) > 0)
	// 验证是 hex
	for _, c := range n1 {
		assert.True(t, strings.ContainsRune("0123456789abcdef", c), "nonce should be hex")
	}
}
