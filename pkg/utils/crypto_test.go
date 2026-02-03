package utils

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestGeneratePasswordHash(t *testing.T) {
	tests := []struct {
		name     string
		password string
		salt     []byte
		expected string
	}{
		// 正常情况
		{
			name:     "正常密码",
			password: "password123",
			salt:     []byte("salt123456789012"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "空密码",
			password: "",
			salt:     []byte("salt123456789012"),
			expected: "",
		},
		{
			name:     "简单密码",
			password: "a",
			salt:     []byte("s"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "复杂密码",
			password: "P@ssw0rd!#$%^&*()",
			salt:     []byte("complexsalt"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "空salt",
			password: "password",
			salt:     []byte{},
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "长密码",
			password: string(make([]byte, 100)),
			salt:     []byte("salt"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "长salt",
			password: "password",
			salt:     make([]byte, 100),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "Unicode密码",
			password: "你好世界",
			salt:     []byte("盐值"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "数字密码",
			password: "123456789",
			salt:     []byte("123"),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "特殊字符密码",
			password: "\x00\x01\x02",
			salt:     []byte("\xFF\xFE"),
			expected: "正常情况不应固定返回值",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GeneratePasswordHash(tt.password, tt.salt)
			if tt.expected == "" && result != tt.expected {
				t.Errorf("GeneratePasswordHash(%q, %v) = %q, want %q", tt.password, tt.salt, result, tt.expected)
				return
			}
			if tt.expected != "" && result == "" {
				t.Errorf("GeneratePasswordHash(%q, %v) returned empty string", tt.password, tt.salt)
				return
			}

			// 验证结果是有效的十六进制字符串
			if result != "" {
				_, err := hex.DecodeString(result)
				if err != nil {
					t.Errorf("GeneratePasswordHash() returned invalid hex: %v", err)
				}
			}
		})
	}
}

func TestGeneratePasswordHashConsistency(t *testing.T) {
	// 测试相同输入产生相同输出
	password := "testpassword"
	salt := []byte("testsalt1234567")

	hash1 := GeneratePasswordHash(password, salt)
	hash2 := GeneratePasswordHash(password, salt)

	if hash1 != hash2 {
		t.Errorf("GeneratePasswordHash() not consistent: %q != %q", hash1, hash2)
	}
}

func TestGeneratePasswordHashUniqueness(t *testing.T) {
	// 测试不同salt产生不同输出
	password := "testpassword"
	salt1 := []byte("salt1")
	salt2 := []byte("salt2")

	hash1 := GeneratePasswordHash(password, salt1)
	hash2 := GeneratePasswordHash(password, salt2)

	if hash1 == hash2 {
		t.Errorf("GeneratePasswordHash() should produce different hashes with different salts")
	}
}

func TestGenerateHashedPassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
		expected string
	}{
		{
			name:     "正常密码",
			password: "password123",
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "空密码",
			password: "",
			expected: "",
		},
		{
			name:     "简单密码",
			password: "a",
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "复杂密码",
			password: "P@ssw0rd!#$%^&*()",
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "长密码",
			password: string(make([]byte, 100)),
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "Unicode密码",
			password: "你好世界",
			expected: "正常情况不应固定返回值",
		},
		{
			name:     "数字密码",
			password: "123456789",
			expected: "正常情况不应固定返回值",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateHashedPassword(tt.password)
			if tt.expected == "" && result != tt.expected {
				t.Errorf("GenerateHashedPassword(%q) = %q, want %q", tt.password, result, tt.expected)
				return
			}
			if tt.expected != "" && result == "" {
				t.Errorf("GenerateHashedPassword(%q) returned empty string", tt.password)
				return
			}

			// 验证结果以*开头（MySQL存储格式）
			if result != "" && result[0] != '*' {
				t.Errorf("GenerateHashedPassword() should start with '*', got: %q", result)
			}

			// 验证结果是有效的十六进制字符串
			if result != "" {
				_, err := hex.DecodeString(result[1:])
				if err != nil {
					t.Errorf("GenerateHashedPassword() returned invalid hex: %v", err)
				}
			}
		})
	}
}

func TestGenerateHashedPasswordConsistency(t *testing.T) {
	// 测试相同输入产生相同输出
	password := "testpassword"

	hash1 := GenerateHashedPassword(password)
	hash2 := GenerateHashedPassword(password)

	if hash1 != hash2 {
		t.Errorf("GenerateHashedPassword() not consistent: %q != %q", hash1, hash2)
	}
}

func TestVerifyPassword(t *testing.T) {
	// 准备测试数据
	password := "testpassword"
	salt := []byte("testsalt1234567")
	authResponse := GeneratePasswordHash(password, salt)
	authResponseBytes, _ := hex.DecodeString(authResponse)

	tests := []struct {
		name         string
		storedHash   string
		password     string
		authResponse []byte
		salt         []byte
		expected     bool
	}{
		{
			name:         "正确密码",
			storedHash:   GenerateHashedPassword(password),
			password:     password,
			authResponse: authResponseBytes,
			salt:         salt,
			expected:     true,
		},
		{
			name:         "错误密码",
			storedHash:   GenerateHashedPassword(password),
			password:     "wrongpassword",
			authResponse: authResponseBytes,
			salt:         salt,
			expected:     false,
		},
		{
			name:         "空密码和空hash",
			storedHash:   "",
			password:     "",
			authResponse: []byte{},
			salt:         salt,
			expected:     true,
		},
		{
			name:         "空密码有hash",
			storedHash:   GenerateHashedPassword("something"),
			password:     "",
			authResponse: authResponseBytes,
			salt:         salt,
			expected:     false,
		},
		{
			name:         "有密码空hash",
			storedHash:   "",
			password:     password,
			authResponse: authResponseBytes,
			salt:         salt,
			expected:     false,
		},
		{
			name:         "空authResponse",
			storedHash:   GenerateHashedPassword(password),
			password:     password,
			authResponse: []byte{},
			salt:         salt,
			expected:     false,
		},
		{
			name:         "简单密码",
			storedHash:   GenerateHashedPassword("a"),
			password:     "a",
			authResponse: func() []byte { h, _ := hex.DecodeString(GeneratePasswordHash("a", []byte("s"))); return h }(),
			salt:         []byte("s"),
			expected:     true,
		},
		{
			name:         "复杂密码",
			storedHash:   GenerateHashedPassword("P@ssw0rd!"),
			password:     "P@ssw0rd!",
			authResponse: func() []byte { h, _ := hex.DecodeString(GeneratePasswordHash("P@ssw0rd!", []byte("salt"))); return h }(),
			salt:         []byte("salt"),
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := VerifyPassword(tt.storedHash, tt.password, tt.authResponse, tt.salt)
			if result != tt.expected {
				t.Errorf("VerifyPassword() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestVerifyPasswordWithHash(t *testing.T) {
	tests := []struct {
		name       string
		storedHash string
		password   string
		expected   bool
	}{
		{
			name:       "正确密码",
			storedHash: GenerateHashedPassword("testpassword"),
			password:   "testpassword",
			expected:   true,
		},
		{
			name:       "错误密码",
			storedHash: GenerateHashedPassword("testpassword"),
			password:   "wrongpassword",
			expected:   false,
		},
		{
			name:       "空密码和空hash",
			storedHash: "",
			password:   "",
			expected:   true,
		},
		{
			name:       "空密码有hash",
			storedHash: GenerateHashedPassword("something"),
			password:   "",
			expected:   false,
		},
		{
			name:       "有密码空hash",
			storedHash: "",
			password:   "testpassword",
			expected:   false,
		},
		{
			name:       "简单密码",
			storedHash: GenerateHashedPassword("a"),
			password:   "a",
			expected:   true,
		},
		{
			name:       "复杂密码",
			storedHash: GenerateHashedPassword("P@ssw0rd!#$%^&*()"),
			password:   "P@ssw0rd!#$%^&*()",
			expected:   true,
		},
		{
			name:       "Unicode密码",
			storedHash: GenerateHashedPassword("你好世界"),
			password:   "你好世界",
			expected:   true,
		},
		{
			name:       "数字密码",
			storedHash: GenerateHashedPassword("123456789"),
			password:   "123456789",
			expected:   true,
		},
		{
			name:       "特殊字符密码",
			storedHash: GenerateHashedPassword("\x00\x01\x02"),
			password:   "\x00\x01\x02",
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := VerifyPasswordWithHash(tt.storedHash, tt.password)
			if result != tt.expected {
				t.Errorf("VerifyPasswordWithHash(%q, %q) = %v, want %v", tt.storedHash, tt.password, result, tt.expected)
			}
		})
	}
}

func TestPasswordHashRoundTrip(t *testing.T) {
	// 测试完整的密码验证流程
	passwords := []string{
		"password123",
		"P@ssw0rd!",
		"你好世界",
		"123456789",
		"a",
		"verylongpasswordwithlotsofcharacters123456789",
	}

	for _, password := range passwords {
		t.Run(password, func(t *testing.T) {
			// 1. 生成存储的哈希
			storedHash := GenerateHashedPassword(password)

			// 2. 用salt生成认证响应
			salt := []byte("testsalt1234567")
			authResponse := GeneratePasswordHash(password, salt)
			authResponseBytes, _ := hex.DecodeString(authResponse)

			// 3. 验证密码
			result1 := VerifyPassword(storedHash, password, authResponseBytes, salt)
			if !result1 {
				t.Errorf("VerifyPassword() failed for correct password")
			}

			// 4. 使用VerifyPasswordWithHash验证
			result2 := VerifyPasswordWithHash(storedHash, password)
			if !result2 {
				t.Errorf("VerifyPasswordWithHash() failed for correct password")
			}

			// 5. 验证错误密码失败
			wrongHash := GenerateHashedPassword("wrong")
			result3 := VerifyPasswordWithHash(wrongHash, password)
			if result3 {
				t.Errorf("VerifyPasswordWithHash() should fail for wrong password")
			}
		})
	}
}

func TestPasswordHashCollision(t *testing.T) {
	// 测试不同密码不应产生相同哈希
	passwords := []string{
		"password1",
		"password2",
		"PASSWORD1",
		"pAssword1",
	}

	hashes := make(map[string]bool)
	for _, password := range passwords {
		hash := GenerateHashedPassword(password)
		if hashes[hash] {
			t.Errorf("Hash collision detected for password %q", password)
		}
		hashes[hash] = true
	}
}

func TestPasswordHashOutputFormat(t *testing.T) {
	// 测试哈希输出格式
	hash := GenerateHashedPassword("test")

	// 应该以*开头
	if hash[0] != '*' {
		t.Errorf("Hash should start with '*', got: %q", hash)
	}

	// 应该是41个字符（* + 40个十六进制字符）
	if len(hash) != 41 {
		t.Errorf("Hash should be 41 characters, got %d", len(hash))
	}

	// 去掉*后应该是有效的十六进制
	hexPart := hash[1:]
	_, err := hex.DecodeString(hexPart)
	if err != nil {
		t.Errorf("Hash hex part is invalid: %v", err)
	}
}

func TestPasswordHashLength(t *testing.T) {
	// 测试不同长度密码的哈希长度一致
	passwords := []string{
		"",
		"a",
		"ab",
		"abc",
		string(make([]byte, 10)),
		string(make([]byte, 100)),
		string(make([]byte, 1000)),
	}

	for _, password := range passwords {
		if password == "" {
			continue
		}

		hash := GenerateHashedPassword(password)
		if len(hash) != 41 {
			t.Errorf("Hash length for password of length %d should be 41, got %d", len(password), len(hash))
		}
	}
}

func BenchmarkGeneratePasswordHash(b *testing.B) {
	password := "testpassword"
	salt := []byte("testsalt1234567")

	for i := 0; i < b.N; i++ {
		GeneratePasswordHash(password, salt)
	}
}

func BenchmarkGenerateHashedPassword(b *testing.B) {
	password := "testpassword"

	for i := 0; i < b.N; i++ {
		GenerateHashedPassword(password)
	}
}

func BenchmarkVerifyPasswordWithHash(b *testing.B) {
	password := "testpassword"
	hash := GenerateHashedPassword(password)

	for i := 0; i < b.N; i++ {
		VerifyPasswordWithHash(hash, password)
	}
}

func ExampleGenerateHashedPassword() {
	hash := GenerateHashedPassword("mypassword")
	fmt.Println(len(hash))
	fmt.Println(hash[:10])
	// Output: 41
	// *fabe5482d
}

func ExampleVerifyPasswordWithHash() {
	hash := GenerateHashedPassword("mypassword")
	result := VerifyPasswordWithHash(hash, "mypassword")
	fmt.Println(result)

	result2 := VerifyPasswordWithHash(hash, "wrongpassword")
	fmt.Println(result2)
	// Output: true
	// false
}
