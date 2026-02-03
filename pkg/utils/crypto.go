package utils

import (
	"crypto/sha1"
	"encoding/hex"
)

// GeneratePasswordHash 生成MySQL native password hash
// Algorithm: SHA1(SHA1(password)) ^ SHA1(salt + SHA1(SHA1(password)))
// 用于MySQL认证协议
func GeneratePasswordHash(password string, salt []byte) string {
	if password == "" {
		return ""
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password))
	hash2 := sha1.Sum(hash1[:])

	// SHA1(salt + SHA1(SHA1(password)))
	combined := append(salt, hash2[:]...)
	hash3 := sha1.Sum(combined)

	// XOR two hashes
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = hash1[i] ^ hash3[i]
	}

	return hex.EncodeToString(result)
}

// GenerateHashedPassword 生成存储在数据库中的哈希密码
// 这是mysql.user表中的存储格式
// 格式: *SHA1(SHA1(password))
func GenerateHashedPassword(password string) string {
	if password == "" {
		return ""
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password)) - stored format
	hash2 := sha1.Sum(hash1[:])

	return "*" + hex.EncodeToString(hash2[:])
}

// VerifyPassword 验证密码
// 参数:
//   - storedHash: 存储的密码哈希
//   - password: 待验证的明文密码
//   - authResponse: 客户端发送的认证响应
//   - salt: salt数据
// 返回: 密码是否匹配
func VerifyPassword(storedHash string, password string, authResponse []byte, salt []byte) bool {
	// Check if password is empty (no password)
	if password == "" && storedHash == "" {
		return true
	}

	// Generate expected auth response from password
	expectedResponse := GeneratePasswordHash(password, salt)

	// Compare with client's auth response
	clientResponse := hex.EncodeToString(authResponse)

	return expectedResponse == clientResponse
}

// VerifyPasswordWithHash 仅验证密码是否匹配存储的哈希
// 用于测试和直接密码验证
func VerifyPasswordWithHash(storedHash, password string) bool {
	if password == "" && storedHash == "" {
		return true
	}

	expectedHash := GenerateHashedPassword(password)
	return expectedHash == storedHash
}
