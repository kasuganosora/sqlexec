package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Encryptor 加密器
type Encryptor struct {
	key []byte
}

// NewEncryptor 创建加密器
func NewEncryptor(password string) (*Encryptor, error) {
	if len(password) < 8 {
		return nil, errors.New("password must be at least 8 characters")
	}

	// 使用SHA256生成32字节的密钥
	hash := sha256.Sum256([]byte(password))
	key := hash[:]

	return &Encryptor{
		key: key,
	}, nil
}

// Encrypt 加密数据
func (e *Encryptor) Encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	// 使用GCM模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// 加密
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt 解密数据
func (e *Encryptor) Decrypt(ciphertext string) (string, error) {
	// Base64解码
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertextBytes := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// EncryptField 加密字段值
func (e *Encryptor) EncryptField(fieldName, value string) (string, error) {
	if value == "" {
		return "", nil
	}

	return e.Encrypt(value)
}

// DecryptField 解密字段值
func (e *Encryptor) DecryptField(fieldName, encryptedValue string) (string, error) {
	if encryptedValue == "" {
		return "", nil
	}

	return e.Decrypt(encryptedValue)
}

// HashPassword 哈希密码
func HashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", hash)
}

// VerifyPassword 验证密码
func VerifyPassword(password, hash string) bool {
	computed := HashPassword(password)
	return subtle.ConstantTimeCompare([]byte(computed), []byte(hash)) == 1
}

// SensitiveFieldsManager 敏感字段管理器
type SensitiveFieldsManager struct {
	mu               sync.RWMutex
	encryptor        *Encryptor
	sensitiveFields  map[string]bool // table.field -> true
}

// NewSensitiveFieldsManager 创建敏感字段管理器
func NewSensitiveFieldsManager(password string, fields []string) (*SensitiveFieldsManager, error) {
	encryptor, err := NewEncryptor(password)
	if err != nil {
		return nil, err
	}

	manager := &SensitiveFieldsManager{
		encryptor:       encryptor,
		sensitiveFields: make(map[string]bool),
	}

	for _, field := range fields {
		manager.sensitiveFields[field] = true
	}

	return manager, nil
}

// AddSensitiveField 添加敏感字段
func (m *SensitiveFieldsManager) AddSensitiveField(table, field string) {
	key := fmt.Sprintf("%s.%s", table, field)
	m.mu.Lock()
	m.sensitiveFields[key] = true
	m.mu.Unlock()
}

// RemoveSensitiveField 移除敏感字段
func (m *SensitiveFieldsManager) RemoveSensitiveField(table, field string) {
	key := fmt.Sprintf("%s.%s", table, field)
	m.mu.Lock()
	delete(m.sensitiveFields, key)
	m.mu.Unlock()
}

// IsSensitive 检查字段是否敏感
func (m *SensitiveFieldsManager) IsSensitive(table, field string) bool {
	key := fmt.Sprintf("%s.%s", table, field)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sensitiveFields[key]
}

// EncryptFieldIfSensitive 如果字段敏感则加密
func (m *SensitiveFieldsManager) EncryptFieldIfSensitive(table, field, value string) (string, error) {
	if !m.IsSensitive(table, field) {
		return value, nil
	}

	return m.encryptor.EncryptField(field, value)
}

// DecryptFieldIfSensitive 如果字段敏感则解密
func (m *SensitiveFieldsManager) DecryptFieldIfSensitive(table, field, value string) (string, error) {
	if !m.IsSensitive(table, field) {
		return value, nil
	}

	return m.encryptor.DecryptField(field, value)
}

// EncryptRecord 加密记录中的敏感字段
func (m *SensitiveFieldsManager) EncryptRecord(table string, record map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for field, value := range record {
		strValue, ok := value.(string)
		if !ok {
			result[field] = value
			continue
		}

		encrypted, err := m.EncryptFieldIfSensitive(table, field, strValue)
		if err != nil {
			return nil, err
		}

		result[field] = encrypted
	}

	return result, nil
}

// DecryptRecord 解密记录中的敏感字段
func (m *SensitiveFieldsManager) DecryptRecord(table string, record map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for field, value := range record {
		strValue, ok := value.(string)
		if !ok {
			result[field] = value
			continue
		}

		decrypted, err := m.DecryptFieldIfSensitive(table, field, strValue)
		if err != nil {
			return nil, err
		}

		result[field] = decrypted
	}

	return result, nil
}
