package security

import (
	"testing"
)

func TestNewEncryptor(t *testing.T) {
	tests := []struct {
		name        string
		password    string
		expectError bool
	}{
		{"Valid password", "mysecretpassword", false},
		{"Exactly 8 characters", "12345678", false},
		{"Too short", "short", true},
		{"Empty password", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encryptor, err := NewEncryptor(tt.password)
			if (err != nil) != tt.expectError {
				t.Errorf("NewEncryptor() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && encryptor == nil {
				t.Error("Expected non-nil encryptor")
			}
		})
	}
}

func TestEncryptDecrypt(t *testing.T) {
	encryptor, err := NewEncryptor("testpassword123")
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	tests := []struct {
		name  string
		value string
	}{
		{"Simple text", "Hello World"},
		{"Numbers", "1234567890"},
		{"Special characters", "!@#$%^&*()"},
		{"Unicode", "你好世界"},
		{"Long text", "This is a longer text that tests encryption of multiple words and characters."},
		{"SQL query", "SELECT * FROM users WHERE id = 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, err := encryptor.Encrypt(tt.value)
			if err != nil {
				t.Errorf("Encrypt() error = %v", err)
				return
			}

			if encrypted == tt.value {
				t.Error("Encrypted text should be different from original")
			}

			decrypted, err := encryptor.Decrypt(encrypted)
			if err != nil {
				t.Errorf("Decrypt() error = %v", err)
				return
			}

			if decrypted != tt.value {
				t.Errorf("Decrypt() = %s, want %s", decrypted, tt.value)
			}
		})
	}
}

func TestEncryptEmptyString(t *testing.T) {
	encryptor, err := NewEncryptor("testpassword123")
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	encrypted, err := encryptor.Encrypt("")
	if err != nil {
		t.Errorf("Encrypt() error = %v", err)
	}

	decrypted, err := encryptor.Decrypt(encrypted)
	if err != nil {
		t.Errorf("Decrypt() error = %v", err)
	}

	if decrypted != "" {
		t.Errorf("Decrypt() = %s, want empty string", decrypted)
	}
}

func TestDecryptInvalidCiphertext(t *testing.T) {
	encryptor, err := NewEncryptor("testpassword123")
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	tests := []struct {
		name        string
		ciphertext  string
		expectError bool
	}{
		{"Invalid base64", "not valid base64!!!", true},
		{"Empty string", "", true},
		{"Too short", "short", true},
		{"Valid base64 but invalid ciphertext", "dGVzdA==", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encryptor.Decrypt(tt.ciphertext)
			if (err != nil) != tt.expectError {
				t.Errorf("Decrypt() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestDifferentPasswords(t *testing.T) {
	password1 := "password123"
	password2 := "password456"

	encryptor1, _ := NewEncryptor(password1)
	encryptor2, _ := NewEncryptor(password2)

	plaintext := "secret data"

	encrypted1, _ := encryptor1.Encrypt(plaintext)
	encrypted2, _ := encryptor2.Encrypt(plaintext)

	// 加密后的文本应该不同
	if encrypted1 == encrypted2 {
		t.Error("Different passwords should produce different ciphertexts")
	}

	// 用不同的密钥解密应该失败
	decrypted1, err := encryptor2.Decrypt(encrypted1)
	if err == nil {
		t.Error("Decrypting with wrong password should fail")
	}
	if decrypted1 == plaintext {
		t.Error("Should not be able to decrypt with wrong password")
	}
}

func TestEncryptFieldDecryptField(t *testing.T) {
	encryptor, err := NewEncryptor("testpassword123")
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	fieldName := "email"
	value := "test@example.com"

	encrypted, err := encryptor.EncryptField(fieldName, value)
	if err != nil {
		t.Errorf("EncryptField() error = %v", err)
	}

	decrypted, err := encryptor.DecryptField(fieldName, encrypted)
	if err != nil {
		t.Errorf("DecryptField() error = %v", err)
	}

	if decrypted != value {
		t.Errorf("DecryptField() = %s, want %s", decrypted, value)
	}
}

func TestEncryptFieldEmptyValue(t *testing.T) {
	encryptor, err := NewEncryptor("testpassword123")
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	encrypted, err := encryptor.EncryptField("email", "")
	if err != nil {
		t.Errorf("EncryptField() error = %v", err)
	}

	if encrypted != "" {
		t.Errorf("EncryptField() empty value should return empty string, got %s", encrypted)
	}

	decrypted, err := encryptor.DecryptField("email", "")
	if err != nil {
		t.Errorf("DecryptField() error = %v", err)
	}

	if decrypted != "" {
		t.Error("DecryptField() empty value should return empty string")
	}
}

func TestHashPassword(t *testing.T) {
	password := "mypassword123"

	hash := HashPassword(password)

	if hash == "" {
		t.Error("HashPassword() should return non-empty hash")
	}

	if hash == password {
		t.Error("Hash should be different from password")
	}

	// SHA256哈希应该是64字符的十六进制字符串
	if len(hash) != 64 {
		t.Errorf("Hash length = %d, want 64", len(hash))
	}
}

func TestVerifyPassword(t *testing.T) {
	password := "mypassword123"
	hash := HashPassword(password)

	tests := []struct {
		name     string
		password string
		hash     string
		expected bool
	}{
		{"Valid password", password, hash, true},
		{"Invalid password", "wrongpassword", hash, false},
		{"Empty password", "", hash, false},
		{"Different hash", password, HashPassword("different"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := VerifyPassword(tt.password, tt.hash)
			if result != tt.expected {
				t.Errorf("VerifyPassword() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestHashConsistency(t *testing.T) {
	password := "testpassword123"

	hash1 := HashPassword(password)
	hash2 := HashPassword(password)

	if hash1 != hash2 {
		t.Error("Same password should produce same hash")
	}
}

func TestNewSensitiveFieldsManager(t *testing.T) {
	fields := []string{"users.email", "users.password", "profiles.phone"}

	manager, err := NewSensitiveFieldsManager("password123", fields)
	if err != nil {
		t.Fatalf("NewSensitiveFieldsManager() error = %v", err)
	}

	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}

	if manager.encryptor == nil {
		t.Error("encryptor should be initialized")
	}

	if manager.sensitiveFields == nil {
		t.Error("sensitiveFields map should be initialized")
	}

	// 验证字段已添加
	for _, field := range fields {
		if !manager.sensitiveFields[field] {
			t.Errorf("Field %s should be sensitive", field)
		}
	}
}

func TestSensitiveFieldsManagerAddRemove(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{})

	table := "users"
	field := "email"

	// 添加敏感字段
	manager.AddSensitiveField(table, field)
	if !manager.IsSensitive(table, field) {
		t.Error("Field should be sensitive after adding")
	}

	// 移除敏感字段
	manager.RemoveSensitiveField(table, field)
	if manager.IsSensitive(table, field) {
		t.Error("Field should not be sensitive after removing")
	}
}

func TestIsSensitive(t *testing.T) {
	fields := []string{"users.email", "users.password", "orders.creditcard"}
	manager, _ := NewSensitiveFieldsManager("password123", fields)

	tests := []struct {
		table    string
		field    string
		expected bool
	}{
		{"users", "email", true},
		{"users", "password", true},
		{"orders", "creditcard", true},
		{"users", "name", false},
		{"orders", "id", false},
		{"profiles", "email", false},
	}

	for _, tt := range tests {
		t.Run(tt.table+"."+tt.field, func(t *testing.T) {
			result := manager.IsSensitive(tt.table, tt.field)
			if result != tt.expected {
				t.Errorf("IsSensitive(%s, %s) = %v, want %v",
					tt.table, tt.field, result, tt.expected)
			}
		})
	}
}

func TestEncryptFieldIfSensitive(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{"users.email"})

	tests := []struct {
		name     string
		table    string
		field    string
		value    string
		expected string // empty means should be encrypted
	}{
		{"Sensitive field", "users", "email", "test@example.com", ""},
		{"Non-sensitive field", "users", "name", "John Doe", "John Doe"},
		{"Different table", "profiles", "email", "test@example.com", "test@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := manager.EncryptFieldIfSensitive(tt.table, tt.field, tt.value)
			if err != nil {
				t.Errorf("EncryptFieldIfSensitive() error = %v", err)
				return
			}

			if tt.expected != "" {
				// 非敏感字段，应该返回原值
				if result != tt.expected {
					t.Errorf("Expected original value %s, got %s", tt.expected, result)
				}
			} else {
				// 敏感字段，应该被加密
				if result == tt.value {
					t.Error("Sensitive field should be encrypted")
				}
			}
		})
	}
}

func TestDecryptFieldIfSensitive(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{"users.email"})

	// 首先加密敏感字段
	encryptedEmail, _ := manager.EncryptFieldIfSensitive("users", "email", "test@example.com")

	tests := []struct {
		name  string
		table string
		field string
		value string
	}{
		{"Decrypt sensitive field", "users", "email", encryptedEmail},
		{"Non-sensitive field", "users", "name", "John Doe"},
		{"Different table", "profiles", "email", "test@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := manager.DecryptFieldIfSensitive(tt.table, tt.field, tt.value)
			if err != nil {
				t.Errorf("DecryptFieldIfSensitive() error = %v", err)
				return
			}

			if tt.table == "users" && tt.field == "email" {
				if result != "test@example.com" {
					t.Errorf("Expected decrypted value, got %s", result)
				}
			} else {
				if result != tt.value {
					t.Errorf("Expected original value, got %s", result)
				}
			}
		})
	}
}

func TestEncryptRecord(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{"users.email", "users.password"})

	record := map[string]interface{}{
		"id":       1,
		"name":     "John Doe",
		"email":    "test@example.com",
		"password": "secret123",
		"age":      30,
	}

	encrypted, err := manager.EncryptRecord("users", record)
	if err != nil {
		t.Fatalf("EncryptRecord() error = %v", err)
	}

	// 检查敏感字段是否加密
	if encrypted["email"] == record["email"] {
		t.Error("email should be encrypted")
	}
	if encrypted["password"] == record["password"] {
		t.Error("password should be encrypted")
	}

	// 检查非敏感字段是否未改变
	if encrypted["name"] != record["name"] {
		t.Error("name should not be encrypted")
	}
	if encrypted["id"] != record["id"] {
		t.Error("id should not be changed")
	}
	if encrypted["age"] != record["age"] {
		t.Error("age should not be changed")
	}
}

func TestDecryptRecord(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{"users.email", "users.password"})

	record := map[string]interface{}{
		"id":       1,
		"name":     "John Doe",
		"email":    "test@example.com",
		"password": "secret123",
	}

	// 加密记录
	encrypted, _ := manager.EncryptRecord("users", record)

	// 解密记录
	decrypted, err := manager.DecryptRecord("users", encrypted)
	if err != nil {
		t.Fatalf("DecryptRecord() error = %v", err)
	}

	// 验证所有字段是否正确解密
	if decrypted["email"] != record["email"] {
		t.Errorf("email = %s, want %s", decrypted["email"], record["email"])
	}
	if decrypted["password"] != record["password"] {
		t.Errorf("password = %s, want %s", decrypted["password"], record["password"])
	}
	if decrypted["name"] != record["name"] {
		t.Errorf("name = %s, want %s", decrypted["name"], record["name"])
	}
	if decrypted["id"] != record["id"] {
		t.Error("id should match")
	}
}

func TestEncryptRecordWithNonStringValues(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{})

	record := map[string]interface{}{
		"id":     123,
		"active": true,
		"score":  95.5,
		"email":  "test@example.com",
	}

	decrypted, err := manager.DecryptRecord("users", record)
	if err != nil {
		t.Errorf("DecryptRecord() error = %v", err)
	}

	// 非字符串值应该保持不变
	if decrypted["id"] != 123 {
		t.Error("id should remain as int")
	}
	if decrypted["active"] != true {
		t.Error("active should remain as bool")
	}
	if decrypted["score"] != 95.5 {
		t.Error("score should remain as float")
	}
}

func TestEncryptDecryptRecordEmpty(t *testing.T) {
	manager, _ := NewSensitiveFieldsManager("password123", []string{})

	record := map[string]interface{}{}

	encrypted, err := manager.EncryptRecord("users", record)
	if err != nil {
		t.Errorf("EncryptRecord() error = %v", err)
	}

	decrypted, err := manager.DecryptRecord("users", encrypted)
	if err != nil {
		t.Errorf("DecryptRecord() error = %v", err)
	}

	if len(decrypted) != 0 {
		t.Error("Empty record should remain empty")
	}
}
