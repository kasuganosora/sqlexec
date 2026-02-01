package acl

import (
	"encoding/hex"
	"testing"
)

func TestNewAuthenticator(t *testing.T) {
	auth := NewAuthenticator()
	if auth == nil {
		t.Fatal("NewAuthenticator() returned nil")
	}
}

func TestGeneratePasswordHash(t *testing.T) {
	auth := NewAuthenticator()
	salt := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	tests := []struct {
		name     string
		password string
		salt     []byte
		wantEmpty bool
	}{
		{
			name:       "Empty password",
			password:   "",
			salt:       salt,
			wantEmpty: true,
		},
		{
			name:       "Non-empty password",
			password:   "test123",
			salt:       salt,
			wantEmpty: false,
		},
		{
			name:       "Password with special chars",
			password:   "p@ssw0rd!",
			salt:       salt,
			wantEmpty: false,
		},
		{
			name:       "Long password",
			password:   "verylongpasswordwithmanycharacters123456789",
			salt:       salt,
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := auth.GeneratePasswordHash(tt.password, tt.salt)
			if tt.wantEmpty && result != "" {
				t.Errorf("GeneratePasswordHash() = %v, want empty string", result)
			}
			if !tt.wantEmpty && result == "" {
				t.Errorf("GeneratePasswordHash() returned empty string, want non-empty")
			}
		})
	}
}

func TestGeneratePasswordHashConsistency(t *testing.T) {
	auth := NewAuthenticator()
	salt := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	password := "test123"

	hash1 := auth.GeneratePasswordHash(password, salt)
	hash2 := auth.GeneratePasswordHash(password, salt)

	if hash1 != hash2 {
		t.Errorf("GeneratePasswordHash() is not consistent: %v != %v", hash1, hash2)
	}
}

func TestGeneratePasswordHashDifferentSalt(t *testing.T) {
	auth := NewAuthenticator()
	salt1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	salt2 := []byte{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	password := "test123"

	hash1 := auth.GeneratePasswordHash(password, salt1)
	hash2 := auth.GeneratePasswordHash(password, salt2)

	if hash1 == hash2 {
		t.Errorf("GeneratePasswordHash() produces same hash for different salts")
	}
}

func TestGenerateHashedPassword(t *testing.T) {
	auth := NewAuthenticator()

	tests := []struct {
		name       string
		password   string
		wantPrefix string
	}{
		{
			name:       "Empty password",
			password:   "",
			wantPrefix: "",
		},
		{
			name:       "Non-empty password",
			password:   "test123",
			wantPrefix: "*",
		},
		{
			name:       "Password with special chars",
			password:   "p@ssw0rd!",
			wantPrefix: "*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := auth.GenerateHashedPassword(tt.password)
			if tt.wantPrefix != "" && len(result) == 0 {
				t.Errorf("GenerateHashedPassword() = %v, want non-empty", result)
			}
			if tt.wantPrefix == "" && result != "" {
				t.Errorf("GenerateHashedPassword() = %v, want empty", result)
			}
		if tt.wantPrefix != "" && len(result) > 0 && result[0] != '*' {
			t.Errorf("GenerateHashedPassword() = %v, want prefix '%s'", result, tt.wantPrefix)
			}
		})
	}
}

func TestGenerateHashedPasswordLength(t *testing.T) {
	auth := NewAuthenticator()
	password := "test123"
	hash := auth.GenerateHashedPassword(password)

	// MySQL hashed password is * + 40 hex chars = 41 chars
	expectedLen := 41
	if len(hash) != expectedLen {
		t.Errorf("GenerateHashedPassword() length = %v, want %v", len(hash), expectedLen)
	}
}

func TestVerifyPasswordWithHash(t *testing.T) {
	auth := NewAuthenticator()

	tests := []struct {
		name        string
		storedHash  string
		password    string
		want        bool
	}{
		{
			name:        "Correct password",
			storedHash:  auth.GenerateHashedPassword("test123"),
			password:    "test123",
			want:        true,
		},
		{
			name:        "Wrong password",
			storedHash:  auth.GenerateHashedPassword("test123"),
			password:    "wrongpassword",
			want:        false,
		},
		{
			name:        "Empty password with empty hash",
			storedHash:  "",
			password:    "",
			want:        true,
		},
		{
			name:        "Non-empty password with empty hash",
			storedHash:  "",
			password:    "test123",
			want:        false,
		},
		{
			name:        "Empty password with non-empty hash",
			storedHash:  auth.GenerateHashedPassword("test123"),
			password:    "",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := auth.VerifyPasswordWithHash(tt.storedHash, tt.password)
			if got != tt.want {
				t.Errorf("VerifyPasswordWithHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyPassword(t *testing.T) {
	auth := NewAuthenticator()
	salt := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	password := "test123"
	storedHash := auth.GenerateHashedPassword(password)

	// Generate the expected auth response (hex string)
	expectedResponseHex := auth.GeneratePasswordHash(password, salt)

	// Convert hex string to bytes (this is what the client would send)
	expectedResponseBytes, _ := hex.DecodeString(expectedResponseHex)

	tests := []struct {
		name         string
		storedHash   string
		password     string
		authResponse []byte
		salt         []byte
		want         bool
	}{
		{
			name:         "Correct auth response",
			storedHash:   storedHash,
			password:     password,
			authResponse: expectedResponseBytes,
			salt:         salt,
			want:         true,
		},
		{
			name:         "Wrong auth response",
			storedHash:   storedHash,
			password:     password,
			authResponse: []byte("wrongresponse"),
			salt:         salt,
			want:         false,
		},
		{
			name:         "Empty password with empty hash and empty response",
			storedHash:   "",
			password:     "",
			authResponse: []byte(""),
			salt:         salt,
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := auth.VerifyPassword(tt.storedHash, tt.password, tt.authResponse, tt.salt)
			if got != tt.want {
				t.Errorf("VerifyPassword() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyPasswordDifferentSalts(t *testing.T) {
	auth := NewAuthenticator()
	password := "test123"
	storedHash := auth.GenerateHashedPassword(password)
	salt1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	salt2 := []byte{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	authResponseHex1 := auth.GeneratePasswordHash(password, salt1)
	authResponseHex2 := auth.GeneratePasswordHash(password, salt2)

	// Convert hex to bytes
	authResponseBytes1, _ := hex.DecodeString(authResponseHex1)
	authResponseBytes2, _ := hex.DecodeString(authResponseHex2)

	// Should verify correctly with matching salt
	if !auth.VerifyPassword(storedHash, password, authResponseBytes1, salt1) {
		t.Error("VerifyPassword() failed with matching salt1")
	}
	if !auth.VerifyPassword(storedHash, password, authResponseBytes2, salt2) {
		t.Error("VerifyPassword() failed with matching salt2")
	}

	// Should fail with mismatching salt because authResponse doesn't match
	if auth.VerifyPassword(storedHash, password, authResponseBytes1, salt2) {
		t.Error("VerifyPassword() should fail with mismatching salt - authResponseBytes1 is for salt1, not salt2")
	}
}

func TestPasswordHashingDeterministic(t *testing.T) {
	auth := NewAuthenticator()
	password := "test123"
	salt := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	// Generate hash multiple times and ensure consistency
	hashes := make([]string, 10)
	for i := 0; i < 10; i++ {
		hashes[i] = auth.GeneratePasswordHash(password, salt)
	}

	// All hashes should be identical
	for i := 1; i < len(hashes); i++ {
		if hashes[i] != hashes[0] {
			t.Errorf("GeneratePasswordHash() is not deterministic: %v != %v", hashes[i], hashes[0])
		}
	}
}

func TestHashedPasswordFormat(t *testing.T) {
	auth := NewAuthenticator()
	password := "test123"
	hash := auth.GenerateHashedPassword(password)

	// Hash should start with *
		if len(hash) == 0 || hash[0] != '*' {
			t.Errorf("GenerateHashedPassword() should start with *, got %v", hash)
	}

	// Hash should contain only hex characters after *
	for i := 1; i < len(hash); i++ {
		c := hash[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("GenerateHashedPassword() contains non-hex character at position %d: %v", i, c)
		}
	}
}
