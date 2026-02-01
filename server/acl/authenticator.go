package acl

import (
	"crypto/sha1"
	"encoding/hex"
)

// Authenticator handles MySQL native password authentication
type Authenticator struct{}

// NewAuthenticator creates a new authenticator
func NewAuthenticator() *Authenticator {
	return &Authenticator{}
}

// GeneratePasswordHash generates MySQL native password hash
// Algorithm: SHA1(SHA1(password)) ^ SHA1(salt + SHA1(SHA1(password)))
func (a *Authenticator) GeneratePasswordHash(password string, salt []byte) string {
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

	// XOR the two hashes
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = hash1[i] ^ hash3[i]
	}

	return hex.EncodeToString(result)
}

// GenerateHashedPassword generates a hashed password for storage in users.json
// This is the format stored in mysql.user table
func (a *Authenticator) GenerateHashedPassword(password string) string {
	if password == "" {
		return ""
	}

	// SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password)) - stored format
	hash2 := sha1.Sum(hash1[:])

	return "*" + hex.EncodeToString(hash2[:])
}

// VerifyPassword verifies a password against stored hash and authentication response
func (a *Authenticator) VerifyPassword(storedHash string, password string, authResponse []byte, salt []byte) bool {
	// Check if password is empty (no password)
	if password == "" && storedHash == "" {
		return true
	}

	// Generate expected auth response from password
	expectedResponse := a.GeneratePasswordHash(password, salt)

	// Compare with client's auth response
	clientResponse := hex.EncodeToString(authResponse)

	return expectedResponse == clientResponse
}

// VerifyPasswordWithHash verifies a password against stored hash only
// Used for testing and direct password verification
func (a *Authenticator) VerifyPasswordWithHash(storedHash, password string) bool {
	if password == "" && storedHash == "" {
		return true
	}

	expectedHash := a.GenerateHashedPassword(password)
	return expectedHash == storedHash
}
