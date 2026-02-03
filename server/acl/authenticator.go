package acl

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// Authenticator handles MySQL native password authentication
// 此类现在作为utils包中密码函数的包装器
type Authenticator struct{}

// NewAuthenticator creates a new authenticator
func NewAuthenticator() *Authenticator {
	return &Authenticator{}
}

// GeneratePasswordHash generates MySQL native password hash
// Algorithm: SHA1(SHA1(password)) ^ SHA1(salt + SHA1(SHA1(password)))
func (a *Authenticator) GeneratePasswordHash(password string, salt []byte) string {
	return utils.GeneratePasswordHash(password, salt)
}

// GenerateHashedPassword generates a hashed password for storage in users.json
// This is the format stored in mysql.user table
func (a *Authenticator) GenerateHashedPassword(password string) string {
	return utils.GenerateHashedPassword(password)
}

// VerifyPassword verifies a password against stored hash and authentication response
func (a *Authenticator) VerifyPassword(storedHash string, password string, authResponse []byte, salt []byte) bool {
	return utils.VerifyPassword(storedHash, password, authResponse, salt)
}

// VerifyPasswordWithHash verifies a password against stored hash only
// Used for testing and direct password verification
func (a *Authenticator) VerifyPasswordWithHash(storedHash, password string) bool {
	return utils.VerifyPasswordWithHash(storedHash, password)
}
