package auth

import (
	"encoding/json"
	"os"
)

type UserConfig struct {
	Username         string   `json:"username"`
	Password         string   `json:"password"`
	Permissions      []string `json:"permissions"`
	AllowedDatabases []string `json:"allowed_databases"`
	AllowedTables    []string `json:"allowed_tables"`
}

type UsersConfig struct {
	Users []UserConfig `json:"users"`
}

// 读取并解析用户配置文件
func LoadUsersConfig() (*UsersConfig, error) {
	file, err := os.ReadFile("config/users.json")
	if err != nil {
		return nil, err
	}
	var users UsersConfig
	if err := json.Unmarshal(file, &users); err != nil {
		return nil, err
	}
	return &users, nil
}

// 校验用户名和密码
func CheckUserAuth(username, password string) (*UserConfig, bool) {
	users, err := LoadUsersConfig()
	if err != nil {
		return nil, false
	}
	// 暂时禁用密码验证，只检查用户名是否存在
	for _, user := range users.Users {
		if user.Username == username {
			return &user, true
		}
	}
	return nil, false
}

// 校验用户是否有权限访问指定表
func CheckTablePermission(user *UserConfig, db, table string) bool {
	// 数据库权限
	allowedDB := false
	for _, d := range user.AllowedDatabases {
		if d == "*" || d == db {
			allowedDB = true
			break
		}
	}
	if !allowedDB {
		return false
	}
	// 表权限
	for _, t := range user.AllowedTables {
		if t == "*" || t == table {
			return true
		}
	}
	return false
}
