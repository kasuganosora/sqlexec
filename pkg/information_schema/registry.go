package information_schema

import (
	"sync"

	"github.com/kasuganosora/sqlexec/server/acl"
)

// 全局ACL Manager（用于权限表）
var (
	globalACLManager *acl.ACLManager
	aclManagerMutex  sync.RWMutex
)

// RegisterACLManager 注册全局ACL Manager
func RegisterACLManager(aclMgr *acl.ACLManager) {
	aclManagerMutex.Lock()
	defer aclManagerMutex.Unlock()
	globalACLManager = aclMgr
}

// GetACLManager 获取全局ACL Manager
func GetACLManager() *acl.ACLManager {
	aclManagerMutex.RLock()
	defer aclManagerMutex.RUnlock()
	return globalACLManager
}

// GetACLManagerAdapter 获取适配后的ACL Manager（实现ACLManager接口）
func GetACLManagerAdapter() ACLManager {
	aclManagerMutex.RLock()
	defer aclManagerMutex.RUnlock()
	if globalACLManager == nil {
		return nil
	}
	return &aclManagerAdapter{aclMgr: globalACLManager}
}

// aclManagerAdapter 将acl.ACLManager适配为ACLManager接口
type aclManagerAdapter struct {
	aclMgr *acl.ACLManager
}

func (a *aclManagerAdapter) CheckPermission(user, host, permission, db, table, column string) bool {
	privType := acl.PermissionType(permission)
	return a.aclMgr.CheckPermission(user, host, privType, db, table, column)
}

func (a *aclManagerAdapter) HasGrantOption(user, host string) bool {
	return a.aclMgr.HasGrantOption(user, host)
}

func (a *aclManagerAdapter) GetUsers() []*acl.User {
	return a.aclMgr.GetUsers()
}

func (a *aclManagerAdapter) IsLoaded() bool {
	return a.aclMgr.IsLoaded()
}
