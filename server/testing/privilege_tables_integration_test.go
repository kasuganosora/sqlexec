package testing

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/server/acl"
)

// TestPrivilegeTablesIntegration 集成测试：复现权限表查询问题
// 这个测试直接测试provider的行为
func TestPrivilegeTablesIntegration(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()

	// 步骤1：创建ACL Manager（模拟server.go中的初始化）
	aclMgr, err := acl.NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create ACLManager: %v", err)
	}

	if !aclMgr.IsLoaded() {
		t.Fatal("ACL Manager should be loaded")
	}

	t.Logf("✓ ACL Manager created with %d users", len(aclMgr.GetUsers()))

	// 步骤2：创建DataSourceManager
	dsManager := application.NewDataSourceManager()

	// 步骤3：创建Provider（问题所在：这里需要NewProviderWithACL）
	// 模拟optimized_executor.go第741行的问题：provider := information_schema.NewProvider(e.dsManager)
	// 这里没有传入ACL Manager！

	// 测试情况1：使用不带ACL的Provider（问题场景）
	t.Run("WithoutACL", func(t *testing.T) {
		provider := information_schema.NewProvider(dsManager)

		// 尝试获取USER_PRIVILEGES表
		table, err := provider.GetVirtualTable("USER_PRIVILEGES")
		if err != nil {
			// 如果没有ACL Manager，表可能不存在
			t.Logf("✅ ISSUE CONFIRMED: USER_PRIVILEGES not available without ACL Manager: %v", err)
			return
		}

		if table == nil {
			t.Log("✅ ISSUE CONFIRMED: USER_PRIVILEGES is nil without ACL Manager")
			return
		}

		// 如果表存在，查询它
		result, err := table.Query(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}

		if result.Total == 0 {
			t.Log("✅ ISSUE CONFIRMED: USER_PRIVILEGES returns 0 rows without ACL Manager")
		} else {
			t.Logf("USER_PRIVILEGES returned %d rows without ACL Manager", result.Total)
		}
	})

	// 测试情况2：使用带ACL的Provider（正确场景）
	t.Run("WithACL", func(t *testing.T) {
		// 创建adapter将acl.ACLManager适配为information_schema.ACLManager
		aclAdapter := &ACLManagerAdapter{aclMgr: aclMgr}

		provider := information_schema.NewProviderWithACL(dsManager, aclAdapter)

		// 获取USER_PRIVILEGES表
		table, err := provider.GetVirtualTable("USER_PRIVILEGES")
		if err != nil {
			t.Fatalf("Failed to get USER_PRIVILEGES: %v", err)
		}

		if table == nil {
			t.Fatal("USER_PRIVILEGES should not be nil with ACL Manager")
		}

		// 查询表
		result, err := table.Query(context.Background(), nil, nil)
		if err != nil {
			t.Fatalf("Query error: %v", err)
		}

		// 验证结果
		if result == nil {
			t.Fatal("Result should not be nil")
		}

		if result.Total == 0 {
			t.Error("✅ ISSUE: USER_PRIVILEGES should return rows with ACL Manager")
			return
		}

		if len(result.Rows) == 0 {
			t.Error("✅ ISSUE: USER_PRIVILEGES should have rows with ACL Manager")
			return
		}

		// 验证包含root用户
		hasRoot := false
		for _, row := range result.Rows {
			if grantee, ok := row["GRANTEE"].(string); ok {
				if grantee == "'root'@'%'" {
					hasRoot = true
					t.Logf("✓ Found root user: %v", row)
					break
				}
			}
		}

		if !hasRoot {
			t.Error("✅ ISSUE: Root user should be found with ACL Manager")
		}

		t.Logf("✓ USER_PRIVILEGES returned %d rows (with ACL Manager)", result.Total)
	})
}

// ACLManagerAdapter 将acl.ACLManager适配为information_schema.ACLManager接口
type ACLManagerAdapter struct {
	aclMgr *acl.ACLManager
}

func (a *ACLManagerAdapter) CheckPermission(user, host, permission, db, table, column string) bool {
	// 将permission字符串转换为acl.PermissionType
	privType := acl.PermissionType(permission)
	return a.aclMgr.CheckPermission(user, host, privType, db, table, column)
}

func (a *ACLManagerAdapter) HasGrantOption(user, host string) bool {
	return a.aclMgr.HasGrantOption(user, host)
}

func (a *ACLManagerAdapter) GetUsers() []*acl.User {
	return a.aclMgr.GetUsers()
}

func (a *ACLManagerAdapter) IsLoaded() bool {
	return a.aclMgr.IsLoaded()
}
