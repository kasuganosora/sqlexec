package testing

import (
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/server/acl"
)

// TestPrivilegeTablesReproduction 复现权限表查询问题
// 这个测试验证ACL Manager在不同dataDir下的行为
func TestPrivilegeTablesReproduction(t *testing.T) {
	// 测试场景1：使用空字符串创建ACL Manager（模拟问题场景）
	t.Run("EmptyDataDir", func(t *testing.T) {
		// 这模拟了如果server.go中dataDir为空字符串的情况
		aclMgr, err := acl.NewACLManager("")
		if err != nil {
			t.Fatalf("NewACLManager with empty dataDir failed: %v", err)
		}

		// 在server.go中，如果NewACLManager失败，会将aclManager设置为nil
		// 这会导致权限表无法正常工作
		if aclMgr == nil {
			// 这就是问题的根源！如果ACL Manager为nil，权限表无法工作
			t.Error("ACL Manager is nil - this causes privilege tables to return empty results")
			return
		}

		if !aclMgr.IsLoaded() {
			t.Error("ACL Manager should be loaded")
		}

		// 验证有默认用户
		users := aclMgr.GetUsers()
		if len(users) == 0 {
			t.Error("Should have at least one user (root)")
		}

		// 验证root用户存在
		hasRoot := false
		for _, user := range users {
			if user.User == "root" {
				hasRoot = true
				break
			}
		}
		if !hasRoot {
			t.Error("Root user should exist")
		}

		t.Logf("ACL Manager with empty dataDir: %d users loaded", len(users))
	})

	// 测试场景2：使用"."作为dataDir（当前目录）
	t.Run("DotAsDataDir", func(t *testing.T) {
		tmpDir := t.TempDir()

		// 切换到临时目录
		originalDir, _ := os.Getwd()
		defer os.Chdir(originalDir)
		os.Chdir(tmpDir)

		// 使用"."创建ACL Manager（这是server.go第87行的修复）
		aclMgr, err := acl.NewACLManager(".")
		if err != nil {
			t.Fatalf("NewACLManager with '.' dataDir failed: %v", err)
		}

		if aclMgr == nil {
			t.Fatal("ACL Manager should not be nil")
		}

		if !aclMgr.IsLoaded() {
			t.Error("ACL Manager should be loaded")
		}

		users := aclMgr.GetUsers()
		if len(users) == 0 {
			t.Error("Should have at least one user (root)")
		}

		t.Logf("ACL Manager with '.' dataDir: %d users loaded", len(users))
	})

	// 测试场景3：使用绝对路径作为dataDir
	t.Run("AbsolutePathDataDir", func(t *testing.T) {
		tmpDir := t.TempDir()

		// 使用绝对路径创建ACL Manager
		aclMgr, err := acl.NewACLManager(tmpDir)
		if err != nil {
			t.Fatalf("NewACLManager with absolute path failed: %v", err)
		}

		if aclMgr == nil {
			t.Fatal("ACL Manager should not be nil")
		}

		if !aclMgr.IsLoaded() {
			t.Error("ACL Manager should be loaded")
		}

		users := aclMgr.GetUsers()
		if len(users) == 0 {
			t.Error("Should have at least one user (root)")
		}

		// 验证文件在正确的位置创建
		usersFile := tmpDir + "/users.json"
		if _, err := os.Stat(usersFile); os.IsNotExist(err) {
			t.Errorf("users.json should be created at %s", usersFile)
		}

		t.Logf("ACL Manager with absolute path: %d users loaded", len(users))
	})
}

// TestServerInitializationACL 测试Server初始化时ACL Manager的行为
func TestServerInitializationACL(t *testing.T) {
	tmpDir := t.TempDir()

	// 切换到临时目录
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// 模拟Server初始化中的ACL Manager创建
	// 在server.go第87行：dataDir := "."
	// 在server.go第88行：aclManager, err := acl.NewACLManager(dataDir)
	// 在server.go第89-92行：如果错误，将aclManager设置为nil

	// 测试1：正常情况 - dataDir为"."
	t.Run("NormalInitialization", func(t *testing.T) {
		dataDir := "."
		aclManager, err := acl.NewACLManager(dataDir)
		if err != nil {
			// 在server.go中，这里会将aclManager设置为nil
			t.Errorf("ACL Manager initialization failed: %v", err)
			aclManager = nil
		}

		if aclManager == nil {
			// 这就是问题的根源！如果ACL Manager为nil，权限表无法工作
			t.Error("ACL Manager is nil - this causes privilege tables to return empty results")
		}

		if aclManager != nil && !aclManager.IsLoaded() {
			t.Error("ACL Manager should be loaded")
		}

		if aclManager != nil {
			t.Logf("ACL Manager initialized successfully with %d users", len(aclManager.GetUsers()))
		}
	})

	// 测试2：dataDir为空字符串
	t.Run("EmptyStringInitialization", func(t *testing.T) {
		// 模拟server.go第87行如果是：dataDir := ""
		dataDir := ""
		aclManager, err := acl.NewACLManager(dataDir)
		if err != nil {
			// 在server.go中，这里会将aclManager设置为nil
			t.Errorf("ACL Manager initialization failed with empty string: %v", err)
			aclManager = nil
		}

		// 验证空字符串会被转换为"."
		// acl.NewACLManager内部第33-35行会处理空字符串：if dataDir == "" { dataDir = "." }
		// 所以应该仍然创建成功
		if aclManager == nil {
			// 如果为nil，说明初始化失败，权限表将返回空结果
			t.Error("ACL Manager is nil - privilege tables will return empty results")
		}

		if aclManager != nil && !aclManager.IsLoaded() {
			t.Error("ACL Manager should be loaded even with empty string")
		}

		if aclManager != nil {
			t.Logf("ACL Manager initialized successfully with empty string: %d users", len(aclManager.GetUsers()))
		}
	})
}

// TestPrivilegeTablesDataFiles 测试数据文件的创建和加载
func TestPrivilegeTablesDataFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// 切换到临时目录
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	// 创建ACL Manager
	aclMgr, err := acl.NewACLManager(".")
	if err != nil {
		t.Fatalf("Failed to create ACLManager: %v", err)
	}

	// 验证文件是否被创建
	usersFile := "./users.json"
	permsFile := "./permissions.json"

	if _, err := os.Stat(usersFile); os.IsNotExist(err) {
		t.Error("users.json should be created")
	}

	if _, err := os.Stat(permsFile); os.IsNotExist(err) {
		t.Error("permissions.json should be created")
	}

	// 验证users.json包含root用户
	users := aclMgr.GetUsers()
	if len(users) == 0 {
		t.Fatal("Should have at least one user")
	}

	// 验证root用户有权限
	var rootUser *acl.User
	for _, user := range users {
		if user.User == "root" {
			rootUser = user
			break
		}
	}

	if rootUser == nil {
		t.Fatal("Root user should exist")
	}

	// 验证root用户有权限
	hasPrivilege := false
	privilegeCount := 0
	for _, granted := range rootUser.Privileges {
		if granted {
			hasPrivilege = true
			privilegeCount++
		}
	}

	if !hasPrivilege {
		t.Error("Root user should have privileges")
	}

	t.Logf("Root user has %d privileges", privilegeCount)

	// 测试重新加载
	aclMgr2, err := acl.NewACLManager(".")
	if err != nil {
		t.Fatalf("Failed to reload ACLManager: %v", err)
	}

	users2 := aclMgr2.GetUsers()
	if len(users2) != len(users) {
		t.Errorf("Reloaded users count mismatch: %d vs %d", len(users2), len(users))
	}

	t.Logf("Successfully reloaded ACL Manager with %d users", len(users2))
}

// TestACLManagerNilBehavior 测试ACL Manager为nil时的行为
func TestACLManagerNilBehavior(t *testing.T) {
	// 这个测试验证：当ACL Manager为nil时，GetUsers()等方法应该如何处理
	var aclMgr *acl.ACLManager = nil

	// 如果ACL Manager为nil，调用GetUsers()会导致panic
	// 所以在information_schema的权限表中，需要先检查aclMgr == nil
	// 参见privileges.go第41行：if t.aclMgr == nil || !t.aclMgr.IsLoaded()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Calling GetUsers() on nil ACL Manager caused panic (expected): %v", r)
		}
	}()

	users := aclMgr.GetUsers()
	_ = users // 避免未使用变量警告
	// 如果执行到这里，说明没有panic（不太可能）
}

// TestRootUserPrivileges 验证root用户的权限配置
func TestRootUserPrivileges(t *testing.T) {
	tmpDir := t.TempDir()

	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	os.Chdir(tmpDir)

	aclMgr, err := acl.NewACLManager(".")
	if err != nil {
		t.Fatalf("Failed to create ACLManager: %v", err)
	}

	users := aclMgr.GetUsers()
	if len(users) == 0 {
		t.Fatal("Should have at least one user")
	}

	// 查找root用户
	var rootUser *acl.User
	for _, user := range users {
		if user.User == "root" && user.Host == "%" {
			rootUser = user
			break
		}
	}

	if rootUser == nil {
		t.Fatal("Root user should exist with host='%'")
	}

	// 验证root用户有所有权限
	expectedPrivileges := acl.AllPermissionTypes()
	for _, expectedPriv := range expectedPrivileges {
		granted, exists := rootUser.Privileges[string(expectedPriv)]
		if !exists {
			t.Errorf("Root user should have privilege: %s", expectedPriv)
		}
		if !granted {
			t.Errorf("Root user should have privilege granted: %s", expectedPriv)
		}
	}

	t.Logf("Root user has all %d expected privileges", len(expectedPrivileges))
}
