package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDBConfig_QueryTimeout 测试DBConfig的超时配置
func TestDBConfig_QueryTimeout(t *testing.T) {
	tests := []struct {
		name         string
		config       *DBConfig
		expectedTime time.Duration
	}{
		{
			name: "默认配置",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				DebugMode:    false,
			},
			expectedTime: 0, // 默认不限制
		},
		{
			name: "设置超时30秒",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 30 * time.Second,
			},
			expectedTime: 30 * time.Second,
		},
		{
			name: "设置超时5分钟",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 5 * time.Minute,
			},
			expectedTime: 5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedTime, tt.config.QueryTimeout)
		})
	}
}

// TestSessionOptions_QueryTimeout 测试SessionOptions的超时配置
func TestSessionOptions_QueryTimeout(t *testing.T) {
	tests := []struct {
		name         string
		options      *SessionOptions
		expectedTime time.Duration
	}{
		{
			name: "默认Session配置",
			options: &SessionOptions{
				DataSourceName: "default",
				Isolation:      IsolationRepeatableRead,
				ReadOnly:       false,
				CacheEnabled:   true,
			},
			expectedTime: 0, // 默认不限制
		},
		{
			name: "Session设置超时10秒",
			options: &SessionOptions{
				DataSourceName: "default",
				Isolation:      IsolationRepeatableRead,
				ReadOnly:       false,
				CacheEnabled:   true,
				QueryTimeout:   10 * time.Second,
			},
			expectedTime: 10 * time.Second,
		},
		{
			name: "Session设置超时1分钟",
			options: &SessionOptions{
				DataSourceName: "default",
				Isolation:      IsolationRepeatableRead,
				ReadOnly:       false,
				CacheEnabled:   true,
				QueryTimeout:   1 * time.Minute,
			},
			expectedTime: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedTime, tt.options.QueryTimeout)
		})
	}
}

// TestSession_ThreadID 测试Session的ThreadID功能
func TestSession_ThreadID(t *testing.T) {
	tests := []struct {
		name     string
		threadID uint32
	}{
		{"ThreadID=0", 0},
		{"ThreadID=1", 1},
		{"ThreadID=123", 123},
		{"ThreadID=MAX_UINT32", 4294967295},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := &Session{}
			sess.SetThreadID(tt.threadID)
			assert.Equal(t, tt.threadID, sess.GetThreadID())

			// 验证CoreSession也设置了ThreadID
			if sess.coreSession != nil {
				assert.Equal(t, tt.threadID, sess.coreSession.GetThreadID())
			}
		})
	}
}

// TestNewDBWithQueryTimeout 测试创建带超时的DB
func TestNewDBWithQueryTimeout(t *testing.T) {
	tests := []struct {
		name        string
		config      *DBConfig
		expectError bool
	}{
		{
			name: "有效配置-不限制超时",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 0,
			},
			expectError: false,
		},
		{
			name: "有效配置-限制超时",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 30 * time.Second,
			},
			expectError: false,
		},
		{
			name: "有效配置-短超时",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 100 * time.Millisecond,
			},
			expectError: false,
		},
		{
			name: "有效配置-长超时",
			config: &DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: 1 * time.Hour,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := NewDB(tt.config)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, db)
				assert.Equal(t, tt.config.QueryTimeout, db.config.QueryTimeout)
			}
		})
	}
}

// TestErrorCode_Timeout 测试超时错误码
func TestErrorCode_Timeout(t *testing.T) {
	tests := []struct {
		name   string
		code   ErrorCode
		errMsg string
	}{
		{"超时错误", ErrCodeTimeout, "TIMEOUT"},
		{"查询被Kill错误", ErrCodeQueryKilled, "QUERY_KILLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewError(tt.code, "test error", nil)
			assert.True(t, IsErrorCode(err, tt.code))
			assert.Contains(t, err.Error(), string(tt.code))
		})
	}
}

// TestSession_QueryTimeoutOverride 测试Session覆盖DB超时
func TestSession_QueryTimeoutOverride(t *testing.T) {
	// 创建DB(设置超时60秒)
	db, err := NewDB(&DBConfig{
		CacheEnabled: false,
		QueryTimeout: 60 * time.Second,
	})
	assert.NoError(t, err)

	// 注册一个mock数据源
	ds := newMockDataSource()
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// 创建Session(设置超时10秒,覆盖DB配置)
	sess := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   true,
		QueryTimeout:   10 * time.Second,
	})

	// 验证Session超时覆盖了DB超时
	assert.NotNil(t, sess.coreSession)
	assert.Equal(t, 10*time.Second, sess.coreSession.GetQueryTimeout())

	// 创建另一个Session(不设置超时,使用DB配置)
	sess2 := db.Session()
	assert.NotNil(t, sess2.coreSession)
	assert.Equal(t, 60*time.Second, sess2.coreSession.GetQueryTimeout())
}

// TestSessionWithoutTimeout 测试不设置超时的Session
func TestSessionWithoutTimeout(t *testing.T) {
	// 创建DB(不设置超时)
	db, err := NewDB(nil)
	assert.NoError(t, err)

	// 注册一个mock数据源
	ds := newMockDataSource()
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// 创建Session(不设置超时)
	sess := db.Session()
	assert.NotNil(t, sess.coreSession)
	assert.Equal(t, time.Duration(0), sess.coreSession.GetQueryTimeout())
}

// TestSession_QueryTimeoutPriority 测试Session超时优先级
func TestSession_QueryTimeoutPriority(t *testing.T) {
	tests := []struct {
		name            string
		dbTimeout       time.Duration
		sessionTimeout  time.Duration
		expectedTimeout time.Duration
	}{
		{
			name:            "DB和Session都设置,优先Session",
			dbTimeout:       30 * time.Second,
			sessionTimeout:  10 * time.Second,
			expectedTimeout: 10 * time.Second,
		},
		{
			name:            "DB设置,Session不设置,使用DB",
			dbTimeout:       30 * time.Second,
			sessionTimeout:  0,
			expectedTimeout: 30 * time.Second,
		},
		{
			name:            "DB和Session都不设置,不限制",
			dbTimeout:       0,
			sessionTimeout:  0,
			expectedTimeout: 0,
		},
		{
			name:            "DB不设置,Session设置,使用Session",
			dbTimeout:       0,
			sessionTimeout:  15 * time.Second,
			expectedTimeout: 15 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建DB
			db, err := NewDB(&DBConfig{
				CacheEnabled: true,
				CacheSize:    1000,
				CacheTTL:     300,
				QueryTimeout: tt.dbTimeout,
			})
			assert.NoError(t, err)

			// 注册一个mock数据源
			ds := newMockDataSource()
			err = db.RegisterDataSource("test", ds)
			assert.NoError(t, err)

			// 创建Session
			sess := db.SessionWithOptions(&SessionOptions{
				DataSourceName: "test",
				Isolation:      IsolationRepeatableRead,
				ReadOnly:       false,
				CacheEnabled:   true,
				QueryTimeout:   tt.sessionTimeout,
			})

			// 验证超时设置
			assert.NotNil(t, sess.coreSession)
			assert.Equal(t, tt.expectedTimeout, sess.coreSession.GetQueryTimeout())
		})
	}
}
