package session

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config"
)

var (
	// SessionMaxAge 会话最大存活时间
	SessionMaxAge = 24 * time.Hour
	// SessionGCInterval 会话GC间隔
	SessionGCInterval = time.Minute
)

type SessionMgr struct {
	driver SessionDriver
}

// InitSessionConfig 初始化会话配置
func InitSessionConfig(cfg *config.SessionConfig) {
	if cfg == nil {
		return
	}
	SessionMaxAge = cfg.MaxAge
	SessionGCInterval = cfg.GCInterval
}

func NewSessionMgr(ctx context.Context, driver SessionDriver) *SessionMgr {
	sess := &SessionMgr{
		driver: driver,
	}

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(SessionGCInterval)
			sess.GC()
		}
	}()
	return sess
}

func (m *SessionMgr) GetOrCreateSession(ctx context.Context, addr string, port string) (sess *Session, err error) {
	sessionID := m.GenerateSessionID(addr, port)
	sess, err = m.GetSession(ctx, sessionID)
	if err != nil && !strings.Contains(err.Error(), "session not found") {
		return
	}
	if sess != nil {
		return
	}
	sess, err = m.CreateSession(ctx, addr, port)
	if err != nil {
		return
	}
	return

}

func (m *SessionMgr) CreateSession(ctx context.Context, addr string, port string) (sess *Session, err error) {
	sessionID := m.GenerateSessionID(addr, port)
	sess = &Session{
		ID:         sessionID,
		Created:    time.Now(),
		LastUsed:   time.Now(),
		RemoteIP:   addr,
		RemotePort: port,
		driver:     m.driver,
	}
	sess.ThreadID = m.GetThreadId(ctx)
	err = m.driver.SetThreadId(ctx, sess.ThreadID, sess)
	if err != nil {
		return
	}
	err = m.driver.CreateSession(ctx, sess)
	if err != nil {
		return
	}
	return
}

func (m *SessionMgr) GenerateSessionID(addr string, port string) string {
	hash := md5.New()
	hash.Write([]byte(addr + port))
	return hex.EncodeToString(hash.Sum(nil))
}

func (m *SessionMgr) GetThreadId(ctx context.Context) uint32 {
	// 先生成一个随机数
	randId := uint32(1)
	for {
		// 看看这个随机数是否存在
		_, err := m.driver.GetThreadId(ctx, randId)
		if err != nil {
			return randId
		}

		randId++

		// 如果存在，则继续生成
		time.Sleep(time.Millisecond * 5)
	}
}

func (m *SessionMgr) GetSession(ctx context.Context, sessionID string) (sess *Session, err error) {
	sess, err = m.driver.GetSession(ctx, sessionID)
	if err != nil {
		return
	}

	sess.driver = m.driver
	return
}

func (m *SessionMgr) DeleteSession(ctx context.Context, sessionID string) error {
	return m.driver.DeleteSession(ctx, sessionID)
}

func (m *SessionMgr) GetSessions(ctx context.Context) (sesses []*Session, err error) {
	sesses, err = m.driver.GetSessions(ctx)
	if err != nil {
		return
	}

	for _, sess := range sesses {
		sess.driver = m.driver
	}

	return
}

// GC 清理过期会话
func (m *SessionMgr) GC() (err error) {
	now := time.Now()
	expiredAt := now.Add(-SessionMaxAge)
	ctx := context.Background()
	sessions, err := m.GetSessions(ctx)
	if err != nil {
		return err
	}

	for _, s := range sessions {
		if s.LastUsed.Before(expiredAt) {
			if err := m.DeleteSession(ctx, s.ID); err != nil {
				// 删除失败， 打印日志
				log.Printf("delete session %s failed: %v", s.ID, err)
				continue
			}
		}
	}

	return
}

type Session struct {
	driver     SessionDriver
	ID         string    `json:"id"`
	ThreadID   uint32    `json:"thread_id"`
	User       string    `json:"user"`
	Created    time.Time `json:"created"`
	LastUsed   time.Time `json:"last_used"`
	RemoteIP   string    `json:"remote_ip"`
	RemotePort string    `json:"remote_port"`
	SequenceID uint8     `json:"sequence_id"` // 添加序列号字段
}

// Get 获取会话值
func (s *Session) Get(key string) (val any, err error) {
	val, err = s.driver.GetKey(context.Background(), s.ID, key)
	return
}

// Set 设置会话值
func (s *Session) Set(key string, val any) error {
	return s.driver.SetKey(context.Background(), s.ID, key, val)
}

// Delete 删除会话值
func (s *Session) Delete(key string) error {
	return s.driver.DeleteKey(context.Background(), s.ID, key)
}

// SetUser 设置用户名
func (s *Session) SetUser(user string) {
	s.User = user
}

// SetVariable 设置会话变量（用于 SET 命令）
func (s *Session) SetVariable(name string, value interface{}) error {
	// 使用 "@@" 或 "@" 前缀标识变量
	key := "var:" + name
	log.Printf("设置会话变量: %s = %v", name, value)
	return s.Set(key, value)
}

// GetVariable 获取会话变量
func (s *Session) GetVariable(name string) (interface{}, error) {
	key := "var:" + name
	val, err := s.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// DeleteVariable 删除会话变量
func (s *Session) DeleteVariable(name string) error {
	key := "var:" + name
	return s.Delete(key)
}

// GetAllVariables 获取所有会话变量
func (s *Session) GetAllVariables() (map[string]interface{}, error) {
	vars := make(map[string]interface{})
	
	// 获取所有键
	keys, err := s.driver.GetAllKeys(context.Background(), s.ID)
	if err != nil {
		return nil, err
	}
	
	// 过滤出以 "var:" 开头的键
	for _, key := range keys {
		if strings.HasPrefix(key, "var:") {
			// 移除 "var:" 前缀
			varName := key[4:]
			val, err := s.Get(key)
			if err == nil {
				vars[varName] = val
			}
		}
	}
	
	return vars, nil
}

// GetNextSequenceID 获取下一个序列号并递增
func (s *Session) GetNextSequenceID() uint8 {
	s.SequenceID++
	return s.SequenceID
}

// ResetSequenceID 重置序列号为0
func (s *Session) ResetSequenceID() {
	s.SequenceID = 0
}

type SessionDriver interface {
	CreateSession(ctx context.Context, session *Session) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	GetSessions(ctx context.Context) ([]*Session, error)
	DeleteSession(ctx context.Context, sessionID string) error
	GetKey(ctx context.Context, sessionID string, key string) (any, error)
	SetKey(ctx context.Context, sessionID string, key string, value any) error
	DeleteKey(ctx context.Context, sessionID string, key string) error
	GetAllKeys(ctx context.Context, sessionID string) ([]string, error)
	Touch(ctx context.Context, sessionID string) error
	GetThreadId(ctx context.Context, threadID uint32) (uint32, error)
	SetThreadId(ctx context.Context, threadID uint32, sess *Session) error
	DeleteThreadId(ctx context.Context, threadID uint32) error
}
