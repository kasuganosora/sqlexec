package session

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"
	"strings"
	"time"
)

const (
	SessionMaxAge = 24 * time.Hour
)

type SessionMgr struct {
	driver SessionDriver
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
			time.Sleep(time.Minute) // 每分钟清理一次
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
	for {
		randId := uint32(1)
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

func (s *Session) Get(key string) (val any, err error) {
	val, err = s.driver.GetKey(context.Background(), s.ID, key)
	return
}

func (s *Session) Set(key string, val any) error {
	return s.driver.SetKey(context.Background(), s.ID, key, val)
}

func (s *Session) Delete(key string) error {
	return s.driver.DeleteKey(context.Background(), s.ID, key)
}

func (s *Session) SetUser(user string) {
	s.User = user
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
	Touch(ctx context.Context, sessionID string) error
	GetThreadId(ctx context.Context, threadID uint32) (uint32, error)
	SetThreadId(ctx context.Context, threadID uint32, sess *Session) error
	DeleteThreadId(ctx context.Context, threadID uint32) error
}
