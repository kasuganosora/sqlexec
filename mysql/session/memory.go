package session

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

type MemoryDriver struct {
	SessionMap map[string]*Session
	Values     map[string]map[string]any
	Mutex      sync.RWMutex
}

func NewMemoryDriver() *MemoryDriver {
	d := &MemoryDriver{}
	d.SessionMap = make(map[string]*Session)
	d.Values = make(map[string]map[string]any)
	return d
}

func (d *MemoryDriver) CreateSession(ctx context.Context, session *Session) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	d.SessionMap[session.ID] = session
	d.Values[session.ID] = make(map[string]any)
	return nil
}

func (d *MemoryDriver) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	session, ok := d.SessionMap[sessionID]
	if !ok {
		return nil, errors.New("session not found")
	}
	return session, nil
}

func (d *MemoryDriver) GetSessions(ctx context.Context) ([]*Session, error) {
	sessions := make([]*Session, 0, len(d.SessionMap))
	for _, session := range d.SessionMap {
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (d *MemoryDriver) DeleteSession(ctx context.Context, sessionID string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	delete(d.SessionMap, sessionID)
	delete(d.Values, sessionID)
	return nil
}

func (d *MemoryDriver) GetKey(ctx context.Context, sessionID string, key string) (any, error) {
	_, ok := d.SessionMap[sessionID]
	if !ok {
		delete(d.SessionMap, sessionID)
		delete(d.Values, sessionID)
		return nil, errors.New("session not found")
	}
	values, ok := d.Values[sessionID]
	if !ok {
		return nil, errors.New("values not found")
	}
	val, ok := values[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	d.Touch(ctx, sessionID)

	return val, nil
}

func (d *MemoryDriver) SetKey(ctx context.Context, sessionID string, key string, value any) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	_, ok := d.SessionMap[sessionID]
	if !ok {
		delete(d.SessionMap, sessionID)
		delete(d.Values, sessionID)
		return errors.New("session not found")
	}
	d.Values[sessionID][key] = value
	d.Touch(ctx, sessionID)
	return nil
}

func (d *MemoryDriver) DeleteKey(ctx context.Context, sessionID string, key string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	_, ok := d.SessionMap[sessionID]
	if !ok {
		delete(d.SessionMap, sessionID)
		delete(d.Values, sessionID)
		return errors.New("session not found")
	}
	delete(d.Values[sessionID], key)
	d.Touch(ctx, sessionID)
	return nil
}

func (d *MemoryDriver) Touch(ctx context.Context, sessionID string) error {
	_, ok := d.SessionMap[sessionID]
	if !ok {
		delete(d.SessionMap, sessionID)
		delete(d.Values, sessionID)
		return errors.New("session not found")
	}
	d.SessionMap[sessionID].LastUsed = time.Now()
	return nil
}

func (d *MemoryDriver) GetThreadId(ctx context.Context, threadID uint32) (uint32, error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	threadIdMap, ok := d.Values["thread_id"]
	if !ok {
		d.Values["thread_id"] = make(map[string]any)
	}

	threadIdStr := strconv.FormatUint(uint64(threadID), 10)
	threadId, ok := threadIdMap[threadIdStr]
	if !ok {
		return 0, errors.New("thread id not found")
	}
	return threadId.(uint32), nil
}

func (d *MemoryDriver) SetThreadId(ctx context.Context, threadID uint32, sess *Session) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	threadIdMap, ok := d.Values["thread_id"]
	if !ok {
		d.Values["thread_id"] = make(map[string]any)
	}
	threadIdMap[strconv.FormatUint(uint64(threadID), 10)] = sess
	return nil
}

func (d *MemoryDriver) DeleteThreadId(ctx context.Context, threadID uint32) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	threadIdMap, ok := d.Values["thread_id"]
	if !ok {
		return errors.New("thread id not found")
	}
	delete(threadIdMap, strconv.FormatUint(uint64(threadID), 10))
	return nil
}
