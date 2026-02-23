package handler

import (
	"fmt"
	"sync"
)

// HandlerRegistry 命令处理器注册中心（并发安全）
type HandlerRegistry struct {
	handlers map[uint8]Handler
	mu       sync.RWMutex
	logger   Logger
}

// NewHandlerRegistry 创建注册中心
func NewHandlerRegistry(logger Logger) *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[uint8]Handler),
		logger:   logger,
	}
}

// Register 注册处理器（并发安全）
func (r *HandlerRegistry) Register(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("cannot register nil handler")
	}

	cmd := handler.Command()
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[cmd]; exists {
		return fmt.Errorf("handler for command 0x%02x already registered", cmd)
	}
	r.handlers[cmd] = handler
	if r.logger != nil {
		r.logger.Printf("Registered handler: %s (0x%02x)", handler.Name(), cmd)
	}
	return nil
}

// Get 获取处理器（并发安全）
func (r *HandlerRegistry) Get(commandType uint8) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[commandType]
	return handler, exists
}

// Handle 处理命令
func (r *HandlerRegistry) Handle(ctx *HandlerContext, commandType uint8, packet interface{}) error {
	handler, exists := r.Get(commandType)
	if !exists {
		if r.logger != nil {
			r.logger.Printf("[ERROR] No handler registered for command 0x%02x", commandType)
		}
		return fmt.Errorf("no handler registered for command 0x%02x", commandType)
	}
	return handler.Handle(ctx, packet)
}

// List 列出所有注册的处理器
func (r *HandlerRegistry) List() []Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handlers := make([]Handler, 0, len(r.handlers))
	for _, h := range r.handlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// Count 获取已注册的处理器数量
func (r *HandlerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}
