package api

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// LogLevel 日志级别
type LogLevel int

const (
	LogError LogLevel = iota
	LogWarn
	LogInfo
	LogDebug
)

// String 返回日志级别字符串
func (l LogLevel) String() string {
	switch l {
	case LogError:
		return "ERROR"
	case LogWarn:
		return "WARN"
	case LogInfo:
		return "INFO"
	case LogDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// Logger 日志接口
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	SetLevel(level LogLevel)
	GetLevel() LogLevel
}

// DefaultLogger 默认日志实现
type DefaultLogger struct {
	level  LogLevel
	mu     sync.Mutex
	output io.Writer
}

// NewDefaultLogger 创建默认日志
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		output: os.Stdout,
	}
}

// NewDefaultLoggerWithOutput 创建带输出的默认日志
func NewDefaultLoggerWithOutput(level LogLevel, output io.Writer) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		output: output,
	}
}

// SetLevel 设置日志级别
func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel 获取日志级别
func (l *DefaultLogger) GetLevel() LogLevel {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Debug 输出 DEBUG 级别日志
func (l *DefaultLogger) Debug(format string, args ...interface{}) {
	if !l.shouldLog(LogDebug) {
		return
	}
	l.log(LogDebug, format, args...)
}

// Info 输出 INFO 级别日志
func (l *DefaultLogger) Info(format string, args ...interface{}) {
	if !l.shouldLog(LogInfo) {
		return
	}
	l.log(LogInfo, format, args...)
}

// Warn 输出 WARN 级别日志
func (l *DefaultLogger) Warn(format string, args ...interface{}) {
	if !l.shouldLog(LogWarn) {
		return
	}
	l.log(LogWarn, format, args...)
}

// Error 输出 ERROR 级别日志
func (l *DefaultLogger) Error(format string, args ...interface{}) {
	if !l.shouldLog(LogError) {
		return
	}
	l.log(LogError, format, args...)
}

// shouldLog 判断是否应该输出日志
func (l *DefaultLogger) shouldLog(level LogLevel) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return level <= l.level
}

// log 实际日志输出
func (l *DefaultLogger) log(level LogLevel, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	message := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.output, "[%s] %s\n", level.String(), message)
}

// NoOpLogger 空日志实现（用于禁用日志）
type NoOpLogger struct{}

// NewNoOpLogger 创建空日志
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

func (l *NoOpLogger) Debug(format string, args ...interface{}) {}
func (l *NoOpLogger) Info(format string, args ...interface{}) {}
func (l *NoOpLogger) Warn(format string, args ...interface{}) {}
func (l *NoOpLogger) Error(format string, args ...interface{}) {}
func (l *NoOpLogger) SetLevel(level LogLevel)                {}
func (l *NoOpLogger) GetLevel() LogLevel                       { return LogInfo }
