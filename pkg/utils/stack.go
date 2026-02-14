package utils

import (
	"path/filepath"
	"runtime"
	"strings"
)

// CaptureStackTrace captures the current call stack trace
// skip indicates how many callers to skip (0 = include CaptureStackTrace itself)
func CaptureStackTrace(skip int) []string {
	const maxDepth = 32
	pcs := make([]uintptr, maxDepth)
	n := runtime.Callers(skip+2, pcs) // +2 to skip CaptureStackTrace and runtime.Callers
	if n == 0 {
		return nil
	}

	frames := runtime.CallersFrames(pcs[:n])
	var stack []string

	for {
		frame, more := frames.Next()
		// Skip runtime internal frames
		if strings.HasPrefix(frame.Function, "runtime.") {
			if !more {
				break
			}
			continue
		}

		// Format: file:line function
		entry := formatStackEntry(frame)
		stack = append(stack, entry)

		if !more {
			break
		}
	}

	return stack
}

// CaptureStackTraceWithDepth captures the call stack with a custom maximum depth
func CaptureStackTraceWithDepth(skip, maxDepth int) []string {
	if maxDepth <= 0 {
		maxDepth = 32
	}

	pcs := make([]uintptr, maxDepth)
	n := runtime.Callers(skip+2, pcs)
	if n == 0 {
		return nil
	}

	frames := runtime.CallersFrames(pcs[:n])
	var stack []string

	for {
		frame, more := frames.Next()
		if strings.HasPrefix(frame.Function, "runtime.") {
			if !more {
				break
			}
			continue
		}

		entry := formatStackEntry(frame)
		stack = append(stack, entry)

		if !more {
			break
		}
	}

	return stack
}

// formatStackEntry formats a single stack frame entry
func formatStackEntry(frame runtime.Frame) string {
	// Get relative path from project root
	file := frame.File
	if idx := strings.Index(file, "/pkg/"); idx > 0 {
		file = file[idx+1:] // Keep pkg/... path
	} else if idx := strings.Index(file, "\\pkg\\"); idx > 0 {
		file = file[idx+1:]
	} else {
		file = filepath.Base(file)
	}

	// Simplify function name
	fn := frame.Function
	if idx := strings.LastIndex(fn, "/"); idx > 0 {
		fn = fn[idx+1:]
	}

	return fn + "() at " + file + ":" + itoa(frame.Line)
}

// itoa converts int to string without importing strconv
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	negative := n < 0
	if negative {
		n = -n
	}

	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	if negative {
		digits = append([]byte{'-'}, digits...)
	}

	return string(digits)
}

// GetCallerInfo returns information about the caller at the specified skip level
// skip=0 returns the caller of GetCallerInfo
func GetCallerInfo(skip int) (function, file string, line int) {
	pcs := make([]uintptr, 1)
	n := runtime.Callers(skip+2, pcs)
	if n == 0 {
		return "", "", 0
	}

	frame, _ := runtime.CallersFrames(pcs).Next()
	return frame.Function, frame.File, frame.Line
}

// GetCallerFunction returns the name of the calling function
func GetCallerFunction(skip int) string {
	fn, _, _ := GetCallerInfo(skip)
	return fn
}
