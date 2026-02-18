package statistics

import "fmt"

// debugEnabled controls whether debug logging is active.
// Default is false for production performance.
var debugEnabled = false

// SetDebug enables or disables debug logging for the statistics package.
func SetDebug(enabled bool) { debugEnabled = enabled }

// IsDebugEnabled returns whether debug logging is enabled.
func IsDebugEnabled() bool { return debugEnabled }

func debugf(format string, args ...interface{}) {
	if debugEnabled {
		fmt.Printf(format, args...)
	}
}

func debugln(args ...interface{}) {
	if debugEnabled {
		fmt.Println(args...)
	}
}
