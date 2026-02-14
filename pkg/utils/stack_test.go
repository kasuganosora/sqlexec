package utils

import (
	"strings"
	"testing"
)

func TestCaptureStackTrace(t *testing.T) {
	stack := CaptureStackTrace(0)
	if len(stack) == 0 {
		t.Error("CaptureStackTrace() returned empty stack")
	}

	// Check that it contains this test function
	found := false
	for _, entry := range stack {
		if strings.Contains(entry, "TestCaptureStackTrace") {
			found = true
			break
		}
	}
	if !found {
		t.Error("CaptureStackTrace() should contain TestCaptureStackTrace")
	}
}

func TestCaptureStackTraceWithDepth(t *testing.T) {
	stack := CaptureStackTraceWithDepth(0, 5)
	// Should return at most 5 entries
	if len(stack) > 5 {
		t.Errorf("CaptureStackTraceWithDepth(depth=5) returned %d entries, want <= 5", len(stack))
	}
}

func TestGetCallerInfo(t *testing.T) {
	fn, file, line := GetCallerInfo(0)
	if fn == "" {
		t.Error("GetCallerInfo() returned empty function name")
	}
	if file == "" {
		t.Error("GetCallerInfo() returned empty file name")
	}
	if line == 0 {
		t.Error("GetCallerInfo() returned line 0")
	}
}

func TestGetCallerFunction(t *testing.T) {
	fn := GetCallerFunction(0)
	// skip=0 returns GetCallerFunction itself
	if !strings.Contains(fn, "GetCallerFunction") {
		t.Errorf("GetCallerFunction() = %q, should contain GetCallerFunction", fn)
	}

	// skip=1 returns the caller (this test function)
	fn = GetCallerFunction(1)
	if !strings.Contains(fn, "TestGetCallerFunction") {
		t.Errorf("GetCallerFunction(1) = %q, should contain TestGetCallerFunction", fn)
	}
}

func helperFunctionForStackTrace() []string {
	return CaptureStackTrace(1)
}

func TestCaptureStackTraceSkip(t *testing.T) {
	stack := helperFunctionForStackTrace()

	// Should skip helperFunctionForStackTrace, but still have entries
	if len(stack) == 0 {
		t.Error("CaptureStackTrace with skip should still return entries")
	}

	// Should contain TestCaptureStackTraceSkip
	found := false
	for _, entry := range stack {
		if strings.Contains(entry, "TestCaptureStackTraceSkip") {
			found = true
			break
		}
	}
	if !found {
		t.Error("CaptureStackTrace(skip=1) should contain TestCaptureStackTraceSkip")
	}
}
