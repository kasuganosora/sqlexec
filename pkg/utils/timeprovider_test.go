package utils

import (
	"testing"
	"time"
)

func TestSystemTimeProvider_Now(t *testing.T) {
	provider := NewSystemTimeProvider()

	before := time.Now()
	result := provider.Now()
	after := time.Now()

	if result.Before(before) || result.After(after) {
		t.Errorf("Now() = %v, want between %v and %v", result, before, after)
	}
}

func TestSystemTimeProvider_Since(t *testing.T) {
	provider := NewSystemTimeProvider()

	start := time.Now().Add(-1 * time.Second)
	duration := provider.Since(start)

	if duration < time.Second {
		t.Errorf("Since() = %v, want >= 1s", duration)
	}
}

func TestSystemTimeProvider_Until(t *testing.T) {
	provider := NewSystemTimeProvider()

	future := time.Now().Add(1 * time.Second)
	duration := provider.Until(future)

	if duration > time.Second {
		t.Errorf("Until() = %v, want <= 1s", duration)
	}
}

func TestFixedTimeProvider_Now(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	provider := NewFixedTimeProvider(fixed)

	// Multiple calls should return the same time
	result1 := provider.Now()
	result2 := provider.Now()

	if !result1.Equal(fixed) {
		t.Errorf("Now() = %v, want %v", result1, fixed)
	}

	if !result1.Equal(result2) {
		t.Error("multiple calls should return the same time")
	}
}

func TestFixedTimeProvider_SetTime(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	provider := NewFixedTimeProvider(fixed)

	newTime := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
	provider.SetTime(newTime)

	if !provider.Now().Equal(newTime) {
		t.Errorf("Now() = %v, want %v", provider.Now(), newTime)
	}
}

func TestFixedTimeProvider_Add(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	provider := NewFixedTimeProvider(fixed)

	provider.Add(1 * time.Hour)

	expected := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	if !provider.Now().Equal(expected) {
		t.Errorf("Now() = %v, want %v", provider.Now(), expected)
	}
}

func TestFixedTimeProvider_Sleep(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	provider := NewFixedTimeProvider(fixed)

	// Sleep should not advance time in FixedTimeProvider
	before := provider.Now()
	provider.Sleep(1 * time.Second)
	after := provider.Now()

	if !before.Equal(after) {
		t.Error("Sleep should not advance time in FixedTimeProvider")
	}
}

func TestMockTimeProvider_Now(t *testing.T) {
	provider := NewMockTimeProvider()

	// Initial time should be approximately now
	before := time.Now()
	result := provider.Now()
	after := time.Now()

	if result.Before(before.Add(-1*time.Second)) || result.After(after.Add(1*time.Second)) {
		t.Errorf("Now() = %v, want approximately current time", result)
	}
}

func TestMockTimeProvider_SetTime(t *testing.T) {
	provider := NewMockTimeProvider()

	newTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	provider.SetTime(newTime)

	if !provider.Now().Equal(newTime) {
		t.Errorf("Now() = %v, want %v", provider.Now(), newTime)
	}
}

func TestMockTimeProvider_Add(t *testing.T) {
	provider := NewMockTimeProvider()

	before := provider.Now()
	provider.Add(1 * time.Hour)
	after := provider.Now()

	if !after.Equal(before.Add(1 * time.Hour)) {
		t.Errorf("Add should advance time by 1 hour")
	}
}

func TestMockTimeProvider_Sleep(t *testing.T) {
	provider := NewMockTimeProvider()

	before := provider.Now()
	provider.Sleep(1 * time.Second)
	after := provider.Now()

	// Sleep should advance time by default (no hook set)
	if !after.Equal(before.Add(1 * time.Second)) {
		t.Errorf("Sleep should advance time, before=%v, after=%v", before, after)
	}
}

func TestMockTimeProvider_SleepWithHook(t *testing.T) {
	provider := NewMockTimeProvider()

	var sleptDuration time.Duration
	provider.SetSleepHook(func(d time.Duration) {
		sleptDuration = d
	})

	provider.Sleep(500 * time.Millisecond)

	if sleptDuration != 500*time.Millisecond {
		t.Errorf("expected sleep duration 500ms, got %v", sleptDuration)
	}
}

func TestGlobalTimeProvider(t *testing.T) {
	// Save and restore
	original := defaultTimeProvider
	defer func() {
		defaultTimeProvider = original
	}()

	// Test with fixed provider
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	SetTimeProvider(NewFixedTimeProvider(fixed))

	if !Now().Equal(fixed) {
		t.Errorf("global Now() = %v, want %v", Now(), fixed)
	}

	// Reset
	ResetTimeProvider()

	// After reset, should return approximately current time
	result := Now()
	if result.Year() < 2020 {
		t.Errorf("after reset, Now() should return current time, got %v", result)
	}
}

func TestGlobalTimeProvider_Nil(t *testing.T) {
	// Save and restore
	original := defaultTimeProvider
	defer func() {
		defaultTimeProvider = original
	}()

	// Set to nil should not change the provider
	SetTimeProvider(nil)
	// defaultTimeProvider should still be valid
	if defaultTimeProvider == nil {
		t.Error("SetTimeProvider(nil) should not set provider to nil")
	}
}

// TestTimeProviderInterface verifies all providers implement the interface
func TestTimeProviderInterface(t *testing.T) {
	var _ TimeProvider = NewSystemTimeProvider()
	var _ TimeProvider = NewFixedTimeProvider(time.Now())
	var _ TimeProvider = NewMockTimeProvider()
}
