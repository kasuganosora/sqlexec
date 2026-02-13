package utils

import "time"

// TimeProvider defines an interface for time-related operations.
// This abstraction allows for easier testing by enabling time injection.
type TimeProvider interface {
	// Now returns the current time
	Now() time.Time

	// Since returns the time elapsed since t
	Since(t time.Time) time.Duration

	// Until returns the duration until t
	Until(t time.Time) time.Duration

	// Sleep pauses the current goroutine for at least the duration d
	Sleep(d time.Duration)
}

// SystemTimeProvider is the default implementation using real system time.
type SystemTimeProvider struct{}

// NewSystemTimeProvider creates a new SystemTimeProvider
func NewSystemTimeProvider() *SystemTimeProvider {
	return &SystemTimeProvider{}
}

// Now returns the current system time
func (p *SystemTimeProvider) Now() time.Time {
	return time.Now()
}

// Since returns the time elapsed since t
func (p *SystemTimeProvider) Since(t time.Time) time.Duration {
	return time.Since(t)
}

// Until returns the duration until t
func (p *SystemTimeProvider) Until(t time.Time) time.Duration {
	return time.Until(t)
}

// Sleep pauses the current goroutine for at least the duration d
func (p *SystemTimeProvider) Sleep(d time.Duration) {
	time.Sleep(d)
}

// FixedTimeProvider returns a fixed time for testing purposes.
type FixedTimeProvider struct {
	fixedTime time.Time
}

// NewFixedTimeProvider creates a provider that always returns the given time
func NewFixedTimeProvider(fixedTime time.Time) *FixedTimeProvider {
	return &FixedTimeProvider{
		fixedTime: fixedTime,
	}
}

// Now returns the fixed time
func (p *FixedTimeProvider) Now() time.Time {
	return p.fixedTime
}

// Since returns the duration since the fixed time
func (p *FixedTimeProvider) Since(t time.Time) time.Duration {
	return p.fixedTime.Sub(t)
}

// Until returns the duration until the fixed time
func (p *FixedTimeProvider) Until(t time.Time) time.Duration {
	return t.Sub(p.fixedTime)
}

// Sleep does nothing in the fixed time provider
func (p *FixedTimeProvider) Sleep(d time.Duration) {
	// No-op for testing
}

// SetTime allows updating the fixed time (useful for advancing time in tests)
func (p *FixedTimeProvider) SetTime(t time.Time) {
	p.fixedTime = t
}

// Add advances the fixed time by the given duration
func (p *FixedTimeProvider) Add(d time.Duration) {
	p.fixedTime = p.fixedTime.Add(d)
}

// MockTimeProvider allows full control over time for testing.
type MockTimeProvider struct {
	currentTime time.Time
	sleepHook   func(d time.Duration)
}

// NewMockTimeProvider creates a new mock time provider
func NewMockTimeProvider() *MockTimeProvider {
	return &MockTimeProvider{
		currentTime: time.Now(),
		sleepHook:   nil,
	}
}

// Now returns the mock current time
func (p *MockTimeProvider) Now() time.Time {
	return p.currentTime
}

// Since returns the duration since t
func (p *MockTimeProvider) Since(t time.Time) time.Duration {
	return p.currentTime.Sub(t)
}

// Until returns the duration until t
func (p *MockTimeProvider) Until(t time.Time) time.Duration {
	return t.Sub(p.currentTime)
}

// Sleep calls the sleep hook if set, otherwise advances time
func (p *MockTimeProvider) Sleep(d time.Duration) {
	if p.sleepHook != nil {
		p.sleepHook(d)
	} else {
		p.currentTime = p.currentTime.Add(d)
	}
}

// SetTime sets the mock current time
func (p *MockTimeProvider) SetTime(t time.Time) {
	p.currentTime = t
}

// Add advances the mock time by the given duration
func (p *MockTimeProvider) Add(d time.Duration) {
	p.currentTime = p.currentTime.Add(d)
}

// SetSleepHook sets a custom function to be called on Sleep
func (p *MockTimeProvider) SetSleepHook(hook func(d time.Duration)) {
	p.sleepHook = hook
}

// Global default time provider
var defaultTimeProvider TimeProvider = NewSystemTimeProvider()

// GetTimeProvider returns the default time provider
func GetTimeProvider() TimeProvider {
	return defaultTimeProvider
}

// SetTimeProvider sets the default time provider (useful for testing)
func SetTimeProvider(provider TimeProvider) {
	if provider != nil {
		defaultTimeProvider = provider
	}
}

// ResetTimeProvider resets to the system time provider
func ResetTimeProvider() {
	defaultTimeProvider = NewSystemTimeProvider()
}

// Now returns the current time using the default provider
func Now() time.Time {
	return defaultTimeProvider.Now()
}

// Since returns the time elapsed since t using the default provider
func Since(t time.Time) time.Duration {
	return defaultTimeProvider.Since(t)
}

// Until returns the duration until t using the default provider
func Until(t time.Time) time.Duration {
	return defaultTimeProvider.Until(t)
}

// Sleep pauses for the given duration using the default provider
func Sleep(d time.Duration) {
	defaultTimeProvider.Sleep(d)
}
