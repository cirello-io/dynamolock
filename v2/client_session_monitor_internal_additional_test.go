package dynamolock

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

// These tests directly exercise branches inside lockSessionMonitorChecker that
// are harder to reach through only the public API.

func TestLockSessionMonitorChecker_ExpiredEarlyExit(t *testing.T) {
	t.Parallel()

	c := &Client{
		logger: &contextLoggerAdapter{
			logger: log.New(io.Discard, "", 0),
		},
	}

	lock := &Lock{
		sessionMonitor: &sessionMonitor{safeTime: 0, callback: func() {}},
		lookupTime:     time.Now().Add(-time.Hour),
		leaseDuration:  time.Nanosecond,
	}

	// Should hit immediate isExpired branch and return quickly.
	c.lockSessionMonitorChecker(context.Background(), "mon-expired", lock)
	// Give the goroutine a brief moment to run.
	time.Sleep(10 * time.Millisecond)
}

func TestLockSessionMonitorChecker_ContextCanceled(t *testing.T) {
	t.Parallel()

	c := &Client{
		logger: &contextLoggerAdapter{
			logger: log.New(io.Discard, "", 0),
		},
	}

	lock := &Lock{
		sessionMonitor: &sessionMonitor{safeTime: 10 * time.Second, callback: func() {}},
		lookupTime:     time.Now(),
		leaseDuration:  time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.lockSessionMonitorChecker(ctx, "mon-cancel", lock)
	// Cancel soon so the goroutine returns via ctx.Done() branch.
	time.Sleep(10 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}
