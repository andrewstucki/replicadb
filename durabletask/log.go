package durabletask

import (
	"context"
	"time"

	"github.com/microsoft/durabletask-go/backend"
)

type noopLogger struct{}

// NoopLogger is a backend logger that logs nothing.
func NoopLogger() backend.Logger {
	return noopLogger{}
}

func (noopLogger) Debug(...any)          {}
func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Error(...any)          {}
func (noopLogger) Errorf(string, ...any) {}
func (noopLogger) Info(...any)           {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warn(...any)           {}
func (noopLogger) Warnf(string, ...any)  {}

// workers is how many workers an executor runs, each with a poller: orchestrations' and
// activities'.
const workers = 2

// stoppedPolling is what a durabletask-go worker logs as its poller's last act.
const stoppedPolling = "%v: stopped listening for new work items"

// pollersStopMost bounds how long shutting down waits for the pollers to say they have
// stopped, in case a version of durabletask-go no longer says so. Chosen here: a stopped
// poller says so at once, after the poll it is in.
const pollersStopMost = 5 * time.Second

// pollerLog is the workers' logger, hearing each poller stop.
type pollerLog struct {
	backend.Logger
	stopped chan struct{}
}

func (l *pollerLog) Infof(format string, v ...any) {
	l.Logger.Infof(format, v...)
	if format == stoppedPolling {
		select {
		case l.stopped <- struct{}{}:
		default:
		}
	}
}

// wait waits for n pollers to stop, up to most, and reports whether they did.
func (l *pollerLog) wait(ctx context.Context, n int, most time.Duration) bool {
	timer := time.NewTimer(most)
	defer timer.Stop()
	for range n {
		select {
		case <-l.stopped:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
	return true
}
