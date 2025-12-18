package durabletask

import "github.com/microsoft/durabletask-go/backend"

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
