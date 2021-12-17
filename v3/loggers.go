/*
Copyright 2021 U. Cirello (cirello.io and github.com/cirello-io)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamolock

import "context"

type plainLogger struct {
	logger interface {
		Println(...interface{})
	}
}

func (p *plainLogger) Info(_ context.Context, v ...interface{}) {
	p.logger.Println(v)
}

func (p *plainLogger) Error(_ context.Context, v ...interface{}) {
	p.logger.Println(v)
}

// Logger defines the minimum desired logger interface for the lock client.
type Logger interface {
	Println(v ...interface{})
}

// LeveledLogger defines the minimum desired logger interface for the lock
// client.
type LeveledLogger interface {
	Info(v ...interface{})
	Error(v ...interface{})
}

// ContextLeveledLogger defines a logger interface that can be used to pass
// extra information to the implementation. For example, if you use zap, you may
// have extra fields you want to add to the log line. You can add those extra
// fields to the parent context of calls like AcquireLock, and then retrieve
// them in your implementation of ContextLeveledLogger.
type ContextLeveledLogger interface {
	Info(ctx context.Context, v ...interface{})
	Error(ctx context.Context, v ...interface{})
}

type contextLoggerAdapter struct {
	logger LeveledLogger
}

func (cla *contextLoggerAdapter) Info(_ context.Context, v ...interface{}) {
	cla.logger.Info(v)
}

func (cla *contextLoggerAdapter) Error(_ context.Context, v ...interface{}) {
	cla.logger.Error(v)
}
