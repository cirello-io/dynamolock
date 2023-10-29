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

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrReadOnlyLockHeartbeat indicates that the given *Lock is not really a lock,
// but a read-only copy from a Get call.
var ErrReadOnlyLockHeartbeat = errors.New("cannot send heartbeats to a read-only lock")

// ErrClientClosed reports the client cannot be used because it is already
// closed.
var ErrClientClosed = errors.New("client already closed")

// TimeoutError indicates that the dynamolock gave up acquiring the lock. It
// holds the length of the attempt that resulted in the error.
type TimeoutError struct {
	Age time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout: %s", e.Age)
}

// LockNotGrantedError indicates that an AcquireLock call has failed to
// establish a lock because of its current lifecycle state.
type LockNotGrantedError struct {
	msg   string
	cause error
}

func (e *LockNotGrantedError) Error() string {
	msg := e.msg
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg
}

// Unwrap reveals the underlying cause why the lock was not granted.
func (e *LockNotGrantedError) Unwrap() error {
	return e.cause
}

func parseDynamoDBError(err error, msg string) error {
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	if errors.As(err, &conditionalCheckFailedException) {
		return &LockNotGrantedError{
			msg:   msg,
			cause: conditionalCheckFailedException,
		}
	}
	return err
}
