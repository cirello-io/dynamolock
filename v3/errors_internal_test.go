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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestLockNotGrantedError(t *testing.T) {
	t.Parallel()
	t.Run("simply not granted", func(t *testing.T) {
		var errNotGranted *LockNotGrantedError
		notGranted := &LockNotGrantedError{msg: "not granted"}
		if !errors.As(notGranted, &errNotGranted) {
			t.Error("mismatched error type check: ", notGranted)
		}
		vanilla := errors.New("vanilla error")
		if errors.As(vanilla, &errNotGranted) {
			t.Error("mismatched error type check: ", vanilla)
		}
	})
	t.Run("not granted with cause", func(t *testing.T) {
		const expectedAge = 5 * time.Minute
		notGranted := &LockNotGrantedError{
			msg:   "not granted with cause",
			cause: &TimeoutError{expectedAge},
		}
		t.Log(notGranted.Error())
		var errTimeout *TimeoutError
		if !errors.As(notGranted, &errTimeout) {
			t.Fatal("expected to find TimeoutError")
		}
		if errTimeout.Age != expectedAge {
			t.Fatal("age information lost along the way")
		}
	})
}

func TestParseDynamoDBError(t *testing.T) {
	t.Parallel()

	vanilla := errors.New("root error")
	if err := parseDynamoDBError(vanilla, ""); err != vanilla {
		t.Error("wrong error wrapping (vanilla):", err)
	}
	msg := "conditional check failed"
	awserr := fmt.Errorf("envelope: %w", &types.ConditionalCheckFailedException{Message: &msg})
	if err, ok := parseDynamoDBError(awserr, "").(*LockNotGrantedError); err == nil || !ok {
		t.Error("wrong error wrapping (awserr):", err)
	}
}
