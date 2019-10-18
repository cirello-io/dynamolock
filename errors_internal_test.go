/*
Copyright 2019 github.com/ucirello

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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/xerrors"
)

func TestLockNotGrantedError(t *testing.T) {
	t.Parallel()
	t.Run("simply not granted", func(t *testing.T) {
		notGranted := &LockNotGrantedError{msg: "not granted"}
		if !isLockNotGrantedError(notGranted) {
			t.Error("mismatched error type check: ", notGranted)
		}
		vanilla := errors.New("vanilla error")
		if isLockNotGrantedError(vanilla) {
			t.Error("mismatched error type check: ", vanilla)
		}
	})
	t.Run("not granted with cause", func(t *testing.T) {
		const expectedAge = 5 * time.Minute
		notGranted := &LockNotGrantedError{
			msg:   "not granted with cause",
			cause: &TimeoutError{expectedAge},
		}
		var errTimeout *TimeoutError
		if !xerrors.As(notGranted, &errTimeout) {
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
	awserr := awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "conditional check failed", vanilla)
	if err, ok := parseDynamoDBError(awserr, "").(*LockNotGrantedError); err == nil || !ok {
		t.Error("wrong error wrapping (awserr):", err)
	}
}
