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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// LockNotGrantedError indicates that an AcquireLock call has failed to
// establish a lock because of its current lifecycle state.
type LockNotGrantedError struct {
	msg string
}

func (e *LockNotGrantedError) Error() string {
	return e.msg
}

func isLockNotGrantedError(err error) bool {
	_, ok := err.(*LockNotGrantedError)
	return ok
}

func parseDynamoDBError(err error, msg string) error {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case dynamodb.ErrCodeConditionalCheckFailedException:
			return &LockNotGrantedError{msg + ": " + aerr.Error()}
		}
	}
	return err
}
