/*
Copyright 2024 U. Cirello (cirello.io and github.com/cirello-io)

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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Lock item properly speaking.
type Lock struct {
	semaphore sync.Mutex

	client       *Client
	partitionKey string

	data                []byte
	ownerName           string
	deleteLockOnRelease bool
	isReleased          bool

	lookupTime           time.Time
	recordVersionNumber  string
	leaseDuration        time.Duration
	additionalAttributes map[string]types.AttributeValue
}

// Data returns the content of the lock, if any is available.
func (l *Lock) Data() []byte {
	if l == nil {
		return nil
	}
	return l.data
}

func (l *Lock) uniqueIdentifier() string {
	return l.partitionKey
}

// IsExpired returns if the lock is expired, released, or neither.
func (l *Lock) IsExpired() bool {
	if l == nil {
		return true
	}
	l.semaphore.Lock()
	defer l.semaphore.Unlock()
	return l.isExpired()
}

func (l *Lock) isExpired() bool {
	if l.isReleased {
		return true
	}
	return time.Since(l.lookupTime) > l.leaseDuration
}

func (l *Lock) updateRVN(rvn string, lastUpdate time.Time, leaseDuration time.Duration) {
	l.recordVersionNumber = rvn
	l.lookupTime = lastUpdate
	l.leaseDuration = leaseDuration
}

// OwnerName returns the lock's owner.
func (l *Lock) OwnerName() string {
	if l == nil {
		return ""
	}
	return l.ownerName
}

// AdditionalAttributes returns the lock's additional data stored during
// acquisition.
func (l *Lock) AdditionalAttributes() map[string]types.AttributeValue {
	addAttr := make(map[string]types.AttributeValue)
	if l != nil {
		for k, v := range l.additionalAttributes {
			addAttr[k] = v
		}
	}
	return addAttr
}

var (
	ErrCannotReleaseNullLock = errors.New("cannot release null lock item")
	ErrOwnerMismatched       = errors.New("lock owner mismatched")
)
