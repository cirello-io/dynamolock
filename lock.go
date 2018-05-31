/*
Copyright 2015 github.com/ucirello

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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Lock item properly speaking.
type Lock struct {
	semaphore sync.Mutex

	client       *Client
	partitionKey string
	sortKey      *string

	data                []byte
	ownerName           string
	deleteLockOnRelease bool
	isReleased          bool

	lookupTime           time.Time
	recordVersionNumber  string
	leaseDuration        time.Duration
	additionalAttributes map[string]*dynamodb.AttributeValue
}

// Data returns the content of the lock, if any is available.
func (l *Lock) Data() []byte {
	if l == nil {
		return nil
	}
	return l.data
}

// Close releases the lock.
func (l *Lock) Close() {
	go l.client.ReleaseLock(l)
}

func (l *Lock) uniqueIdentifier() string {
	if l == nil {
		return ""
	}

	return l.partitionKey + aws.StringValue(l.sortKey)
}

// IsExpired returns if the lock is expired, released, or neither.
func (l *Lock) IsExpired() bool {
	if l == nil {
		return true
	}

	if l.isReleased {
		return true
	}
	return time.Since(l.lookupTime) > l.leaseDuration
}

func (l *Lock) updateRVN(rvn string, lastUpdate time.Time, leaseDurationToEnsure time.Duration) {
	if l == nil {
		return
	}
	l.recordVersionNumber = rvn
	l.lookupTime = lastUpdate
	l.leaseDuration = leaseDurationToEnsure
}

// OwnerName returns the lock's owner.
func (l *Lock) OwnerName() string {
	return l.ownerName
}
