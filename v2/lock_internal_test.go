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
	"sync"
	"testing"
	"time"
)

// RVN exposes internal record version number for testing only.
func (l *Lock) RVN() string {
	l.semaphore.Lock()
	defer l.semaphore.Unlock()
	return l.recordVersionNumber
}

func TestExpiredNilLock(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("unexpected panic:", r)
		}
	}()
	var l *Lock
	if !l.IsExpired() {
		t.Fatal("nil locks should report as expired")
	}
}

func TestLookupTimeAccess(t *testing.T) {
	stopChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	l := &Lock{}

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			l.updateRVN("1", time.Now(), 30*time.Second)
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			l.timeUntilDangerZoneEntered()
		}
	}()

	time.Sleep(2 * time.Second)
	close(stopChan)
	wg.Wait()
}
