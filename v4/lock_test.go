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

package dynamolock_test

import (
	"testing"

	"cirello.io/dynamolock/v4"
)

func TestNilLock(t *testing.T) {
	t.Parallel()
	var l *dynamolock.Lock
	if l.Data() != nil {
		t.Fatal("nil locks should return nil data")
	}
	if !l.IsExpired() {
		t.Fatal("nil locks should report as expired")
	}
	if l.OwnerName() != "" {
		t.Fatal("nil locks should report no owner")
	}
}

func TestEmptyLock(t *testing.T) {
	t.Parallel()
	l := &dynamolock.Lock{}
	if l.Data() != nil {
		t.Fatal("nil locks should return nil data")
	}
	if !l.IsExpired() {
		t.Fatal("nil locks should report as expired")
	}
	if l.OwnerName() != "" {
		t.Fatal("nil locks should report no owner")
	}
}
