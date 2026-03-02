/*
Copyright 2023 U. Cirello (cirello.io and github.com/cirello-io)

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

package sync

import (
	"testing"
)

func TestMap(t *testing.T) {
	var m Map[string, string]
	const (
		key   = "key"
		value = "value"
	)
	m.Store(key, value)
	got, ok := m.Load(key)
	if !ok || got != value {
		t.Fatal("failed to load key")
	}
	m.Range(func(foundKey, foundValue string) bool {
		if foundKey != key || foundValue != value {
			t.Fatal("range failed")
		}
		return false
	})
	{
		m.Delete(key)
		deletedValue, found := m.Load(key)
		if found || deletedValue == value {
			t.Fatal("should have failed to load key")
		}
	}
}
