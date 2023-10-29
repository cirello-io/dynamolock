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

import "sync"

type Map[K comparable, V any] struct {
	syncmap sync.Map
}

func (m *Map[K, V]) Store(key K, value V) {
	m.syncmap.Store(key, value)
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.syncmap.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (m *Map[K, V]) Delete(key K) {
	m.syncmap.Delete(key)
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.syncmap.Load(key)
	if v != nil {
		value = v.(V)
	}
	return value, ok
}
