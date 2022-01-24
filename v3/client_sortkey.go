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
	"context"
	"errors"
)

// ClientWithSortKey is a dynamoDB based distributed lock client, but with a required sort key.
type ClientWithSortKey struct{ *internalClient }

// NewWithSortKey creates a new dynamoDB based distributed lock client.
func NewWithSortKey(dynamoDB DynamoDBClient, tableName, sortKeyName string, opts ...ClientOption) (*ClientWithSortKey, error) {
	if sortKeyName == "" {
		return nil, errors.New("a sortKeyName must be supplied; use `Client` if you don't want a sort key")
	}

	internalClient, err := newInternal(dynamoDB, tableName, sortKeyName, opts...)

	if err != nil {
		return nil, err
	}

	return &ClientWithSortKey{internalClient}, nil
}

// AcquireLock holds the defined lock. The given context is passed
// down to the underlying dynamoDB call.
func (c *ClientWithSortKey) AcquireLock(ctx context.Context, partitionKey, sortKey string, opts ...AcquireLockOption) (*Lock, error) {
	return c.acquireLock(ctx, partitionKey, opts...)
}

// Get finds out who owns the given lock, but does not acquire the
// lock. It returns the metadata currently associated with the given lock. If
// the client currently has the lock, it will return the lock, and operations
// such as releaseLock will work. However, if the client does not have the lock,
// then operations like releaseLock will not work (after calling Get,
// the caller should check lockItem.isExpired() to figure out if it currently
// has the lock.) If the context is canceled, it is going to return the context
// error on local cache hit. The given context is passed down to the underlying
// dynamoDB call.
func (c *ClientWithSortKey) Get(ctx context.Context, partitionKey, sortKey string) (*Lock, error) {
	return c.get(ctx, partitionKey)
}
