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
	"context"
	"time"
)

// Heartbeat is a helper function that assist with keeping a lock fresh.
func Heartbeat(ctx context.Context, c *Client, lockItem *Lock, opts ...SendHeartbeatOption) error {
	for {
		lockItem.semaphore.Lock()
		nextCheck := time.Until(lockItem.lookupTime) / 2
		lockItem.semaphore.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextCheck):
		}
		err := c.SendHeartbeat(ctx, lockItem, opts...)
		if errCtx := ctx.Err(); errCtx != nil {
			return ctx.Err()
		} else if err != nil {
			return err
		}
	}
}
