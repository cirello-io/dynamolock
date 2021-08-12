/*
Copyright 2019 github.com/ucirello and cirello.io

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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestIssue56(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()

	svc := mustNewDynamoDBClient(t)
	lockClient, err := New(svc,
		"locksIssue56",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(100*time.Millisecond),
		WithOwnerName("TestIssue56"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	lockClient.CreateTable("locksIssue56",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	var (
		wg    sync.WaitGroup
		count = 0
	)

	const (
		expectedTimeoutMinimumAge = 15 * time.Second
		expectedCount             = 100
	)

	for i := 0; i < expectedCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lock, err := lockClient.AcquireLock(
					"key",
					WithAdditionalTimeToWaitForLock(expectedTimeoutMinimumAge),
					WithRefreshPeriod(100*time.Millisecond),
				)
				switch err {
				case nil:
					count++
					lockClient.ReleaseLock(lock)
					return
				default:
					var errTimeout *TimeoutError
					if !errors.As(err, &errTimeout) {
						t.Error("unexpected error:", err)
						return
					}
					if errTimeout.Age < expectedTimeoutMinimumAge {
						t.Error("timeout happened too fast:", errTimeout.Age)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	if count != expectedCount {
		t.Fatal("did not achieve expected count:", count)
	}
}
