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

package dynamolock_test

import (
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/xerrors"
)

func TestIssue56(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(mustAWSNewSession(t), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	lockClient, err := dynamolock.New(svc,
		"locksIssue56",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(100*time.Millisecond),
		dynamolock.WithOwnerName("TestIssue56"),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	lockClient.CreateTable("locksIssue56",
		dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
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
					dynamolock.WithAdditionalTimeToWaitForLock(expectedTimeoutMinimumAge),
					dynamolock.WithRefreshPeriod(100*time.Millisecond),
				)
				switch err {
				case nil:
					count++
					lockClient.ReleaseLock(lock)
					return
				default:
					var errTimeout *dynamolock.TimeoutError
					if !xerrors.As(err, &errTimeout) {
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
