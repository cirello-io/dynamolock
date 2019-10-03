/*
Copyright 2019 github.com/ucirello

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
)

func TestSessionMonitor(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(mustAWSNewSession(t), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitor#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("uhura",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithSessionMonitor(500*time.Millisecond, func() {
			mu.Lock()
			sessionMonitorWasTriggered = true
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(4 * time.Second)

	mu.Lock()
	smwt := sessionMonitorWasTriggered
	mu.Unlock()
	if !smwt {
		t.Fatal("session monitor was not triggered")
	}

	t.Log("isExpired", lockedItem.IsExpired())
}

func TestSessionMonitorRemoveBeforeExpiration(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(mustAWSNewSession(t), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorRemoveBeforeExpiration#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("scotty",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithSessionMonitor(50*time.Millisecond, func() {
			mu.Lock()
			sessionMonitorWasTriggered = true
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	go lockedItem.Close()

	mu.Lock()
	triggered := sessionMonitorWasTriggered
	mu.Unlock()
	if triggered {
		t.Fatal("session monitor must not be triggered")
	}

	t.Log("isExpired", lockedItem.IsExpired())
}

func TestSessionMonitorFullCycle(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(mustAWSNewSession(t), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorFullCycle#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	lockedItem, err := c.AcquireLock("sessionMonitor",
		dynamolock.WithSessionMonitor(1*time.Second, func() {
			mu.Lock()
			sessionMonitorWasTriggered = true
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	if ok, err := lockedItem.IsAlmostExpired(); err == nil && !ok {
		t.Fatal("lock is not yet in the danger zone")
	} else if err != nil {
		t.Fatal("cannot assert whether the lock is almost expired:", err)
	}

	mu.Lock()
	smwt := sessionMonitorWasTriggered
	mu.Unlock()
	if !smwt {
		t.Fatal("session monitor was not triggered")
	}

	time.Sleep(2 * time.Second)
	if ok, err := lockedItem.IsAlmostExpired(); err != dynamolock.ErrLockAlreadyReleased {
		t.Error("lockedItem should be already expired:", ok, err)
	}
}
