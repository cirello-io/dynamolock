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
	"errors"
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
		Endpoint: aws.String(DynamoTestHost()),
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
	_, _ = c.CreateTable("locks",
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
		Endpoint: aws.String(DynamoTestHost()),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks-monitor",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorRemoveBeforeExpiration#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	_, _ = c.CreateTable("locks-monitor",
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
	go func() {
		_ = lockedItem.Close()
	}()

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
		Endpoint: aws.String(DynamoTestHost()),
		Region:   aws.String("us-west-2"),
	})
	lease := 3 * time.Second
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(lease),
		dynamolock.WithOwnerName("TestSessionMonitorFullCycle#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	_, _ = c.CreateTable("locks",
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
	safe := time.Second
	lockedItem, err := c.AcquireLock("sessionMonitor",
		dynamolock.WithSessionMonitor(safe, func() {
			mu.Lock()
			sessionMonitorWasTriggered = true
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// our lease is 3 minutes and safe period is 1 minute.  We should be alerted with 1-minute
	// left, not after 1 minute.  This first test is to verify we are not alerted after 1st minute.
	padding := time.Millisecond * 100
	firstSleep := safe + padding
	time.Sleep(firstSleep)
	if ok, err := lockedItem.IsAlmostExpired(); err == nil && ok {
		t.Fatal("lock should not be in the danger zone")
	} else if err != nil {
		t.Fatal("cannot assert whether the lock is almost expired:", err)
	}

	// sleep long enough to get within the final minute (should get callback)
	time.Sleep(lease - firstSleep - safe)
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

	// sleep remaining time of lease
	expiration := time.Until(time.Now().Add(lease))
	time.Sleep(expiration + padding)
	if ok, err := lockedItem.IsAlmostExpired(); !errors.Is(err, dynamolock.ErrLockAlreadyReleased) {
		t.Error("lockedItem should be already expired:", ok, err)
	}
}
