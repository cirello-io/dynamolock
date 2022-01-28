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

package dynamolock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSessionMonitor(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks", "key",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitor#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(context.Background(), "uhura",
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
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks-monitor", "key",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorRemoveBeforeExpiration#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(context.Background(), "scotty",
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
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks", "key",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorFullCycle#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	var (
		mu                         sync.Mutex
		sessionMonitorWasTriggered bool
	)
	lockedItem, err := c.AcquireLock(context.Background(), "sessionMonitor",
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
