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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSessionMonitor(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
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
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
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
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
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
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
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
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
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
	_, _ = c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
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
	if ok, err := lockedItem.IsAlmostExpired(); !errors.Is(err, dynamolock.ErrLockAlreadyReleased) {
		t.Error("lockedItem should be already expired:", ok, err)
	}
}

func TestSessionMonitorMissedCall(t *testing.T) {
	t.Parallel()

	cases := []struct {
		leaseDuration   time.Duration
		heartbeatPeriod time.Duration
	}{
		{6 * time.Second, 1 * time.Second},
		{15 * time.Second, 1 * time.Second},
		{15 * time.Second, 3 * time.Second},
		{20 * time.Second, 5 * time.Second},
	}
	for _, tt := range cases {
		tt := tt
		safeZone := tt.leaseDuration - (3 * tt.heartbeatPeriod)
		t.Run(fmt.Sprintf("%s/%s/%s", tt.leaseDuration, tt.heartbeatPeriod, safeZone), func(t *testing.T) {
			t.Parallel()
			lockName := randStr()
			t.Log("lockName:", lockName)
			cfg, proxyCloser := proxyConfig(t)
			svc := dynamodb.NewFromConfig(cfg)
			logger := &bufferedLogger{}
			c, err := dynamolock.New(svc,
				"locks",
				dynamolock.WithLeaseDuration(tt.leaseDuration),
				dynamolock.WithOwnerName("TestSessionMonitorMissedCall#1"),
				dynamolock.WithHeartbeatPeriod(tt.heartbeatPeriod),
				dynamolock.WithPartitionKeyName("key"),
				dynamolock.WithLogger(logger),
			)
			if err != nil {
				t.Fatal(err)
			}

			t.Log("ensuring table exists")
			_, _ = c.CreateTable("locks",
				dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				}),
				dynamolock.WithCustomPartitionKeyName("key"),
			)

			sessionMonitorWasTriggered := make(chan struct{})

			data := []byte("some content a")
			lockedItem, err := c.AcquireLock(lockName,
				dynamolock.WithData(data),
				dynamolock.ReplaceData(),
				dynamolock.WithSessionMonitor(safeZone, func() {
					close(sessionMonitorWasTriggered)
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("lock acquired, closing proxy")
			proxyCloser()
			t.Log("proxy closed")

			t.Log("waiting", tt.leaseDuration)
			select {
			case <-time.After(tt.leaseDuration):
				t.Error("session monitor was not triggered")
			case <-sessionMonitorWasTriggered:
				t.Log("session monitor was triggered")
			}
			t.Log("isExpired", lockedItem.IsExpired())

			t.Log(logger.String())
		})
	}
}

type bufferedLogger struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	logger *log.Logger
}

func (bl *bufferedLogger) String() string {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.buf.String()
}

func (bl *bufferedLogger) Println(a ...any) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.logger == nil {
		bl.logger = log.New(&bl.buf, "", 0)
	}
	bl.logger.Println(a...)
}

type bufferedContextLogger struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	logger *log.Logger
}

func (bl *bufferedContextLogger) String() string {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.buf.String()
}

func (bl *bufferedContextLogger) Println(_ context.Context, a ...any) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	if bl.logger == nil {
		bl.logger = log.New(&bl.buf, "", 0)
	}
	bl.logger.Println(a...)
}
