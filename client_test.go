/*
Copyright 2015 github.com/ucirello

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
	"net"
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func isDynamoLockAvailable(t *testing.T) {
	_, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		t.Skipf("cannot dial to dynamoDB: %v", err)
	}
}
func TestClientBasicFlow(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(session.New(), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#1"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("spock",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}

	t.Log("cleaning lock")
	success, err := c.ReleaseLock(lockedItem)
	if !success {
		t.Fatal("lost lock before release")
	}
	if err != nil {
		t.Fatal("error releasing lock:", err)
	}
	t.Log("done")

	data2 := []byte("some content b")
	lockedItem2, err := c.AcquireLock("spock",
		dynamolock.WithData(data2),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content (again):", string(lockedItem2.Data()))
	if got := string(lockedItem2.Data()); string(data2) != got {
		t.Error("losing information inside lock storage, wanted:", string(data2), " got:", got)
	}

	c2, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	data3 := []byte("some content c")
	lockedItem3, err := c2.AcquireLock("spock",
		dynamolock.WithData(data3),
		dynamolock.ReplaceData(),
	)
	if err == nil {
		t.Fatal("expected to fail to grab the lock")
	}

	c.ReleaseLock(lockedItem, dynamolock.WithDeleteLock(true))

	lockedItem3, err = c2.AcquireLock("spock",
		dynamolock.WithData(data3),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("lock content (competing client):", string(lockedItem3.Data()))
	if got := string(lockedItem3.Data()); string(data3) != got {
		t.Error("losing information inside lock storage, wanted:", string(data3), " got:", got)
	}
}

func TestHeartbeatHandover(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(session.New(), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatHandover#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("kirk",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i < 3; i++ {
			if err := c.SendHeartbeat(lockedItem); err != nil {
				t.Log("sendHeartbeat error:", err)
			}
			time.Sleep(2 * time.Second)
		}
		time.Sleep(1 * time.Second)
		if err := c.SendHeartbeat(lockedItem); err == nil {
			t.Log("the heartbeat must fail after lock is lost")
		}
	}()

	c2, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatHandover#2"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	data2 := []byte("some content b")
	_, err = c2.AcquireLock("kirk",
		dynamolock.WithData(data2),
		dynamolock.ReplaceData(),
	)
	if err == nil {
		t.Fatal("the first concurrent acquire lock should fail")
	}

	time.Sleep(6 * time.Second)
	lockedItem2, err := c2.AcquireLock("kirk",
		dynamolock.WithData(data2),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content (competing client):", string(lockedItem2.Data()))
	if got := string(lockedItem2.Data()); string(data2) != got {
		t.Error("losing information inside lock storage, wanted:", string(data2), " got:", got)
	}

	wg.Wait()
}

func TestReadLockContent(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(session.New(), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContent#1"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("mccoy",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}

	c2, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContent#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get("mccoy")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
}

func TestSessionMonitor(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(session.New(), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitor#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	var sessionMonitorWasTriggered bool
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("uhura",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithSessionMonitor(500*time.Millisecond, func() {
			sessionMonitorWasTriggered = true
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(4 * time.Second)

	if !sessionMonitorWasTriggered {
		t.Fatal("session monitor was not triggered")
	}

	t.Log("isExpired", lockedItem.IsExpired())
}

func TestSessionMonitorRemoveBeforeExpiration(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.New(session.New(), &aws.Config{
		Endpoint: aws.String("http://localhost:8000/"),
		Region:   aws.String("us-west-2"),
	})
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitorRemoveBeforeExpiration#1"),
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	var sessionMonitorWasTriggered bool
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("scotty",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithSessionMonitor(50*time.Millisecond, func() {
			sessionMonitorWasTriggered = true
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockedItem.Close()

	if sessionMonitorWasTriggered {
		t.Fatal("session monitor must not be triggered")
	}

	t.Log("isExpired", lockedItem.IsExpired())
}
