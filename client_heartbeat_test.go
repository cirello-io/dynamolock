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

package dynamolock

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestCancelationWithoutHearbeat(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic found when closing client without heartbeat")
		}
	}()
	svc := mustNewDynamoDBClient(t)
	c, err := New(svc,
		"locks",
		DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestHeartbeatHandover(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()

	svc := mustNewDynamoDBClient(t)
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithOwnerName("TestHeartbeatHandover#1"),
		DisableHeartbeat(),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("kirk",
		WithData(data),
		ReplaceData(),
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

	c2, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestHeartbeatHandover#2"),
		DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}

	data2 := []byte("some content b")
	_, err = c2.AcquireLock("kirk",
		WithData(data2),
		ReplaceData(),
	)
	if err == nil {
		t.Fatal("the first concurrent acquire lock should fail")
	}

	time.Sleep(6 * time.Second)
	lockedItem2, err := c2.AcquireLock("kirk",
		WithData(data2),
		ReplaceData(),
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

func TestHeartbeatDataOps(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()

	svc := mustNewDynamoDBClient(t)
	newClient := func() (*Client, error) {
		return New(svc,
			"locks",
			WithLeaseDuration(3*time.Second),
			WithOwnerName("TestHeartbeatDataOps#1"),
			DisableHeartbeat(),
			WithPartitionKeyName("key"),
		)
	}
	c, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	t.Run("delete data on heartbeat", func(t *testing.T) {
		const lockName = "delete-data-on-heartbeat"
		data := []byte("some content a")
		lockedItem, err := c.AcquireLock(lockName, WithData(data), ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		if err := c.SendHeartbeat(lockedItem, DeleteData()); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		gotItem, err := c2.Get(lockName)
		if err != nil {
			t.Fatal("cannot lock: ", err)
		}

		if len(gotItem.Data()) != 0 {
			t.Error("data not deleted on heartbeat")
		}
	})

	t.Run("replace data on heartbeat", func(t *testing.T) {
		const lockName = "replace-data-on-heartbeat"
		data := []byte("some content a")
		lockedItem, err := c.AcquireLock(lockName, WithData(data), ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		replacedData := []byte("some content b")
		if err := c.SendHeartbeat(lockedItem, ReplaceHeartbeatData(replacedData)); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		gotItem, err := c2.Get(lockName)
		if err != nil {
			t.Fatal("cannot lock: ", err)
		}

		if !bytes.Equal(gotItem.Data(), replacedData) {
			t.Error("data not replaced on heartbeat")
		}
	})

	t.Run("racy heartbeats", func(t *testing.T) {
		const lockName = "racy-heartbeats"
		lockedItemAlpha, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SendHeartbeat(lockedItemAlpha); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		lockedItemBeta, err := c2.AcquireLock(lockName, WithAdditionalTimeToWaitForLock(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := c2.SendHeartbeat(lockedItemBeta); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		if err := c.SendHeartbeat(lockedItemAlpha); err == nil {
			t.Fatal("concurrent heartbeats should knock one another out")
		} else {
			t.Log("send heartbeat for lockedItemAlpha:", err)
		}
	})
}
