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
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestCancelationWithoutHearbeat(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic found when closing client without heartbeat")
		}
	}()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.DisableHeartbeat(),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.Close(context.Background())
}

func TestHeartbeatHandover(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatHandover#1"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
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

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(context.Background(), "kirk",
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
			if err := c.SendHeartbeat(context.Background(), lockedItem); err != nil {
				t.Log("sendHeartbeat error:", err)
			}
			time.Sleep(2 * time.Second)
		}
		time.Sleep(1 * time.Second)
		if err := c.SendHeartbeat(context.Background(), lockedItem); err == nil {
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
	_, err = c2.AcquireLock(context.Background(), "kirk",
		dynamolock.WithData(data2),
		dynamolock.ReplaceData(),
	)
	if err == nil {
		t.Fatal("the first concurrent acquire lock should fail")
	}

	time.Sleep(6 * time.Second)
	lockedItem2, err := c2.AcquireLock(context.Background(), "kirk",
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

func TestHeartbeatDataOps(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	newClient := func() (*dynamolock.Client, error) {
		return dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatDataOps#1"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
	}
	c, err := newClient()
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

	t.Run("delete data on heartbeat", func(t *testing.T) {
		const lockName = "delete-data-on-heartbeat"
		data := []byte("some content a")
		lockedItem, err := c.AcquireLock(context.Background(), lockName, dynamolock.WithData(data), dynamolock.ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		if err := c.SendHeartbeat(context.Background(), lockedItem, dynamolock.DeleteData()); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		gotItem, err := c2.Get(context.Background(), lockName)
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
		lockedItem, err := c.AcquireLock(context.Background(), lockName, dynamolock.WithData(data), dynamolock.ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		replacedData := []byte("some content b")
		if err := c.SendHeartbeat(context.Background(), lockedItem, dynamolock.ReplaceHeartbeatData(replacedData)); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		gotItem, err := c2.Get(context.Background(), lockName)
		if err != nil {
			t.Fatal("cannot lock: ", err)
		}

		if !bytes.Equal(gotItem.Data(), replacedData) {
			t.Error("data not replaced on heartbeat")
		}
	})

	t.Run("racy heartbeats", func(t *testing.T) {
		const lockName = "racy-heartbeats"
		lockedItemAlpha, err := c.AcquireLock(context.Background(), lockName)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SendHeartbeat(context.Background(), lockedItemAlpha); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		c2, err := newClient()
		if err != nil {
			t.Fatal("cannot open second lock client")
		}
		lockedItemBeta, err := c2.AcquireLock(context.Background(), lockName, dynamolock.WithAdditionalTimeToWaitForLock(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := c2.SendHeartbeat(context.Background(), lockedItemBeta); err != nil {
			t.Fatal("cannot send heartbeat: ", err)
		}

		if err := c.SendHeartbeat(context.Background(), lockedItemAlpha); err == nil {
			t.Fatal("concurrent heartbeats should knock one another out")
		} else {
			t.Log("send heartbeat for lockedItemAlpha:", err)
		}
	})
}
