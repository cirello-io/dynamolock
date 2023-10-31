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
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
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
	c.Close()
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
	_, _ = c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
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
	_, _ = c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	t.Run("delete data on heartbeat", func(t *testing.T) {
		const lockName = "delete-data-on-heartbeat"
		data := []byte("some content a")
		lockedItem, err := c.AcquireLock(lockName, dynamolock.WithData(data), dynamolock.ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		if err := c.SendHeartbeat(lockedItem, dynamolock.DeleteData()); err != nil {
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
		lockedItem, err := c.AcquireLock(lockName, dynamolock.WithData(data), dynamolock.ReplaceData())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("lock content:", string(lockedItem.Data()))
		if got := string(lockedItem.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}

		replacedData := []byte("some content b")
		if err := c.SendHeartbeat(lockedItem, dynamolock.ReplaceHeartbeatData(replacedData)); err != nil {
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
		lockedItemBeta, err := c2.AcquireLock(lockName, dynamolock.WithAdditionalTimeToWaitForLock(2*time.Second))
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

func TestHeartbeatReadOnlyLock(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	newClient := func() (*dynamolock.Client, error) {
		return dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatReadOnlyLock#1"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
	}
	c, err := newClient()
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

	const lockName = "readonly-lock"
	if _, err := c.AcquireLock(lockName); err != nil {
		t.Fatal(err)
	}

	roLockCache, err := c.Get(lockName)
	if err != nil {
		t.Fatal(err)
	}

	err = c.SendHeartbeat(roLockCache)
	if !errors.Is(err, dynamolock.ErrReadOnlyLockHeartbeat) {
		t.Fatal("expected heartbeat to fail a read-only lock (loaded from cache)")
	}

	c2, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	roLockDB, err := c2.Get(lockName)
	if err != nil {
		t.Fatal(err)
	}

	err = c2.SendHeartbeat(roLockDB)
	if !errors.Is(err, dynamolock.ErrReadOnlyLockHeartbeat) {
		t.Fatal("expected heartbeat to fail a read-only lock (loaded from DB)")
	}
}

type interceptedDynamoDBClient struct {
	dynamolock.DynamoDBClient

	createTablePre func(context.Context, *dynamodb.CreateTableInput, []func(*dynamodb.Options)) (context.Context, *dynamodb.CreateTableInput, []func(*dynamodb.Options))
	getItemPost    func(*dynamodb.GetItemOutput, error) (*dynamodb.GetItemOutput, error)
	updateItemPost func(*dynamodb.UpdateItemOutput, error) (*dynamodb.UpdateItemOutput, error)
}

func (m *interceptedDynamoDBClient) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if m.createTablePre != nil {
		ctx, params, optFns = m.createTablePre(ctx, params, optFns)
	}
	return m.DynamoDBClient.CreateTable(ctx, params, optFns...)
}

func (m *interceptedDynamoDBClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	out, err := m.DynamoDBClient.GetItem(ctx, params, optFns...)
	if m.getItemPost == nil {
		return out, err
	}
	return m.getItemPost(out, err)
}

func (m *interceptedDynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	out, err := m.DynamoDBClient.UpdateItem(ctx, params, optFns...)
	if m.updateItemPost == nil {
		return out, err
	}
	return m.updateItemPost(out, err)
}

func TestHeartbeatRetry(t *testing.T) {
	t.Parallel()
	t.Run("noRetry", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"noRetry",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#noRetry"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("noRetry",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		var failUpdateItemOnce sync.Once
		svc.updateItemPost = func(gio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			failUpdateItemOnce.Do(func() {
				t.Log("NETWORK FAILURE")
				gio = nil
				err = errors.New("network failed")
			})
			return gio, err
		}

		err = c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(0, 0))
		if err == nil {
			t.Fatal("unexpected error missing")
		}
	})
	t.Run("retryOnce", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"retryOnce",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#retryOnce"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("retryOnce",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		var failUpdateItemOnce sync.Once
		svc.updateItemPost = func(gio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			failUpdateItemOnce.Do(func() {
				t.Log("NETWORK FAILURE")
				gio = nil
				err = errors.New("network failed")
			})
			return gio, err
		}

		err = c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(1, 0))
		if err != nil {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("retryMany", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"retryMany",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#retryMany"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("retryMany",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		const totalRetries = 2
		var failUpdateItemCount int
		svc.updateItemPost = func(gio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			if failUpdateItemCount < totalRetries {
				t.Log("NETWORK FAILURE", failUpdateItemCount)
				gio = nil
				err = errors.New("network failed")
				failUpdateItemCount++
			}
			return gio, err
		}

		err = c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(totalRetries, 0))
		if err != nil {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("badReadAfterFail", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"badReadAfterFail",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#badReadAfterFail"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("badReadAfterFail",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		var failUpdateItemOnce sync.Once
		svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			failUpdateItemOnce.Do(func() {
				t.Log("NETWORK FAILURE")
				uio = nil
				err = errors.New("network failed")
			})
			return uio, err
		}
		errExpected := errors.New("bad GetItemCall")
		svc.getItemPost = func(gio *dynamodb.GetItemOutput, err error) (*dynamodb.GetItemOutput, error) {
			return nil, errExpected
		}

		if err := c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(1, 0)); !errors.Is(err, errExpected) {
			t.Fatal("unexpected error:", err)
		}
	})
	t.Run("canceledDuringRetry", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"canceledDuringRetry",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#canceledDuringRetry"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("canceledDuringRetry",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}
		var failUpdateItemOnce sync.Once
		svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			failUpdateItemOnce.Do(func() {
				t.Log("NETWORK FAILURE")
				uio = nil
				err = errors.New("network failed")
			})
			return uio, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		svc.getItemPost = func(gio *dynamodb.GetItemOutput, err error) (*dynamodb.GetItemOutput, error) {
			defer cancel()
			return gio, err
		}

		err = c.SendHeartbeatWithContext(ctx, lock, dynamolock.HeartbeatRetries(1, 0))
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error:", err)
		}
	})

	t.Run("lostDuringHeartbeatRetry", func(t *testing.T) {
		t.Parallel()
		svc := &interceptedDynamoDBClient{
			DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
		}
		c, err := dynamolock.New(svc,
			"lostDuringHeartbeatRetry",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRetry#lostDuringHeartbeatRetry"),
			dynamolock.DisableHeartbeat(),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ensuring table exists")
		_, _ = c.CreateTable("lostDuringHeartbeatRetry",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		const lockName = "lock-heartbeat-retry"
		lock, err := c.AcquireLock(lockName)
		if err != nil {
			t.Fatal(err)
		}

		var failUpdateItemOnce sync.Once
		svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
			failUpdateItemOnce.Do(func() {
				t.Log("NETWORK FAILURE")
				uio = nil
				err = errors.New("network failed")
			})
			return uio, err
		}

		svc.getItemPost = func(gio *dynamodb.GetItemOutput, err error) (*dynamodb.GetItemOutput, error) {
			gio.Item["recordVersionNumber"] = &types.AttributeValueMemberS{Value: "stole-rvn"}
			return gio, err
		}

		err = c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(1, 0))
		if !errors.As(err, new(*dynamolock.LockNotGrantedError)) {
			t.Fatal("unexpected error:", err)
		}
	})
}

func TestHeartbeatOwnerMatching(t *testing.T) {
	t.Parallel()

	svc := &interceptedDynamoDBClient{
		DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
	}
	c, err := dynamolock.New(svc,
		"noRetry",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatOwnerMatching"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ensuring table exists")
	_, _ = c.CreateTable("noRetry",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)
	const lockName = "lock-heartbeat-retry"
	lock, err := c.AcquireLock(lockName)
	if err != nil {
		t.Fatal(err)
	}
	var failUpdateItemOnce sync.Once
	svc.updateItemPost = func(gio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
		failUpdateItemOnce.Do(func() {
			t.Log("NETWORK FAILURE")
			gio = nil
			err = errors.New("network failed")
		})
		return gio, err
	}

	// Heartbeat sent, but response missed...
	err = c.SendHeartbeat(lock)
	if err == nil {
		t.Fatal("unexpected error missing on first heartbeat")
	}

	// ... prove the response is missed ...
	err = c.SendHeartbeat(lock)
	if err == nil {
		t.Fatal("expeted error for vanilla retry missing")
	}

	// ... prove the owner is the same.
	err = c.SendHeartbeat(lock, dynamolock.UnsafeMatchOwnerOnly())
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
}
