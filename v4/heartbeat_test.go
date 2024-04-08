/*
Copyright 2024 U. Cirello (cirello.io and github.com/cirello-io)

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
	"testing"
	"time"

	"cirello.io/dynamolock/v4"
	internalstrings "cirello.io/dynamolock/v4/internal/strings"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestHeartbeat(t *testing.T) {
	t.Parallel()

	t.Run("regular", func(t *testing.T) {
		t.Parallel()
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		logger := &bufferedContextLogger{}
		c := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatRegular#1"),
			dynamolock.WithContextLogger(logger),
			dynamolock.WithPartitionKeyName("key"),
		)
		t.Cleanup(func() {
			t.Log(logger.String())
		})

		_, _ = c.CreateTable(context.Background(), "locks",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		lockName := internalstrings.Rand()
		lockItem, err := c.AcquireLock(context.Background(), lockName)
		if err != nil {
			t.Fatal("cannot acquire lock:", err)
		}

		originalRVN := lockItem.RVN()

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Second, cancel)
		err = dynamolock.Heartbeat(ctx, c, lockItem)
		updatedRVN := lockItem.RVN()

		if originalRVN == updatedRVN {
			t.Fatal("heartbeat did not happen")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error:", err)
		}
	})

	t.Run("deleteData", func(t *testing.T) {
		t.Parallel()
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		logger := &bufferedContextLogger{}
		c := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatDeleteData#1"),
			dynamolock.WithContextLogger(logger),
			dynamolock.WithPartitionKeyName("key"),
		)
		t.Cleanup(func() {
			t.Log(logger.String())
		})

		_, _ = c.CreateTable(context.Background(), "locks",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		lockName := internalstrings.Rand()
		lockItem, err := c.AcquireLock(context.Background(), lockName, dynamolock.WithData([]byte("hello world")))
		if err != nil {
			t.Fatal("cannot acquire lock:", err)
		}

		originalRVN := lockItem.RVN()

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Second, cancel)
		err = dynamolock.Heartbeat(ctx, c, lockItem, dynamolock.DeleteData())

		updatedRVN := lockItem.RVN()
		updatedData := lockItem.Data()

		if originalRVN == updatedRVN {
			t.Fatal("heartbeat did not happen")
		}
		if !bytes.Equal(updatedData, []byte{}) {
			t.Fatal("data not deleted after heartbeat:", updatedData)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error:", err)
		}

		c2 := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatDeleteData#2"),
			dynamolock.WithPartitionKeyName("key"),
		)
		loadedLockItem, err := c2.Get(context.Background(), lockName)
		if err != nil {
			t.Fatal("cannot load lock information:", err)
		}
		loadedData := loadedLockItem.Data()
		if !bytes.Equal(loadedData, []byte{}) {
			t.Fatal("data not deleted after heartbeat (remote load):", loadedData)
		}
	})

	t.Run("replaceData", func(t *testing.T) {
		t.Parallel()
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		logger := &bufferedContextLogger{}
		c := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatReplaceData#1"),
			dynamolock.WithContextLogger(logger),
			dynamolock.WithPartitionKeyName("key"),
		)
		t.Cleanup(func() {
			t.Log(logger.String())
		})

		_, _ = c.CreateTable(context.Background(), "locks",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)
		lockName := internalstrings.Rand()
		lockItem, err := c.AcquireLock(context.Background(), lockName, dynamolock.WithData([]byte("hello world")))
		if err != nil {
			t.Fatal("cannot acquire lock:", err)
		}

		originalRVN := lockItem.RVN()

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Second, cancel)
		err = dynamolock.Heartbeat(ctx, c, lockItem, dynamolock.ReplaceHeartbeatData([]byte("foobar")))

		updatedRVN := lockItem.RVN()
		updatedData := lockItem.Data()

		if originalRVN == updatedRVN {
			t.Fatal("heartbeat did not happen")
		}
		if !bytes.Equal(updatedData, []byte("foobar")) {
			t.Fatal("data not updated after heartbeat:", updatedData)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error:", err)
		}

		c2 := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestHeartbeatReplaceData#2"),
			dynamolock.WithPartitionKeyName("key"),
		)
		loadedLockItem, err := c2.Get(context.Background(), lockName)
		if err != nil {
			t.Fatal("cannot load lock information:", err)
		}
		loadedData := loadedLockItem.Data()
		if !bytes.Equal(loadedData, []byte("foobar")) {
			t.Fatal("data not deleted after heartbeat (remote load):", loadedData)
		}
	})
}
