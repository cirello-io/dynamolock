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

package dynamolock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func isDynamoLockAvailable(t *testing.T) {
	_, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		t.Skipf("cannot dial to dynamoDB: %v", err)
	}
}

func mustNewConfig(t *testing.T) aws.Config {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolver(
			aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: "http://localhost:8080/"}, nil
				},
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestClientBasicFlow(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()

	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestClientBasicFlow#1"),
		WithLogger(&testLogger{t: t}),
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
	lockedItem, err := c.AcquireLock("spock",
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
		WithData(data2),
		ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content (again):", string(lockedItem2.Data()))
	if got := string(lockedItem2.Data()); string(data2) != got {
		t.Error("losing information inside lock storage, wanted:", string(data2), " got:", got)
	}

	c2, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestClientBasicFlow#2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	data3 := []byte("some content c")
	_, err = c2.AcquireLock("spock",
		WithData(data3),
		ReplaceData(),
	)
	if err == nil {
		t.Fatal("expected to fail to grab the lock")
	}

	c.ReleaseLock(lockedItem, WithDeleteLock(true))

	lockedItem3, err := c2.AcquireLock("spock",
		WithData(data3),
		ReplaceData(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("lock content (competing client):", string(lockedItem3.Data()))
	if got := string(lockedItem3.Data()); string(data3) != got {
		t.Error("losing information inside lock storage, wanted:", string(data3), " got:", got)
	}
}

func TestReadLockContent(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()

	t.Run("standard load", func(t *testing.T) {
		svc := dynamodb.NewFromConfig(mustNewConfig(t))
		c, err := New(svc,
			"locks",
			WithLeaseDuration(3*time.Second),
			WithHeartbeatPeriod(1*time.Second),
			WithOwnerName("TestReadLockContent#1"),
			WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		t.Log("ensuring table exists")
		c.CreateTable("locks",
			WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			WithCustomPartitionKeyName("key"),
		)

		data := []byte("some content a")
		lockedItem, err := c.AcquireLock("mccoy",
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

		c2, err := New(svc,
			"locks",
			WithLeaseDuration(3*time.Second),
			WithHeartbeatPeriod(1*time.Second),
			WithOwnerName("TestReadLockContent#2"),
		)
		if err != nil {
			t.Fatal(err)
		}

		lockItemRead, err := c2.Get("mccoy")
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()

		t.Log("reading someone else's lock:", string(lockItemRead.Data()))
		if got := string(lockItemRead.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}
	})
	t.Run("cached load", func(t *testing.T) {
		svc := dynamodb.NewFromConfig(mustNewConfig(t))
		c, err := New(svc,
			"locks",
			WithLeaseDuration(3*time.Second),
			WithHeartbeatPeriod(1*time.Second),
			WithOwnerName("TestReadLockContentCachedLoad#1"),
			WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		t.Log("ensuring table exists")
		c.CreateTable("locks",
			WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			WithCustomPartitionKeyName("key"),
		)

		data := []byte("hello janice")
		lockedItem, err := c.AcquireLock("janice",
			WithData(data),
			ReplaceData(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer lockedItem.Close()

		cachedItem, err := c.Get("janice")
		if err != nil {
			t.Fatal(err)
		}
		t.Log("cached item:", string(cachedItem.Data()))
	})
}

func TestReadLockContentAfterRelease(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestReadLockContentAfterRelease#1"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content for scotty")
	lockedItem, err := c.AcquireLock("scotty",
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
	lockedItem.Close()

	c2, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestReadLockContentAfterRelease#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get("scotty")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
}

func TestReadLockContentAfterDeleteOnRelease(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestReadLockContentAfterDeleteOnRelease#1"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content for uhura")
	lockedItem, err := c.AcquireLock("uhura",
		WithData(data),
		ReplaceData(),
		WithDeleteLockOnRelease(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
	lockedItem.Close()

	c2, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestReadLockContentAfterDeleteOnRelease#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get("uhura")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); got != "" {
		t.Error("keeping information inside lock storage, wanted empty got:", got)
	}
}

func TestInvalidLeaseHeartbeatRation(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	_, err := New(svc,
		"locks",
		WithLeaseDuration(1*time.Second),
		WithHeartbeatPeriod(1*time.Second),
	)
	if err == nil {
		t.Fatal("expected error not found")
	}
}

func TestFailIfLocked(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("FailIfLocked#1"),
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

	_, err = c.AcquireLock("failIfLocked")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.AcquireLock("failIfLocked", FailIfLocked())
	if e, ok := err.(*LockNotGrantedError); e == nil || !ok {
		t.Fatal("expected error (LockNotGrantedError) not found:", err)
		return
	}
}

func TestClientWithAdditionalAttributes(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		DisableHeartbeat(),
		WithOwnerName("TestClientWithAdditionalAttributes#1"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	_, err = c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("good attributes", func(t *testing.T) {
		lockedItem, err := c.AcquireLock(
			"good attributes",
			WithAdditionalAttributes(map[string]types.AttributeValue{
				"hello": &types.AttributeValueMemberS{Value: "world"},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		attrs := lockedItem.AdditionalAttributes()
		v, ok := attrs["hello"]
		if !ok || v == nil {
			t.Error("corrupted attribute set")
		}

		if v, err := stringFrommAttributeValue(v); err != nil || v != "world" {
			t.Fatal(err)
		}

		lockedItem.Close()
	})

	t.Run("bad attributes", func(t *testing.T) {
		_, err := c.AcquireLock(
			"bad attributes",
			WithAdditionalAttributes(map[string]types.AttributeValue{
				"ownerName": &types.AttributeValueMemberS{Value: "fakeOwner"},
			}),
		)
		if err == nil {
			t.Fatal("expected error not found")
		}
	})
	t.Run("recover attributes after release", func(t *testing.T) {
		// Cover cirello-io/dynamolock#6
		lockedItem, err := c.AcquireLock(
			"recover attributes after release",
			WithAdditionalAttributes(map[string]types.AttributeValue{
				"hello": &types.AttributeValueMemberS{Value: "world"},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		attrs := lockedItem.AdditionalAttributes()
		v, ok := attrs["hello"]
		if !ok || v == nil {
			t.Fatal("corrupted attribute set")
		}

		if val, err := stringFrommAttributeValue(v); err != nil || val != "world" {
			t.Fatal(err)
		}

		relockedItem, err := c.AcquireLock(
			"recover attributes after release",
		)
		if err != nil {
			t.Fatal(err)
		}

		recoveredAttrs := relockedItem.AdditionalAttributes()

		v, ok = recoveredAttrs["hello"]
		if !ok || v == nil {
			t.Fatal("corrupted attribute set")
		}

		if val, err := stringFrommAttributeValue(v); err != nil || val != "world" {
			t.Fatal("corrupted attribute set")
		}
	})
}

func TestDeleteLockOnRelease(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestDeleteLockOnRelease#1"),
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

	const lockName = "delete-lock-on-release"
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(
		lockName,
		WithData(data),
		ReplaceData(),
		WithDeleteLockOnRelease(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
	lockedItem.Close()

	releasedLock, err := c.Get(lockName)
	if err != nil {
		t.Fatal("cannot load lock from the database:", err)
	}
	if !releasedLock.IsExpired() {
		t.Fatal("non-existent locks should always returned as released")
	}
}

func TestCustomRefreshPeriod(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestCustomRefreshPeriod#1"),
		WithLogger(logger),
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

	lockedItem, err := c.AcquireLock("custom-refresh-period")
	if err != nil {
		t.Fatal(err)
	}
	defer lockedItem.Close()

	c.AcquireLock("custom-refresh-period", WithRefreshPeriod(100*time.Millisecond))
	if !strings.Contains(buf.String(), "Sleeping for a refresh period of  100ms") {
		t.Fatal("did not honor refreshPeriod")
	}
}

func TestCustomAdditionalTimeToWaitForLock(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		DisableHeartbeat(),
		WithOwnerName("TestCustomAdditionalTimeToWaitForLock#1"),
		WithLogger(&testLogger{t: t}),
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

	t.Log("acquire lock")
	l, err := c.AcquireLock("custom-additional-time-to-wait")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 3; i++ {
			c.SendHeartbeat(l)
			time.Sleep(time.Second)
		}
	}()

	t.Log("wait long enough to acquire lock again")
	_, err = c.AcquireLock("custom-additional-time-to-wait",
		WithAdditionalTimeToWaitForLock(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientClose(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestClientClose#1"),
		WithLogger(&testLogger{t: t}),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal("cannot create the client:", err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)

	t.Log("acquiring locks")
	lockItem1, err := c.AcquireLock("bulkClose1")
	if err != nil {
		t.Fatal("cannot acquire lock1:", err)
	}

	if _, err := c.AcquireLock("bulkClose2"); err != nil {
		t.Fatal("cannot acquire lock2:", err)
	}

	if _, err := c.AcquireLock("bulkClose3"); err != nil {
		t.Fatal("cannot acquire lock3:", err)
	}

	t.Log("closing client")
	if err := c.Close(); err != nil {
		t.Fatal("cannot close lock client: ", err)
	}

	t.Log("close after close")
	if err := c.Close(); err != ErrClientClosed {
		t.Error("expected error missing (close after close):", err)
	}
	t.Log("heartbeat after close")
	if err := c.SendHeartbeat(lockItem1); err != ErrClientClosed {
		t.Error("expected error missing (heartbeat after close):", err)
	}
	t.Log("release after close")
	if _, err := c.ReleaseLock(lockItem1); err != ErrClientClosed {
		t.Error("expected error missing (release after close):", err)
	}
	t.Log("get after close")
	if _, err := c.Get("bulkClose1"); err != ErrClientClosed {
		t.Error("expected error missing (get after close):", err)
	}
	t.Log("acquire after close")
	if _, err := c.AcquireLock("acquireAfterClose"); err != ErrClientClosed {
		t.Error("expected error missing (acquire after close):", err)
	}
	t.Log("create table after close")
	if _, err := c.CreateTable("createTableAfterClose"); err != ErrClientClosed {
		t.Error("expected error missing (create table after close):", err)
	}
}

func TestInvalidReleases(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestInvalidReleases#1"),
		WithLogger(&testLogger{t: t}),
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

	t.Run("release nil lock", func(t *testing.T) {
		var l *Lock
		if _, err := c.ReleaseLock(l); err == nil {
			t.Fatal("nil locks should trigger error on release:", err)
		} else {
			t.Log("nil lock:", err)
		}
	})

	t.Run("release empty lock", func(t *testing.T) {
		emptyLock := &Lock{}
		if released, err := c.ReleaseLock(emptyLock); err != ErrOwnerMismatched {
			t.Fatal("empty locks should return error:", err)
		} else {
			t.Log("emptyLock:", released, err)
		}
	})

	t.Run("duplicated lock close", func(t *testing.T) {
		l, err := c.AcquireLock("duplicatedLockRelease")
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal("first close should be flawless:", err)
		}
		if err := l.Close(); err == nil {
			t.Fatal("second close should be fail")
		}
	})

	t.Run("nil lock close", func(t *testing.T) {
		var l *Lock
		if err := l.Close(); err != ErrCannotReleaseNullLock {
			t.Fatal("wrong error when closing nil lock:", err)
		}
	})
}

func TestClientWithDataAfterRelease(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	c, err := New(svc,
		"locks",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(1*time.Second),
		WithOwnerName("TestClientWithDataAfterRelease#1"),
		WithLogger(&testLogger{t: t}),
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

	const lockName = "lockNoData"

	lockItem, err := c.AcquireLock(lockName)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("there is life after release")
	if _, err := c.ReleaseLock(lockItem, WithDataAfterRelease(data)); err != nil {
		t.Fatal(err)
	}

	relockedItem, err := c.AcquireLock(lockName)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(relockedItem.Data(), data) {
		t.Fatal("missing expected data after the release")
	}
}

type testLogger struct {
	t *testing.T
}

func (t *testLogger) Println(v ...interface{}) {
	t.t.Helper()
	t.t.Log(v...)
}

func TestHeartbeatLoss(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))
	heartbeatPeriod := 5 * time.Second
	c, err := New(svc,
		"locks",
		WithLeaseDuration(1*time.Hour),
		WithHeartbeatPeriod(heartbeatPeriod),
		WithOwnerName("TestHeartbeatLoss#1"),
		WithLogger(&testLogger{t: t}),
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

	const lockName = "heartbeatLoss"

	lockItem1, err := c.AcquireLock(lockName + "1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(heartbeatPeriod)
	if _, err := c.ReleaseLock(lockItem1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(heartbeatPeriod)

	lockItem2, err := c.AcquireLock(lockName + "2")
	if err != nil {
		t.Fatal(err)
	}
	defer lockItem2.Close()

	rvn1 := lockItem2.RVN()
	time.Sleep(heartbeatPeriod + 1*time.Second)
	rvn2 := lockItem2.RVN()

	t.Log("RVNs", rvn1, rvn2)
	if rvn1 == rvn2 {
		t.Fatal("is the heartbeat running?")
	}
}

func TestHeartbeatError(t *testing.T) {
	isDynamoLockAvailable(t)
	t.Parallel()
	svc := dynamodb.NewFromConfig(mustNewConfig(t))

	var buf lockStepBuffer
	fatal := func(a ...interface{}) {
		t.Log(buf.String())
		t.Fatal(a...)
	}
	defer func() {
		t.Log(buf.String())
	}()
	logger := log.New(&buf, "", 0)

	heartbeatPeriod := 2 * time.Second
	c, err := New(svc,
		"locksHBError",
		WithLeaseDuration(1*time.Hour),
		WithHeartbeatPeriod(heartbeatPeriod),
		WithOwnerName("TestHeartbeatError#1"),
		WithLogger(logger),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		fatal(err)
	}

	t.Log("ensuring table exists")
	_, err = c.CreateTable("locksHBError",
		WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		WithCustomPartitionKeyName("key"),
	)
	if err != nil {
		fatal("cannot create table")
	}

	const lockName = "heartbeatError"
	if _, err := c.AcquireLock(lockName); err != nil {
		fatal(err)
	}
	time.Sleep(2 * heartbeatPeriod)

	_, err = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String("locksHBError"),
	})
	if err != nil {
		fatal(fmt.Sprintf("could not delete table: %v", err))
	}

	time.Sleep(heartbeatPeriod)

	c.Close()

	time.Sleep(heartbeatPeriod)

	if !strings.Contains(buf.String(), "error sending heartbeat to heartbeatError") {
		fatal("cannot prove that heartbeat failed after the table has been deleted")
	}
}

type lockStepBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockStepBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockStepBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type fakeDynamoDB struct {
	DynamoDBAPI
}

func (f *fakeDynamoDB) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("service is offline")
}

func TestBadDynamoDB(t *testing.T) {
	t.Parallel()
	t.Run("get", func(t *testing.T) {
		svc := &fakeDynamoDB{}
		c, err := New(svc, "locksHBError")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get("bad-dynamodb"); err == nil {
			t.Fatal("expected error missing")
		}
	})
	t.Run("acquire", func(t *testing.T) {
		svc := &fakeDynamoDB{}
		c, err := New(svc, "locksHBError")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.AcquireLock("bad-dynamodb"); err == nil {
			t.Fatal("expected error missing")
		}
	})
}
