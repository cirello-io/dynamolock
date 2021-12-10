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
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	sortKeyTable = "locks_sk"
)

func createSortKeyTable(c *dynamolock.Client, t *testing.T) (*dynamodb.CreateTableOutput, error) {
	t.Helper()
	return c.CreateTable(sortKeyTable,
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
		dynamolock.WithSortKeyName("sortkey"),
	)
}

func TestSortKeyClientBasicFlow(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

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
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#2"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}
	data3 := []byte("some content c")
	_, err = c2.AcquireLock("spock",
		dynamolock.WithData(data3),
		dynamolock.ReplaceData(),
	)
	if err == nil {
		t.Fatal("expected to fail to grab the lock")
	}

	c.ReleaseLock(lockedItem, dynamolock.WithDeleteLock(true))

	lockedItem3, err := c2.AcquireLock("spock",
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

func TestSortKeyReadLockContent(t *testing.T) {
	t.Parallel()

	t.Run("standard load", func(t *testing.T) {
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c, err := dynamolock.New(svc,
			sortKeyTable,
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithHeartbeatPeriod(1*time.Second),
			dynamolock.WithOwnerName("TestReadLockContent#1"),
			dynamolock.WithPartitionKeyName("key"),
			dynamolock.WithSortKey("sortkey", "sortvalue"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		t.Log("ensuring table exists")
		createSortKeyTable(c, t)

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
			sortKeyTable,
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithHeartbeatPeriod(1*time.Second),
			dynamolock.WithOwnerName("TestReadLockContent#2"),
			dynamolock.WithSortKey("sortkey", "sortvalue"),
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
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c, err := dynamolock.New(svc,
			sortKeyTable,
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithHeartbeatPeriod(1*time.Second),
			dynamolock.WithOwnerName("TestReadLockContentCachedLoad#1"),
			dynamolock.WithPartitionKeyName("key"),
			dynamolock.WithSortKey("sortkey", "sortvalue"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		t.Log("ensuring table exists")
		createSortKeyTable(c, t)

		data := []byte("hello janice")
		lockedItem, err := c.AcquireLock("janice",
			dynamolock.WithData(data),
			dynamolock.ReplaceData(),
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

func TestSortKeyReadLockContentAfterRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	data := []byte("some content for scotty")
	lockedItem, err := c.AcquireLock("scotty",
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
	lockedItem.Close()

	c2, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterRelease#2"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
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

func TestSortKeyReadLockContentAfterDeleteOnRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	data := []byte("some content for uhura")
	lockedItem, err := c.AcquireLock("uhura",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithDeleteLockOnRelease(),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
	lockedItem.Close()

	c2, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#2"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
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

func TestSortKeyInvalidLeaseHeartbeatRation(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	_, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(1*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err == nil {
		t.Fatal("expected error not found")
	}
}

func TestSortKeyFailIfLocked(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("FailIfLocked#1"),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	_, err = c.AcquireLock("failIfLocked")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.AcquireLock("failIfLocked", dynamolock.FailIfLocked())
	if e, ok := err.(*dynamolock.LockNotGrantedError); e == nil || !ok {
		t.Fatal("expected error (LockNotGrantedError) not found:", err)
		return
	}
}

func TestSortKeyClientWithAdditionalAttributes(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithOwnerName("TestClientWithAdditionalAttributes#1"),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	t.Run("good attributes", func(t *testing.T) {
		lockedItem, err := c.AcquireLock(
			"good attributes",
			dynamolock.WithAdditionalAttributes(map[string]types.AttributeValue{
				"hello": &types.AttributeValueMemberS{Value: "world"},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		attrs := lockedItem.AdditionalAttributes()
		if v, ok := attrs["hello"]; !ok || v == nil || readStringAttr(v) != "world" {
			t.Error("corrupted attribute set")
		}
		lockedItem.Close()
	})
	t.Run("bad attributes", func(t *testing.T) {
		_, err := c.AcquireLock(
			"bad attributes",
			dynamolock.WithAdditionalAttributes(map[string]types.AttributeValue{
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
			dynamolock.WithAdditionalAttributes(map[string]types.AttributeValue{
				"hello": &types.AttributeValueMemberS{Value: "world"},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		attrs := lockedItem.AdditionalAttributes()
		if v, ok := attrs["hello"]; !ok || v == nil || readStringAttr(v) != "world" {
			t.Error("corrupted attribute set")
		}

		relockedItem, err := c.AcquireLock(
			"recover attributes after release",
		)
		if err != nil {
			t.Fatal(err)
		}
		recoveredAttrs := relockedItem.AdditionalAttributes()
		if v, ok := recoveredAttrs["hello"]; !ok || v == nil || readStringAttr(v) != "world" {
			t.Error("corrupted attribute set")
		}
	})
}

func TestSortKeyDeleteLockOnRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestDeleteLockOnRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	const lockName = "delete-lock-on-release"
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(
		lockName,
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithDeleteLockOnRelease(),
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

func TestSortKeyCustomRefreshPeriod(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestCustomRefreshPeriod#1"),
		dynamolock.WithLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	lockedItem, err := c.AcquireLock("custom-refresh-period")
	if err != nil {
		t.Fatal(err)
	}
	defer lockedItem.Close()

	c.AcquireLock("custom-refresh-period", dynamolock.WithRefreshPeriod(100*time.Millisecond))
	if !strings.Contains(buf.String(), "Sleeping for a refresh period of  100ms") {
		t.Fatal("did not honor refreshPeriod")
	}
}

func TestSortKeyCustomAdditionalTimeToWaitForLock(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithOwnerName("TestCustomAdditionalTimeToWaitForLock#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

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
		dynamolock.WithAdditionalTimeToWaitForLock(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSortKeyClientClose(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientClose#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal("cannot create the client:", err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

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
	if err := c.Close(); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (close after close):", err)
	}
	t.Log("heartbeat after close")
	if err := c.SendHeartbeat(lockItem1); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (heartbeat after close):", err)
	}
	t.Log("release after close")
	if _, err := c.ReleaseLock(lockItem1); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (release after close):", err)
	}
	t.Log("get after close")
	if _, err := c.Get("bulkClose1"); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (get after close):", err)
	}
	t.Log("acquire after close")
	if _, err := c.AcquireLock("acquireAfterClose"); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (acquire after close):", err)
	}
	t.Log("create table after close")
	if _, err := c.CreateTable("createTableAfterClose"); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (create table after close):", err)
	}
}

func TestSortKeyInvalidReleases(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestInvalidReleases#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	t.Run("release nil lock", func(t *testing.T) {
		var l *dynamolock.Lock
		if _, err := c.ReleaseLock(l); err == nil {
			t.Fatal("nil locks should trigger error on release:", err)
		} else {
			t.Log("nil lock:", err)
		}
	})

	t.Run("release empty lock", func(t *testing.T) {
		emptyLock := &dynamolock.Lock{}
		if released, err := c.ReleaseLock(emptyLock); err != dynamolock.ErrOwnerMismatched {
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
		var l *dynamolock.Lock
		if err := l.Close(); err != dynamolock.ErrCannotReleaseNullLock {
			t.Fatal("wrong error when closing nil lock:", err)
		}
	})
}

func TestSortKeyClientWithDataAfterRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientWithDataAfterRelease#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

	const lockName = "lockNoData"

	lockItem, err := c.AcquireLock(lockName)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("there is life after release")
	if _, err := c.ReleaseLock(lockItem, dynamolock.WithDataAfterRelease(data)); err != nil {
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

func TestSortKeyHeartbeatLoss(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	heartbeatPeriod := 5 * time.Second
	c, err := dynamolock.New(svc,
		sortKeyTable,
		dynamolock.WithLeaseDuration(1*time.Hour),
		dynamolock.WithHeartbeatPeriod(heartbeatPeriod),
		dynamolock.WithOwnerName("TestHeartbeatLoss#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	createSortKeyTable(c, t)

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

func TestSortKeyHeartbeatError(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))

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
	c, err := dynamolock.New(svc,
		"locksSkHBError",
		dynamolock.WithLeaseDuration(1*time.Hour),
		dynamolock.WithHeartbeatPeriod(heartbeatPeriod),
		dynamolock.WithOwnerName("TestHeartbeatError#1"),
		dynamolock.WithLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
		dynamolock.WithSortKey("sortkey", "sortvalue"),
	)
	if err != nil {
		fatal(err)
	}

	t.Log("ensuring table exists")
	_, err = c.CreateTable("locksSkHBError",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
		dynamolock.WithSortKeyName("sortkey"),
	)
	if err != nil {
		fatal("cannot create table", err)
	}

	const lockName = "heartbeatError"
	if _, err := c.AcquireLock(lockName); err != nil {
		fatal(err)
	}
	time.Sleep(2 * heartbeatPeriod)

	_, err = svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String("locksSkHBError"),
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
