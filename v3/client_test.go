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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestMain(m *testing.M) {
	flag.Parse()
	javaPath, err := exec.LookPath("java")
	if err != nil {
		panic("cannot execute tests without Java")
	}
	_ = os.Remove(filepath.Join("local-dynamodb", "shared-local-instance.db")) // unconditionally remove state file
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, javaPath, "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-sharedDb")
	cmd.Dir = "local-dynamodb"
	if err := cmd.Start(); err != nil {
		panic("cannot start local dynamodb:" + err.Error())
	}
	for i := 0; i < 10; i++ {
		c, err := net.Dial("tcp", "localhost:8000")
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		c.Close()
		break
	}
	exitCode := m.Run()
	cancel()
	cmd.Wait()
	os.Exit(exitCode)
}

func defaultConfig(t *testing.T) aws.Config {
	return aws.Config{
		Region: "us-west-2",
		EndpointResolver: aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000/"}, nil
			},
		),
		Credentials: credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "fakeMyKeyId",
				SecretAccessKey: "fakeSecretAccessKey",
			},
		},
	}
}

func TestClientBasicFlow(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
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
	lockedItem, err := c.AcquireLock(context.Background(), "spock",
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
	success, err := c.ReleaseLock(context.Background(), lockedItem)
	if !success {
		t.Fatal("lost lock before release")
	}
	if err != nil {
		t.Fatal("error releasing lock:", err)
	}
	t.Log("done")

	data2 := []byte("some content b")
	lockedItem2, err := c.AcquireLock(context.Background(), "spock",
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
	_, err = c2.AcquireLock(context.Background(), "spock",
		dynamolock.WithData(data3),
		dynamolock.ReplaceData(),
	)
	if err == nil {
		t.Fatal("expected to fail to grab the lock")
	}

	c.ReleaseLock(context.Background(), lockedItem, dynamolock.WithDeleteLock(true))

	lockedItem3, err := c2.AcquireLock(context.Background(), "spock",
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

func TestReadLockContent(t *testing.T) {
	t.Parallel()

	t.Run("standard load", func(t *testing.T) {
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c, err := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithHeartbeatPeriod(1*time.Second),
			dynamolock.WithOwnerName("TestReadLockContent#1"),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())

		t.Log("ensuring table exists")
		c.CreateTable(context.Background(),
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
		)

		data := []byte("some content a")
		lockedItem, err := c.AcquireLock(context.Background(), "mccoy",
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

		lockItemRead, err := c2.Get(context.Background(), "mccoy")
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close(context.Background())

		t.Log("reading someone else's lock:", string(lockItemRead.Data()))
		if got := string(lockItemRead.Data()); string(data) != got {
			t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
		}
	})
	t.Run("cached load", func(t *testing.T) {
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c, err := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithHeartbeatPeriod(1*time.Second),
			dynamolock.WithOwnerName("TestReadLockContentCachedLoad#1"),
			dynamolock.WithPartitionKeyName("key"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())

		t.Log("ensuring table exists")
		c.CreateTable(context.Background(),
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
		)

		data := []byte("hello janice")
		lockedItem, err := c.AcquireLock(context.Background(), "janice",
			dynamolock.WithData(data),
			dynamolock.ReplaceData(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer lockedItem.Close()

		cachedItem, err := c.Get(context.Background(), "janice")
		if err != nil {
			t.Fatal(err)
		}
		t.Log("cached item:", string(cachedItem.Data()))
	})
}

func TestReadLockContentAfterRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	data := []byte("some content for scotty")
	lockedItem, err := c.AcquireLock(context.Background(), "scotty",
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
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterRelease#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get(context.Background(), "scotty")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(context.Background())

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); string(data) != got {
		t.Error("losing information inside lock storage, wanted:", string(data), " got:", got)
	}
}

func TestReadLockContentAfterDeleteOnRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	data := []byte("some content for uhura")
	lockedItem, err := c.AcquireLock(context.Background(), "uhura",
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
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get(context.Background(), "uhura")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(context.Background())

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); got != "" {
		t.Error("keeping information inside lock storage, wanted empty got:", got)
	}
}

func TestInvalidLeaseHeartbeatRation(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	_, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(1*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
	)
	if err == nil {
		t.Fatal("expected error not found")
	}
}

func TestFailIfLocked(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("FailIfLocked#1"),
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

	_, err = c.AcquireLock(context.Background(), "failIfLocked")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.AcquireLock(context.Background(), "failIfLocked", dynamolock.FailIfLocked())
	if e, ok := err.(*dynamolock.LockNotGrantedError); e == nil || !ok {
		t.Fatal("expected error (LockNotGrantedError) not found:", err)
		return
	}
}

func TestClientWithAdditionalAttributes(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithOwnerName("TestClientWithAdditionalAttributes#1"),
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

	t.Run("good attributes", func(t *testing.T) {
		lockedItem, err := c.AcquireLock(context.Background(),
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
		_, err := c.AcquireLock(context.Background(),
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
		lockedItem, err := c.AcquireLock(context.Background(),
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

		relockedItem, err := c.AcquireLock(context.Background(),
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

func TestDeleteLockOnRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestDeleteLockOnRelease#1"),
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

	const lockName = "delete-lock-on-release"
	data := []byte("some content a")
	lockedItem, err := c.AcquireLock(context.Background(),
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

	releasedLock, err := c.Get(context.Background(), lockName)
	if err != nil {
		t.Fatal("cannot load lock from the database:", err)
	}
	if !releasedLock.IsExpired() {
		t.Fatal("non-existent locks should always returned as released")
	}
}

func TestCustomRefreshPeriod(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestCustomRefreshPeriod#1"),
		dynamolock.WithLogger(logger),
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

	lockedItem, err := c.AcquireLock(context.Background(), "custom-refresh-period")
	if err != nil {
		t.Fatal(err)
	}
	defer lockedItem.Close()

	c.AcquireLock(context.Background(), "custom-refresh-period", dynamolock.WithRefreshPeriod(100*time.Millisecond))
	if !strings.Contains(buf.String(), "Sleeping for a refresh period of  100ms") {
		t.Fatal("did not honor refreshPeriod")
	}
}

func TestCustomAdditionalTimeToWaitForLock(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithOwnerName("TestCustomAdditionalTimeToWaitForLock#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
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

	t.Log("acquire lock")
	l, err := c.AcquireLock(context.Background(), "custom-additional-time-to-wait")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 3; i++ {
			c.SendHeartbeat(context.Background(), l)
			time.Sleep(time.Second)
		}
	}()

	t.Log("wait long enough to acquire lock again")
	_, err = c.AcquireLock(context.Background(), "custom-additional-time-to-wait",
		dynamolock.WithAdditionalTimeToWaitForLock(6*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientClose(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientClose#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal("cannot create the client:", err)
	}

	t.Log("ensuring table exists")
	c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)

	t.Log("acquiring locks")
	lockItem1, err := c.AcquireLock(context.Background(), "bulkClose1")
	if err != nil {
		t.Fatal("cannot acquire lock1:", err)
	}

	if _, err := c.AcquireLock(context.Background(), "bulkClose2"); err != nil {
		t.Fatal("cannot acquire lock2:", err)
	}

	if _, err := c.AcquireLock(context.Background(), "bulkClose3"); err != nil {
		t.Fatal("cannot acquire lock3:", err)
	}

	t.Log("closing client")
	if err := c.Close(context.Background()); err != nil {
		t.Fatal("cannot close lock client: ", err)
	}

	t.Log("close after close")
	if err := c.Close(context.Background()); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (close after close):", err)
	}
	t.Log("heartbeat after close")
	if err := c.SendHeartbeat(context.Background(), lockItem1); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (heartbeat after close):", err)
	}
	t.Log("release after close")
	if _, err := c.ReleaseLock(context.Background(), lockItem1); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (release after close):", err)
	}
	t.Log("get after close")
	if _, err := c.Get(context.Background(), "bulkClose1"); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (get after close):", err)
	}
	t.Log("acquire after close")
	if _, err := c.AcquireLock(context.Background(), "acquireAfterClose"); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (acquire after close):", err)
	}
	t.Log("create table after close")
	if _, err := c.CreateTable(context.Background()); err != dynamolock.ErrClientClosed {
		t.Error("expected error missing (create table after close):", err)
	}
}

func TestInvalidReleases(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestInvalidReleases#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
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

	t.Run("release nil lock", func(t *testing.T) {
		var l *dynamolock.Lock
		if _, err := c.ReleaseLock(context.Background(), l); err == nil {
			t.Fatal("nil locks should trigger error on release:", err)
		} else {
			t.Log("nil lock:", err)
		}
	})

	t.Run("release empty lock", func(t *testing.T) {
		emptyLock := &dynamolock.Lock{}
		if released, err := c.ReleaseLock(context.Background(), emptyLock); err != dynamolock.ErrOwnerMismatched {
			t.Fatal("empty locks should return error:", err)
		} else {
			t.Log("emptyLock:", released, err)
		}
	})

	t.Run("duplicated lock close", func(t *testing.T) {
		l, err := c.AcquireLock(context.Background(), "duplicatedLockRelease")
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

func TestClientWithDataAfterRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithOwnerName("TestClientWithDataAfterRelease#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
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

	const lockName = "lockNoData"

	lockItem, err := c.AcquireLock(context.Background(), lockName)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("there is life after release")
	if _, err := c.ReleaseLock(context.Background(), lockItem, dynamolock.WithDataAfterRelease(data)); err != nil {
		t.Fatal(err)
	}

	relockedItem, err := c.AcquireLock(context.Background(), lockName)
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
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	heartbeatPeriod := 5 * time.Second
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(1*time.Hour),
		dynamolock.WithHeartbeatPeriod(heartbeatPeriod),
		dynamolock.WithOwnerName("TestHeartbeatLoss#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
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

	const lockName = "heartbeatLoss"

	lockItem1, err := c.AcquireLock(context.Background(), lockName+"1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(heartbeatPeriod)
	if _, err := c.ReleaseLock(context.Background(), lockItem1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(heartbeatPeriod)

	lockItem2, err := c.AcquireLock(context.Background(), lockName+"2")
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
		"locksHBError",
		dynamolock.WithLeaseDuration(1*time.Hour),
		dynamolock.WithHeartbeatPeriod(heartbeatPeriod),
		dynamolock.WithOwnerName("TestHeartbeatError#1"),
		dynamolock.WithLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		fatal(err)
	}

	t.Log("ensuring table exists")
	_, err = c.CreateTable(context.Background(),
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
	)
	if err != nil {
		fatal("cannot create table", err)
	}

	const lockName = "heartbeatError"
	if _, err := c.AcquireLock(context.Background(), lockName); err != nil {
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

	c.Close(context.Background())

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
	dynamolock.DynamoDBClient
}

func (f *fakeDynamoDB) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("service is offline")
}

func TestBadDynamoDB(t *testing.T) {
	t.Parallel()
	t.Run("get", func(t *testing.T) {
		svc := &fakeDynamoDB{}
		c, err := dynamolock.New(svc, "locksHBError")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(context.Background(), "bad-dynamodb"); err == nil {
			t.Fatal("expected error missing")
		}
	})
	t.Run("acquire", func(t *testing.T) {
		svc := &fakeDynamoDB{}
		c, err := dynamolock.New(svc, "locksHBError")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.AcquireLock(context.Background(), "bad-dynamodb"); err == nil {
			t.Fatal("expected error missing")
		}
	})
}

func readStringAttr(attr types.AttributeValue) string {
	if s, ok := attr.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}
