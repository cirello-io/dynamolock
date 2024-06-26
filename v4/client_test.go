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
	"flag"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"cirello.io/dynamolock/v4"
	internalstrings "cirello.io/dynamolock/v4/internal/strings"
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
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, javaPath, "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory")
	cmd.Dir = "local-dynamodb"
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Start(); err != nil {
		panic("cannot start local dynamodb:" + err.Error())
	}
	for i := 0; i < 10; i++ {
		c, err := net.Dial("tcp", "localhost:8000")
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		_ = c.Close()
		break
	}
	time.Sleep(1 * time.Second)
	exitCode := m.Run()
	cancel()
	_ = cmd.Wait()
	os.Exit(exitCode)
}

func defaultConfig(t *testing.T) aws.Config {
	t.Helper()
	return aws.Config{
		Region: "us-west-2",
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			_ = service
			return aws.Endpoint{URL: "http://localhost:8000/"}, nil
		}),
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
	logger := &bufferedContextLogger{}
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestClientBasicFlow#1"),
		dynamolock.WithContextLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
	)
	t.Cleanup(func() {
		t.Log(logger.String())
	})

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
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
	if err := c.ReleaseLock(context.Background(), lockedItem); err != nil {
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
	go func() {
		for {
			err := c.SendHeartbeat(context.Background(), lockedItem2)
			if err != nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}()

	t.Log("lock content (again):", string(lockedItem2.Data()))
	if got := string(lockedItem2.Data()); string(data2) != got {
		t.Error("losing information inside lock storage, wanted:", string(data2), " got:", got)
	}

	c2 := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
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

	_ = c.ReleaseLock(context.Background(), lockedItem2, dynamolock.WithDeleteLock(true))

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
		t.Parallel()
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestReadLockContent#1"),
			dynamolock.WithPartitionKeyName("key"),
		)
		defer c.Close(context.Background())

		t.Log("ensuring table exists")
		_, _ = c.CreateTable(context.Background(), "locks",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
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

		c2 := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
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
		t.Parallel()
		svc := dynamodb.NewFromConfig(defaultConfig(t))
		c := dynamolock.New(svc,
			"locks",
			dynamolock.WithLeaseDuration(3*time.Second),
			dynamolock.WithOwnerName("TestReadLockContentCachedLoad#1"),
			dynamolock.WithPartitionKeyName("key"),
		)
		defer c.Close(context.Background())

		t.Log("ensuring table exists")
		_, _ = c.CreateTable(context.Background(), "locks",
			dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			}),
			dynamolock.WithCustomPartitionKeyName("key"),
		)

		data := []byte("hello janice")
		lockedItem, err := c.AcquireLock(context.Background(), "janice",
			dynamolock.WithData(data),
			dynamolock.ReplaceData(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.ReleaseLock(context.Background(), lockedItem) }()

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
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
	)
	defer c.Close(context.Background())

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
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
	_ = c.ReleaseLock(context.Background(), lockedItem)

	c2 := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
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
	lockName := internalstrings.Rand()

	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
	)
	defer c.Close(context.Background())

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content for " + lockName)
	lockedItem, err := c.AcquireLock(context.Background(), lockName,
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
	_ = c.ReleaseLock(context.Background(), lockedItem)

	c2 := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestReadLockContentAfterDeleteOnRelease#2"),
	)
	if err != nil {
		t.Fatal(err)
	}

	lockItemRead, err := c2.Get(context.Background(), lockName)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(context.Background())

	t.Log("reading someone else's lock:", string(lockItemRead.Data()))
	if got := string(lockItemRead.Data()); got != "" {
		t.Error("keeping information inside lock storage, wanted empty got:", got)
	}
}

func TestFailIfLocked(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("FailIfLocked#1"),
		dynamolock.WithPartitionKeyName("key"),
	)
	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	_, err := c.AcquireLock(context.Background(), "failIfLocked")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.AcquireLock(context.Background(), "failIfLocked", dynamolock.FailIfLocked())
	if !isLockNotGrantedError(err) {
		t.Fatal("expected error (LockNotGrantedError) not found:", err)
		return
	}
}

func TestClientWithAdditionalAttributes(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestClientWithAdditionalAttributes#1"),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	t.Run("good attributes", func(t *testing.T) {
		t.Parallel()
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
		_ = c.ReleaseLock(context.Background(), lockedItem)
	})
	t.Run("bad attributes", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestDeleteLockOnRelease#1"),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
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
	_ = c.ReleaseLock(context.Background(), lockedItem)

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
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestCustomRefreshPeriod#1"),
		dynamolock.WithLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	lockedItem, err := c.AcquireLock(context.Background(), "custom-refresh-period")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.ReleaseLock(context.Background(), lockedItem) }()

	_, _ = c.AcquireLock(context.Background(), "custom-refresh-period", dynamolock.WithRefreshPeriod(100*time.Millisecond))
	if !strings.Contains(buf.String(), "Sleeping for a refresh period of  100ms") {
		t.Fatal("did not honor refreshPeriod")
	}
}

func TestCustomAdditionalTimeToWaitForLock(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestCustomAdditionalTimeToWaitForLock#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	t.Log("acquire lock")
	l, err := c.AcquireLock(context.Background(), "custom-additional-time-to-wait")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 3; i++ {
			_ = c.SendHeartbeat(context.Background(), l)
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
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestClientClose#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
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
	if err := c.Close(context.Background()); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (close after close):", err)
	}
	t.Log("heartbeat after close")
	if err := c.SendHeartbeat(context.Background(), lockItem1); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (heartbeat after close):", err)
	}
	t.Log("release after close")
	if err := c.ReleaseLock(context.Background(), lockItem1); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (release after close):", err)
	}
	t.Log("get after close")
	if _, err := c.Get(context.Background(), "bulkClose1"); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (get after close):", err)
	}
	t.Log("acquire after close")
	if _, err := c.AcquireLock(context.Background(), "acquireAfterClose"); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (acquire after close):", err)
	}
	t.Log("create table after close")
	if _, err := c.CreateTable(context.Background(), "createTableAfterClose"); !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Error("expected error missing (create table after close):", err)
	}
}

func TestInvalidReleases(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	logger := &bufferedLogger{}
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestInvalidReleases#1"),
		dynamolock.WithLogger(logger),
		dynamolock.WithPartitionKeyName("key"),
	)
	t.Cleanup(func() {
		t.Log(logger.String())
	})

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	t.Run("release nil lock", func(t *testing.T) {
		t.Parallel()
		var l *dynamolock.Lock
		if err := c.ReleaseLock(context.Background(), l); err == nil {
			t.Fatal("nil locks should trigger error on release:", err)
		} else {
			t.Log("nil lock:", err)
		}
	})

	t.Run("release empty lock", func(t *testing.T) {
		t.Parallel()
		emptyLock := &dynamolock.Lock{}
		if err := c.ReleaseLock(context.Background(), emptyLock); !errors.Is(err, dynamolock.ErrOwnerMismatched) {
			t.Fatal("empty locks should return error:", err)
		} else {
			t.Log("emptyLock:", err)
		}
	})

	t.Run("duplicated lock close", func(t *testing.T) {
		t.Parallel()
		l, err := c.AcquireLock(context.Background(), "duplicatedLockRelease")
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ReleaseLock(context.Background(), l); err != nil {
			t.Fatal("first close should be flawless:", err)
		}
		if err := c.ReleaseLock(context.Background(), l); err != nil {
			t.Fatal("second close should be fail")
		}
	})

	t.Run("nil lock close", func(t *testing.T) {
		t.Parallel()
		var l *dynamolock.Lock
		if err := c.ReleaseLock(context.Background(), l); !errors.Is(err, dynamolock.ErrCannotReleaseNullLock) {
			t.Fatal("wrong error when closing nil lock:", err)
		}
	})
}

func TestClientWithDataAfterRelease(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestClientWithDataAfterRelease#1"),
		dynamolock.WithLogger(&testLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)

	t.Log("ensuring table exists")
	_, _ = c.CreateTable(context.Background(), "locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	const lockName = "lockNoData"

	lockItem, err := c.AcquireLock(context.Background(), lockName)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("there is life after release")
	if err := c.ReleaseLock(context.Background(), lockItem, dynamolock.WithDataAfterRelease(data)); err != nil {
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

func (t *testLogger) Println(v ...any) {
	t.t.Helper()
	t.t.Log(v...)
}

type testContextLogger struct {
	t *testing.T
}

func (t *testContextLogger) Println(_ context.Context, v ...any) {
	t.t.Helper()
	t.t.Log(v...)
}

func TestBadDynamoDB(t *testing.T) {
	t.Parallel()
	t.Run("get", func(t *testing.T) {
		t.Parallel()
		svc := &dynamolock.DynamoDBClientMock{
			GetItemFunc: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return nil, errors.New("service is offline")
			},
		}
		c := dynamolock.New(svc, "locksHBError")
		if _, err := c.Get(context.Background(), "bad-dynamodb"); err == nil {
			t.Fatal("expected error missing")
		}
	})
	t.Run("acquire", func(t *testing.T) {
		t.Parallel()
		svc := &dynamolock.DynamoDBClientMock{
			GetItemFunc: func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return nil, errors.New("service is offline")
			},
		}
		c := dynamolock.New(svc, "locksHBError")
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

func isLockNotGrantedError(err error) bool {
	if err == nil {
		return false
	}
	var errLockNotGranted *dynamolock.LockNotGrantedError
	return errors.As(err, &errLockNotGranted)
}

func TestAcquireLockOnCloseClient(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestAcquireLockOnCloseClient#1"),
		dynamolock.WithContextLogger(&testContextLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)
	c.Close(context.Background())

	_, err := c.AcquireLock(context.Background(), "closeClientLock")
	if !errors.Is(err, dynamolock.ErrClientClosed) {
		t.Fatal("missing expected error:", err)
	}
}

func TestAcquireLockOnCanceledContext(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestAcquireLockOnCanceledContext#1"),
		dynamolock.WithContextLogger(&testContextLogger{t: t}),
		dynamolock.WithPartitionKeyName("key"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Get(ctx, "closeClientLock")
	if !errors.Is(err, ctx.Err()) {
		t.Fatal("missing expected error:", err)
	}
}

func TestTableTags(t *testing.T) {
	t.Parallel()
	var gotTags bool
	tableTag := types.Tag{Key: aws.String("tagName"), Value: aws.String("tagValue")}
	svc := &dynamolock.DynamoDBClientMock{
		CreateTableFunc: func(ctx context.Context, cti *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			for _, tag := range cti.Tags {
				if tag.Key == tableTag.Key && tag.Value == tableTag.Value {
					gotTags = true
					break
				}
			}
			return &dynamodb.CreateTableOutput{}, nil
		},
	}
	c := dynamolock.New(svc,
		"locksWithTags",
		dynamolock.WithOwnerName("TestTableTags#1"),
		dynamolock.WithContextLogger(&testContextLogger{t: t}),
	)
	if _, err := c.CreateTable(context.Background(), "locksWithTags", dynamolock.WithTags([]types.Tag{tableTag})); err != nil {
		t.Fatal(err)
	}
	if !gotTags {
		t.Fatal("API request missed tags")
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
