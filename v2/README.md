# DynamoDB Lock Client for Go v2

[![Build status](https://github.com/cirello-io/dynamolock/actions/workflows/v2.yml/badge.svg)](https://github.com/cirello-io/dynamolock/actions/workflows/v2.yml)
[![GoDoc](https://pkg.go.dev/badge/cirello.io/dynamolock)](https://pkg.go.dev/cirello.io/dynamolock/v2)

The dynamoDB Lock Client for Go is a general purpose distributed locking library
built for DynamoDB. The dynamoDB Lock Client for Go supports both fine-grained
and coarse-grained locking as the lock keys can be any arbitrary string, up to a
certain length.

It is a port in Go of Amazon's original [dynamodb-lock-client](https://github.com/awslabs/dynamodb-lock-client) using the AWS's latest Go SDK.

This library is done, and it is in maintenance mode. Updates and bug fixes will
be made as necessary on a best effort basis, but no new features will be added.

## Use cases
A common use case for this lock client is:
let's say you have a distributed system that needs to periodically do work on a
given campaign (or a given customer, or any other object) and you want to make
sure that two boxes don't work on the same campaign/customer at the same time.
An easy way to fix this is to write a system that takes a lock on a customer,
but fine-grained locking is a tough problem. This library attempts to simplify
this locking problem on top of DynamoDB.

Another use case is leader election. If you only want one host to be the leader,
then this lock client is a great way to pick one. When the leader fails, it will
fail over to another host within a customizable leaseDuration that you set.

## Getting Started
To use the DynamoDB Lock Client for Go, you must make it sure it is present in
`$GOPATH` or in your vendor directory.

```sh
$ go get -u cirello.io/dynamolock/v2
```

This package has the `go.mod` file to be used with Go's module system.

Then, you need to set up a DynamoDB table that has a hash key on a key with the
name `key`. For your convenience, there is a function in the package called
`CreateTable` that you can use to set up your table, but it is also possible to
set up the table in the AWS Console. The table should be created in advance,
since it takes a couple minutes for DynamoDB to provision your table for you.
The package level documentation comment has an example of how to use this
package.

First you have to create the table and wait for DynamoDB to complete:
```Go
package main

import (
	"log"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatal(err)
	}
	c, err := dynamolock.New(dynamodb.NewFromConfig(cfg),
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	log.Println("ensuring table exists")
	_, err = c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

Once you see the table is created in the DynamoDB console, you should be ready
to run:

```Go
package main

import (
	"log"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatal(err)
	}
	c, err := dynamolock.New(dynamodb.NewFromConfig(cfg),
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	data := []byte("some content a")
	lockedItem, err := c.AcquireLock("spock",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("lock content:", string(lockedItem.Data()))
	if got := string(lockedItem.Data()); string(data) != got {
		log.Println("losing information inside lock storage, wanted:", string(data), " got:", got)
	}

	log.Println("cleaning lock")
	success, err := c.ReleaseLock(lockedItem)
	if !success {
		log.Fatal("lost lock before release")
	}
	if err != nil {
		log.Fatal("error releasing lock:", err)
	}
	log.Println("done")
}
```

## Selected Features
### Send Automatic Heartbeats
When you create the lock client, you can specify `WithHeartbeatPeriod(time.Duration)`
like in the above example, and it will spawn a background goroutine that
continually updates the record version number on your locks to prevent them from
expiring (it does this by calling the `SendHeartbeat()` method in the lock
client.) This will ensure that as long as your application is running, your
locks will not expire until you call `ReleaseLock()` or `lockItem.Close()`

### Read the data in a lock without acquiring it
You can read the data in the lock without acquiring it, and find out who owns
the lock. Here's how:
```Go
lock, err := lockClient.Get("kirk");
```

## Logic to avoid problems with clock skew
The lock client never stores absolute times in DynamoDB -- only the relative
"lease duration" time is stored in DynamoDB. The way locks are expired is that a
call to acquireLock reads in the current lock, checks the RecordVersionNumber of
the lock (which is a GUID) and starts a timer. If the lock still has the same
GUID after the lease duration time has passed, the client will determine that
the lock is stale and expire it.

What this means is that, even if two different machines disagree about what time
it is, they will still avoid clobbering each other's locks.

## Required DynamoDB Actions
For an IAM role to take full advantage of `dynamolock/v2`, it must be allowed to
perform all of the following actions on the DynamoDB table containing the locks:

* `GetItem`
* `PutItem`
* `UpdateItem`
* `DeleteItem`
* `CreateTable`
