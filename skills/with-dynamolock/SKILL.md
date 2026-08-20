---
name: with-dynamolock
description: Use this skill when building or debugging a Go application that imports cirello.io/dynamolock/v5 for DynamoDB-backed distributed locks, fine-grained coordination, or leader election. Use it for AWS SDK v2 setup, table schema and IAM, acquisition, waiting, heartbeats, release, lock data, session monitors, sort keys, and typed error handling. Do not use it for changing dynamolock's own source or tests.
license: Apache-2.0
compatibility: Requires Go 1.27 or newer, AWS SDK for Go v2, access to DynamoDB, and IAM permissions for the configured lock table.
metadata:
  audience: dynamolock-consumers
  version: "1.0"
---

# Build with dynamolock

Use `cirello.io/dynamolock/v5` as a coordination primitive, not as a general
purpose data store. Establish the table and lifecycle deliberately, bound all
waits with contexts, and stop protected work when ownership becomes uncertain.

## Setup checklist

1. Add the module with `go get cirello.io/dynamolock/v5` and use the AWS SDK for
   Go v2 import paths from `go.mod`, such as
   `github.com/aws/aws-sdk-go-v2/config` and
   `github.com/aws/aws-sdk-go-v2/service/dynamodb`. Do not copy the stale
   `github.com/aws/aws-sdk-go-v5/...` paths that may appear in older README
   snippets.
2. Create a DynamoDB table before acquiring locks, and wait until provisioning
   finishes. The default schema is a string hash key named `key`. A table may
   also have a string sort key; configure both its name and value with
   `WithSortKey` on the client.
3. If using the convenience `CreateTable`, its default billing mode is
   pay-per-request. Use `WithProvisionedThroughput` when appropriate,
   `WithCustomPartitionKeyName` for a non-default hash key,
   `WithSortKeyName` for a range key, and `WithTags` for table tags. Do not
   recreate a table on every application start unless the provisioning policy
   explicitly handles an already-existing table.
4. Grant the application role `GetItem`, `PutItem`, `UpdateItem`, and
   `DeleteItem` on the lock table. Grant `CreateTable` only to the component
   that provisions tables.

## Construct a client

Use the same table/key configuration in every process that competes for a
lock. The following is the normal AWS SDK v2 shape:

```go
cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
if err != nil {
    return err
}

db := dynamodb.NewFromConfig(cfg)
locks, err := dynamolock.New(db, "locks",
    dynamolock.WithLeaseDuration(30*time.Second),
    dynamolock.WithHeartbeatPeriod(5*time.Second),
    dynamolock.WithOwnerName(instanceID),
)
if err != nil {
    return err
}
defer func() { _ = locks.Close(context.Background()) }()
```

The defaults are a 20-second lease and 5-second heartbeat. A heartbeat must
not exceed half the lease; use a larger safety margin when network latency or
process pauses matter. An owner name is generated if omitted; set one only
when its identity is useful and make it unique enough for the deployment.

## Acquire and release safely

Use a bounded context for the acquisition. Without `FailIfLocked`, acquisition
waits and retries after the observed lease; `WithRefreshPeriod` controls retry
spacing and `WithAdditionalTimeToWaitForLock` extends the wait. Use
`FailIfLocked()` for an immediate attempt.

```go
workCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
defer cancel()

lock, err := locks.AcquireLock(workCtx, "campaign:"+campaignID,
    dynamolock.WithData(payload),
    dynamolock.ReplaceData(),
)
if err != nil {
    return err
}
defer func() {
    releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer releaseCancel()
    if err := lock.Close(releaseCtx); err != nil {
        logger.Printf("release lock: %v", err)
    }
}()

// Perform only work that is safe while this lock is owned.
```

Prefer `Lock.Close` for normal cleanup. If using `ReleaseLock` directly, check
both return values: `success == false` means the lock was not safely released.
`Client.Close` also releases locks held by that client and stops its automatic
heartbeat, so close it during graceful shutdown.

## Ownership and observation

- A successful acquire is fenced by a record version number (RVN) and owner.
  Never assume a local `*Lock` remains valid after a heartbeat or conditional
  write error; stop or reconcile the protected work.
- `Get(ctx, key)` reads metadata and data without acquiring. A result loaded
  from DynamoDB is read-only for heartbeat purposes; do not call
  `SendHeartbeat` or use it to release someone else's lock. A missing key is
  represented by an empty expired `Lock`.
- `Get` can return a cached lock previously acquired by the same client. Keep
  ownership decisions tied to the successful acquire and its lifecycle, not to
  a lock object passed between clients.
- For leader election, use a session monitor and relinquish leadership in its
  callback. A session monitor is an early warning, not a lease extension or a
  guarantee that another process cannot acquire the lock.

## Heartbeats and data

Automatic heartbeats are enabled when `WithHeartbeatPeriod` is positive. If
using `DisableHeartbeat()`, call `SendHeartbeat(ctx, lock)` repeatedly before
the lease expires. The options `DeleteData()` and
`ReplaceHeartbeatData(data)` change lock data during a manual heartbeat.
`HeartbeatRetries(retries, wait)` can handle transient failures; retries still
verify the stored lock and must not be used to hide a lost lock.

Use `WithData(data)` to set data on acquisition. Existing data is preserved on
reacquisition unless `ReplaceData()` is supplied. On release, use
`WithDataAfterRelease(data)` to persist replacement data, or
`WithDeleteLockOnRelease()` at acquisition time / `WithDeleteLock(true)` at
release time to remove the row. Without a delete option, release marks the row
released and normally retains its data and additional attributes. Additional
attributes are merged and preserved, but treat the partition key, configured
sort key, `ownerName`, `leaseDuration`, `recordVersionNumber`, `isReleased`,
and `data` as reserved.

For a session monitor, choose a safe time before expiry. The implementation's
rule of thumb is `leaseDuration - (3 * heartbeatPeriod)`; for example:

```go
dynamolock.WithSessionMonitor(30*time.Second-(3*5*time.Second), func() {
    // Stop serving as leader and cancel protected work.
})
```

The callback runs at most once and will not rescue an already expired lock.
`Lock.IsAlmostExpired()` requires a session monitor and reports whether its
danger zone has begun.

## Handle errors by type

Use Go's error wrapping rather than matching strings:

```go
var notGranted *dynamolock.LockNotGrantedError
if errors.As(err, &notGranted) {
    // A competing owner, an expired conditional write, or a timeout.
}

var timeout *dynamolock.TimeoutError
if errors.As(err, &timeout) {
    logger.Printf("lock wait lasted %s", timeout.Age)
}

if errors.Is(err, dynamolock.ErrReadOnlyLockHeartbeat) {
    // The lock came from Get, not from a successful acquire.
}
if errors.Is(err, dynamolock.ErrClientClosed) {
    // Recreate the client only as part of an intentional lifecycle decision.
}
```

Treat conditional-check failures, heartbeat failures, owner mismatches, and
context cancellation as signals to stop or retry the application-level
operation according to its safety policy. Never continue exclusive work merely
because the process still has a `*Lock` value.
