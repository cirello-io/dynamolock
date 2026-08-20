package dynamolock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dynamolock "cirello.io/dynamolock/v5"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This test covers the early return path in SendHeartbeat when the
// underlying UpdateItem returns a context error immediately. That exercises the
// `if errors.Is(err, ctx.Err()) { return ctx.Err() }` branch that was not fully
// covered.
func TestHeartbeatImmediateContextCancel(t *testing.T) {
	svc := &interceptedDynamoDBClient{
		DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
	}
	c, err := dynamolock.New(svc,
		"immediateCancel",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatImmediateContextCancel"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure table exists
	_, _ = c.CreateTable(context.Background(), "immediateCancel",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	lock, err := c.AcquireLock(context.Background(), "lock-heartbeat-immediate-cancel")
	if err != nil {
		t.Fatal(err)
	}

	// Make UpdateItem immediately surface the context cancellation on the first
	// heartbeat attempt (no retries involved).
	svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
		return nil, context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling the heartbeat to hit the early branch

	err = c.SendHeartbeat(ctx, lock)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// This test forces a ConditionalCheckFailedException from UpdateItem so that
// SendHeartbeat takes the retry+parseDynamoDBError path and returns
// a *LockNotGrantedError. It also exercises the branch that attempts to delete
// the lock from the client's internal map.
func TestHeartbeatConditionalCheckFailedTransformsError(t *testing.T) {
	t.Parallel()

	svc := &interceptedDynamoDBClient{
		DynamoDBClient: dynamodb.NewFromConfig(defaultConfig(t)),
	}
	c, err := dynamolock.New(svc,
		"condCheckFailed",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestHeartbeatCCF"),
		dynamolock.DisableHeartbeat(),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure table exists
	_, _ = c.CreateTable(context.Background(), "condCheckFailed",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	lock, err := c.AcquireLock(context.Background(), "lock-heartbeat-ccf")
	if err != nil {
		t.Fatal(err)
	}

	// Force ConditionalCheckFailedException on UpdateItem.
	svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("ccf")}
	}

	err = c.SendHeartbeat(context.Background(), lock, dynamolock.HeartbeatRetries(0, 0))
	if _, ok := errors.AsType[*dynamolock.LockNotGrantedError](err); !ok {
		t.Fatalf("expected LockNotGrantedError, got %v", err)
	}
}
