package dynamolock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This test covers the early return path in SendHeartbeatWithContext when the
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
	_, _ = c.CreateTable("immediateCancel",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	lock, err := c.AcquireLock("lock-heartbeat-immediate-cancel")
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

	err = c.SendHeartbeatWithContext(ctx, lock)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// This test forces a ConditionalCheckFailedException from UpdateItem so that
// SendHeartbeatWithContext takes the retry+parseDynamoDBError path and returns
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
	_, _ = c.CreateTable("condCheckFailed",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	lock, err := c.AcquireLock("lock-heartbeat-ccf")
	if err != nil {
		t.Fatal(err)
	}

	// Force ConditionalCheckFailedException on UpdateItem.
	svc.updateItemPost = func(uio *dynamodb.UpdateItemOutput, err error) (*dynamodb.UpdateItemOutput, error) {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("ccf")}
	}

	err = c.SendHeartbeat(lock, dynamolock.HeartbeatRetries(0, 0))
	var lng *dynamolock.LockNotGrantedError
	if !errors.As(err, &lng) {
		t.Fatalf("expected LockNotGrantedError, got %v", err)
	}
}
