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

package dynamolock

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// SendHeartbeatOption allows to proceed with Lock content changes in the
// heartbeat cycle.
type SendHeartbeatOption func(*sendHeartbeatOptions)

type sendHeartbeatOptions struct {
	data           []byte
	deleteData     bool
	matchOwnerOnly bool
}

// DeleteData removes the Lock data on heartbeat.
func DeleteData() SendHeartbeatOption {
	return func(o *sendHeartbeatOptions) {
		o.deleteData = true
	}
}

// ReplaceHeartbeatData overrides the content of the Lock in the heartbeat cycle.
func ReplaceHeartbeatData(data []byte) SendHeartbeatOption {
	return func(o *sendHeartbeatOptions) {
		o.deleteData = false
		o.data = data
	}
}

// MatchOwnerOnly helps dealing with network transient errors by ignoring
// internal record version number and matching only against the owner and the
// partition key name. If lock owner is globally unique, then this feature is
// safe to use.
func MatchOwnerOnly() SendHeartbeatOption {
	return func(o *sendHeartbeatOptions) {
		o.matchOwnerOnly = true
	}
}

// SendHeartbeat indicates that the given lock is still being worked
// on. The given context is passed down to the underlying dynamoDB call.
func (c *Client) SendHeartbeat(ctx context.Context, lockItem *Lock, opts ...SendHeartbeatOption) error {
	if c.isClosed() {
		return ErrClientClosed
	}
	sho := &sendHeartbeatOptions{}
	for _, opt := range opts {
		opt(sho)
	}
	lockItem.semaphore.Lock()
	defer lockItem.semaphore.Unlock()

	if lockItem.isExpired() || lockItem.ownerName != c.ownerName || lockItem.isReleased {
		return &LockNotGrantedError{msg: "cannot send heartbeat because lock is not granted"}
	}
	currentRecordVersionNumber := lockItem.recordVersionNumber
	if currentRecordVersionNumber == "" {
		return ErrReadOnlyLockHeartbeat
	}

	targetRecordVersionNumber := c.generateRecordVersionNumber()
	leaseDuration := c.leaseDuration

	cond := unsafeOwnershipLockCondition(c.partitionKeyName, currentRecordVersionNumber, lockItem.ownerName, sho.matchOwnerOnly)
	update := expression.
		Set(leaseDurationAttr, expression.Value(leaseDuration.String())).
		Set(rvnAttr, expression.Value(targetRecordVersionNumber))

	if sho.deleteData {
		update.Remove(dataAttr)
	} else if len(sho.data) > 0 {
		update.Set(dataAttr, expression.Value(sho.data))
	}
	updateExpr, _ := expression.NewBuilder().WithCondition(cond).WithUpdate(update).Build()

	updateItemInput := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(c.tableName),
		Key:                       c.getItemKeys(lockItem),
		ConditionExpression:       updateExpr.Condition(),
		UpdateExpression:          updateExpr.Update(),
		ExpressionAttributeNames:  updateExpr.Names(),
		ExpressionAttributeValues: updateExpr.Values(),
	}

	lastUpdateOfLock := time.Now()

	_, err := c.dynamoDB.UpdateItem(ctx, updateItemInput)
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	} else if err != nil {
		return err
	}

	lockItem.updateRVN(targetRecordVersionNumber, lastUpdateOfLock, leaseDuration)
	if sho.deleteData {
		lockItem.data = nil
	} else if len(sho.data) > 0 {
		lockItem.data = sho.data
	}
	return nil
}
