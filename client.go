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

// Package dynamolock provides a simple utility for using DynamoDB's consistent
// read/write feature to use it for managing distributed locks.
//
// In order to use this package, the client must create a table in DynamoDB,
// although the client provides a convenience method for creating that table
// (CreateTable).
//
// Basic usage:
//
//	import (
//		"log"
//
//		"cirello.io/dynamolock"
//		"github.com/aws/aws-sdk-go/aws"
//		"github.com/aws/aws-sdk-go/aws/session"
//		"github.com/aws/aws-sdk-go/service/dynamodb"
//	)
//
//	// ---
//
//	svc := dynamodb.New(session.New(), &aws.Config{
//		Region:   aws.String("us-west-2"),
//	})
//	c, err := dynamolock.New(svc,
//		"locks",
//		dynamolock.WithLeaseDuration(3*time.Second),
//		dynamolock.WithHeartbeatPeriod(1*time.Second),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	log.Println("ensuring table exists")
//	c.CreateTable("locks",
//		&dynamodb.ProvisionedThroughput{
//			ReadCapacityUnits:  aws.Int64(5),
//			WriteCapacityUnits: aws.Int64(5),
//		},
//		dynamolock.WithCustomPartitionKeyName("key"),
//	)
//
//	data := []byte("some content a")
//	lockedItem, err := c.AcquireLock("spock",
//		dynamolock.WithData(data),
//		dynamolock.ReplaceData(),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	log.Println("lock content:", string(lockedItem.Data()))
//	if got := string(lockedItem.Data()); string(data) != got {
//		log.Println("losing information inside lock storage, wanted:", string(data), " got:", got)
//	}
//
//	log.Println("cleaning lock")
//	success, err := c.ReleaseLock(lockedItem)
//	if !success {
//		log.Fatal("lost lock before release")
//	}
//	if err != nil {
//		log.Fatal("error releasing lock:", err)
//	}
//	log.Println("done")
package dynamolock

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	dataPathExpressionVariable               = "#d"
	dataValueExpressionVariable              = ":d"
	isReleasedPathExpressionVariable         = "#ir"
	isReleasedValue                          = "1"
	isReleasedValueExpressionVariable        = ":ir"
	leaseDurationPathValueExpressionVariable = "#ld"
	leaseDurationValueExpressionVariable     = ":ld"
	newRvnValueExpressionVariable            = ":newRvn"
	ownerNamePathExpressionVariable          = "#on"
	ownerNameValueExpressionVariable         = ":on"
	pkPathExpressionVariable                 = "#pk"
	rvnPathExpressionVariable                = "#rvn"
	rvnValueExpressionVariable               = ":rvn"
	skPathExpressionVariable                 = "#sk"
)

var (
	pkExistsAndSkExistsAndOwnerNameSameAndRvnSameCondition = fmt.Sprintf(
		"%s AND %s = %s",
		pkExistsAndSkExistsAndRvnIsTheSameCondition, ownerNamePathExpressionVariable, ownerNameValueExpressionVariable)

	pkExistsAndOwnerNameSameAndRvnSameCondition = fmt.Sprintf("%s AND %s = %s",
		pkExistsAndRvnIsTheSameCondition, ownerNamePathExpressionVariable, ownerNameValueExpressionVariable)

	updateLeaseDurationAndRvnAndRemoveData = fmt.Sprintf("%s REMOVE %s",
		updateLeaseDurationAndRvn, dataPathExpressionVariable)

	updateLeaseDurationAndRvnAndData = fmt.Sprintf("%s, %s = %s",
		updateLeaseDurationAndRvn, dataPathExpressionVariable, dataValueExpressionVariable)

	updateLeaseDurationAndRvn = fmt.Sprintf(
		"SET %s = %s, %s = %s",
		leaseDurationPathValueExpressionVariable, leaseDurationValueExpressionVariable,
		rvnPathExpressionVariable, newRvnValueExpressionVariable)

	updateIsReleasedAndData = fmt.Sprintf("%s, %s = %s",
		updateIsReleased, dataPathExpressionVariable, dataValueExpressionVariable)

	updateIsReleased = fmt.Sprintf("SET %s = %s", isReleasedPathExpressionVariable, isReleasedValueExpressionVariable)
)

var isReleasedAttributeValue = &dynamodb.AttributeValue{S: aws.String(isReleasedValue)}
var acquireLockThatDoesntExistOrIsReleasedCondition = fmt.Sprintf(
	"attribute_not_exists(%s) OR (attribute_exists(%s) AND %s = %s)",
	pkPathExpressionVariable, pkPathExpressionVariable,
	isReleasedPathExpressionVariable, isReleasedValueExpressionVariable)

// Logger defines the minimum desired logger interface for the lock client.
type Logger interface {
	Println(v ...interface{})
}

// Client is a dynamoDB based distributed lock client.
type Client struct {
	dynamoDB *dynamodb.DynamoDB

	tableName        string
	partitionKeyName string
	sortKeyName      *string

	leaseDuration   time.Duration
	heartbeatPeriod time.Duration
	ownerName       string
	locks           sync.Map

	logger Logger

	mu            sync.Mutex
	lastHeartbeat time.Time
}

const (
	defaultPartitionKeyName = "key"
	defaultLeaseDuration    = 20 * time.Second
	defaultHeartbeatPeriod  = 5 * time.Second
)

// New creates a new dynamoDB based distributed lock client.
func New(dynamoDB *dynamodb.DynamoDB, tableName string, opts ...ClientOption) (*Client, error) {
	c := &Client{
		dynamoDB:         dynamoDB,
		tableName:        tableName,
		partitionKeyName: defaultPartitionKeyName,
		leaseDuration:    defaultLeaseDuration,
		heartbeatPeriod:  defaultHeartbeatPeriod,
		ownerName:        randString(32),
		logger:           log.New(ioutil.Discard, "", 0),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.leaseDuration < 2*c.heartbeatPeriod {
		return nil, errors.New("Heartbeat period must be no more than half the length of the Lease Duration, " +
			"or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example " +
			"4+ times greater)")
	}

	return c, nil
}

// ClientOption reconfigure the lock client creation.
type ClientOption func(*Client)

// WithPartitionKeyName defines the key name used for asserting keys uniqueness.
func WithPartitionKeyName(s string) ClientOption {
	return func(c *Client) { c.partitionKeyName = s }
}

// WithOwnerName changes the owner linked to the client, and by consequence to
// locks.
func WithOwnerName(s string) ClientOption {
	return func(c *Client) { c.ownerName = s }
}

// WithLeaseDuration defines how long should the lease be held.
func WithLeaseDuration(d time.Duration) ClientOption {
	return func(c *Client) { c.leaseDuration = d }
}

// WithHeartbeatPeriod defines the frequency of the heartbeats. Set to zero to
// disable it. Heartbeats should have no more than half of the duration of the
// lease.
func WithHeartbeatPeriod(d time.Duration) ClientOption {
	return func(c *Client) { c.heartbeatPeriod = d }
}

// DisableHeartbeat disables automatic hearbeats. Use SendHeartbeat to freshen
// up the lock.
func DisableHeartbeat() ClientOption {
	return WithHeartbeatPeriod(0)
}

// WithLogger injects a logger into the client, so its internals can be
// recorded.
func WithLogger(l Logger) ClientOption {
	return func(c *Client) { c.logger = l }
}

const (
	attrData                = "data"
	attrOwnerName           = "ownerName"
	attrLeaseDuration       = "leaseDuration"
	attrRecordVersionNumber = "recordVersionNumber"
	attrIsReleased          = "isReleased"

	defaultBuffer = 1 * time.Second
)

// AcquireLockOption allows to change how the lock is actually held by the
// client.
type AcquireLockOption func(*acquireLockOptions)

// WithData stores the content into the lock itself.
func WithData(b []byte) AcquireLockOption {
	return func(opt *acquireLockOptions) {
		opt.data = b
	}
}

// ReplaceData will force the new content to be stored in the key
func ReplaceData() AcquireLockOption {
	return func(opt *acquireLockOptions) {
		opt.replaceData = true
	}
}

// AcquireLock holds the defined lock.
func (c *Client) AcquireLock(key string, opts ...AcquireLockOption) (*Lock, error) {
	req := &acquireLockOptions{
		partitionKey: key,
	}
	for _, opt := range opts {
		opt(req)
	}
	return c.acquireLock(req)
}

func (c *Client) acquireLock(opt *acquireLockOptions) (*Lock, error) {
	key := opt.partitionKey
	sortKey := opt.sortKey

	attrs := opt.additionalAttributes
	contains := func(k string) bool {
		_, ok := attrs[k]
		return ok
	}

	if contains(c.partitionKeyName) || contains(attrOwnerName) ||
		contains(attrLeaseDuration) || contains(attrRecordVersionNumber) ||
		contains(attrData) || (c.sortKeyName != nil && contains(*c.sortKeyName)) {
		return nil, fmt.Errorf("Additional attribute cannot be one of the following types: %s, %s, %s, %s, %s",
			c.partitionKeyName, attrOwnerName, attrLeaseDuration, attrRecordVersionNumber, attrData)
	}

	millisecondsToWait := defaultBuffer
	if opt.additionalTimeToWaitForLock > 0 {
		millisecondsToWait = opt.additionalTimeToWaitForLock
	}

	refreshPeriodInMilliseconds := defaultBuffer
	if opt.refreshPeriod > 0 {
		refreshPeriodInMilliseconds = opt.refreshPeriod
	}

	deleteLockOnRelease := opt.deleteLockOnRelease
	replaceData := opt.replaceData

	currentTimeMillis := time.Now()

	var lockTryingToBeAcquired *Lock
	var alreadySleptOnceForOneLeasePeriod bool

	getLockOptions := getLockOptions{
		partitionKeyName:    key,
		sortKeyName:         sortKey,
		deleteLockOnRelease: deleteLockOnRelease,
	}

	for {

		c.logger.Println("Call GetItem to see if the lock for ",
			c.partitionKeyName, " =", key, ", ",
			aws.StringValue(c.sortKeyName), "=", sortKey,
			" exists in the table")
		existingLock, err := c.getLockFromDynamoDB(getLockOptions)
		if err != nil {
			return nil, err
		}

		var newLockData []byte
		if replaceData {
			newLockData = opt.data
		} else if existingLock != nil {
			newLockData = existingLock.data
		}

		if newLockData == nil {
			// If there is no existing data, we write the input data to the lock.
			newLockData = opt.data
		}

		item := make(map[string]*dynamodb.AttributeValue)

		for k, v := range opt.additionalAttributes {
			item[k] = v
		}
		item[c.partitionKeyName] = &dynamodb.AttributeValue{S: aws.String(key)}
		item[attrOwnerName] = &dynamodb.AttributeValue{S: aws.String(c.ownerName)}
		item[attrLeaseDuration] = &dynamodb.AttributeValue{S: aws.String(c.leaseDuration.String())}

		recordVersionNumber := c.generateRecordVersionNumber()
		item[attrRecordVersionNumber] = &dynamodb.AttributeValue{S: aws.String(recordVersionNumber)}

		if c.sortKeyName != nil {
			item[aws.StringValue(c.sortKeyName)] = &dynamodb.AttributeValue{S: sortKey}
		}

		if newLockData != nil {
			item[attrData] = &dynamodb.AttributeValue{B: newLockData}
		}

		//if the existing lock does not exist or exists and is released
		if existingLock == nil || existingLock.isReleased {
			return c.upsertAndMonitorNewOrReleasedLock(opt, key,
				sortKey, deleteLockOnRelease,
				newLockData, item, recordVersionNumber)
		}

		// we know that we didnt enter the if block above because it returns at the end.
		// we also know that the existingLock.isPresent() is true
		if lockTryingToBeAcquired == nil {
			//this branch of logic only happens once, in the first iteration of the while loop
			//lockTryingToBeAcquired only ever gets set to non-null values after this point.
			//so it is impossible to get in this
			/*
			 * Someone else has the lock, and they have the lock for LEASE_DURATION time. At this point, we need
			 * to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock.
			 */
			lockTryingToBeAcquired = existingLock
			if !alreadySleptOnceForOneLeasePeriod {
				alreadySleptOnceForOneLeasePeriod = true
				millisecondsToWait += existingLock.leaseDuration
			}
		} else {
			if lockTryingToBeAcquired.recordVersionNumber == existingLock.recordVersionNumber {
				/* If the version numbers match, then we can acquire the lock, assuming it has already expired */
				if lockTryingToBeAcquired.IsExpired() {
					return c.upsertAndMonitorExpiredLock(opt,
						key, sortKey, deleteLockOnRelease,
						existingLock, newLockData, item,
						recordVersionNumber)
				}
			} else {
				/*
				 * If the version number changed since we last queried the lock, then we need to update
				 * lockTryingToBeAcquired as the lock has been refreshed since we last checked
				 */
				lockTryingToBeAcquired = existingLock
			}
		}

		if t := time.Since(currentTimeMillis); t > millisecondsToWait {
			return nil, &LockNotGrantedError{"Didn't acquire lock after sleeping for " + t.String() + " milliseconds"}
		}
		c.logger.Println("Sleeping for a refresh period of ", refreshPeriodInMilliseconds)
		time.Sleep(refreshPeriodInMilliseconds)
	}
}

var pkExistsAndRvnIsTheSameCondition = fmt.Sprintf(
	"attribute_exists(%s) AND %s = %s",
	pkPathExpressionVariable, rvnPathExpressionVariable, rvnValueExpressionVariable)
var pkExistsAndSkExistsAndRvnIsTheSameCondition = fmt.Sprintf(
	"attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
	pkPathExpressionVariable, skPathExpressionVariable, rvnPathExpressionVariable, rvnValueExpressionVariable)

func (c *Client) upsertAndMonitorExpiredLock(
	opt *acquireLockOptions,
	key string,
	sortKey *string,
	deleteLockOnRelease bool,
	existingLock *Lock,
	newLockData []byte,
	item map[string]*dynamodb.AttributeValue,
	recordVersionNumber string,
) (*Lock, error) {
	var conditionalExpression string
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		rvnValueExpressionVariable: &dynamodb.AttributeValue{S: aws.String(existingLock.recordVersionNumber)},
	}

	expressionAttributeNames := map[string]*string{
		pkPathExpressionVariable:  aws.String(c.partitionKeyName),
		rvnPathExpressionVariable: aws.String(attrRecordVersionNumber),
	}

	if c.sortKeyName != nil {
		conditionalExpression = pkExistsAndSkExistsAndRvnIsTheSameCondition
		expressionAttributeNames[skPathExpressionVariable] = c.sortKeyName
	} else {
		conditionalExpression = pkExistsAndRvnIsTheSameCondition
	}

	putItemRequest := &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(c.tableName),
		ConditionExpression:       aws.String(conditionalExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	c.logger.Println("Acquiring an existing lock whose revisionVersionNumber did not change for ",
		c.partitionKeyName, " partitionKeyName=", key, ", ", c.sortKeyName, "=", sortKey)
	return c.putLockItem(opt, key, sortKey,
		deleteLockOnRelease, newLockData,
		recordVersionNumber, putItemRequest)
}

func (c *Client) upsertAndMonitorNewOrReleasedLock(
	opt *acquireLockOptions,
	key string,
	sortKey *string,
	deleteLockOnRelease bool,
	newLockData []byte,
	item map[string]*dynamodb.AttributeValue,
	recordVersionNumber string,
) (*Lock, error) {

	expressionAttributeNames := map[string]*string{
		pkPathExpressionVariable:         aws.String(c.partitionKeyName),
		isReleasedPathExpressionVariable: aws.String(attrIsReleased),
	}

	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		isReleasedValueExpressionVariable: isReleasedAttributeValue,
	}

	req := &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 aws.String(c.tableName),
		ConditionExpression:       aws.String(acquireLockThatDoesntExistOrIsReleasedCondition),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	// No one has the lock, go ahead and acquire it. The person storing the
	// lock into DynamoDB should err on the side of thinking the lock will
	// expire sooner than it actually will, so they start counting towards
	// its expiration before the Put succeeds
	c.logger.Println("Acquiring a new lock or an existing yet released lock on ",
		c.partitionKeyName, "=", key, ", ",
		aws.StringValue(c.sortKeyName), "=", aws.StringValue(sortKey),
	)
	return c.putLockItem(opt, key, sortKey,
		deleteLockOnRelease, newLockData,
		recordVersionNumber, req)
}

func (c *Client) putLockItem(
	opt *acquireLockOptions,
	key string,
	sortKey *string,
	deleteLockOnRelease bool,
	newLockData []byte,
	recordVersionNumber string,
	putItemRequest *dynamodb.PutItemInput) (*Lock, error) {

	lastUpdatedTime := time.Now()

	_, err := c.dynamoDB.PutItem(putItemRequest)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return nil, &LockNotGrantedError{"Could not acquire lock because someone else acquired it: " + aerr.Error()}
			}
		}
		return nil, fmt.Errorf("cannot store lock item: %s", err)
	}

	lockItem := &Lock{
		client:               c,
		partitionKey:         key,
		sortKey:              sortKey,
		data:                 newLockData,
		deleteLockOnRelease:  deleteLockOnRelease,
		ownerName:            c.ownerName,
		leaseDuration:        c.leaseDuration,
		lookupTime:           lastUpdatedTime,
		recordVersionNumber:  recordVersionNumber,
		additionalAttributes: opt.additionalAttributes,
	}

	c.locks.Store(lockItem.uniqueIdentifier(), lockItem)
	c.enforceHeartbeat()
	return lockItem, nil
}

func (c *Client) getLockFromDynamoDB(opt getLockOptions) (*Lock, error) {
	res, err := c.readFromDynamoDB(opt.partitionKeyName, opt.sortKeyName)
	if err != nil {
		return nil, err
	}

	item := res.Item
	if item == nil {
		return nil, nil
	}

	return c.createLockItem(opt, item)
}

func (c *Client) readFromDynamoDB(key string, sortKey *string) (*dynamodb.GetItemOutput, error) {
	dynamoDBKey := map[string]*dynamodb.AttributeValue{
		c.partitionKeyName: &dynamodb.AttributeValue{S: aws.String(key)},
	}
	if sortKey != nil {
		dynamoDBKey[aws.StringValue(sortKey)] = &dynamodb.AttributeValue{S: sortKey}
	}
	return c.dynamoDB.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       dynamoDBKey,
	})
}

func (c *Client) createLockItem(opt getLockOptions, item map[string]*dynamodb.AttributeValue) (*Lock, error) {
	var data []byte
	if r, ok := item[attrData]; ok {
		data = r.B
		delete(item, attrData)
	}

	ownerName := item[attrOwnerName]
	delete(item, attrOwnerName)

	leaseDuration := item[attrLeaseDuration]
	delete(item, attrLeaseDuration)

	recordVersionNumber := item[attrRecordVersionNumber]
	delete(item, attrRecordVersionNumber)

	_, isReleased := item[attrIsReleased]
	delete(item, attrIsReleased)
	delete(item, c.partitionKeyName)

	// The person retrieving the lock in DynamoDB should err on the side of
	// not expiring the lock, so they don't start counting until after the
	// call to DynamoDB succeeds
	lookupTime := time.Now()

	parsedLeaseDuration, err := time.ParseDuration(aws.StringValue(leaseDuration.S))
	if err != nil {
		return nil, fmt.Errorf("cannot parse lease duration: %s", err)
	}

	lockItem := &Lock{
		client:               c,
		partitionKey:         opt.partitionKeyName,
		sortKey:              opt.sortKeyName,
		data:                 data,
		deleteLockOnRelease:  opt.deleteLockOnRelease,
		ownerName:            aws.StringValue(ownerName.S),
		leaseDuration:        parsedLeaseDuration,
		lookupTime:           lookupTime,
		recordVersionNumber:  aws.StringValue(recordVersionNumber.S),
		isReleased:           isReleased,
		additionalAttributes: item,
	}
	return lockItem, nil
}

func (c *Client) generateRecordVersionNumber() string {
	// TODO: improve me
	return randString(32)
}

var letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		// ignoring error as the only possible error is for io.ReadFull
		r, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letterRunes))))
		b[i] = letterRunes[r.Int64()]
	}
	return string(b)
}

func (c *Client) enforceHeartbeat() {
	if c.heartbeatPeriod == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	lastHeartbeat := c.lastHeartbeat
	isHeartbeatDead := time.Since(lastHeartbeat) > 2*c.heartbeatPeriod
	if isHeartbeatDead {
		go c.heartbeat()
	}
}

func (c *Client) heartbeat() {
	c.logger.Println("starting heartbeats")
	for range time.Tick(c.heartbeatPeriod) {

		touchedAnyLock := false

		c.locks.Range(func(_ interface{}, value interface{}) bool {
			touchedAnyLock = true

			lockItem := value.(*Lock)
			if err := c.SendHeartbeat(value.(*Lock)); err != nil {
				c.logger.Println("error sending heartbeat to", lockItem.partitionKey, ":", err)
			}

			return true
		})

		if !touchedAnyLock {
			c.logger.Println("no locks in the client, stopping heartbeat")
			break
		}

		c.mu.Lock()
		c.lastHeartbeat = time.Now()
		c.mu.Unlock()
	}
}

// SendHeartbeat indicatee that the given lock is still being worked on. If
// using WithHeartbeatPeriod > 0 when setting up this object, then this method
// is unnecessary, because the background thread will be periodically calling it
// and sending heartbeats. However, if WithHeartbeatPeriod = 0, then this method
// must be called to instruct DynamoDB that the lock should not be expired.
func (c *Client) SendHeartbeat(lockItem *Lock) error {
	return c.sendHeartbeat(&sendHeartbeatOptions{
		lockItem: lockItem,
	})
}

func (c *Client) sendHeartbeat(options *sendHeartbeatOptions) error {
	if options.deleteData && len(options.data) > 0 {
		return errors.New("data must not be present if deleteData is true")
	}

	leaseDurationToEnsure := c.leaseDuration
	if options.leaseDurationToEnsure > 0 {
		leaseDurationToEnsure = options.leaseDurationToEnsure
	}

	lockItem := options.lockItem
	lockItem.semaphore.Lock()
	defer lockItem.semaphore.Unlock()

	if lockItem.IsExpired() || lockItem.ownerName != c.ownerName || lockItem.isReleased {
		c.locks.Delete(lockItem.uniqueIdentifier())
		return &LockNotGrantedError{"cannot send heartbeat because lock is not granted"}
	}

	// Set up condition for UpdateItem. Basically any changes require:
	// 1. I own the lock
	// 2. I know the current version number
	// 3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)

	var conditionalExpression string
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		rvnValueExpressionVariable:       &dynamodb.AttributeValue{S: aws.String(lockItem.recordVersionNumber)},
		ownerNameValueExpressionVariable: &dynamodb.AttributeValue{S: aws.String(lockItem.ownerName)},
	}
	expressionAttributeNames := map[string]*string{
		pkPathExpressionVariable:                 aws.String(c.partitionKeyName),
		leaseDurationPathValueExpressionVariable: aws.String(attrLeaseDuration),
		rvnPathExpressionVariable:                aws.String(attrRecordVersionNumber),
		ownerNamePathExpressionVariable:          aws.String(attrOwnerName),
	}

	if c.sortKeyName != nil {
		conditionalExpression = pkExistsAndSkExistsAndOwnerNameSameAndRvnSameCondition
		expressionAttributeNames[skPathExpressionVariable] = c.sortKeyName
	} else {
		conditionalExpression = pkExistsAndOwnerNameSameAndRvnSameCondition
	}

	rvn := c.generateRecordVersionNumber()

	var updateExpression string
	expressionAttributeValues[newRvnValueExpressionVariable] = &dynamodb.AttributeValue{S: aws.String(rvn)}
	expressionAttributeValues[leaseDurationValueExpressionVariable] = &dynamodb.AttributeValue{S: aws.String(leaseDurationToEnsure.String())}
	if options.deleteData {
		expressionAttributeNames[dataPathExpressionVariable] = aws.String(attrData)
		updateExpression = updateLeaseDurationAndRvnAndRemoveData
	} else if len(options.data) > 0 {
		expressionAttributeNames[dataPathExpressionVariable] = aws.String(attrData)
		expressionAttributeValues[dataValueExpressionVariable] = &dynamodb.AttributeValue{B: options.data}
		updateExpression = updateLeaseDurationAndRvnAndData
	} else {
		updateExpression = updateLeaseDurationAndRvn
	}

	updateItemInput := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(c.tableName),
		Key:                       c.getItemKeys(lockItem),
		ConditionExpression:       aws.String(conditionalExpression),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	lastUpdateOfLock := time.Now()

	_, err := c.dynamoDB.UpdateItem(updateItemInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				c.locks.Delete(lockItem.uniqueIdentifier())
				return &LockNotGrantedError{"already acquired lock, stopping heartbeats: " + aerr.Error()}
			}
		}

		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		return err
	}

	lockItem.updateRVN(rvn, lastUpdateOfLock, leaseDurationToEnsure)
	return nil
}

// CreateTable prepares a DynamoDB table with the right schema for it to be used
// by this locking library. The table should be set up in advance, because it
// takes a few minutes for DynamoDB to provision a new instance. Also, if the
// table already exists, this will throw an exception.
//
// This method lets you specify a sort key to be used by the lock client. This
// sort key then needs to be specified in the AmazonDynamoDBLockClientOptions
// when the lock client object is created.
func (c *Client) CreateTable(tableName string, provisionedThroughput *dynamodb.ProvisionedThroughput, opts ...CreateTableOption) (*dynamodb.CreateTableOutput, error) {
	createTableOptions := &createDynamoDBTableOptions{
		tableName:             tableName,
		provisionedThroughput: provisionedThroughput,
		partitionKeyName:      defaultPartitionKeyName,
	}

	for _, opt := range opts {
		opt(createTableOptions)
	}

	return c.createTable(createTableOptions)
}

// CreateTableOption is an options type for the CreateTable method in the lock
// client. This allows the user to create a DynamoDB table that is lock
// client-compatible and specify optional parameters such as the desired
// throughput and whether or not to use a sort key.
type CreateTableOption func(*createDynamoDBTableOptions)

// WithCustomPartitionKeyName changes the partition key name of the table. If
// not specified, the default "key" will be used.
func WithCustomPartitionKeyName(s string) CreateTableOption {
	return func(opt *createDynamoDBTableOptions) {
		opt.partitionKeyName = s
	}
}

// WithCustomSortKeyName changes the sort key name of the table. If not
// specified, the table will only have a partition key.
func WithCustomSortKeyName(s string) CreateTableOption {
	return func(opt *createDynamoDBTableOptions) {
		opt.sortKeyName = &s
	}
}

func (c *Client) createTable(opt *createDynamoDBTableOptions) (*dynamodb.CreateTableOutput, error) {
	keySchema := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: aws.String(opt.partitionKeyName),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}

	attributeDefinitions := []*dynamodb.AttributeDefinition{
		&dynamodb.AttributeDefinition{
			AttributeName: aws.String(opt.partitionKeyName),
			AttributeType: aws.String("S"),
		},
	}

	if opt.sortKeyName != nil {
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: opt.sortKeyName,
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		})

		attributeDefinitions = append(attributeDefinitions,
			&dynamodb.AttributeDefinition{
				AttributeName: opt.sortKeyName,
				AttributeType: aws.String("S"),
			})
	}

	createTableInput := &dynamodb.CreateTableInput{
		TableName:             aws.String(opt.tableName),
		KeySchema:             keySchema,
		ProvisionedThroughput: opt.provisionedThroughput,
		AttributeDefinitions:  attributeDefinitions,
	}

	return c.dynamoDB.CreateTable(createTableInput)
}

// ReleaseLock releases the given lock if the current user still has it,
// returning true if the lock was successfully released, and false if someone
// else already stole the lock. Deletes the lock item if it is released and
// deleteLockItemOnClose is set. Return true if the lock is released, false
// otherwise.
func (c *Client) ReleaseLock(lockItem *Lock, opts ...ReleaseLockOption) (bool, error) {
	releaseLockOptions := &releaseLockOptions{
		lockItem:   lockItem,
		deleteLock: lockItem.deleteLockOnRelease,
	}

	for _, opt := range opts {
		opt(releaseLockOptions)
	}
	return c.releaseLockOptions(releaseLockOptions)
}

// WithDeleteLock defines whether or not to delete the lock when releasing it.
// If set to false, the lock row will continue to be in DynamoDB, but it will be
// marked as released.
func WithDeleteLock(deleteLock bool) ReleaseLockOption {
	return func(opt *releaseLockOptions) {
		opt.deleteLock = deleteLock
	}
}

// WithDataAfterRelease is the new data to persist to the lock (only used if
// deleteLock=false.) If the data is null, then the lock client will keep the
// data as-is and not change it.
func WithDataAfterRelease(data []byte) ReleaseLockOption {
	return func(opt *releaseLockOptions) {
		opt.data = data
	}
}

// ReleaseLockOption provides options for releasing a lock when calling the
// releaseLock() method. This class contains the options that may be configured
// during the act of releasing a lock.
type ReleaseLockOption func(*releaseLockOptions)

func (c *Client) releaseLockOptions(options *releaseLockOptions) (bool, error) {
	lockItem := options.lockItem
	if lockItem == nil {
		return false, errors.New("cannot release null lock item")
	}
	deleteLock := options.deleteLock
	data := options.data

	if lockItem.ownerName != c.ownerName {
		return false, nil
	}

	lockItem.semaphore.Lock()
	defer lockItem.semaphore.Unlock()

	c.locks.Delete(lockItem.uniqueIdentifier())

	var conditionalExpression string
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		rvnValueExpressionVariable:       &dynamodb.AttributeValue{S: aws.String(lockItem.recordVersionNumber)},
		ownerNameValueExpressionVariable: &dynamodb.AttributeValue{S: aws.String(lockItem.ownerName)},
	}
	expressionAttributeNames := map[string]*string{
		pkPathExpressionVariable:        aws.String(c.partitionKeyName),
		ownerNamePathExpressionVariable: aws.String(attrOwnerName),
		rvnPathExpressionVariable:       aws.String(attrRecordVersionNumber),
	}

	if c.sortKeyName != nil {
		conditionalExpression = pkExistsAndSkExistsAndOwnerNameSameAndRvnSameCondition
		expressionAttributeNames[skPathExpressionVariable] = c.sortKeyName
	} else {
		conditionalExpression = pkExistsAndOwnerNameSameAndRvnSameCondition
	}

	key := c.getItemKeys(lockItem)
	if deleteLock {
		deleteItemRequest := &dynamodb.DeleteItemInput{
			TableName:                 aws.String(c.tableName),
			Key:                       key,
			ConditionExpression:       aws.String(conditionalExpression),
			ExpressionAttributeNames:  expressionAttributeNames,
			ExpressionAttributeValues: expressionAttributeValues,
		}
		_, err := c.dynamoDB.DeleteItem(deleteItemRequest)
		if err != nil {
			return false, err
		}
	} else {
		var updateExpression string
		expressionAttributeNames[isReleasedPathExpressionVariable] = aws.String(attrIsReleased)
		expressionAttributeValues[isReleasedValueExpressionVariable] = isReleasedAttributeValue

		if len(data) > 0 {
			updateExpression = updateIsReleasedAndData
			expressionAttributeNames[dataPathExpressionVariable] = aws.String(attrData)
			expressionAttributeValues[dataValueExpressionVariable] = &dynamodb.AttributeValue{B: data}
		} else {
			updateExpression = updateIsReleased
		}

		updateItemRequest := &dynamodb.UpdateItemInput{
			TableName:                 aws.String(c.tableName),
			Key:                       key,
			UpdateExpression:          aws.String(updateExpression),
			ConditionExpression:       aws.String(conditionalExpression),
			ExpressionAttributeNames:  expressionAttributeNames,
			ExpressionAttributeValues: expressionAttributeValues,
		}

		_, err := c.dynamoDB.UpdateItem(updateItemRequest)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *Client) releaseAllLocks() error {
	var err error
	c.locks.Range(func(key interface{}, value interface{}) bool {
		_, err = c.ReleaseLock(value.(*Lock))
		return err == nil
	})
	return err
}

func (c *Client) getItemKeys(lockItem *Lock) map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{
		c.partitionKeyName: &dynamodb.AttributeValue{S: aws.String(lockItem.partitionKey)},
	}
	if lockItem.sortKey != nil {
		key[*c.sortKeyName] = &dynamodb.AttributeValue{S: lockItem.sortKey}
	}
	return key
}

// GetOptions allows to configure lock reads.
type GetOptions func(*getLockOptions)

// WithSortKeyName defines the sort key necessary to load the lock content.
func WithSortKeyName(s string) GetOptions {
	return func(o *getLockOptions) {
		o.sortKeyName = &s
	}
}

// Get finds out who owns the given lock, but does not acquire the lock. It
// returns the metadata currently associated with the given lock. If the client
// currently has the lock, it will return the lock, and operations such as
// releaseLock will work. However, if the client does not have the lock, then
// operations like releaseLock will not work (after calling Get, the caller
// should check lockItem.isExpired() to figure out if it currently has the
// lock.)
func (c *Client) Get(key string, opts ...GetOptions) (*Lock, error) {
	getLockOption := getLockOptions{
		partitionKeyName: key,
	}
	for _, opt := range opts {
		opt(&getLockOption)
	}

	keyName := getLockOption.partitionKeyName
	if getLockOption.sortKeyName != nil {
		keyName += *getLockOption.sortKeyName
	}

	v, ok := c.locks.Load(keyName)
	if ok {
		return v.(*Lock), nil
	}

	lockItem, err := c.getLockFromDynamoDB(getLockOption)
	if err != nil {
		return nil, err
	}

	if lockItem.isReleased {
		return &Lock{}, nil
	}

	lockItem.updateRVN("", time.Time{}, lockItem.leaseDuration)
	return lockItem, nil
}

// Close releases all of the locks.
func (c *Client) Close() {
	c.releaseAllLocks()
}
