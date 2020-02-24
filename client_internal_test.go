/*
Copyright 2019 github.com/ucirello & cirello.io

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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}
func (m *mockDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}
func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}
func (m *mockDynamoDBClient) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

/*
This test checks for lock leaks during closing, that is, to make sure that no locks
are able to be acquired while the client is closing, and to ensure that we don't have
any locks in the internal lock map after a client is closed.
 */
func TestCloseRace(t *testing.T) {
	mockSvc := &mockDynamoDBClient{}

	// Most of the input into New isn't relevant since we're mocking
	lockClient, err := New(mockSvc, "locksCloseRace",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(100*time.Millisecond),
		WithOwnerName("CloseRace"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	n := 500

	// Create goroutines that acquire a lock
	for i := 0; i < n; i++ {
		si := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockClient.AcquireLock(strconv.Itoa(si))
		}()
	}

	// Close the lock client
	wg.Add(1)
	go func() {
		defer wg.Done()
		lockClient.Close()
	}()

	// Check for any leaked locks
	wg.Wait()
	length := 0
	lockClient.locks.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	if length > 0 {
		t.Fatal(fmt.Sprintf("lock client still has %d locks after Close()", length))
	}
}

func TestBadCreateLockItem(t *testing.T) {
	c := &Client{}
	_, err := c.createLockItem(getLockOptions{}, map[string]*dynamodb.AttributeValue{
		attrLeaseDuration: &dynamodb.AttributeValue{S: aws.String("bad duration")},
	})
	if err == nil {
		t.Fatal("bad duration should prevent the creation of the lock")
	}
}
