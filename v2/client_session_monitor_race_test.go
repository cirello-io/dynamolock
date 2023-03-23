//go:build race
// +build race

/*
Copyright 2023 U. Cirello (cirello.io and github.com/cirello-io)

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
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSessionMonitorRaceCondition(t *testing.T) {
	t.Parallel()
	svc := dynamodb.NewFromConfig(defaultConfig(t))
	c, err := dynamolock.New(svc,
		"locks",
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithOwnerName("TestSessionMonitor#1"),
		dynamolock.WithHeartbeatPeriod(1*time.Nanosecond),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("ensuring table exists")
	c.CreateTable("locks",
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)

	data := []byte("some content a")
	_, err = c.AcquireLock("uhura",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithSessionMonitor(500*time.Millisecond, func() {}),
	)
	if err != nil {
		t.Fatal(err)
	}
}
