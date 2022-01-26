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
//		"context"
//		"log"
//
//		"cirello.io/dynamolock/v3"
//		"github.com/aws/aws-sdk-go-v2/aws"
//		"github.com/aws/aws-sdk-go-v2/config"
//		"github.com/aws/aws-sdk-go-v2/service/dynamodb"
//		"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
//	)
//
//	//---
//
//	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-west-2"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	c, err := dynamolock.New(dynamodb.NewFromConfig(cfg),
//		"locks",
//		dynamolock.WithLeaseDuration(3*time.Second),
//		dynamolock.WithHeartbeatPeriod(1*time.Second),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer c.Close(context.Background())
//
//	log.Println("ensuring table exists")
//	c.CreateTable(context.Background(),
//		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
//			ReadCapacityUnits:  aws.Int64(5),
//			WriteCapacityUnits: aws.Int64(5),
//		}),
//	)
//
//  //-- at this point you must wait for DynamoDB to complete the creation.
//
//	data := []byte("some content a")
//	lockedItem, err := c.AcquireLock(context.Background(), "spock",
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
//	success, err := c.ReleaseLock(context.Background(), lockedItem)
//	if !success {
//		log.Fatal("lost lock before release")
//	}
//	if err != nil {
//		log.Fatal("error releasing lock:", err)
//	}
//	log.Println("done")
//
// This package is covered by this SLA:
// https://github.com/cirello-io/public/blob/master/SLA.md
//
package dynamolock // import "cirello.io/dynamolock/v3"
