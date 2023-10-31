# DynamoDB Lock Client for Go

** dynamolock v1 is now retired. Please use the [dynamolock/v2][https://cirello.io/dynamolock/v2]. **

This repository is covered by this [SLA](https://github.com/cirello-io/public/blob/master/SLA.md).

The dymanoDB Lock Client for Go is a general purpose distributed locking library
built for DynamoDB. The dynamoDB Lock Client for Go supports both fine-grained
and coarse-grained locking as the lock keys can be any arbitrary string, up to a
certain length. Please create issues in the GitHub repository with questions,
pull request are very much welcome.

It is a port in Go of Amazon's original [dynamodb-lock-client](https://github.com/awslabs/dynamodb-lock-client).

