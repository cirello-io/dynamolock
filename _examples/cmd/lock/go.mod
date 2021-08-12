module cirello.io/dynamolock/_examples/cmd/lock

go 1.13

replace cirello.io/dynamolock => ../../..

require (
	cirello.io/dynamolock v1.3.1
	github.com/aws/aws-sdk-go-v2 v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.6.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.4.2 // indirect
	github.com/aws/smithy-go v1.7.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/urfave/cli v1.21.0
)
