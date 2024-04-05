//go:generate go run cirello.io/moq -pkg dynamolock -out client_mock_dynamo_db_client_test.go . DynamoDBClient:DynamoDBClientMock
package dynamolock
