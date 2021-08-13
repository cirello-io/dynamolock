local-dynamodb:
	mkdir local-dynamodb
	wget -O local-dynamodb/latest.zip https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip
	(cd local-dynamodb; unzip latest.zip)

test: local-dynamodb
	go test -v

test-race:
	go test -race -count=1000
