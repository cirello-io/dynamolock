linters:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2
	golangci-lint -j 1 run --disable-all -E "errcheck" -E "godot" -E "govet" -E "ineffassign" -E "staticcheck" -E "unparam" -E "unused"

local-dynamodb:
	mkdir local-dynamodb
	wget -O local-dynamodb/latest.zip https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip
	(cd local-dynamodb; unzip latest.zip)

test: linters local-dynamodb
	GOEXPERIMENT=loopvar go test -v

test-race:
	GOEXPERIMENT=loopvar go test -race -count=1000