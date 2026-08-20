test: linters local-dynamodb
	go test -v -failfast

test-race: local-dynamodb
	go test -race -count=1000

linters:
	go run -mod=readonly github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --fix --default=none \
		-E "errcheck" \
		-E "errname" \
		-E "errorlint" \
		-E "exhaustive" \
		-E "gocritic" \
		-E "godot" \
		-E "govet" \
		-E "grouper" \
		-E "ineffassign" \
		-E "misspell" \
		-E "prealloc" \
		-E "predeclared" \
		-E "staticcheck" \
		-E "thelper" \
		-E "unparam" \
		-E "unused" \
		./...

.PHONY: check-java
check-java:
	@java -version >/dev/null 2>&1 || { printf '%s\n' 'A Java runtime is required to run DynamoDB Local.' >&2; exit 1; }

local-dynamodb: check-java
	@if [ ! -f local-dynamodb/DynamoDBLocal.jar ]; then \
		mkdir -p local-dynamodb; \
		curl -fL -o local-dynamodb/latest.zip https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip; \
		unzip -oq local-dynamodb/latest.zip -d local-dynamodb; \
	fi

