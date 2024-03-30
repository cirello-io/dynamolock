linters:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2
	golangci-lint -j 1 run --disable-all \
		--exclude ".*Load returns interface.*" \
		-E "errcheck" \
		-E "errname" \
		-E "errorlint" \
		-E "exhaustive" \
		-E "exportloopref" \
		-E "gci" \
		-E "gocritic" \
		-E "godot" \
		-E "gofmt" \
		-E "goimports" \
		-E "govet" \
		-E "grouper" \
		-E "ineffassign" \
		-E "ireturn" \
		-E "misspell" \
		-E "prealloc" \
		-E "predeclared" \
		-E "staticcheck" \
		-E "thelper" \
		-E "unparam" \
		-E "unused" \
		./...

local-dynamodb:
	mkdir local-dynamodb
	wget -O local-dynamodb/latest.zip https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.zip
	(cd local-dynamodb; unzip latest.zip)

test: local-dynamodb
	GOEXPERIMENT=loopvar go test -v

test-race:
	GOEXPERIMENT=loopvar go test -race -count=1000
