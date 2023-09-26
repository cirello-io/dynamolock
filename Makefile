linters:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2
	golangci-lint -j 1 run --disable-all -E "errcheck" -E "godot" -E "govet" -E "ineffassign" -E "staticcheck" -E "unparam" -E "unused"
test: linters
	GOEXPERIMENT=loopvar go test -v -count 1 -failfast