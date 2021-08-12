GO := go

test:
	$(GO) test -race ./...

test-integration: stop
	GOPATH=$$(go env GOPATH) docker-compose up -d
	# Wait for DB to be available
	sleep 5
	make test
	make stop

start: stop
	GOPATH=$$(go env GOPATH) docker-compose up

stop:
	GOPATH=$$(go env GOPATH) docker-compose down