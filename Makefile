.PHONY: build
build:
	go build -o .bin/screamer cmd/main.go

.PHONY: test
test:
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...

# Assumes v3.5.0 mockery installed.
gen-mocks:
	mockery
