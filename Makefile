.PHONY: build
build:
	go build -o .bin/screamer main.go

.PHONY: test
test:
	go test ./...

# Assumes v3.5.0 mockery installed.
gen-mocks:
	mockery
