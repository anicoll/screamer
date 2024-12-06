.PHONY: build
build:
	go build -o .bin/screamer main.go

.PHONY: test
test:
	go test ./...
