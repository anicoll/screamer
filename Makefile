.PHONY: build
build:
	go build -o .bin/screamer main.go

.PHONY: test
test:
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: lint
lint:
	golangci-lint run ./...

# Assumes v3.5.0 mockery installed.
gen-mocks:
	mockery

SPANNER_EMULATOR_IMAGE := gcr.io/cloud-spanner-emulator/emulator
SPANNER_CONTAINER_NAME := screamer-spanner-emulator

.PHONY: start-emulator
start-emulator:
	@echo "Starting Spanner Emulator..."
	@docker run -d --rm --name $(SPANNER_CONTAINER_NAME) \
		-p 9010:9010 -p 9020:9020 \
		$(SPANNER_EMULATOR_IMAGE)
	@echo "Waiting for emulator to be ready..."
	@sleep 5

.PHONY: stop-emulator
stop-emulator:
	@echo "Stopping Spanner Emulator..."
	@docker stop $(SPANNER_CONTAINER_NAME) || true

.PHONY: test-integration
test-integration: stop-emulator start-emulator
	@echo "Running integration tests..."
	@export SPANNER_EMULATOR_HOST=localhost:9010 && \
	go test -v ./... || (make stop-emulator && exit 1)
	@make stop-emulator
