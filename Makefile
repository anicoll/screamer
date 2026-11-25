.PHONY: build
build:
	go build -o .bin/screamer cmd/main.go
	go build -o .bin/soak ./cmd/soak

GO_TEST := go test -race -count=1 -covermode=atomic

# Test suites that start the Spanner emulator with testcontainers (needs Docker).
INTEGRATION_TESTS := ^(TestIntegrationTestSuite|TestSpannerTestSuite|TestSoakTestSuite)$$
INTEGRATION_PKGS := . ./pkg/partitionstorage ./internal/soak
SIMULATOR_PKGS := ./pkg/simulator/...
UNIT_PKGS = $(shell go list ./... | grep -v /pkg/simulator)

# Runs every test, including the emulator suites.
.PHONY: test
test:
	$(GO_TEST) -coverprofile=coverage.out ./...

# The three groups below run as separate CI jobs. Each writes a JSON report,
# prints a per-package summary and fails if any test failed.
.PHONY: test-unit
test-unit:
	@$(GO_TEST) -json -coverprofile=coverage-unit.out -skip '$(INTEGRATION_TESTS)' $(UNIT_PKGS) > test-unit.json; \
	status=$$?; ./scripts/test-summary.sh "Unit tests" test-unit.json; exit $$status

.PHONY: test-integration
test-integration:
	@$(GO_TEST) -json -coverprofile=coverage-integration.out -run '$(INTEGRATION_TESTS)' $(INTEGRATION_PKGS) > test-integration.json; \
	status=$$?; ./scripts/test-summary.sh "Integration tests (Spanner emulator)" test-integration.json; exit $$status

.PHONY: test-simulator
test-simulator:
	@$(GO_TEST) -json -coverprofile=coverage-simulator.out $(SIMULATOR_PKGS) > test-simulator.json; \
	status=$$?; ./scripts/test-summary.sh "Simulator tests" test-simulator.json; exit $$status

# Assumes v3.5.0 mockery installed.
gen-mocks:
	mockery
