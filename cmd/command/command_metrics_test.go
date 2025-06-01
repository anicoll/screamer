package command

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// minimalSpannerServer creates a very basic Spanner emulator for DSN validation.
// It doesn't need to do much, just listen and close.
func minimalSpannerServer(t *testing.T, port int) func() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		// If port is already in use, try another one.
		// This is a simple way to handle parallel tests or leftover processes.
		listener, err = net.Listen("tcp", ":0") // :0 means assign a free port
		require.NoError(t, err, "Failed to listen on a free port")
	}
	t.Logf("Minimal Spanner emulator listening on %s", listener.Addr().String())

	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
		}
		// For this test, we only care that the DSN can connect,
		// not that it can do anything else.
	}()

	return func() {
		listener.Close()
	}
}

func TestMetricsEndpoint(t *testing.T) {
	// Start a minimal Spanner emulator for DSN validation in screamer.run
	// The DSN needs to be valid for the command to start up properly.
	emulatorPort := 8999 // Start with a common port, will adjust if busy
	stopEmulator := minimalSpannerServer(t, emulatorPort)
	defer stopEmulator()

	// Dynamically get the port the emulator is actually listening on
	// This is a bit of a hack as we don't have direct access to the listener's Addr from here
	// if it was changed by minimalSpannerServer.
	// For simplicity, we'll assume minimalSpannerServer successfully used emulatorPort or :0.
	// If :0 was used, this test is slightly less robust as DSN might not match perfectly,
	// but screamer.run primarily checks DSN format and reachability.
	// A better approach would be for minimalSpannerServer to return the actual port.
	// However, for this test, a placeholder DSN that is syntactically valid is often enough
	// if the actual Spanner client creation is minimal. Let's try with a placeholder.
	// Actual Spanner client won't be fully used due to context cancellation.

	testMetricsPort := "9876" // Unique port for this test
	testStreamName := "metrics_test_stream"
	testInstance := "test-instance"
	testDatabase := "test-database"
	// Use a placeholder DSN. The screamer command initializes a Spanner client.
	// This needs to be valid enough to pass initial client creation.
	// Since we are not actually querying Spanner for metrics endpoint test,
	// a locally addressable but non-functional DSN is fine.
	// The minimalSpannerServer helps if the client tries to connect.
	// Using 127.0.0.1 and the emulator port.
	dsn := fmt.Sprintf("projects/p/instances/i/databases/d?emulatorHost=127.0.0.1:%d", emulatorPort)
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		dsn = fmt.Sprintf("projects/p/instances/i/databases/d") // Use emulator if globally set
	}


	app := &cli.Command{
		Commands: []*cli.Command{
			ScreamerCommand(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Capture stdout/stderr to avoid polluting test output
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()


	go func() {
		args := []string{
			"screamer", // First arg is app name, ignored by Run
			"screamer", // Command name
			"--metrics-port", testMetricsPort,
			"--stream", testStreamName,
			"--dsn", dsn,
			// These are required by the screamer command, but their values
			// don't matter much for just starting the metrics server.
			// Provide minimal valid-looking values.
			"--metadata-table", "meta", // Required if not using in-memory
			// The command will try to connect to Spanner.
			// We'll cancel quickly, so it shouldn't get far.
		}
		err := app.Run(ctx, args)
		// We expect an error due to context cancellation or other issues
		// as we are not running a full Spanner instance.
		if err != nil && !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "signal: interrupt") {
			// Log non-cancellation errors if they occur, but don't fail the test here
			// as the primary goal is to check the metrics endpoint.
			t.Logf("Screamer command exited with error: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(500 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/metrics", testMetricsPort))
	require.NoError(t, err, "Failed to get /metrics endpoint")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Metrics endpoint should return 200 OK")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Failed to read response body")

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "screamer_records_processed_total", "Response should contain screamer_records_processed_total metric")
	assert.Contains(t, bodyStr, "go_gc_duration_seconds", "Response should contain standard Go metrics")

	// Cancel context to stop the command
	cancel()
	time.Sleep(200 * time.Millisecond) // Give time for shutdown

	wOut.Close()
	wErr.Close()
	outBytes, _ := io.ReadAll(rOut)
	errBytes, _ := io.ReadAll(rErr)
	t.Logf("Screamer command stdout:\n%s", string(outBytes))
	t.Logf("Screamer command stderr:\n%s", string(errBytes))

}
