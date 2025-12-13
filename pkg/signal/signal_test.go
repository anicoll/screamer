package signal

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSignalHandler_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context immediately
	cancel()

	err := SignalHandler(ctx)
	if err != nil {
		t.Errorf("expected nil error on context cancellation, got %v", err)
	}
}

func TestSignalHandler_ContextCancellationAfterDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := SignalHandler(ctx)
	if err != nil {
		t.Errorf("expected nil error on context timeout, got %v", err)
	}
}

func TestSignalHandler_SignalReception(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to receive the result
	errCh := make(chan error, 1)

	go func() {
		errCh <- SignalHandler(ctx)
	}()

	// Give SignalHandler time to set up
	time.Sleep(50 * time.Millisecond)

	// Send interrupt signal to current process
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("failed to find current process: %v", err)
	}

	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send signal: %v", err)
	}

	// Wait for result with timeout
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSignal) {
			t.Errorf("expected ErrSignal, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for signal handler to return")
	}
}

func TestSignalHandler_MultipleSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		errCh <- SignalHandler(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("failed to find current process: %v", err)
	}

	// Send first signal
	if err := p.Signal(os.Interrupt); err != nil {
		t.Fatalf("failed to send first signal: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSignal) {
			t.Errorf("expected ErrSignal, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for signal handler to return")
	}
}

func TestErrSignal(t *testing.T) {
	if ErrSignal == nil {
		t.Error("ErrSignal should not be nil")
	}

	expectedMsg := "received the quit signal"
	if ErrSignal.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, ErrSignal.Error())
	}
}

func TestSignalHandler_ConcurrentCalls(t *testing.T) {
	// Test that multiple concurrent calls don't interfere with each other
	const numHandlers = 3

	ctxs := make([]context.Context, numHandlers)
	cancels := make([]context.CancelFunc, numHandlers)
	errChs := make([]chan error, numHandlers)

	for i := 0; i < numHandlers; i++ {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
		defer cancels[i]()
		errChs[i] = make(chan error, 1)
	}

	// Start all handlers
	for i := range numHandlers {
		go func() {
			errChs[i] <- SignalHandler(ctxs[i])
		}()
	}

	time.Sleep(50 * time.Millisecond)

	// Cancel all contexts
	for i := range numHandlers {
		cancels[i]()
	}

	// Wait for all to complete
	for i := range numHandlers {
		select {
		case err := <-errChs[i]:
			if err != nil {
				t.Errorf("handler %d: expected nil error on context cancellation, got %v", i, err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("handler %d: timeout waiting for completion", i)
		}
	}
}
