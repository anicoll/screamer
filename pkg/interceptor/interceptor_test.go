package interceptor

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestQueueInterceptor_UnaryInterceptor(t *testing.T) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	// Mock gRPC server
	s := grpc.NewServer()
	defer s.Stop()

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("Server exited with error: %v", err)
		}
	}()

	// Create a client connection
	dialer := func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.Dial("", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufconn: %v", err)
	}
	defer conn.Close()

	// Create QueueInterceptor
	interceptor := NewQueueInterceptor(2)

	// Mock invoker
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		time.Sleep(100 * time.Millisecond) // Simulate processing time
		return nil
	}

	t.Run("SingleRequest", func(t *testing.T) {
		err := interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockInvoker)
		if err != nil {
			t.Errorf("UnaryInterceptor returned error: %v", err)
		}
	})

	t.Run("ConcurrentRequestsWithinQueueLimit", func(t *testing.T) {
		errCh := make(chan error, 2)

		go func() {
			errCh <- interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockInvoker)
		}()
		go func() {
			errCh <- interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockInvoker)
		}()

		for i := 0; i < 2; i++ {
			if err := <-errCh; err != nil {
				t.Errorf("UnaryInterceptor returned error: %v", err)
			}
		}
	})

	t.Run("ConcurrentRequestsExceedingQueueLimit", func(t *testing.T) {
		errCh := make(chan error, 3)

		go func() {
			errCh <- interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockInvoker)
		}()
		go func() {
			errCh <- interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockInvoker)
		}()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			errCh <- interceptor.UnaryInterceptor(ctx, "/test.Service/Method", nil, nil, conn, mockInvoker)
		}()

		var errorsCount int
		for i := 0; i < 3; i++ {
			if err := <-errCh; err != nil {
				errorsCount++
			}
		}

		if errorsCount != 1 {
			t.Errorf("Expected 1 error due to queue limit, got %d", errorsCount)
		}
	})

	t.Run("InvokerErrorPropagation", func(t *testing.T) {
		mockErrorInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return errors.New("mock error")
		}

		err := interceptor.UnaryInterceptor(context.Background(), "/test.Service/Method", nil, nil, conn, mockErrorInvoker)
		if err == nil || err.Error() != "mock error" {
			t.Errorf("Expected 'mock error', got %v", err)
		}
	})
}
