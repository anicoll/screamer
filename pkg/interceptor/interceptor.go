package interceptor

import (
	"context"
	"sync"

	"google.golang.org/grpc"
)

// QueueInterceptor is a gRPC client interceptor that queues requests and processes them one by one.
type QueueInterceptor struct {
	mu    sync.Mutex
	queue chan struct{}
}

// NewQueueInterceptor creates a new QueueInterceptor with a given queue size.
func NewQueueInterceptor(queueSize int) *QueueInterceptor {
	return &QueueInterceptor{
		queue: make(chan struct{}, queueSize),
	}
}

// UnaryInterceptor is a unary client interceptor implementation that serializes requests and enforces queue limits.
// It ensures only one request is processed at a time and respects context cancellation.
func (qi *QueueInterceptor) UnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// Check if the context is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Add to the queue
	qi.queue <- struct{}{}
	defer func() { <-qi.queue }()

	// Lock to ensure one-by-one execution
	qi.mu.Lock()
	defer qi.mu.Unlock()

	// Check if the context is cancelled before invoking the RPC
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Invoke the actual RPC
	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamInterceptor is a stream client interceptor implementation that serializes requests and enforces queue limits.
// It ensures only one stream is processed at a time and respects context cancellation.
func (qi *QueueInterceptor) StreamInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	// Add to the queue
	qi.queue <- struct{}{}
	defer func() { <-qi.queue }()

	// Lock to ensure one-by-one execution
	qi.mu.Lock()
	defer qi.mu.Unlock()

	// Invoke the actual RPC
	return streamer(ctx, desc, cc, method, opts...)
}
