package screamer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// PartitionWatermark is a watermark to persist for one partition.
type PartitionWatermark struct {
	PartitionToken string
	Watermark      time.Time
}

// WatermarkBatchUpdater is an optional extension of PartitionStorage that
// persists many partitions' watermarks in one write. Both built-in storages
// implement it. When a storage does not, batched checkpoints fall back to one
// UpdateWatermark call per partition per flush.
type WatermarkBatchUpdater interface {
	// UpdateWatermarks persists the given watermarks. It must never move a
	// watermark backwards, and must ignore partitions it does not hold.
	UpdateWatermarks(ctx context.Context, watermarks []PartitionWatermark) error
}

// defaultCheckpointInterval is how often buffered watermarks are written.
const defaultCheckpointInterval = time.Second

// maxCheckpointFailures is how many flushes in a row may fail before the
// subscriber stops. Each failed flush keeps its watermarks for the next one.
const maxCheckpointFailures = 5

// checkpointer buffers the latest delivered watermark of each partition until
// the next flush.
type checkpointer struct {
	mu      sync.Mutex
	pending map[string]time.Time
}

func newCheckpointer() *checkpointer {
	return &checkpointer{pending: make(map[string]time.Time)}
}

// set records wm for token, keeping the later of wm and any watermark already pending.
func (c *checkpointer) set(token string, wm time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.pending[token]; !ok || wm.After(cur) {
		c.pending[token] = wm
	}
}

// forget drops the pending watermark of a partition that has finished.
func (c *checkpointer) forget(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, token)
}

// take returns every pending watermark and clears them.
func (c *checkpointer) take() []PartitionWatermark {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	out := make([]PartitionWatermark, 0, len(c.pending))
	for token, wm := range c.pending {
		out = append(out, PartitionWatermark{PartitionToken: token, Watermark: wm})
	}
	c.pending = make(map[string]time.Time, len(out))
	return out
}

// restore puts back watermarks from a failed flush, unless a later one was set meanwhile.
func (c *checkpointer) restore(ws []PartitionWatermark) {
	for _, w := range ws {
		c.set(w.PartitionToken, w.Watermark)
	}
}

// pendingCount returns the number of partitions with a watermark waiting to be written.
func (c *checkpointer) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// checkpoint records a partition's watermark after a batch was delivered. With
// batched checkpoints it is buffered until the next flush; otherwise it is
// written straight away, as before.
func (s *Subscriber) checkpoint(ctx context.Context, p *PartitionMetadata, wm time.Time) error {
	if s.checkpointInterval <= 0 {
		return s.partitionStorage.UpdateWatermark(ctx, p, wm)
	}
	if wm.After(p.Watermark) {
		s.checkpoints.set(p.PartitionToken, wm)
	}
	return nil
}

// flushCheckpoints writes every buffered watermark. On failure they are kept
// for the next flush.
func (s *Subscriber) flushCheckpoints(ctx context.Context) error {
	ws := s.checkpoints.take()
	if len(ws) == 0 {
		return nil
	}
	var err error
	if b, ok := s.partitionStorage.(WatermarkBatchUpdater); ok {
		err = b.UpdateWatermarks(ctx, ws)
	} else {
		for _, w := range ws {
			if err = s.partitionStorage.UpdateWatermark(ctx, &PartitionMetadata{PartitionToken: w.PartitionToken}, w.Watermark); err != nil {
				break
			}
		}
	}
	if err != nil {
		s.checkpoints.restore(ws)
		return fmt.Errorf("failed to write %d partition watermarks: %w", len(ws), err)
	}
	return nil
}

// checkpointLoop flushes buffered watermarks every checkpointInterval, and once
// more when the subscriber stops so that as little as possible is redelivered
// on restart.
func (s *Subscriber) checkpointLoop(ctx context.Context, done <-chan struct{}) error {
	if s.checkpointInterval <= 0 {
		return nil
	}
	ticker := time.NewTicker(s.checkpointInterval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ticker.C:
			if err := s.flushCheckpoints(ctx); err != nil {
				if ctx.Err() != nil {
					continue // stopping; the final flush below handles it
				}
				failures++
				log.Warn().Err(err).Int("consecutive_failures", failures).Msg("failed to write checkpoints, will retry")
				if failures >= maxCheckpointFailures {
					return fmt.Errorf("checkpoints failed %d times in a row: %w", failures, err)
				}
				continue
			}
			failures = 0
		case <-done:
			s.finalFlush(ctx)
			return nil
		case <-ctx.Done():
			s.finalFlush(context.WithoutCancel(ctx))
			return ctx.Err()
		}
	}
}

func (s *Subscriber) finalFlush(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := s.flushCheckpoints(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to write final checkpoints; those partitions will resume from an earlier watermark")
	}
}
