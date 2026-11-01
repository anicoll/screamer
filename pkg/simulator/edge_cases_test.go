package simulator_test

// Edge cases in how the subscriber delivers, checkpoints and schedules. Each test
// states the behaviour it pins down and why it matters. They run against the
// simulator, with storage wrappers that record or break watermark writes.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// recordingStorage records the highest watermark written for each partition,
// and can fail batched watermark writes.
type recordingStorage struct {
	*partitionstorage.InmemoryPartitionStorage

	mu        sync.Mutex
	written   map[string]time.Time
	earliest  map[string]time.Time // lowest watermark ever written, per partition
	backwards []string             // partitions whose watermark was written lower than before

	failBatches atomic.Int64 // UpdateWatermarks calls left to fail; -1 fails forever
	batchCalls  atomic.Int64
}

func newRecordingStorage() *recordingStorage {
	return &recordingStorage{InmemoryPartitionStorage: partitionstorage.NewInmemory(), written: map[string]time.Time{}, earliest: map[string]time.Time{}}
}

func (r *recordingStorage) record(token string, wm time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.earliest[token]; !ok || wm.Before(e) {
		r.earliest[token] = wm
	}
	if prev, ok := r.written[token]; ok && wm.Before(prev) {
		r.backwards = append(r.backwards, token)
		return
	}
	r.written[token] = wm
}

func (r *recordingStorage) writtenWatermark(token string) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wm, ok := r.written[token]
	return wm, ok
}

func (r *recordingStorage) UpdateWatermark(ctx context.Context, p *screamer.PartitionMetadata, wm time.Time) error {
	r.record(p.PartitionToken, wm)
	return r.InmemoryPartitionStorage.UpdateWatermark(ctx, p, wm)
}

var errStorageDown = errors.New("metadata table unavailable")

func (r *recordingStorage) UpdateWatermarks(ctx context.Context, ws []screamer.PartitionWatermark) error {
	r.batchCalls.Add(1)
	if n := r.failBatches.Load(); n != 0 {
		if n > 0 {
			r.failBatches.Add(-1)
		}
		return errStorageDown
	}
	for _, w := range ws {
		r.record(w.PartitionToken, w.Watermark)
	}
	return r.InmemoryPartitionStorage.UpdateWatermarks(ctx, ws)
}

func simOptions(sim *simulator.Simulator, start, end time.Time, extra ...screamer.Option) []screamer.Option {
	return append([]screamer.Option{
		screamer.WithChangeStreamReader(sim),
		screamer.WithHeartbeatInterval(time.Second),
		screamer.WithLogLevel("error"),
		screamer.WithStartTimestamp(start),
		screamer.WithEndTimestamp(end),
	}, extra...)
}

func smallChurn(start time.Time) simulator.Config {
	return simulator.Config{
		Seed:                  31,
		Start:                 start,
		Horizon:               3 * time.Minute,
		InitialPartitions:     10,
		KeySpace:              1_000,
		MeanPartitionLifetime: time.Minute,
		MergeFraction:         0.4,
		MinPartitions:         4,
		MaxPartitions:         30,
		WritesPerSecond:       30,
	}
}

// A few failed checkpoint flushes in a row (a metadata table blip) do not stop
// the subscriber or lose progress: the watermarks are kept and written by the
// next flush that succeeds.
func TestEdge_CheckpointFlushRecoversFromTransientFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		end := start.Add(3 * time.Minute)
		sim := simulator.New(smallChurn(start))
		checker := simulator.NewChecker(sim)
		store := newRecordingStorage()
		store.failBatches.Store(4) // one fewer than the limit

		sub := screamer.NewSubscriber(nil, "SimStream", "runner-1", store, simOptions(sim, start, end)...)
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		require.Zero(t, r.Missing)
		require.Zero(t, store.failBatches.Load(), "every planned failure happened")
		require.Greater(t, store.batchCalls.Load(), int64(4))
		require.Empty(t, store.backwards)
	})
}

// If checkpoints keep failing, the subscriber stops after five failed flushes in
// a row (about five seconds) with an error naming the cause, rather than running
// on while nothing it delivers is ever recorded.
func TestEdge_CheckpointFlushStopsSubscriberWhenStorageStaysDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(smallChurn(start))
		store := newRecordingStorage()
		store.failBatches.Store(-1)

		sub := screamer.NewSubscriber(nil, "SimStream", "runner-1", store, simOptions(sim, start, start.Add(3*time.Minute))...)
		err := sub.Subscribe(context.Background(), simulator.NewChecker(sim))
		require.ErrorIs(t, err, errStorageDown)
		require.ErrorContains(t, err, "checkpoints failed 5 times in a row")
		require.Less(t, time.Since(start), 10*time.Second)
	})
}

// A retried partition resumes from the watermark it reached in memory, not from
// its last stored checkpoint. With checkpoints written only once an hour, a
// retry that went back to the stored watermark would redeliver minutes of
// changes; resuming from memory redelivers at most the last record read
// before each failure, whose commit timestamp is the watermark.
func TestEdge_RetryResumesFromInMemoryWatermarkNotLastCheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		end := start.Add(5 * time.Minute)
		sim := simulator.New(flakyConfig(start))
		checker := simulator.NewChecker(sim)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end),
			screamer.WithCheckpointInterval(time.Hour))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		st := sim.Stats()
		t.Logf("%s injected_failures=%d", r, st.InjectedFailures)
		require.Positive(t, st.InjectedFailures)
		require.Zero(t, r.Missing)
		require.LessOrEqual(t, r.Duplicates, int(st.InjectedFailures))
	})
}

// Acks can arrive later and from other goroutines, as with an asynchronous
// output. Every record is still delivered once, in per-key order, and the
// subscriber finishes.
func TestEdge_AsyncAcksDeliverEverythingInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		end := start.Add(3 * time.Minute)
		sim := simulator.New(smallChurn(start))
		checker := simulator.NewChecker(sim)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end))
		err := sub.SubscribeWithAck(context.Background(), screamer.ConsumerFuncWithAck(func(b []byte, ack screamer.AckFunc) error {
			if err := checker.Consume(b); err != nil {
				return err
			}
			go func() {
				time.Sleep(50 * time.Millisecond)
				ack(nil)
			}()
			return nil
		}))
		require.NoError(t, err)

		r := checker.Report(end)
		require.Zero(t, r.Missing)
		require.Zero(t, r.Duplicates)
		require.Zero(t, r.OrderViolations)
	})
}

// A nack stops the subscriber with the consumer's error and is not retried. The
// watermark stored for that partition, including the final flush on stop, never
// passes the nacked record, so a restart delivers it again: at-least-once holds
// with batched checkpoints.
func TestEdge_NackedRecordIsNeverCheckpointedPast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sim := simulator.New(smallChurn(start))
		store := newRecordingStorage()

		var (
			n      atomic.Int64
			nacked struct {
				PartitionToken  string    `json:"partition_token"`
				CommitTimestamp time.Time `json:"commit_timestamp"`
			}
		)
		boom := errors.New("output rejected the record")
		sub := screamer.NewSubscriber(nil, "SimStream", "runner-1", store, simOptions(sim, start, start.Add(3*time.Minute))...)
		err := sub.SubscribeWithAck(context.Background(), screamer.ConsumerFuncWithAck(func(b []byte, ack screamer.AckFunc) error {
			if n.Add(1) == 500 {
				require.NoError(t, json.Unmarshal(b, &nacked))
				ack(boom)
				return nil
			}
			ack(nil)
			return nil
		}))
		require.ErrorIs(t, err, boom)
		require.Zero(t, sub.GetMetrics().PartitionRetries, "consumer errors are not retried")

		wm, ok := store.writtenWatermark(nacked.PartitionToken)
		require.True(t, ok, "the partition should have checkpointed before the nack")
		require.False(t, wm.After(nacked.CommitTimestamp),
			"stored watermark %s is past the nacked record at %s", wm, nacked.CommitTimestamp)
	})
}

// Two runners sharing one metadata store split the partitions between them.
// Every write is delivered exactly once, and parent gating holds across
// runners: a merged child whose parents were read by different runners still
// waits for both.
func TestEdge_TwoRunnersShareTheWorkExactlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now().Add(-10 * time.Minute)
		end := start.Add(10 * time.Minute)
		sim := simulator.New(orderScenario(start))
		checker := simulator.NewChecker(sim)
		store := partitionstorage.NewInmemory()

		var delivered [2]atomic.Int64
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range 2 {
			sub := screamer.NewSubscriber(nil, "SimStream", []string{"runner-a", "runner-b"}[i], store, simOptions(sim, start, end)...)
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = sub.Subscribe(context.Background(), screamer.ConsumerFunc(func(b []byte) error {
					delivered[i].Add(1)
					return checker.Consume(b)
				}))
			}()
		}
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		r := checker.Report(end)
		t.Logf("%s runner-a=%d runner-b=%d", r, delivered[0].Load(), delivered[1].Load())
		require.Zero(t, r.Missing)
		require.Zero(t, r.Duplicates)
		require.Zero(t, r.OrderViolations)
		require.Positive(t, delivered[0].Load())
		require.Positive(t, delivered[1].Load())
	})
}

// Parent gating and a partition limit together do not deadlock: a child that is
// waiting for its parents does not hold a slot, so the parents can always be
// read. With a limit of 4 and heavy merging, everything is delivered in order.
func TestEdge_ParentGatingWithPartitionLimitDoesNotDeadlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now().Add(-10 * time.Minute)
		end := start.Add(10 * time.Minute)
		sim := simulator.New(orderScenario(start))
		checker := simulator.NewChecker(sim)

		sub := newSubscriber(sim, screamer.WithStartTimestamp(start), screamer.WithEndTimestamp(end), screamer.WithMaxConcurrentPartitions(4))
		require.NoError(t, sub.Subscribe(context.Background(), checker))

		r := checker.Report(end)
		require.Zero(t, r.Missing)
		require.Zero(t, r.OrderViolations)
	})
}

// emptyBatchReader is a change stream with one partition that first returns a
// row with no records, then a heartbeat, then ends.
type emptyBatchReader struct{ start time.Time }

func (e emptyBatchReader) Read(ctx context.Context, p *screamer.PartitionMetadata, fn func([]*screamer.ChangeRecord) error) error {
	if p.IsRootPartition() {
		return fn([]*screamer.ChangeRecord{{ChildPartitionsRecords: []*screamer.ChildPartitionsRecord{{
			StartTimestamp:  e.start,
			ChildPartitions: []*screamer.ChildPartition{{Token: "only"}},
		}}}})
	}
	if err := fn([]*screamer.ChangeRecord{}); err != nil {
		return err
	}
	if err := fn([]*screamer.ChangeRecord{{}}); err != nil {
		return err
	}
	return fn([]*screamer.ChangeRecord{{HeartbeatRecords: []*screamer.HeartbeatRecord{{Timestamp: e.start.Add(time.Second)}}}})
}

// A row that carries no records says nothing about time, so it must not move
// the watermark. Writing the zero time there would send the partition back to
// year 1 on its next read, which Spanner rejects as outside retention. This
// holds with batched checkpoints and with WithCheckpointInterval(0).
func TestEdge_EmptyBatchDoesNotMoveWatermarkBackwards(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				store := newRecordingStorage()
				sub := screamer.NewSubscriber(nil, "Stream", "runner-1", store,
					screamer.WithChangeStreamReader(emptyBatchReader{start}),
					screamer.WithLogLevel("error"),
					screamer.WithStartTimestamp(start),
					screamer.WithEndTimestamp(start.Add(time.Minute)),
					screamer.WithCheckpointInterval(interval),
				)
				require.NoError(t, sub.Subscribe(context.Background(), screamer.ConsumerFunc(func([]byte) error { return nil })))

				require.Empty(t, store.backwards, "a watermark was written lower than before")
				store.mu.Lock()
				earliest, ok := store.earliest["only"]
				store.mu.Unlock()
				if interval == 0 {
					// Written straight away. With batching, the partition finishes before
					// the next flush and its pending watermark is dropped.
					require.True(t, ok, "the heartbeat's watermark should have been written")
				}
				if ok {
					require.False(t, earliest.Before(start), "watermark %s was written, before the partition start %s", earliest, start)
				}
			})
		})
	}
}
