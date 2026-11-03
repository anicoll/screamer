package simulator_test

// How partition ownership moves between runners with leases (the default).
// Each test runs two runners on one shared store and makes one of them stop,
// die or freeze, then checks that the other takes over, that nothing is lost,
// and that the frozen one cannot overwrite the new owner's progress.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/simulator"
	"github.com/stretchr/testify/require"
)

// runnerLink is one runner's connection to the shared store and to the change
// stream. It can be cut (every call fails, as if the runner's host lost the
// network or died) or frozen (every call blocks, as if the process stopped in a
// long GC pause or was suspended).
type runnerLink struct {
	*recordingStorage
	sim *simulator.Simulator

	cut atomic.Bool

	mu     sync.Mutex
	frozen chan struct{} // closed when the runner is thawed; nil when not frozen

	takeovers []time.Time // when this runner claimed a partition someone else had held
}

func newRunnerLink(store *recordingStorage, sim *simulator.Simulator) *runnerLink {
	return &runnerLink{recordingStorage: store, sim: sim}
}

func (l *runnerLink) freeze() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.frozen = make(chan struct{})
}

func (l *runnerLink) thaw() {
	l.mu.Lock()
	defer l.mu.Unlock()
	close(l.frozen)
	l.frozen = nil
}

// gate blocks while the runner is frozen and fails while it is cut.
func (l *runnerLink) gate(ctx context.Context) error {
	l.mu.Lock()
	f := l.frozen
	l.mu.Unlock()
	if f != nil {
		select {
		case <-f:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if l.cut.Load() {
		return errStorageDown
	}
	return nil
}

func (l *runnerLink) Read(ctx context.Context, p *screamer.PartitionMetadata, fn func([]*screamer.ChangeRecord) error) error {
	if err := l.gate(ctx); err != nil {
		return err
	}
	return l.sim.Read(ctx, p, func(rs []*screamer.ChangeRecord) error {
		if err := l.gate(ctx); err != nil {
			return err
		}
		return fn(rs)
	})
}

func (l *runnerLink) ClaimPartitions(ctx context.Context, req screamer.ClaimRequest) ([]*screamer.PartitionMetadata, error) {
	if err := l.gate(ctx); err != nil {
		return nil, err
	}
	ps, err := l.recordingStorage.ClaimPartitions(ctx, req)
	for _, p := range ps {
		if *p.Epoch > 1 {
			l.mu.Lock()
			l.takeovers = append(l.takeovers, time.Now())
			l.mu.Unlock()
		}
	}
	return ps, err
}

func (l *runnerLink) RenewLeases(ctx context.Context, runnerID string, ttl time.Duration, leases []screamer.Lease) ([]string, error) {
	if err := l.gate(ctx); err != nil {
		return nil, err
	}
	return l.recordingStorage.RenewLeases(ctx, runnerID, ttl, leases)
}

func (l *runnerLink) ReleaseLeases(ctx context.Context, runnerID string, leases []screamer.Lease) error {
	if err := l.gate(ctx); err != nil {
		return err
	}
	return l.recordingStorage.ReleaseLeases(ctx, runnerID, leases)
}

func (l *runnerLink) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*screamer.PartitionMetadata, error) {
	if err := l.gate(ctx); err != nil {
		return nil, err
	}
	return l.recordingStorage.GetUnfinishedMinWatermarkPartition(ctx)
}

func (l *runnerLink) AddChildPartitions(ctx context.Context, p *screamer.PartitionMetadata, r *screamer.ChildPartitionsRecord) error {
	if err := l.gate(ctx); err != nil {
		return err
	}
	return l.recordingStorage.AddChildPartitions(ctx, p, r)
}

func (l *runnerLink) UpdateToRunning(ctx context.Context, p *screamer.PartitionMetadata) error {
	if err := l.gate(ctx); err != nil {
		return err
	}
	return l.recordingStorage.UpdateToRunning(ctx, p)
}

func (l *runnerLink) UpdateToFinished(ctx context.Context, p *screamer.PartitionMetadata, runnerID string) error {
	if err := l.gate(ctx); err != nil {
		return err
	}
	return l.recordingStorage.UpdateToFinished(ctx, p, runnerID)
}

func (l *runnerLink) firstTakeoverAfter(t time.Time) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, at := range l.takeovers {
		if !at.Before(t) {
			return at, true
		}
	}
	return time.Time{}, false
}

// leaseRun is two runners, a and b, reading one stream into one checker.
type leaseRun struct {
	sim     *simulator.Simulator
	checker *simulator.Checker
	store   *recordingStorage
	start   time.Time
	end     time.Time

	links [2]*runnerLink
	subs  [2]*screamer.Subscriber
	stop  [2]context.CancelFunc
	errs  [2]error
	wg    sync.WaitGroup
}

func startLeaseRun(t *testing.T, extra ...screamer.Option) *leaseRun {
	t.Helper()
	start := time.Now()
	r := &leaseRun{start: start, end: start.Add(3 * time.Minute)}
	r.sim = simulator.New(smallChurn(start))
	r.checker = simulator.NewChecker(r.sim)
	r.store = newRecordingStorage()
	// Fair share also moves partitions between runners; these tests are about
	// what leases do on their own.
	extra = append([]screamer.Option{screamer.WithFairShare(false)}, extra...)
	for i, id := range []string{"runner-a", "runner-b"} {
		link := newRunnerLink(r.store, r.sim)
		opts := append(simOptions(r.sim, start, r.end, extra...), screamer.WithChangeStreamReader(link))
		sub := screamer.NewSubscriber(nil, "SimStream", id, link, opts...)
		ctx, cancel := context.WithCancel(context.Background())
		r.links[i], r.subs[i], r.stop[i] = link, sub, cancel
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.errs[i] = sub.Subscribe(ctx, r.checker)
		}()
	}
	return r
}

func (r *leaseRun) wait() {
	r.wg.Wait()
	r.stop[0]()
	r.stop[1]()
}

// A runner that dies without a word (its host loses the network, or it is
// killed) keeps its leases until they expire. Then the other runner claims its
// partitions from their last checkpoint, and nothing is lost: the records after
// that checkpoint are delivered again, which is the at-least-once contract.
func TestLease_DeadRunnerIsTakenOverAfterTheTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ttl = 30 * time.Second
		r := startLeaseRun(t, screamer.WithLeaseTTL(ttl))

		time.Sleep(40 * time.Second)
		synctest.Wait()
		held := r.subs[0].GetMetrics().LeasesHeld
		require.Positive(t, held, "runner-a should be reading some partitions")
		died := time.Now()
		r.links[0].cut.Store(true)
		r.stop[0]() // its final release fails: the link is cut

		r.wait()
		require.ErrorIs(t, r.errs[0], context.Canceled)
		require.NoError(t, r.errs[1])

		first, ok := r.links[1].firstTakeoverAfter(died)
		require.True(t, ok, "runner-b should have taken over runner-a's partitions")
		waited := first.Sub(died)
		t.Logf("runner-a held %d leases when it died; runner-b took over after %s", held, waited)
		require.GreaterOrEqual(t, waited, ttl-renewSlack(ttl), "a lease must not be taken before it can have expired")
		require.LessOrEqual(t, waited, ttl+5*time.Second)

		rep := r.checker.Report(r.end)
		t.Log(rep)
		require.Zero(t, rep.Missing)
		require.Zero(t, rep.OrderViolations)
		require.Empty(t, r.store.backwards)
	})
}

// renewSlack is how long before a dead runner's lease can expire after it was
// last renewed: leases are renewed every third of the TTL.
func renewSlack(ttl time.Duration) time.Duration { return ttl / 3 }

// A runner that stops cleanly (a rollout, a scale-down) releases its leases
// with its last watermarks, so the other runner takes its partitions over on
// its next claim, about a second later, instead of waiting out the TTL.
func TestLease_StoppedRunnerHandsOverAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := startLeaseRun(t)

		time.Sleep(40 * time.Second)
		synctest.Wait()
		held := r.subs[0].GetMetrics().LeasesHeld
		require.Positive(t, held)
		stopped := time.Now()
		r.stop[0]()

		r.wait()
		require.ErrorIs(t, r.errs[0], context.Canceled)
		require.NoError(t, r.errs[1])

		first, ok := r.links[1].firstTakeoverAfter(stopped)
		require.True(t, ok)
		t.Logf("runner-a released %d leases; runner-b took over after %s", held, first.Sub(stopped))
		require.LessOrEqual(t, first.Sub(stopped), 2*time.Second)

		rep := r.checker.Report(r.end)
		require.Zero(t, rep.Missing)
		require.Zero(t, rep.OrderViolations)
		require.Empty(t, r.store.backwards)
	})
}

// A runner that freezes for longer than the TTL (a long GC pause, a suspended
// VM) loses its leases to the other runner while it is frozen. When it thaws it
// still has batches in flight and still thinks it owns those partitions. Its
// next checkpoint is rejected because the epoch moved on, so it stops reading
// them and writes nothing: the new owner's watermarks never go backwards, and
// the frozen runner keeps running and goes on to claim other work.
func TestLease_FrozenRunnerIsFencedWhenItWakesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ttl = 30 * time.Second
		r := startLeaseRun(t, screamer.WithLeaseTTL(ttl))

		time.Sleep(40 * time.Second)
		synctest.Wait()
		held := r.subs[0].GetMetrics().LeasesHeld
		require.Positive(t, held)

		r.links[0].freeze()
		frozeAt := time.Now()
		time.Sleep(2 * ttl)
		_, ok := r.links[1].firstTakeoverAfter(frozeAt)
		require.True(t, ok, "runner-b should have taken over while runner-a was frozen")
		r.links[0].thaw()

		r.wait()
		require.NoError(t, r.errs[0], "losing a lease must not stop the runner")
		require.NoError(t, r.errs[1])

		m := r.subs[0].GetMetrics()
		t.Logf("runner-a held %d leases when it froze, lost %d", held, m.LeasesLost)
		require.Positive(t, m.LeasesLost, "runner-a should have found its leases taken")
		require.Positive(t, r.subs[1].GetMetrics().LeaseTakeovers)

		rep := r.checker.Report(r.end)
		t.Log(rep)
		require.Zero(t, rep.Missing)
		require.Zero(t, rep.OrderViolations)
		require.Empty(t, r.store.backwards, "a fenced runner must not write a watermark")
	})
}

// A pause shorter than the TTL changes nothing: no lease is lost, nothing is
// taken over and nothing is delivered twice.
func TestLease_ShortPauseKeepsEveryLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ttl = 30 * time.Second
		r := startLeaseRun(t, screamer.WithLeaseTTL(ttl))

		time.Sleep(40 * time.Second)
		synctest.Wait()
		r.links[0].freeze()
		time.Sleep(ttl / 2)
		r.links[0].thaw()

		r.wait()
		require.NoError(t, r.errs[0])
		require.NoError(t, r.errs[1])
		require.Zero(t, r.subs[0].GetMetrics().LeasesLost)
		require.Zero(t, r.subs[1].GetMetrics().LeaseTakeovers)

		rep := r.checker.Report(r.end)
		require.Zero(t, rep.Missing)
		require.Zero(t, rep.Duplicates)
		require.Zero(t, rep.OrderViolations)
	})
}
