package screamer

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// PartitionStateCounter is an optional extension of PartitionStorage that
// counts unfinished partitions by state. Both built-in storages implement it.
// With it, runners share streaming partitions fairly (see WithFairShare), and
// DesiredRunners accounts for parked partitions, which cost far less than
// streaming ones.
type PartitionStateCounter interface {
	// CountPartitionsByState returns the number of partitions in each state
	// other than FINISHED. States with no partitions may be left out.
	CountPartitionsByState(ctx context.Context) (map[State]int, error)
}

const (
	// fairShareHeadroom lets each runner hold a little over an even share, so
	// that small differences do not move partitions back and forth.
	fairShareHeadroom = 1.1
	// fairShareReleaseShare is the most of its partitions a runner hands back
	// per cluster stats refresh: 5% every 15 seconds, about 20% a minute.
	fairShareReleaseShare = 0.05
	// defaultSliceEstimate is the assumed length of a catch-up read until one
	// has been measured.
	defaultSliceEstimate = time.Second
)

// errRebalance is the cause given to a partition's context when this runner
// hands it to another runner to even out the load.
var errRebalance = errors.New("partition handed over to even out load")

// fairShare is this runner's share of the cluster's streaming partitions.
type fairShare struct {
	target    atomic.Int64 // most partitions this runner should hold; 0 = no limit
	handovers atomic.Int64 // partitions handed to other runners
	sliceNs   atomic.Int64 // moving average of catch-up read time, in nanoseconds

	mu    sync.Mutex
	given map[string]time.Time // partitions handed over, and when
}

// fairShareTarget returns ceil(streaming / runners × headroom), or 0 (no limit)
// when this is the only runner.
func fairShareTarget(streaming, runners int) int {
	if runners <= 1 {
		return 0
	}
	return int(math.Ceil(float64(streaming) / float64(runners) * fairShareHeadroom))
}

// observeSlice records how long a catch-up read took.
func (f *fairShare) observeSlice(d time.Duration) {
	for {
		old := f.sliceNs.Load()
		next := int64(d)
		if old > 0 {
			next = (old*7 + int64(d)) / 8
		}
		next = max(next, 1) // 0 means not measured yet
		if f.sliceNs.CompareAndSwap(old, next) {
			return
		}
	}
}

func (f *fairShare) sliceEstimate() time.Duration {
	if d := f.sliceNs.Load(); d > 0 {
		return time.Duration(d)
	}
	return defaultSliceEstimate
}

// demand is the number of query slots the cluster needs: one for each
// partition that streams or waits to, and the share of a slot each parked
// partition uses for its catch-up reads.
func (s *Subscriber) demand(counts map[State]int) int {
	d := float64(counts[StateCreated] + counts[StateScheduled] + counts[StateRunning])
	if parked := counts[StateParked]; parked > 0 && s.parkInterval > 0 {
		d += float64(parked) * float64(s.fair.sliceEstimate()) / float64(s.parkInterval)
	}
	return int(math.Ceil(d))
}

// handOverCooldown is how long a runner leaves a partition it handed over for
// other runners to claim.
const handOverCooldown = time.Minute

// recentlyHandedOver returns the partitions this runner handed over within the
// cooldown, which it must not claim back, and forgets older ones.
func (f *fairShare) recentlyHandedOver() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.given) == 0 {
		return nil
	}
	out := make([]string, 0, len(f.given))
	for token, at := range f.given {
		if time.Since(at) >= handOverCooldown {
			delete(f.given, token)
			continue
		}
		out = append(out, token)
	}
	return out
}

func (f *fairShare) gave(token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.given == nil {
		f.given = make(map[string]time.Time)
	}
	f.given[token] = time.Now()
}

// rebalance hands back this runner's quietest partitions when it holds more
// than its fair share, a few at a time. The runner does not claim them back for
// handOverCooldown, so they go to runners with room. It still claims anything
// else, so fair share never leaves a partition unread: a runner that stops or
// dies is taken over as quickly as without it.
func (s *Subscriber) rebalance() {
	target := int(s.fair.target.Load())
	held := s.leases.count()
	if target <= 0 || held <= target {
		return
	}
	n := min(held-target, max(1, int(math.Ceil(float64(held)*fairShareReleaseShare))))
	tokens := s.leases.handOver(n)
	if len(tokens) > 0 {
		log.Info().Str("runner_id", s.runnerID).Int("held", held).Int("fair_share", target).Int("handing_over", len(tokens)).
			Msg("holding more than a fair share of partitions; handing some to other runners")
	}
}

// release hands p's lease back with its last watermark, so another runner can
// claim it straight away.
func (s *Subscriber) release(ctx context.Context, p *PartitionMetadata) {
	epoch, ok := s.leases.epoch(p.PartitionToken)
	if !ok {
		return
	}
	s.leases.remove(p.PartitionToken)
	s.checkpoints.forget(p.PartitionToken)
	ctx = context.WithoutCancel(ctx)
	if err := s.leaser.ReleaseLeases(ctx, s.runnerID, []Lease{{PartitionToken: p.PartitionToken, Epoch: epoch, Watermark: p.Watermark}}); err != nil {
		log.Warn().Err(err).Str("partition_token", p.PartitionToken).
			Msg("failed to hand partition over; another runner will take it when its lease expires")
		return
	}
	s.fair.gave(p.PartitionToken)
	s.fair.handovers.Add(1)
}

// handOver picks the n held partitions that have gone longest without data and
// stops reading them with errRebalance. Their goroutines release the leases.
func (l *leaseSet) handOver(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	type cand struct {
		token string
		last  time.Time
	}
	cands := make([]cand, 0, len(l.held))
	for token, h := range l.held {
		if !h.handingOver {
			cands = append(cands, cand{token, h.lastData})
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].last.Before(cands[j].last) })
	out := make([]string, 0, n)
	for _, c := range cands[:min(n, len(cands))] {
		h := l.held[c.token]
		h.handingOver = true
		l.held[c.token] = h
		h.cancel(errRebalance)
		out = append(out, c.token)
	}
	return out
}

// touch records that token just delivered data.
func (l *leaseSet) touch(token string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if h, ok := l.held[token]; ok {
		h.lastData = time.Now()
		l.held[token] = h
	}
}

// copyCounts makes a copy of counts that is safe to keep.
func copyCounts(counts map[State]int) map[State]int {
	out := make(map[State]int, len(counts))
	for k, v := range counts {
		out[k] = v
	}
	return out
}
